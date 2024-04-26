// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "db/version_edit.h"
#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

  namespace {

    typedef Iterator *(*BlockFunction)(void *, const ReadOptions &,
                                       const Slice &);

    class TwoLevelIterator : public Iterator {
     public:
      TwoLevelIterator(Iterator *indexIter, BlockFunction blockFunction,
                       void *arg, const ReadOptions &options);

      ~TwoLevelIterator() override;

      auto WhichFile() -> FileMetaData * override {
        return indexIter_.WhichFile();
      }

      void Seek(const Slice &target) override;

      void SeekToFirst() override;

      void SeekToLast() override;

      void Next() override;

      void Prev() override;

      [[nodiscard]] auto Valid() const -> bool override {
        return dataIter_.Valid();
      }

      [[nodiscard]] auto key() const -> Slice override {
        assert(Valid());
        return dataIter_.key();
      }

      [[nodiscard]] auto value() const -> Slice override {
        assert(Valid());
        return dataIter_.value();
      }

      [[nodiscard]] auto status() const -> Status override {
        // It'd be nice if status() returned a const Status& instead of a Status
        if(!indexIter_.status().ok()) {
          return indexIter_.status();
        } else if(dataIter_.iter() != nullptr && !dataIter_.status().ok()) {
          return dataIter_.status();
        } else {
          return status_;
        }
      }

      auto GetDataNodeInNvm() -> pmCache::PmSsTable::DataNode * override {
        return dataIter_.dataNodeInNvm;
      }



      /// GetCleanupNodeForCompactionPrevNodeOfIteratorForFilesInOneLevel()
      /// \return
      auto GetCleanupNodeForCompactionPrevNode() -> CleanupNode * override {
        return GetCleanupNodeForCompactionPrevNodeOfIteratorForFilesInOneLevel();
      }

      auto GetCleanupNodeForCompactionPrevNodeOfIteratorForFilesInOneLevel()
        -> CleanupNode * override {
        return dataIter_.iter()
          ->GetCleanupNodeForCompactionPrevNodeOfIteratorForSingleFile();
      }

      auto GetCleanupNodeForCompactionPrevNodeOfIteratorForSingleFile()
        -> CleanupNode * override {
        return &dataIter_.iter()->cleanupHead;
      }

      auto GetKeyString() -> std::string * override {
        return GetKeyStringForFilesInOneLevel();
      }

      auto GetKeyStringForFilesInOneLevel() -> std::string * override {
        return dataIter_.iter()->GetKeyStringForSingleFile();
      }

      auto GetKeyStringForSingleFile() -> std::string * override {
        return dataIter_.iter()->GetKeyString();
      }

     private:
      void SaveError_(const Status &s) {
        if(status_.ok() && !s.ok())
          status_ = s;
      }

      void SkipEmptyDataBlocksForward_();

      void SkipEmptyDataBlocksBackward_();

      void SetDataIterator_(Iterator *dataIter);

      void InitDataBlock_();

      BlockFunction blockFunction_;
      void *arg_;
      const ReadOptions options_;
      Status status_;
      IteratorWrapper indexIter_;
      IteratorWrapper dataIter_; // May be nullptr
      // If data_iter_ is non-null, then "data_block_handle_" holds the
      // "index_value" passed to block_function_ to create the data_iter_.
      std::string dataBlockHandle_;
    };

    TwoLevelIterator::TwoLevelIterator(Iterator *indexIter,
                                       BlockFunction blockFunction, void *arg,
                                       const ReadOptions &options)
        : blockFunction_(blockFunction), arg_(arg), options_(options),
          indexIter_(indexIter), dataIter_(nullptr) {
#ifndef NDEBUG
      std::cout << "";
#endif
    }

    TwoLevelIterator::~TwoLevelIterator() = default;

    void TwoLevelIterator::Seek(const Slice &target) {
      indexIter_.Seek(target);
      InitDataBlock_();
      if(dataIter_.iter() != nullptr)
        dataIter_.Seek(target);
      SkipEmptyDataBlocksForward_();
    }

    void TwoLevelIterator::SeekToFirst() {
      indexIter_.SeekToFirst();
      InitDataBlock_();
      if(dataIter_.iter() != nullptr)
        dataIter_.SeekToFirst();
      SkipEmptyDataBlocksForward_();
    }

    void TwoLevelIterator::SeekToLast() {
      indexIter_.SeekToLast();
      InitDataBlock_();
      if(dataIter_.iter() != nullptr)
        dataIter_.SeekToLast();
      SkipEmptyDataBlocksBackward_();
    }

    void TwoLevelIterator::Next() {
      assert(Valid());
      dataIter_.Next();
      SkipEmptyDataBlocksForward_();
    }

    void TwoLevelIterator::Prev() {
      assert(Valid());
      dataIter_.Prev();
      SkipEmptyDataBlocksBackward_();
    }

    void TwoLevelIterator::SkipEmptyDataBlocksForward_() {
      while(dataIter_.iter() == nullptr || !dataIter_.Valid()) {
        // Move to next block
        if(!indexIter_.Valid()) {
          SetDataIterator_(nullptr);
          return;
        }
        indexIter_.Next();
        InitDataBlock_();
        if(dataIter_.iter() != nullptr)
          dataIter_.SeekToFirst();
      }
    }

    void TwoLevelIterator::SkipEmptyDataBlocksBackward_() {
      while(dataIter_.iter() == nullptr || !dataIter_.Valid()) {
        // Move to next block
        if(!indexIter_.Valid()) {
          SetDataIterator_(nullptr);
          return;
        }
        indexIter_.Prev();
        InitDataBlock_();
        if(dataIter_.iter() != nullptr)
          dataIter_.SeekToLast();
      }
    }

    void TwoLevelIterator::SetDataIterator_(Iterator *dataIter) {
      if(dataIter_.iter() != nullptr)
        SaveError_(dataIter_.status());
      dataIter_.Set(dataIter);
    }

    void TwoLevelIterator::InitDataBlock_() {
      if(!indexIter_.Valid()) {
        SetDataIterator_(nullptr);
      } else {
        Slice handle = indexIter_.value();
        if(dataIter_.iter() != nullptr &&
           handle.compare(Slice{dataBlockHandle_}) == 0) {
          // data_iter_ is already constructed with this iterator, so
          // no need to change anything
        } else {
          //            auto *files = (std::vector<FileMetaData *> *)arg_;
          Iterator *iter = (*blockFunction_)(arg_, options_, handle);
          dataBlockHandle_.assign(handle.data(), handle.size());
          SetDataIterator_(iter);
        }
      }
    }

  } // namespace

  auto
  NewTwoLevelIterator(Iterator *indexIter, BlockFunction blockFunction,
                      void *arg, const ReadOptions &options) -> Iterator * {
    return new TwoLevelIterator(indexIter, blockFunction, arg, options);
  }

} // namespace leveldb
