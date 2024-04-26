// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "db/version_edit.h"
#include "dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

  // An internal wrapper class with an interface similar to Iterator that
  // caches the valid() and key() results for an underlying iterator.
  // This can help avoid virtual function calls and also gives better
  // cache locality.
  class IteratorWrapper {
   public:
    IteratorWrapper() : iter_(nullptr) {}

    explicit IteratorWrapper(Iterator *iter) : iter_(nullptr) { Set(iter); }

    ~IteratorWrapper() { delete iter_; }

    [[nodiscard]] inline auto iter() const -> Iterator * { return iter_; }

    // Takes ownership of "iter" and will delete it when destroyed, or
    // when Set() is invoked again.
    inline void Set(Iterator *iter) {
      delete iter_;
      iter_ = iter;
      if(iter_ == nullptr)
        valid_ = false;
      else
        Update_();
    }

    // Iterator interface methods
    [[nodiscard]] inline auto Valid() const -> bool { return valid_; }

    [[nodiscard]] inline auto key() const -> Slice {
      assert(Valid());
      return key_;
    }

    [[nodiscard]] inline auto value() const -> Slice {
      assert(Valid());
      return iter_->value();
    }

    // Methods below require iter() != nullptr
    [[nodiscard]] inline auto status() const -> Status {
      assert(iter_);
      return iter_->status();
    }

    void inline Next() {
      assert(iter_);
      iter_->Next();
      Update_();
    }

    void inline Prev() {
      assert(iter_);
      iter_->Prev();
      Update_();
    }

    void inline Seek(const Slice &k) {
      assert(iter_);
      iter_->Seek(k);
      Update_();
    }

    void inline SeekToFirst() {
      assert(iter_);
      iter_->SeekToFirst();
      Update_();
    }

    void inline SeekToLast() {
      assert(iter_);
      iter_->SeekToLast();
      Update_();
    }

    inline auto WhichFile() { return fromWhichFile_; }

    bool isInNvm = false;
    pmCache::PmSsTable::DataNode *dataNodeInNvm{};

   private:
    void inline Update_() {
      valid_ = iter_->Valid();
      if(valid_) {
        key_ = iter_->key();
        dataNodeInNvm = iter_->GetDataNodeInNvm();
        if(dataNodeInNvm)
          isInNvm = true;
        fromWhichFile_ = iter_->WhichFile();
      }
    }

    Iterator *iter_;
    bool valid_{};
    Slice key_;
    FileMetaData *fromWhichFile_{};
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
