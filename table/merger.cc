// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

  namespace {
    class MergingIterator : public Iterator {
     public:
      MergingIterator(const Comparator *comparator, Iterator **children, int n)
          : comparator_(comparator), children_(new IteratorWrapper[n]), n_(n),
            current_(nullptr), direction_(kForward) {
        for(int i = 0; i < n; i++) {
          children_[i].Set(children[i]);
        }
      }

      ~MergingIterator() override { delete[] children_; }

      auto WhichFile() -> FileMetaData * override {
        return current_->WhichFile();
      }

      [[nodiscard]] auto Valid() const -> bool override {
        return (current_ != nullptr);
      }

      void SeekToFirst() override {
        for(int i = 0; i < n_; i++)
          children_[i].SeekToFirst();
        FindSmallest_();
        direction_ = kForward;
      }

      void SeekToLast() override {
        for(int i = 0; i < n_; i++) {
          children_[i].SeekToLast();
        }
        FindLargest_();
        direction_ = kReverse;
      }

      void Seek(const Slice &target) override {
        for(int i = 0; i < n_; i++) {
          children_[i].Seek(target);
        }
        FindSmallest_();
        direction_ = kForward;
      }

      void Next() override {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if(direction_ != kForward) {
          for(int i = 0; i < n_; i++) {
            IteratorWrapper *child = &children_[i];
            if(child != current_) {
              child->Seek(key());
              if(child->Valid() &&
                 comparator_->Compare(key(), child->key()) == 0)
                child->Next();
            }
          }
          direction_ = kForward;
        }
        current_->Next();
        FindSmallest_();
      }

      void Prev() override {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if(direction_ != kReverse) {
          for(int i = 0; i < n_; i++) {
            IteratorWrapper *child = &children_[i];
            if(child != current_) {
              child->Seek(key());
              if(child->Valid()) {
                // Child is at first entry >= key().  Step back one to be < key()
                child->Prev();
              } else {
                // Child has no entries >= key().  Position at last entry.
                child->SeekToLast();
              }
            }
          }
          direction_ = kReverse;
        }

        current_->Prev();
        FindLargest_();
      }

      Slice key() const override {
        assert(Valid());
        return current_->key();
      }

      Slice value() const override {
        assert(Valid());
        return current_->value();
      }

      Status status() const override {
        Status status;
        for(int i = 0; i < n_; i++) {
          status = children_[i].status();
          if(!status.ok()) {
            break;
          }
        }
        return status;
      }

      [[nodiscard]] auto IsInNvm() -> bool override {
        return current_->isInNvm;
      }

      auto GetDataNodeInNvm() -> pmCache::PmSsTable::DataNode * override {
        //        assert(current_->isInNVM || current_->dataNodeInNVM == nullptr);
        return current_->dataNodeInNvm;
      }

      //    enum WhereIsFile { kNVM = 0, kSSD = 1, kUnKnown = 2 };
      //    WhereIsFile whereIsFile = kUnKnown;

      auto GetCleanupNodeForCompactionPrevNode() -> CleanupNode * override {
        return current_->iter()
          ->GetCleanupNodeForCompactionPrevNodeOfIteratorForFilesInOneLevel();
      }

      auto GetKeyString() -> std::string * override {
        return current_->iter()->GetKeyStringForFilesInOneLevel();
      }

     private:
      // Which direction is the iterator moving?
      enum Direction { kForward, kReverse };

      void FindSmallest_();

      void FindLargest_();

      // We might want to use a heap in case there are lots of children.
      // For now we use a simple array since we expect a very small number
      // of children in leveldb.
      const Comparator *comparator_;
      IteratorWrapper *children_;
      int n_; 
      IteratorWrapper *current_;
      Direction direction_;
    };

    void MergingIterator::FindSmallest_() {
      IteratorWrapper *smallest = nullptr;
      for(int i = 0; i < n_; i++) {
        IteratorWrapper *child = &children_[i];
        if(child->Valid()) {
          if(smallest == nullptr ||
             comparator_->Compare(child->key(), smallest->key()) < 0) {
            smallest = child;
          }

          //          std::stringstream ss;
          //          ss << "r12 " << child->key().size() +
          //          smallest->key().size()
          //             << std::endl;
          //          std::cout << ss.str() << std::flush;
        }
      }
      current_ = smallest;
    }

    void MergingIterator::FindLargest_() {
      IteratorWrapper *largest = nullptr;
      for(int i = n_ - 1; i >= 0; i--) {
        IteratorWrapper *child = &children_[i];
        if(child->Valid()) {
          if(largest == nullptr) {
            largest = child;
          } else if(comparator_->Compare(child->key(), largest->key()) > 0) {
            largest = child;
          }
        }
      }
      current_ = largest;
    }
  } // namespace

  auto NewMergingIterator(const Comparator *comparator, Iterator **children,
                          int n) -> Iterator * {
    assert(n >= 0);
    return n == 0   ? NewEmptyIterator()
           : n == 1 ? children[0]
                    : new MergingIterator(comparator, children, n);
  }

  auto NewMergingIterator(const Comparator *comparator, Iterator **children,
                          int n, std::vector<bool> &isInNvm) -> Iterator * {
    assert(n >= 0);
    return n == 0 ? NewEmptyIterator()
           : n == 1 && !isInNvm[0]
             ? children[0]
             : new MergingIterator(comparator, children, n);
  }

} // namespace leveldb
