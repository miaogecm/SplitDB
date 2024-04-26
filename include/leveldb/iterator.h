// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

// #include "db/version_edit.h"
#include "dbWithPmCache/pmem_sstable/pm_SSTable.h"
#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
namespace leveldb {
  class FileMetaData;
  class LEVELDB_EXPORT Iterator {
   public:
    Iterator();

    Iterator(const Iterator &) = delete;

    auto operator=(const Iterator &) -> Iterator & = delete;

    virtual ~Iterator();

    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    [[nodiscard]] virtual auto Valid() const -> bool = 0;

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last key in the source.  The iterator is
    // Valid() after this call iff the source is not empty.
    virtual void SeekToLast() = 0;

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    virtual void Seek(const Slice &target) = 0;

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    virtual int GetValueLen() { return value().size(); }

    // Return the key for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    [[nodiscard]] virtual auto key() const -> Slice = 0;

    // Return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    [[nodiscard]] virtual auto value() const -> Slice = 0;

    // If an error has occurred, return it.  Else return an ok status.
    [[nodiscard]] virtual auto status() const -> Status = 0;

    virtual auto IsInNvm() -> bool { return false; }

    virtual auto GetDataNodeInNvm() -> pmCache::PmSsTable::DataNode * {
      return nullptr;
    }

    virtual auto WhichFile() -> FileMetaData * { return nullptr; }
    virtual auto WhichNvmNode() -> pmCache::PmSsTable * { return nullptr; }

    // Clients are allowed to register function/arg1/arg2 triples that
    // will be invoked when this iterator is destroyed.
    //
    // Note that unlike all the preceding methods, this method is
    // not abstract and therefore clients should not override it.
    using CleanupFunction = void (*)(void *arg1, void *arg2);

    void RegisterCleanup(CleanupFunction function, void *arg1, void *arg2);

    //   private:
    // Cleanup functions are stored in a single-linked list.
    // The list's head node is inlined in the iterator.
    struct CleanupNode {
      // True if the node is not used. Only head nodes might be unused.
      bool IsEmpty() const { return function == nullptr; }

      // Invokes the cleanup function.
      void Run() {
        assert(function != nullptr);
        (*function)(arg1, arg2);
      }

      // The head node is used if the function pointer is not null.
      CleanupFunction function;
      void *arg1;
      void *arg2;
      CleanupNode *next;
    };

    CleanupNode cleanupHead;

    virtual auto GetCleanupNodeForCompactionPrevNode() -> CleanupNode * {
      return nullptr;
    }

    virtual auto
    GetCleanupNodeForCompactionPrevNodeOfIteratorForFilesInOneLevel()
      -> CleanupNode * {
      return nullptr;
    }

    virtual auto GetCleanupNodeForCompactionPrevNodeOfIteratorForSingleFile()
      -> CleanupNode * {
      return nullptr;
    }

    virtual auto GetKeyString() -> std::string * { return nullptr; }
    virtual auto GetKeyStringForSingleFile() -> std::string * {
      return nullptr;
    }
    virtual auto GetKeyStringForFilesInOneLevel() -> std::string * {
      return nullptr;
    }
  };

  // Return an empty iterator (yields nothing).
  LEVELDB_EXPORT auto NewEmptyIterator() -> Iterator *;

  // Return an empty iterator with the specified status.
  LEVELDB_EXPORT auto NewErrorIterator(const Status &status) -> Iterator *;

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
