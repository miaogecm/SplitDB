// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_DB_MEMTABLE_H_
#define LEVELDB_DB_MEMTABLE_H_

#include <string>
#include <utility>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "util/arena.h"

namespace leveldb {

  class InternalKeyComparator;

  class MemTableIterator;

  class MemTable {
   public:
    // MemTables are reference counted.  The initial reference count
    // is zero and the caller must call Ref() at least once.
    explicit MemTable(const InternalKeyComparator &comparator);

    MemTable(const MemTable &) = delete;

    auto operator=(const MemTable &) -> MemTable & = delete;

    // Increase reference count.
    void Ref() { ++refs_; }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
      --refs_;
      assert(refs_ >= 0);
      if(refs_ <= 0) {
        delete this;
      }
    }

    // Returns an estimate of the number of bytes of data in use by this
    // data structure. It is safe to call when MemTable is being modified.
    auto ApproximateMemoryUsage() -> size_t;

    // Return an iterator that yields the contents of the memtable.
    //
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // db/format.{h,cc} module.
    auto NewIterator() -> Iterator *;

    // Add an entry into memtable that maps key to value at the
    // specified sequence number and with the specified type.
    // Typically, value will be empty if type==kTypeDeletion.
    void Add(SequenceNumber seq, ValueType type, const Slice &key,
             const Slice &value);

    void AddWithPmOff(SequenceNumber seq, ValueType type, const Slice &key,
                      const Slice &value, uint64_t pmOff);

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey &key, std::string *value, Status *s);

   private:
    friend class MemTableIterator;

    friend class MemTableBackwardIterator;

    struct KeyComparator {
      const InternalKeyComparator comparator;

      explicit KeyComparator(InternalKeyComparator c)
          : comparator(std::move(c)) {}

      int operator()(const char *aPtr, const char *bPtr) const;
    };

    typedef SkipList<const char *, KeyComparator> Table;

   public:
    ~MemTable(); // Private since only Unref() should be used to delete it

   private:
    KeyComparator comparator_;
    int refs_;
    Arena arena_{};
    Table table_;
  };

} // namespace leveldb

#endif // LEVELDB_DB_MEMTABLE_H_
