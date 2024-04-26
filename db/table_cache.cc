// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

  struct TableAndFile {
    RandomAccessFile *file;
    Table *table;
  };

  static void DeleteEntry(const Slice &key, void *value) {
    TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);
    delete tf->table;
    delete tf->file;
    delete tf;
  }

  static void UnrefEntry(void *arg1, void *arg2) {
    Cache *cache = reinterpret_cast<Cache *>(arg1);
    Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
    cache->Release(h);
  }

  TableCache::TableCache(const std::string &dbname, const Options &options,
                         int entries)
      : env_(options.env), dbname_(dbname), options_(options),
        cache_(NewLRUCache(entries)) {}

  TableCache::~TableCache() { delete cache_; }

  Status TableCache::FindTable(uint64_t fileNumber, uint64_t fileSize,
                               Cache::Handle **handle) {
    Status s;
    char buf[sizeof(fileNumber)];
    EncodeFixed64(buf, fileNumber);
    Slice key(buf, sizeof(buf));
    *handle = cache_->Lookup(key);
    if(*handle == nullptr) {
      std::string fname = TableFileName(dbname_, fileNumber);
      RandomAccessFile *file = nullptr;
      Table *table = nullptr;
      s = env_->NewRandomAccessFile(fname, &file);
      if(!s.ok()) {
        std::string old_fname = SSTTableFileName(dbname_, fileNumber);
        if(env_->NewRandomAccessFile(old_fname, &file).ok()) {
          s = Status::OK();
        }
      }
      if(s.ok()) {
        s = Table::Open(options_, file, fileSize, &table);
      }

      if(!s.ok()) {
        assert(table == nullptr);
        delete file;
        // We do not cache error results so that if the error is transient,
        // or somebody repairs the file, we recover automatically.
      } else {
        TableAndFile *tf = new TableAndFile;
        tf->file = file;
        tf->table = table;
        *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
      }
    }
    return s;
  }

  auto
  TableCache::NewIterator(const ReadOptions &options, uint64_t fileNumber,
                          uint64_t fileSize, Table **tablePtr) -> Iterator * {
    if(tablePtr != nullptr) {
      *tablePtr = nullptr;
    }

    Cache::Handle *handle = nullptr;
    Status s = FindTable(fileNumber, fileSize, &handle);
    if(!s.ok()) {
      return NewErrorIterator(s);
    }

    Table *table =
      reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    Iterator *result = table->NewIterator(options);
    result->RegisterCleanup(&UnrefEntry, cache_, handle);
    if(tablePtr != nullptr) {
      *tablePtr = table;
    }
    return result;
  }

  auto
  TableCache::Get(const ReadOptions &options, uint64_t fileNumber,
                  uint64_t fileSize, const Slice &k, void *arg,
                  void (*handleResult)(void *, const Slice &, const Slice &))
    -> Status {
    Cache::Handle *handle = nullptr;
    Status s = FindTable(fileNumber, fileSize, &handle);
    if(s.ok()) {
      Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
      s = t->InternalGet(options, k, arg, handleResult);
      cache_->Release(handle);
    }
    return s;
  }

  void TableCache::Evict(uint64_t fileNumber) {
    char buf[sizeof(fileNumber)];
    EncodeFixed64(buf, fileNumber);
    cache_->Erase(Slice(buf, sizeof(buf)));
  }

} // namespace leveldb
