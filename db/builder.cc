// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {


  auto BuildTable(const std::string &dbname, Env *env, const Options &options,
                  TableCache *tableCache, Iterator *iter, FileMetaData *meta)
    -> Status {
    Status s;
    meta->fileSize = 0;
    iter->SeekToFirst();

    std::string fname = TableFileName(dbname, meta->number);
    if(iter->Valid()) {
      WritableFile *file;
      s = env->NewWritableFile(fname, &file);
      if(!s.ok()) {
        return s;
      }

      auto *builder = new TableBuilder(options, file);
      meta->smallest.DecodeFrom(iter->key());
      Slice key;
      for(; iter->Valid(); iter->Next()) {
        key = iter->key();
        builder->Add(key, iter->value());
      }
      if(!key.empty()) {
        meta->largest.DecodeFrom(key);
      }

      
      s = builder->Finish(); 
      if(s.ok()) {
        meta->fileSize = builder->FileSize();
        assert(meta->fileSize > 0);
      }
      delete builder;

      // Finish and check for file errors
      if(s.ok())
        s = file->Sync();
      if(s.ok())
        s = file->Close();
      delete file;
      file = nullptr;
      //        if (s.ok()) {
      // Verify that the table is usable
      //            Iterator *it = table_cache->NewIterator(ReadOptions(),
      //            meta->number, meta->file_size); s = it->status(); delete
      //            it;
      //        }
    }

    // Check for input iterator errors
    if(!iter->status().ok())
      s = iter->status();

    if(s.ok() && meta->fileSize > 0) {
      // Keep it
    } else
      env->RemoveFile(fname);

    return s;
  }

} // namespace leveldb
