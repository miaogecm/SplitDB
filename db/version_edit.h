// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "dbWithPmCache/count_min_sketch/count_min_sketch.h"
#include "dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include "dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h"
namespace leveldb {

  class VersionSet;

  class FileMetaData {
   public:
    enum WhereIsFile { K_SSD = 0, K_NVM = 1, K_BOTH = 2 };

    FileMetaData() : allowedSeeks(1 << 30) {}

    FileMetaData(FileMetaData &&f) noexcept {
      refs = f.refs;
      allowedSeeks = f.allowedSeeks;
      number = f.number;
      fileSize = f.fileSize;
      smallest = f.smallest;
      largest = f.largest;
      whereIsFile = f.whereIsFile;
      nvmSingleNode = f.nvmSingleNode;
      assert(nvmSingleNode == nullptr || number == nvmSingleNode->fileNum);
      countMinSketch = f.countMinSketch;
      bufferForEachFile = f.bufferForEachFile;
    }

    FileMetaData(const FileMetaData &f) {
      refs = f.refs;
      allowedSeeks = f.allowedSeeks;
      number = f.number;
      fileSize = f.fileSize;
      smallest = f.smallest;
      largest = f.largest;
      whereIsFile = f.whereIsFile;
      nvmSingleNode = f.nvmSingleNode;
      assert(number == nvmSingleNode->fileNum);
    }

    int refs{0};
    int allowedSeeks; // Seeks allowed until compaction
    uint64_t number{};
    uint64_t fileSize{0}; // File size in bytes
    InternalKey smallest; // Smallest internal key served by table
    InternalKey largest;  // Largest internal key served by table
    WhereIsFile whereIsFile = K_SSD;
    pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *nvmSingleNode =
      nullptr;
    std::shared_ptr<pmCache::CountMinSketch> countMinSketch{nullptr};
    std::shared_ptr<pmCache::BufferForEachFile> bufferForEachFile{nullptr};

    std::atomic<bool> compacting{false};
    //    pmCache::DramLruNode * lruNode;
  };

  class VersionEdit {
   public:
    VersionEdit() { Clear(); }

    ~VersionEdit() = default;

    void Clear();

    void SetComparatorName(const Slice &name) {
      has_comparator_ = true;
      comparator_ = name.ToString();
    }

    void SetLogNumber(uint64_t num) {
      has_log_number_ = true;
      log_number_ = num;
    }

    void SetPrevLogNumber(uint64_t num) {
      has_prev_log_number_ = true;
      prev_log_number_ = num;
    }

    void SetNextFile(uint64_t num) {
      has_next_file_number_ = true;
      next_file_number_ = num;
    }

    void SetLastSequence(SequenceNumber seq) {
      has_last_sequence_ = true;
      last_sequence_ = seq;
    }

    //    void SetCompactPointer(int level, const InternalKey &key) {
    //      compact_pointers_.emplace_back(level, key);
    //    }

    void SetCompactPointer(int level, InternalKey &&key) {
      compact_pointers_.emplace_back(level, std::move(key));
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    void AddFile(int level, uint64_t file, uint64_t fileSize,
                 const InternalKey &smallest, const InternalKey &largest) {
      FileMetaData f{};
      f.number = file;
      assert(f.nvmSingleNode == nullptr ||
             f.number == f.nvmSingleNode->fileNum);
      f.fileSize = fileSize;
      f.smallest = smallest;
      f.largest = largest;
      new_files_.emplace_back(level, std::move(f));
    }

    void AddFileWithWhereIsFile(
      int level, // NOLINT(bugprone-easily-swappable-parameters)
      uint64_t file, uint64_t fileSize, const InternalKey &smallest,
      const InternalKey &largest, FileMetaData::WhereIsFile whereIsFile,
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *nvmSingleNode =
        nullptr) {
      FileMetaData f;
      f.number = file;
      f.fileSize = fileSize;
      f.smallest = smallest;
      f.largest = largest;
      f.whereIsFile = whereIsFile;
      f.nvmSingleNode = nvmSingleNode;
      assert(nvmSingleNode == nullptr || f.number == nvmSingleNode->fileNum);
      if(whereIsFile == FileMetaData::K_SSD) {
        f.countMinSketch = std::make_shared<pmCache::CountMinSketch>();
        f.bufferForEachFile = std::make_shared<pmCache::BufferForEachFile>();
      }
      new_files_.emplace_back(level, std::move(f));
    }

    // Delete the specified "file" from the specified "level".
    void RemoveFile(int level, uint64_t file) {
      //        if(file == 30){
      //            std::cout<<"test";
      //        }
      deleted_files_.insert(std::make_pair(level, file));
    }

    void EncodeTo(std::string *dst) const;

    auto DecodeFrom(const Slice &src) -> Status;

    [[nodiscard]] auto DebugString() const -> std::string;

    //  private:
    friend class VersionSet;

    using DeletedFileSet = std::set<std::pair<int, uint64_t>>;

    std::string comparator_;
    uint64_t log_number_{};
    uint64_t prev_log_number_{};
    uint64_t next_file_number_{};
    SequenceNumber last_sequence_{};
    bool has_comparator_{};
    bool has_log_number_{};
    bool has_prev_log_number_{};
    bool has_next_file_number_{};
    bool has_last_sequence_{};

    std::vector<std::pair<int, InternalKey>> compact_pointers_;
    DeletedFileSet deleted_files_;
    std::vector<std::pair<int, FileMetaData>> new_files_;
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
