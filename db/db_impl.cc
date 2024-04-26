// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include "dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h"
#include "dbWithPmCache/pm_file_in_one_level_opt/pm_file_in_one_level_opt.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/mutexlock.h"
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <experimental/filesystem>
#include <libpmemobj/tx.h>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <sys/syscall.h>
#include <vector>
#include <x86intrin.h>

namespace leveldb {
  const int hotKeyPromotionThreshold[] = {0, 0, 0, 0, 0, 1, 1};
  const int hotKeyReserveThreshold[] = {0, 0, 0, 0, 0, 1, 1};
  const int kNumNonTableCacheFiles = 10;

  struct DBImpl::CompactionState {
    // Files produced by compaction
    struct Output {
      uint64_t number{};
      uint64_t fileSize{};
      InternalKey smallest, largest;
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *pmFile = nullptr;
    };

    auto current_output() -> Output * { return &outputs[outputs.size() - 1]; }
    auto current_reserveHotKeyOutputs() -> Output * {
      return &reserveHotKeyOutputs[reserveHotKeyOutputs.size() - 1];
    }

    explicit CompactionState(Compaction *c) : compaction(c) {}

    Compaction *const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore, if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    //

    SequenceNumber smallestSnapshot{0};

    std::vector<Output> outputs, reserveHotKeyOutputs;

    // State kept for output being generated
    WritableFile *outfile{nullptr}, *reserveHotKeyOutfile{nullptr};
    TableBuilder *builder{nullptr}, *reserveHotKeyBuilder{nullptr};

    std::unique_ptr<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>
      pmSkiplistNvmSingleNodeBuilder = nullptr;

    //    uint64_t totalBytes{0};
  };

  // Fix user-supplied options to be reasonable
  template <class T, class V>
  static void
  ClipToRange(T *ptr,
              V minvalue, // NOLINT(bugprone-easily-swappable-parameters)
              V maxvalue) {
    if(static_cast<V>(*ptr) > maxvalue)
      *ptr = maxvalue;
    if(static_cast<V>(*ptr) < minvalue)
      *ptr = minvalue;
  }

  auto
  SanitizeOptions(const std::string &dbname, const InternalKeyComparator *icmp,
                  const InternalFilterPolicy *internalFilterPolicy,
                  const Options &src) -> Options {
    Options result = src;
    result.comparator = icmp;
    result.filter_policy =
      (src.filter_policy != nullptr) ? internalFilterPolicy : nullptr;
    ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);
    ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);
    ClipToRange(&result.block_size, 1 << 10, 4 << 20);
    if(result.info_log == nullptr) {
      // Open a log file in the same directory as the db
      src.env->CreateDir(dbname); // In case it does not exist
      src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
      Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
      if(!s.ok()) {
        // No place suitable for logging
        result.info_log = nullptr;
      }
    }
    if(result.block_cache == nullptr)
      result.block_cache = NewLRUCache(8 << 20);

    return result;
  }

  static auto TableCacheSize(const Options &sanitizedOptions) -> int {
    // Reserve ten files or so for other uses and give the rest to TableCache.
    return sanitizedOptions.max_open_files - kNumNonTableCacheFiles;
  }

  DBImpl::DBImpl(const Options &rawOptions, const std::string &dbname)
      : env_(rawOptions.env), internalComparator_(rawOptions.comparator),
        internalFilterPolicy_(rawOptions.filter_policy),
        options_(SanitizeOptions(dbname, &internalComparator_,
                                 &internalFilterPolicy_, rawOptions)),
        ownsInfoLog_(options_.info_log != rawOptions.info_log),
        ownsCache_(options_.block_cache != rawOptions.block_cache),
        dbname_(dbname), tableCache_(new TableCache(dbname_, options_,
                                                    TableCacheSize(options_))),
        dbLock_(nullptr), shuttingDown_(false),
        backgroundWorkFinishedSignal_(&mutex_), cMem_(nullptr), cImm_(nullptr),
        hasImm_(false), seed_(0), tmpBatch_(new WriteBatch),
        backGroundWorkCnt_(0), manualCompaction_(nullptr),
        versions_(new VersionSet(dbname_, &options_, tableCache_,
                                 &internalComparator_)) {
    std::string const pmCachePath = PMEM_CACHE_PATH;
    pmCache::PmCachePoolRoot *root = nullptr;
    const uint64_t pmemCacheSize = (uint64_t)PMEM_CACHE_SIZE;
    try {
      if(std::experimental::filesystem::exists(pmCachePath)) {
        pmemPool_ = pmCache::pool<pmCache::PmCachePoolRoot>::open(pmCachePath,
                                                                  "PMEM_CACHE");
        root = pmemPool_.root().get();
        //        pmem::obj::flat_transaction::manual const tx{pmemPool_};
        root->InitWhenOpen();
        //        pmemobj_tx_commit();
        timeStamp = root->GetFileInOneLevelOptDramEntrance(0)->timestamp;
      } else {
        pmemPool_ = pmCache::pool<pmCache::PmCachePoolRoot>::create(
          pmCachePath, "PMEM_CACHE", pmemCacheSize);
        //        pmem::obj::flat_transaction::manual const tx{pmemPool_};
        new(root = pmemPool_.root().get()) pmCache::PmCachePoolRoot();
        timeStamp = root->GetFileInOneLevelOptDramEntrance(0)->timestamp;
        //        pmemobj_tx_commit();
      }
    }
    catch(pmem::pool_error &e) {
      std::cerr << "Error: " << e.what() << std::endl;
      exit(1);
    }
    _pobj_cached_pool.pop = pmemPool_.handle();
    pmCachePoolRoot_ = root;

    ////////////////////////////////////////new1
    for(int i = 0; i < config::kNumLevels; ++i)
      this->pmSkiplistDramEntrancePtrArray[i] =
        versions_->pmSkiplistDramEntrance[i] =
          root->GetFileInOneLevelOptDramEntrance(i);

    pmCacheBgGcWorkThreadPool_ = root->threadPool;

    auto sortNum = 4;
    pmCacheBgSubCompactionThreadPool_ =
      new pmCache::ThreadPool(sortNum, _pobj_cached_pool.pop);

    auto parallelCompaction = 4;
    pmCacheBgCompactionThreadPool_ =
      new pmCache::ThreadPool(parallelCompaction, _pobj_cached_pool.pop);

    //    std::cout<<"sortNum = "<<sortNum<<std::endl;

    ////////////////////////////////////////new2

    //    versions_->pmCachePoolRoot = root;
    assert(pmCachePoolRoot_->GetFileInOneLevelOptDramEntrance(0));
  }

  DBImpl::~DBImpl() {
    // Wait for background work to finish.
    mutex_.Lock();

    //    std::cout << "delay nano seconds = " << delayNanoTimes_ << std::endl;
    //    std::cout << "slow times = " << slowCnt_.load() << std::endl;

    std::stringstream ss;
    for(int i = 0; i < config::kNumLevels; ++i)
      ss << "level " << i << " hit " << getHitCnt_[i].load() << std::endl;
    ss << "level memtable hit " << getHitMemCnt_.load() << std::endl;
    std::cout << ss.str() << std::flush;

    shuttingDown_.store(true, std::memory_order_release);

    //    while(backgroundCompactionScheduled_) {
    while(backGroundWorkCnt_.load(std::memory_order_acquire) > 0) {
      backgroundWorkFinishedSignal_.Wait();
    }

    mutex_.Unlock();

    if(dbLock_ != nullptr)
      env_->UnlockFile(dbLock_);

    delete tmpBatch_;
    delete tableCache_;
    if(ownsInfoLog_)
      delete options_.info_log;
    if(ownsCache_)
      delete options_.block_cache;

    assert(pmCachePoolRoot_->threadPool == pmCacheBgGcWorkThreadPool_);
    delete pmCacheBgSubCompactionThreadPool_;
    delete pmCacheBgGcWorkThreadPool_;
    delete pmCacheBgCompactionThreadPool_;

    //    if(backGroundCompactionThread_)
    //      backGroundCompactionThread_->detach();

    if(cMem_ != nullptr)
      cMem_->Unref();
    if(cImm_ != nullptr)
      cImm_->Unref();

    delete versions_;

    _mm_mfence();

    pmemPool_.close();
  }

  auto DBImpl::NewDb_() -> Status {
    VersionEdit newDb;
    newDb.SetComparatorName(Slice{UserComparator_()->Name()});
    newDb.SetLogNumber(0);
    newDb.SetNextFile(2);
    newDb.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_, 1);
    WritableFile *file;
    Status s = env_->NewWritableFile(manifest, &file);
    if(!s.ok()) {
      return s;
    }
    {
      log::Writer log(file);
      std::string record;
      newDb.EncodeTo(&record);
      s = log.AddRecord(Slice{record});
      if(s.ok()) {
        s = file->Sync();
      }
      if(s.ok()) {
        s = file->Close();
      }
    }
    delete file;
    if(s.ok()) {
      // Make "CURRENT" file that points to the new manifest file.
      s = SetCurrentFile(env_, dbname_, 1);
    } else {
      env_->RemoveFile(manifest);
    }
    return s;
  }

  [[maybe_unused]] void DBImpl::MaybeIgnoreError_(Status *s) const {
    if(s->ok() || options_.paranoid_checks) {
      // No change needed
    } else {
      //        Log(options_.info_log, "Ignoring error %s",
      //        s->ToString().c_str());
      *s = Status::OK();
    }
  }

  void DBImpl::RemoveObsoleteFiles_() { 
    mutex_.AssertHeld();

    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    if(!bgError_.ok())
      return;

    // Make a set of all of the live files

    std::set<uint64_t> live = pendingOutputs_;
    versions_->AddLiveFiles(&live);


    uint64_t number;
    FileType type;
    std::vector<std::string> filesToDelete;

    std::vector<std::string> filenames;
    env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
    for(std::string &filename : filenames) {
      if(ParseFileName(filename, &number, &type)) {
        bool keep = true;
        switch(type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into
          // "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile: keep = true; break;
        }

        if(!keep) { 
          filesToDelete.push_back(std::move(filename));
          if(type == kTableFile)
            tableCache_->Evict(number);
        }
      }
    }

    // While deleting all files unblock other threads. All files being deleted
    // have unique names which will not collide with newly created files and
    // are therefore safe to delete while allowing other threads to proceed.
    mutex_.Unlock();
    for(const std::string &filename : filesToDelete)
      env_->RemoveFile(dbname_ + "/" + filename);

    //    pmCache::GlobalTimestamp::TimestampToken t{};
    //    timeStamp->ReferenceTimestamp(t);
    //    auto currentTime = t.thisTime;

    //    timeStamp->DereferenceTimestamp(t);
    mutex_.Lock();
  }

  static auto LookFor(const std::vector<FileMetaData *> &files,
                      const unsigned long long num) -> int {
    int i = 0;
    int const s = (int)files.size();
    while(i < s) {
      if(files[i]->number == num)
        return i;
      i++;
    }
    return -1;
  }


  auto DBImpl::Recover_(VersionEdit *edit, bool *saveManifest) -> Status {
    mutex_.AssertHeld();

    // Ignore error from CreateDir since the creation of the DB is
    // committed only when the descriptor is created, and this directory
    // may already exist from a previous failed creation attempt.
    env_->CreateDir(dbname_);
    assert(dbLock_ == nullptr);
    Status s = env_->LockFile(LockFileName(dbname_), &dbLock_);
    if(!s.ok())
      return s;

    if(!env_->FileExists(CurrentFileName(dbname_))) {
      if(options_.create_if_missing) {
        s = NewDb_(); 
        if(!s.ok())
          return s;
      } else
        return Status::InvalidArgument(
          Slice{dbname_}, Slice{"does not exist (create_if_missing is false)"});
    } else if(options_.error_if_exists)
      return Status::InvalidArgument(Slice{dbname_},
                                     Slice{"exists (error_if_exists is true)"});


    s = versions_->Recover(saveManifest, pmCachePoolRoot_);
    if(!s.ok())
      return s;
    SequenceNumber maxSequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t minLog = versions_->LogNumber();
    const uint64_t prevLog = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);
    if(!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for(auto &filename : filenames) {
      if(ParseFileName(filename, &number, &type)) {
        expected.erase(number);
        if(type == kLogFile && ((number >= minLog) || (number == prevLog)))
          logs.push_back(number);
      }
    }
    auto currentVersion = versions_->current();
    auto &files = currentVersion->files;
    std::vector<uint64_t> okToDisappear;
    for(auto &fileInOneLevel : files) {
      okToDisappear.clear();
      for(auto &disappearNode : expected) {
        int index = LookFor(fileInOneLevel, disappearNode);
        if(index >= 0 &&
           fileInOneLevel[index]->whereIsFile == FileMetaData::K_NVM)
          okToDisappear.push_back(disappearNode);
      }
      for(auto fileInNvm : okToDisappear)
        expected.erase(fileInNvm);
    }

    if(!expected.empty()) {
      std::array<char, 50> buf{};
      std::snprintf(buf.data(), sizeof(buf), "%d missing files; e.g.",
                    static_cast<int>(expected.size()));
      return Status::Corruption(
        Slice{buf.data()}, Slice{TableFileName(dbname_, *(expected.begin()))});
    }

    RecoverNvmLog_(saveManifest, edit, &maxSequence);

    if(versions_->LastSequence() < maxSequence)
      versions_->SetLastSequence(maxSequence);

    return Status::OK();
  }

  namespace recoverLog {
    auto RecoverPmWal(pmCache::PmWal *pmWal,
                      const InternalKeyComparator &internalComparator,
                      DBImpl *db, leveldb::VersionEdit *edit) -> bool {
      if(pmWal && !pmWal->IsEmpty()) {
        MemTable memTable{internalComparator};
        auto dummyHead = pmWal->GetDummyHead();
        auto dummyTail = pmWal->GetDummyTail();
        auto currentNode = dummyHead->NextNode();

        while(currentNode != dummyTail) {
          auto sqAndKind = currentNode->GetSqAndKind();
          memTable.AddWithPmOff(
            sqAndKind >> 8,
            *(char *)(&sqAndKind) == kTypeValue ? kTypeValue : kTypeDeletion,
            {currentNode->GetUserKey(), currentNode->GetUserKeyLen()},
            {currentNode->GetValue(), currentNode->GetValueLen()},
            pmCache::GetOffset(currentNode));
          currentNode = currentNode->NextNode();
        }
        db->WriteLevel0TableWithCertainMemtable(pmWal, edit, &memTable);
        return true;
      } else
        return false;
    }
  } // namespace recoverLog

  auto DBImpl::RecoverNvmLog_(bool *saveManifest, leveldb::VersionEdit *edit,
                              leveldb::SequenceNumber *maxSequence) -> Status {
    mutex_.AssertHeld();
    auto imm = pmCachePoolRoot_->GetWalForImm();
    *saveManifest |=
      recoverLog::RecoverPmWal(imm, internalComparator_, this, edit);
    //    if(imm && !imm->IsEmpty()) {
    //      auto memtableUnique =
    //      std::make_unique<MemTable>(internalComparator_); auto dummyHead =
    //      imm->GetDummyHead(); auto dummyTail = imm->GetDummyTail(); auto
    //      currentNode = dummyHead->NextNode();
    //      //        WriteBatch batch;
    //      while(currentNode != dummyTail) {
    //        auto sqAndKind = currentNode->GetSqAndKind();
    //        if(*(char *)(&sqAndKind) == kTypeValue) {
    //          memtableUnique->AddWithPmOff(
    //            sqAndKind >> 8, kTypeValue,
    //            {currentNode->GetUserKey(), currentNode->GetUserKeyLen()},
    //            {currentNode->GetValue(), currentNode->GetValueLen()},
    //            pmCache::getOffset(currentNode));
    //        } else {
    //          memtableUnique->AddWithPmOff(
    //            sqAndKind >> 8, kTypeDeletion,
    //            {currentNode->GetUserKey(), currentNode->GetUserKeyLen()},
    //            {currentNode->GetValue(), currentNode->GetValueLen()},
    //            pmCache::getOffset(currentNode));
    //        }
    //        currentNode = currentNode->NextNode();
    //      }
    //      WriteLevel0TableWithCertainMemtable(imm, edit, nullptr,
    //      &newVersionTime,
    //                                          memtableUnique.get());
    //      //        pmCache::printTimestampReference("d4",
    //      newVersionTime.thisTime);
    //      timeStamp->DereferenceTimestamp(newVersionTime);
    //    }

    auto mm = pmCachePoolRoot_->GetWalForMem();
    *saveManifest |=
      recoverLog::RecoverPmWal(mm, internalComparator_, this, edit);

    //    if(mm && !mm->IsEmpty()) {
    //      auto memtableUnique =
    //      std::make_unique<MemTable>(internalComparator_); auto dummyHead =
    //      mm->GetDummyHead(); auto dummyTail = mm->GetDummyTail(); auto
    //      currentNode = dummyHead->NextNode();
    //      //        WriteBatch batch;
    //      while(currentNode != dummyTail) {
    //        auto sqAndKind = currentNode->GetSqAndKind();
    //        if(*(char *)(&sqAndKind) == kTypeValue) {
    //          memtableUnique->AddWithPmOff(
    //            sqAndKind >> 8, kTypeValue,
    //            {currentNode->GetUserKey(), currentNode->GetUserKeyLen()},
    //            {currentNode->GetValue(), currentNode->GetValueLen()},
    //            pmCache::getOffset(currentNode));
    //        } else {
    //          memtableUnique->AddWithPmOff(
    //            sqAndKind >> 8, kTypeDeletion,
    //            {currentNode->GetUserKey(), currentNode->GetUserKeyLen()},
    //            {currentNode->GetValue(), currentNode->GetValueLen()},
    //            pmCache::getOffset(currentNode));
    //        }
    //        currentNode = currentNode->NextNode();
    //      }
    //      WriteLevel0TableWithCertainMemtable(mm, edit, nullptr,
    //      &newVersionTime,
    //                                          memtableUnique.get());
    //      //        pmCache::printTimestampReference("d5",
    //      newVersionTime.thisTime);
    //      timeStamp->DereferenceTimestamp(newVersionTime);
    //    }
    return Status::OK();
  }


  [[maybe_unused]] auto
  DBImpl::WriteLevel0Table_(MemTable *mem, VersionEdit *edit, Version *base)
    -> Status {
    assert(false);
    mutex_.AssertHeld();
    const uint64_t startMicros = env_->NowMicros(); 
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();

    pendingOutputs_.insert(meta.number);
    Iterator *iter = mem->NewIterator();
    Status s;
    {
      mutex_.Unlock();
      s = BuildTable(dbname_, env_, options_, tableCache_, iter, &meta);
      mutex_.Lock();
    }
    delete iter;
    pendingOutputs_.erase(meta.number);

    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    int level = 0;
    if(s.ok() && meta.fileSize > 0) {
      const Slice minUserKey = meta.smallest.UserKey();
      const Slice maxUserKey = meta.largest.UserKey();
      if(base !=
         nullptr) 
        level = base->PickLevelForMemTableOutput(minUserKey, maxUserKey);
      edit->AddFile(level, meta.number, meta.fileSize, meta.smallest,
                    meta.largest);
    }
    CompactionStats stats;
    stats.micros = (int64_t)env_->NowMicros() - (int64_t)startMicros;
    stats.bytes_written = (int64_t)meta.fileSize;
    stats_[level].Add(stats);
    return s;
  }

  auto DBImpl::WriteLevel0Table_(
    pmCache::PmWal *immWal, VersionEdit *edit, Version *base,
    pmCache::GlobalTimestamp::TimestampToken *newVersionTime) -> Status {
    mutex_.AssertHeld();

    //    const uint64_t startMicros = env_->NowMicros();
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();
    pendingOutputs_.insert(meta.number); 

    Status s;

    uint64_t fileSize = 0;
    pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *newNvmSingleNode;
    pmCache::PmSsTable *nvTableNode;
    std::unique_ptr<Iterator> iter;
    { 
      mutex_.Unlock();
      iter.reset(cImm_->NewIterator());
      iter->SeekToFirst();
      {
        //        std::unique_ptr<pmem::obj::flat_transaction::manual> tx =
        //        nullptr; if(pmemobj_tx_stage() == TX_STAGE_NONE)
        //          tx =
        //          std::make_unique<pmem::obj::flat_transaction::manual>(pmemPool_);

        //        assert(pmemobj_tx_stage() == TX_STAGE_WORK);
        auto newNvTableBuilder =
          pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder{};
        iter->SeekToFirst();
        while(iter->Valid()) {
          auto v = iter->value();
          newNvTableBuilder.AddNodeToIndexOfSsTableAndUpdateSizeAllocated(
            DecodeFixed64(v.data() + v.size()), iter->key().size(), v.size());
          iter->Next();
        }
        newNvmSingleNode =
          newNvTableBuilder.Generate(0, UINT64_MAX, meta.number);

        newNvTableBuilder.ssTableBuilder
          .GetControlFromPmWalAndAddRangeWithoutAddIndex(immWal);
        nvTableNode = newNvTableBuilder.ssTableBuilder.GetPmSsTable();
        assert(nvTableNode == newNvmSingleNode->GetPmSsTable());
        //        if(tx != nullptr) {
        //          pmemobj_tx_commit();
        //          tx.reset(nullptr);
        //        }

        fileSize += newNvTableBuilder.sizeAllocated;
      }
      mutex_.Lock();
      pmSkiplistDramEntrancePtrArray[0]->ReplaceNodes({newNvmSingleNode}, {});
    }

    timeStamp->ReferenceTimestamp(*newVersionTime);

    meta.fileSize = fileSize;

    iter->SeekToFirst();
    meta.smallest.DecodeFrom(iter->key());
    iter->SeekToLast();
    meta.largest.DecodeFrom(iter->key());

    if(s.ok() && meta.fileSize > 0)
      edit->AddFileWithWhereIsFile(
        0, meta.number, meta.fileSize, meta.smallest, meta.largest,
        FileMetaData::WhereIsFile::K_NVM, newNvmSingleNode);

    pendingOutputs_.erase(meta.number);
    return s;
  }

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wthread-safety-analysis"
  auto DBImpl::WriteLevel0TableWithCertainMemtable(pmCache::PmWal *immWal,
                                                   VersionEdit *edit,
                                                   MemTable *mem) -> Status {
    mutex_.AssertHeld();

    FileMetaData meta;
    meta.number = versions_->NewFileNumber();

    pendingOutputs_.insert(meta.number);

    Status s;

    uint64_t fileSize = 0;
    pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *newNvmSingleNode;
    pmCache::PmSsTable *nvTableNode;
    std::unique_ptr<Iterator> iter;
    {
      mutex_.Unlock();
      iter.reset(mem->NewIterator());
      iter->SeekToFirst();
      {
        //        pmem::obj::flat_transaction::manual tx{pmemPool_};
        auto newNvTableBuilder =
          pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder{};

        iter->SeekToFirst();
        while(iter->Valid()) {
          auto v = iter->value();
          newNvTableBuilder.AddNodeToIndexOfSsTableAndUpdateSizeAllocated(
            DecodeFixed64(v.data() + v.size()), iter->key().size(), v.size());
          iter->Next();
        }

        newNvmSingleNode =
          newNvTableBuilder.Generate(0, UINT64_MAX, meta.number);
        newNvTableBuilder.ssTableBuilder
          .GetControlFromPmWalAndAddRangeWithoutAddIndex(immWal);
        //        pmemobj_tx_commit();
        fileSize += newNvTableBuilder.sizeAllocated;
      }

      mutex_.Lock();
      pmSkiplistDramEntrancePtrArray[0]->ReplaceNodes({newNvmSingleNode}, {});
    }

    meta.fileSize = fileSize;

    iter->SeekToFirst();
    meta.smallest.DecodeFrom(iter->key());
    iter->SeekToLast();
    meta.largest.DecodeFrom(iter->key());

    if(s.ok() && meta.fileSize > 0)
      edit->AddFileWithWhereIsFile(
        0, meta.number, meta.fileSize, meta.smallest, meta.largest,
        FileMetaData::WhereIsFile::K_NVM, newNvmSingleNode);

    return s;
  }
#pragma clang diagnostic pop


  void DBImpl::CompactMemTable_() {

    //    std::stringstream ss;
    //    ss << "-1" << std::endl;
    //    std::cout << ss.str() << std::flush;

    //    auto timeStartFlush = std::chrono::high_resolution_clock::now();

    mutex_.AssertHeld();
    assert(cImm_ != nullptr);
    VersionEdit edit;
    Version *base = versions_->current();

    base->Ref();
    pmCache::GlobalTimestamp::TimestampToken newVersionTime{};

    Status s = WriteLevel0Table_(pmCachePoolRoot_->GetWalForImm(), &edit, base,
                                 &newVersionTime);
    base->Unref();

    if(s.ok()) {
#ifndef NDEBUG
//      std::cout << "2\n" << std::flush;
#endif
      s = versions_->LogAndApply(&edit, &mutex_, &newVersionTime, timeStamp,
                                 false);
    }

    if(s.ok()) {
      assert(pmCachePoolRoot_->GetWalForImm()->IsEmpty());
      pmCachePoolRoot_->ClearImmWal(pmemPool_);
      cImm_->Unref();
      cImm_ = nullptr;
      hasImm_.store(false, std::memory_order_release);
    } else {
      RecordBackgroundError_(s);
    }

    //    LruNvmCache();

    //    auto totalSize0 = 0L;
    //    for(auto *file : versions_->current()->files[0])
    //      totalSize0 += (long long)file->fileSize;
    //    totalSize0 += options_.write_buffer_size * 2;
    //    std::cout << "L0+1 " +
    //                   std::to_string((long long)lru_.maxSize -
    //                                  (long long)lru_.totalSize.load() -
    //                                  totalSize0) +
    //                   "\n"
    //              << std::flush;
    //    if((long long)lru_.maxSize - (long long)lru_.totalSize.load() -
    //    totalSize0 <
    //       0)
    //      std::cout << "";

    //    auto timeEndFlush = std::chrono::high_resolution_clock::now();
    //    std::stringstream ss;
    //    ss << "FT "
    //       <<
    //       std::chrono::duration_cast<std::chrono::nanoseconds>(timeEndFlush -
    //                                                                timeStartFlush)
    //            .count()
    //       << std::endl;
    //    std::cout << ss.str() << std::flush;
  }

  void DBImpl::CompactRange(const Slice *begin, const Slice *end) {
    int maxLevelWithFiles = 1;
    {
      MutexLock const l(&mutex_);
      Version *base = versions_->current();
      for(int level = 1; level < config::kNumLevels; level++) {
        if(base->OverlapInLevel(level, begin, end)) {
          maxLevelWithFiles = level;
        }
      }
    }
    //    TEST_CompactMemTable();
    for(int level = 0; level < maxLevelWithFiles; level++) {
      TEST_CompactRange(level, begin, end);
    }
  }

  void
  DBImpl::TEST_CompactRange(int level, const Slice *begin, const Slice *end) {
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);

    InternalKey beginStorage, endStorage;

    ManualCompaction manual;
    manual.level = level;
    manual.done = false;
    if(begin == nullptr) {
      manual.begin = nullptr;
    } else {
      beginStorage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
      manual.begin = &beginStorage;
    }
    if(end == nullptr) {
      manual.end = nullptr;
    } else {
      endStorage = InternalKey(*end, 0, static_cast<ValueType>(0));
      manual.end = &endStorage;
    }

    MutexLock const l(&mutex_);
    while(!manual.done && !shuttingDown_.load(std::memory_order_acquire) &&
          bgError_.ok()) {
      if(manualCompaction_ == nullptr) { // Idle
        manualCompaction_ = &manual;
        MaybeScheduleCompactionOrPromotion_();
      } else { // Running either my compaction or another compaction.
        backgroundWorkFinishedSignal_.Wait();
      }
    }
    if(manualCompaction_ == &manual) {
      // Cancel my manual compaction since we aborted early for some reason.
      manualCompaction_ = nullptr;
    }
  }

  //  Status DBImpl::TEST_CompactMemTable() {
  //     nullptr batch means just wait for earlier writes to be done
  //    Status s = Write(WriteOptions(), nullptr);
  //    if(s.ok()) {
  // //  Wait until the compaction completes
  //      MutexLock l(&mutex_);
  //      while(imm_ != nullptr && bg_error_.ok()) {
  //        backgroundWorkFinishedSignal_.Wait();
  //      }
  //      if(imm_ != nullptr) {
  //        s = bg_error_;
  //      }
  //    }
  //    return s;
  //  }
  //
  void DBImpl::RecordBackgroundError_(const Status &s) {
    mutex_.AssertHeld();
    if(bgError_.ok()) {
      bgError_ = s;
      backgroundWorkFinishedSignal_.SignalAll();
    }
  }

  void DBImpl::MaybeScheduleCompactionOrPromotion_() {

    if(backGroundWorkCnt_.load() >=
       pmCacheBgCompactionThreadPool_->GetWorkerCnt())
      return;

    mutex_.AssertHeld();

    bool enoughRoomForNvm = lru_.totalSize.load(std::memory_order_relaxed) +
                              versions_->current()->GetLevel0FileSize() +
                              options_.write_buffer_size * 2 <=
                            lru_.maxSize;

    if(backGroundWorkCnt_.load(std::memory_order_acquire) >
       (config::kNumLevels + 1) / 2) { // NOLINT(bugprone-branch-clone)

    } else if(shuttingDown_.load(std::memory_order_acquire)) {
      // DB is being deleted; no more background compactions
    } else if(!bgError_.ok()) {
      // Already got an error; no more changes
    } else if((cImm_ == nullptr || doingFlush_.load() ||
               (!enoughRoomForNvm && doingLru_.load())) &&
              manualCompaction_ == nullptr &&
              !versions_->NeedsCompactionOrPromotion(enoughRoomForNvm) &&
              (doingLru_.load() || enoughRoomForNvm ||
               versions_->LevelBelowIsInfluencedByCompaction(NVM_MAX_LEVEL))) {
      // No work to be done
    } else {
      backGroundWorkCnt_.fetch_add(1, std::memory_order_release);
      pmCacheBgCompactionThreadPool_->PostWork(
        {{[this] { leveldb::DBImpl::BgWork_(this); }}, false});
      return;
    }
  }

  void DBImpl::BgWork_(void *db) {
    static_cast<DBImpl *>(db)->BackgroundCall_();
  }

  static const int kGcCntMax = 6;
  class GcCnt {
   public:
    GcCnt() = default;
    auto ShouldGc() -> bool {
      if(cnt_++ > kGcCntMax) {
        cnt_ = 0;
        return true;
      }
      return false;
    }
    [[maybe_unused]] void FocusGc() {
      return;
      cnt_ = 255;
    }

   private:
    int cnt_ = 0;
  };

  auto GcFlag() -> GcCnt & {
    thread_local static GcCnt gcCnt;
    return gcCnt;
  }

  void DBImpl::BackgroundCall_() {
    MutexLock const l(&mutex_);
    assert(backGroundWorkCnt_.load(std::memory_order_acquire) > 0);

    bool enoughRoomForNvm{false};

    if(shuttingDown_.load(std::memory_order_acquire) || !bgError_.ok()) {
      // No more background work when shutting down.
      // No more background work after a background error.
    } else {



      enoughRoomForNvm = lru_.totalSize.load(std::memory_order_relaxed) +
                           versions_->current()->GetLevel0FileSize() +
                           options_.write_buffer_size * 2 <=
                         lru_.maxSize;
      if(enoughRoomForNvm) {
        BackgroundCompactionOrPromotion_(enoughRoomForNvm);
        LruNvmCache();
      } else {
        LruNvmCache();
        BackgroundCompactionOrPromotion_(enoughRoomForNvm);
      }
    }

    //    std::cerr << std::to_string(pmCache::GetNumOfPmPieces()) +
    //                   " pieces in pm cache\n"
    //              << std::flush;

    if(GcFlag().ShouldGc())
      for(int i = 0; i <= NVM_MAX_LEVEL; ++i)
        pmSkiplistDramEntrancePtrArray[i]->ScheduleBackgroundGcAndSmr();

    backGroundWorkCnt_.fetch_add(-1, std::memory_order_release);
    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.

    MaybeScheduleCompactionOrPromotion_();
    backgroundWorkFinishedSignal_.SignalAll();

    //    exit(0);
  }

  void DBImpl::LruNvmCache() {
    if(doingLru_.load())
      return;

    mutex_.AssertHeld();
    bool isThisThreadDoingLru = false;
    size_t totalSize = versions_->current()->GetLevel0FileSize() +
                       options_.write_buffer_size * 2;


    pmCache::DramLru::MakeSpaceIterator it{(int64_t)(totalSize), &lru_};
    it.SeekFirst();
    if(it.Valid()) {
      pmCache::DramLruNode *node = nullptr;
      uint8_t level = 0;
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *nvmNode = nullptr;
      while(it.Valid()) {

        Version *current = versions_->current();

        auto &files = current->files;
        current->Ref();

        node = it.currentNode;
        level = node->whichLevel;

        if(versions_->LevelIsInfluencedByCompaction(level) ||
           versions_->LevelIsInfluencedByCompaction(level + 1)) {
          current->Unref();
          break;
        }

        //        nvmNode = node->nvmNode;

        int const k = LookFor(files.at(level), (int)node->fileNum);
        assert(k >= 0);

        gsl::unique_ptr<Compaction> c{
          versions_->PickCompactionByLru(files.at(level)[k], level)};

        if(c) {
          versions_->MarkLevelInfluencedByCompaction(level);
          versions_->MarkLevelInfluencedByCompaction(level + 1);

          assert(isThisThreadDoingLru || !doingLru_.load());

          doingLru_.store(true);
          isThisThreadDoingLru = true;

          MaybeScheduleCompactionOrPromotion_();
        }

        Status status;
        if(c->level() == NVM_MAX_LEVEL)
          for(auto file : c->inputs[0])
            it.madeSpace += (long long)file->fileSize;

        if(c->IsTrivialMove()) {
          gsl::owner<DBImpl::CompactionState *> compact{
            new CompactionState(c.get())};
          DoNvmMoveCompactionWork_(compact);
          if(!status.ok())
            RecordBackgroundError_(status);
          CleanupCompaction_(compact);
          c->ReleaseInputs();
        } else {
          gsl::owner<DBImpl::CompactionState *> compact{
            new CompactionState(c.get())};
          status = DoCompactionAndPromotionWork_(compact);
          if(!status.ok())
            RecordBackgroundError_(status);
          CleanupCompaction_(compact);
          c->ReleaseInputs();
        }

        versions_->UnMarkLevelInfluencedByCompaction(level);
        versions_->UnMarkLevelInfluencedByCompaction(level + 1);

        c.reset(nullptr);
        it.currentNode = nullptr;
        it.Next();
        current->Unref();
        RemoveObsoleteFiles_();
      }
    }
    if(isThisThreadDoingLru)
      doingLru_.store(false);
  }

  void DBImpl::BackgroundCompactionOrPromotion_(bool enoughRoomForNvm) {


    mutex_.AssertHeld();

    if(_pobj_cached_pool.pop == nullptr)
      _pobj_cached_pool.pop = pmemPool_.handle();

    auto fa = false;

    if(cImm_ && doingFlush_.compare_exchange_strong(fa, true)) {

      CompactMemTable_();

      doingFlush_.store(false);
      return;
    }

    gsl::owner<Compaction *> c = nullptr;
    bool const isManual = (manualCompaction_ != nullptr);
    InternalKey manualEnd;

    if(!isManual && !versions_->NeedsCompactionOrPromotion(enoughRoomForNvm))
      return;

    if(isManual) {
      std::cout << "running manual compaction\n which is not supported to be "
                   "run parallel with common compaction\n"
                << std::flush;
      ManualCompaction *m = manualCompaction_;
      c = versions_->CompactRange(m->level, m->begin, m->end);
      m->done = (c == nullptr);
      if(c != nullptr)
        manualEnd = c->input(0, c->NumInputFiles(0) - 1)->largest;
    } else {
      c = versions_->PickCompactionOrPromotion(true);
    }

    if(c) {
      
      versions_->MarkLevelInfluencedByCompaction(c->level());
      versions_->MarkLevelInfluencedByCompaction(c->level() + 1);

      MaybeScheduleCompactionOrPromotion_();

      assert(c->level() >= 4 || !c->inputs[0].empty());
    }

    Status status;
    if(c == nullptr) {
      // Nothing to do
    } else if(!isManual &&
              c->IsTrivialMove()) { 

      // Move file to next level

      assert(c->NumInputFiles(0) == 1);

      FileMetaData *f = c->input(0, 0);
      if(f->whereIsFile == FileMetaData::K_SSD) { // ssd to ssd
        c->edit()->RemoveFile(c->level(), f->number);
        c->edit()->AddFile(c->level() + 1, f->number, f->fileSize, f->smallest,
                           f->largest);
        auto &file = c->edit()->new_files_.rbegin()->second;
        file.countMinSketch = std::make_shared<pmCache::CountMinSketch>();
        file.bufferForEachFile = std::make_shared<pmCache::BufferForEachFile>();
        pmCache::GlobalTimestamp::TimestampToken t{};
        timeStamp->ReferenceTimestamp(t);

        status = versions_->LogAndApply(c->edit(), &mutex_, &t, timeStamp);
        if(!status.ok())
          RecordBackgroundError_(status);
      } else { // nvm to nvm
        gsl::owner<CompactionState *> compact{new CompactionState(c)};

        DoNvmMoveCompactionWork_(compact);

        if(!status.ok()) {
          std::cout << "error in NVM trivial move\n" << std::flush;
          RecordBackgroundError_(status);
        }
        CleanupCompaction_(compact);
        c->ReleaseInputs();
      }
    } else { 
      gsl::owner<CompactionState *> compact{new CompactionState(c)};

      //      auto tBegin = std::chrono::high_resolution_clock::now();

      //      auto t1 = std::chrono::high_resolution_clock::now();

      status = DoCompactionAndPromotionWork_(compact);

      //      auto t2 = std::chrono::high_resolution_clock::now();
      //
      //      if(compact->compaction->level() >= 4) {
      //        auto duration =
      //          std::chrono::duration_cast<std::chrono::microseconds>(t2 -
      //          t1).count();
      //        std::cout << std::to_string(compact->compaction->level()) + " " +
      //                       std::to_string(compact->compaction->inputs[0].size())
      //                       + " " +
      //                       std::to_string(compact->compaction->inputs[1].size())
      //                       + " " + "compaction time: " +
      //                       std::to_string(duration / 1000) + "ms\n"
      //                  << std::flush;
      //  }

      //      auto tEnd = std::chrono::high_resolution_clock::now();
      //      std::cout << std::to_string(compact->compaction->level()) + " " +
      //                     std::to_string(compact->compaction->inputs[0].size())
      //                     + " " +
      //                     std::to_string(compact->compaction->inputs[1].size())
      //                     + " " + "compaction time: " + std::to_string(
      //                       std::chrono::duration_cast<std::chrono::microseconds>(
      //                         tEnd - tBegin)
      //                         .count()) +
      //                     " us\n"
      //                << std::flush;
      //      exit(0);

      if(!status.ok())
        RecordBackgroundError_(status);
      CleanupCompaction_(compact);
      c->ReleaseInputs();
      if(c->level() >= NVM_MAX_LEVEL)
        RemoveObsoleteFiles_();
    }

    if(c) {
      versions_->UnMarkLevelInfluencedByCompaction(c->level());
      versions_->UnMarkLevelInfluencedByCompaction(c->level() + 1);

      //      if(c->level() >= 4) {
      //        std::cout << "CE\n" << std::flush;
      //      }
    }

    delete c;

    if(status.ok()) { // NOLINT(bugprone-branch-clone)
      // Done
    } else if(shuttingDown_.load(std::memory_order_acquire)) {
      // Ignore compaction errors found during shutting down
    }

    if(isManual) {
      ManualCompaction *m = manualCompaction_;
      if(!status.ok())
        m->done = true;
      if(!m->done) {
        // We only compacted part of the requested range.  Update *m
        // to the range that is left to be compacted.
        m->tmpStorage = manualEnd;
        m->begin = &m->tmpStorage;
      }
      manualCompaction_ = nullptr;
    }
  }

  void DBImpl::CleanupCompaction_(gsl::not_null<CompactionState *> compact) {
    mutex_.AssertHeld();
    if(compact->builder != nullptr) {
      // May happen if we get a shutdown call in the middle of compaction
      compact->builder->Abandon();
      delete compact->builder;
    } else
      assert(compact->outfile == nullptr);

    delete compact->outfile;
    for(auto &out : compact->outputs)
      pendingOutputs_.erase(out.number);
    for(auto &out : compact->reserveHotKeyOutputs)
      pendingOutputs_.erase(out.number);
    delete compact;
  }

  namespace openCompactionOutputFile {
    auto OpenCompactionOutputFile(
      leveldb::DBImpl::CompactionState *compact, TableBuilder *&builder,
      port::Mutex *mutex, VersionSet *versions,
      std::set<uint64_t> &pendingOutputs, const std::basic_string<char> &dbName,
      Env *env, WritableFile *&outFile, const Options &options,
      std::vector<leveldb::DBImpl::CompactionState::Output> &outputsList) {
      assert(compact != nullptr);
      assert(builder == nullptr);
      assert(&builder == &compact->builder ||
             &builder == &compact->reserveHotKeyBuilder);
      uint64_t fileNumber;
      {
        MutexLock lock(mutex);
        fileNumber = versions->NewFileNumber();
        pendingOutputs.insert(fileNumber);
        leveldb::DBImpl::CompactionState::Output out;
        out.number = fileNumber;
        out.smallest.Clear();
        out.largest.Clear();
        outputsList.push_back(out);
      }
      std::string fileName = TableFileName(dbName, fileNumber);
      Status s = env->NewWritableFile(fileName, &outFile);
      if(s.ok())
        builder = new TableBuilder(options, outFile);
      return s;
    }
  } // namespace openCompactionOutputFile

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wthread-safety-reference"
  auto DBImpl::OpenCompactionOutputFile(CompactionState *compact) -> Status {
    //    assert(compact != nullptr);
    //    assert(compact->builder == nullptr);
    //    uint64_t fileNumber;
    //    {
    //      mutex_.Lock();
    //      fileNumber = versions_->NewFileNumber();
    //      pendingOutputs_.insert(fileNumber);
    //      CompactionState::Output out;
    //      out.number = fileNumber;
    //      out.smallest.Clear();
    //      out.largest.Clear();
    //      compact->outputs.push_back(out);
    //      mutex_.Unlock();
    //    }
    //
    //    // Make the output file
    //    std::string fileName = TableFileName(dbname_, fileNumber);
    //    Status s = env_->NewWritableFile(fileName, &compact->outfile);
    //    if(s.ok())
    //      compact->builder = new TableBuilder(options_, compact->outfile);
    //    return s;
    return openCompactionOutputFile::OpenCompactionOutputFile(
      compact, compact->builder, &mutex_, versions_, pendingOutputs_, dbname_,
      env_, compact->outfile, options_, compact->outputs);
  }

  auto
  DBImpl::OpenCompactionOutputFileForReversedHotKey(CompactionState *compact)
    -> Status {
    //    assert(compact != nullptr);
    //    assert(compact->reserveHotKeyBuilder == nullptr);
    //    uint64_t fileNumber;
    //    {
    //      mutex_.Lock();
    //      fileNumber = versions_->NewFileNumber();
    //      pendingOutputs_.insert(fileNumber);
    //      CompactionState::Output out;
    //      out.number = fileNumber;
    //      out.smallest.Clear();
    //      out.largest.Clear();
    //      compact->outputs.push_back(out);
    //      mutex_.Unlock();
    //    }
    //
    //    // Make the output file
    //    std::string fileName = TableFileName(dbname_, fileNumber);
    //    Status s = env_->NewWritableFile(fileName,
    //    &compact->reserveHotKeyOutfile); if(s.ok())
    //      compact->reserveHotKeyBuilder =
    //        new TableBuilder(options_, compact->reserveHotKeyOutfile);
    //    return s;
    return openCompactionOutputFile::OpenCompactionOutputFile(
      compact, compact->reserveHotKeyBuilder, &mutex_, versions_,
      pendingOutputs_, dbname_, env_, compact->reserveHotKeyOutfile, options_,
      compact->reserveHotKeyOutputs);
  }
#pragma clang diagnostic pop

  namespace finishCompactionOutputFile {
    auto
    FinishCompactionOutputFile(DBImpl::CompactionState *compact,
                               WritableFile *&outfile, TableBuilder *&builder,
                               DBImpl::CompactionState::Output *currentOutput) {
      assert(compact);
      assert(outfile);
      assert(builder);
      //      std::cout<<builder->NumEntries();
      auto s = builder->Finish();
      const auto currentBytes = builder->FileSize();
      currentOutput->fileSize = currentBytes;
      delete builder;
      builder = nullptr;

      if(s.ok())
        s = outfile->Sync();
      if(s.ok())
        s = outfile->Close();
      delete outfile;
      outfile = nullptr;

      return s;
    }
  } // namespace finishCompactionOutputFile

  auto DBImpl::FinishCompactionOutputFile(CompactionState *compact) -> Status {
    //    assert(compact != nullptr);
    //    assert(compact->outfile != nullptr);
    //    assert(compact->builder != nullptr);
    //
    //    auto s = compact->builder->Finish();
    //    const uint64_t currentBytes = compact->builder->FileSize();
    //    compact->current_output()->fileSize = currentBytes;
    //    delete compact->builder;
    //    compact->builder = nullptr;
    //
    //    // Finish and check for file errors
    //    if(s.ok())
    //      s = compact->outfile->Sync();
    //    if(s.ok())
    //      s = compact->outfile->Close();
    //    delete compact->outfile;
    //    compact->outfile = nullptr;
    //
    //    return s;

    return finishCompactionOutputFile::FinishCompactionOutputFile(
      compact, compact->outfile, compact->builder, compact->current_output());
  }

  auto DBImpl::FinishCompactionOutputReservedFile(CompactionState *compact)
    -> Status {
    return finishCompactionOutputFile::FinishCompactionOutputFile(
      compact, compact->reserveHotKeyOutfile, compact->reserveHotKeyBuilder,
      compact->current_reserveHotKeyOutputs());
  }

  [[maybe_unused]] auto
  DBImpl::InstallCompactionResults_(CompactionState *compact) -> Status {
    mutex_.AssertHeld();
    // Add compaction outputs
    
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    for(size_t i = 0; i < compact->outputs.size(); i++) {
      const CompactionState::Output &out = compact->outputs[i];
      compact->compaction->edit()->AddFile(level + 1, out.number, out.fileSize,
                                           out.smallest, out.largest);
    }
    pmCache::GlobalTimestamp::TimestampToken t{};
    timeStamp->ReferenceTimestamp(t);
#ifndef NDEBUG
//    std::cout << "3\n" << std::flush;
#endif
    return versions_->LogAndApply(compact->compaction->edit(), &mutex_, &t,
                                  timeStamp);
  }

  auto DBImpl::InstallCompactionResults_(CompactionState *compact, bool isInNvm,
                                         bool sync) -> Status {
    mutex_.AssertHeld();
    // Add compaction outputs
    
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();

    for(auto &out : compact->outputs) {
      compact->compaction->edit()->AddFileWithWhereIsFile(
        level + 1, out.number, out.fileSize, out.smallest, out.largest,
        isInNvm ? FileMetaData::WhereIsFile::K_NVM
                : FileMetaData::WhereIsFile::K_SSD,
        out.pmFile);
      // #ifndef NDEBUG
      //      auto a = out.smallest.UserKey();
      //      auto b = out.largest.UserKey();
      //      if(a.data()[5] == '0' && b.data()[5] == '7') {
      //        std::cout << "InstallCompactionResults1_ " << a.ToString() << " "
      //                  << b.ToString() << std::endl;
      //        int aa = 1;
      //        std::cin >> aa;
      //        auto s = versions_->current()->files[compact->compaction->level()
      //        + 2]; for(auto &file : s) {
      //          std::cout << file->smallest.UserKey().ToString() << " "
      //                    << file->largest.UserKey().ToString() << std::endl;
      //        }
      //        std::cin >> aa;
      //      }
      // #endif
    }

    for(auto &out : compact->reserveHotKeyOutputs) {
      compact->compaction->edit()->AddFileWithWhereIsFile(
        level, out.number, out.fileSize, out.smallest, out.largest,
        FileMetaData::WhereIsFile::K_SSD, nullptr);

      // #ifndef NDEBUG
      //      auto a = out.smallest.UserKey();
      //      auto b = out.largest.UserKey();
      //      if(a.data()[5] == '0' && b.data()[5] == '7') {
      //        std::cout << "InstallCompactionResults1_ " << a.ToString() << " "
      //                  << b.ToString() << std::endl;
      //        int aa = 1;
      //        std::cin >> aa;
      //        auto s = versions_->current()->files[compact->compaction->level()
      //        + 1]; for(auto &file : s) {
      //          std::cout << file->smallest.UserKey().ToString() << " "
      //                    << file->largest.UserKey().ToString() << std::endl;
      //        }
      //        std::cin >> aa;
      //      }
      // #endif
    }

    pmCache::GlobalTimestamp::TimestampToken t{};
    timeStamp->ReferenceTimestamp(t);
#ifndef NDEBUG
//    std::cout << "4\n" << std::flush;
#endif

    return versions_->LogAndApply(compact->compaction->edit(), &mutex_, &t,
                                  timeStamp, sync);
  }

  namespace parallelListCompaction {
    void FinishCompactionOutputNvmFile(
      DBImpl *dbImpl, DBImpl::CompactionState *compact,
      std::vector<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *>
        &newNodesList,
      pmCache::DramLru &lru, std::vector<pmCache::DramLruNode *> *nodeToAdd) {

      auto newNvmSingleNode = compact->pmSkiplistNvmSingleNodeBuilder->Generate(
        0, UINT64_MAX, compact->current_output()->number);

      assert(newNvmSingleNode->fileNum == compact->current_output()->number);

      auto lruNode = new pmCache::DramLruNode;
      //      lruNode->nvmNode = newNvmSingleNode;
      lruNode->fileNum = compact->current_output()->number;
      newNvmSingleNode->lruNode = lruNode;

      lruNode->whichLevel = compact->compaction->level() + 1;
      lruNode->nodeSize =
        compact->pmSkiplistNvmSingleNodeBuilder->sizeAllocated;
      //      lru.InsertNode(lruNode);
      nodeToAdd->push_back(lruNode);


      auto *currentOutput = compact->current_output();

      if(!compact->pmSkiplistNvmSingleNodeBuilder->smallest.rep.empty()) {
        currentOutput->smallest =
          std::move(compact->pmSkiplistNvmSingleNodeBuilder->smallest);
        currentOutput->largest =
          std::move(compact->pmSkiplistNvmSingleNodeBuilder->largest);
      } else {
        auto *newNode = newNvmSingleNode->GetPmSsTable();
        currentOutput->smallest.DecodeFrom(newNode->GetMinInternalKey());
        currentOutput->largest.DecodeFrom(newNode->GetMaxInternalKey());
      }

      currentOutput->pmFile = newNvmSingleNode;
      assert(currentOutput->number == newNvmSingleNode->fileNum);
      currentOutput->fileSize =
        compact->pmSkiplistNvmSingleNodeBuilder->sizeAllocated;

      compact->pmSkiplistNvmSingleNodeBuilder.reset(nullptr);

      newNodesList.emplace_back(newNvmSingleNode);
    }
    class WaitMeta {
     public:
      WaitMeta() = default;
      ~WaitMeta() = default;
      std::mutex mutex{};
      bool done = false;
      std::condition_variable cv{};
    };


    void ParallelKeyValueDoubleListConnectWork(
      WaitMeta *newMeta,
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *node) {
      pmCache::PmSsTable::DataNode *currentNode{}, *newPrevNode{nullptr},
        *newNextNode{};
      //      pmCache::PmSsTable::DataNode *prevNode{}, *nextNode{};
      pmCache::PmSsTable *pmSsTable;
      pmCache::PmSsTable::Index *indexArray;
      {
        pmSsTable = node->GetPmSsTable();
        indexArray = pmSsTable->GetIndexS();
        auto indexLen = pmSsTable->GetIndexLen();
        for(int i = 0; i < indexLen; ++i) {

          // S
          //          std::stringstream ss;
          //          ss << "wL " << 2 * sizeof(uint64_t) << std::endl;
          //          std::cout << ss.str() << std::flush;

          currentNode = indexArray[i].GetData();
          bool lockOk = false;
          while(!lockOk) {
            lockOk = true;
            //            {
            //              if(!currentNode->mutex.try_lock()) {
            //                lockOk = false;
            //                continue;
            //              }
            //              prevNode = currentNode->PrevNode();
            //              nextNode = currentNode->NextNode();
            //              currentNode->mutex.unlock();
            //            }

            //            if(prevNode == nullptr) {
            //              std::stringstream ss;
            //              ss << &currentNode->prevNodeOff << std::endl;
            //              if(nextNode != nullptr) {
            //                std::cerr << "error when link doubly list1\n" <<
            //                std::flush;
            //              } else {
            //                currentNode->mutex.lock();
            //              }
            //            } else {
            //              if(prevNode->mutex.try_lock()) {
            //                if(currentNode->mutex.try_lock()) {
            //                  if(nextNode->mutex.try_lock()) {
            //                  } else {
            //                    currentNode->mutex.unlock();
            //                    prevNode->mutex.unlock();
            //                    lockOk = false;
            //                    continue;
            //                  }
            //                } else {
            //                  prevNode->mutex.unlock();
            //                  lockOk = false;
            //                  continue;
            //                }
            //              } else {
            //                lockOk = false;
            //                continue;
            //              }

            //              if(prevNode->NextNode() != currentNode ||
            //                 currentNode->NextNode() != nextNode) {
            //                prevNode->mutex.unlock();
            //                currentNode->mutex.unlock();
            //                nextNode->mutex.unlock();
            //                lockOk = false;
            //                continue;
            //              }
            //            }
            //            if(prevNode) {
            //              prevNode->SetNextNodeOff(nextNode);
            //              nextNode->SetPrevNodeOff(prevNode);
            //            }

            if(!newPrevNode)
              newPrevNode = pmSsTable->GetDataNodeHead();
            if(!newNextNode)
              newNextNode = newPrevNode->NextNode();
            //            newPrevNode->mutex.lock();
            //            newNextNode->mutex.lock();
            //            if(newNextNode->PrevNode() != newPrevNode) {
            //              std::cerr << "error when link doubly list2\n" <<
            //              std::flush; exit(-12);
            //            }
            newPrevNode->SetNextNodeOff(currentNode);
            newNextNode->SetPrevNodeOff(currentNode);
            currentNode->SetNextNodeOff(newNextNode);
            currentNode->SetPrevNodeOff(newPrevNode);

            newNextNode = currentNode;
          }
        }
      }
      //      _mm_mfence();
      {
        std::lock_guard<std::mutex> const guard{newMeta->mutex};
        newMeta->done = true;
      }
      newMeta->cv.notify_all();
    }

    void ParallelKeyValueDoubleListConnectWork(
      WaitMeta *newMeta, pmCache::PmSsTable *node,
      std::vector<pmCache::PmSsTable::Index> &newIndexArray) {
      pmCache::PmSsTable::DataNode *currentNode{}, *newPrevNode{nullptr},
        *newNextNode{};
      pmCache::PmSsTable *pmSsTable;
      pmCache::PmSsTable::Index *indexArray;
      {
        pmSsTable = node;
        indexArray = &newIndexArray[0];
        auto indexLen = newIndexArray.size();

        //        std::stringstream ss;
        //        ss << uint64_t(node)
        //           << " erase " + std::to_string(indexLen) + " nodes\n";
        //        std::cerr << ss.str() << std::flush;

        auto dummyHeadOfKvList = pmSsTable->GetDataNodeHead();
        auto dummyTailOfKvList = dummyHeadOfKvList + 1;
        dummyHeadOfKvList->SetNextNodeOff(dummyTailOfKvList);
        dummyTailOfKvList->SetPrevNodeOff(dummyHeadOfKvList);

        for(int i = 0; i < indexLen; ++i) {
          currentNode = indexArray[i].GetData();
          bool lockOk = false;
          while(!lockOk) {
            lockOk = true;
            if(!newPrevNode)
              newPrevNode = pmSsTable->GetDataNodeHead();
            if(!newNextNode)
              newNextNode = newPrevNode->NextNode();
            newPrevNode->SetNextNodeOff(currentNode);
            newNextNode->SetPrevNodeOff(currentNode);
            currentNode->SetNextNodeOff(newNextNode);
            currentNode->SetPrevNodeOff(newPrevNode);

            newNextNode = currentNode;
          }
        }
      }
      {
        std::lock_guard<std::mutex> const guard{newMeta->mutex};
        newMeta->done = true;
      }
      newMeta->cv.notify_all();
    }

    class ThreadSortData {
     public:
      explicit ThreadSortData(pmCache::PmSsTable *nvTable,
                              uint32_t maxIndexLen) {
        if(nvTable)
          usefulIndexLen = nvTable->GetIndexLen();

        buffer[currentBuffer].reserve(maxIndexLen);
        buffer[currentBuffer ^ 1].reserve(maxIndexLen);

        auto &currentBufferRef = buffer[currentBuffer];
        if(nvTable)
          for(auto &it : *nvTable)
            currentBufferRef.emplace_back(it, nvTable);
      }

      inline auto operator[](int i) const -> pmCache::PmSsTable::Index {
        return buffer[currentBuffer][i].first;
      }

      inline void SwitchBuffer() { currentBuffer ^= 1; }

      uint32_t usefulIndexLen = 0;
      //      uint32_t logicalIndexLen = 0;
      uint8_t currentBuffer = 0;
      std::array<
        std::vector<std::pair<pmCache::PmSsTable::Index, pmCache::PmSsTable *>>,
        2>
        buffer;
    }; // class ThreadSortData


    class WaitPreRoundMeta {
     public:
      __attribute__((aligned(L1_CACHE_BYTES))) std::mutex mutex;
      __attribute__((aligned(L1_CACHE_BYTES))) std::condition_variable cv;
      uint8_t whichRound = 0;
    }; // class WaitPreRoundMeta

    class FastSortIterator : public leveldb::Iterator {
     public:
      FastSortIterator(std::vector<ThreadSortData> &fileList, int totalFile)
          : threadSortDataList_(fileList), totalFileCnt_(totalFile) {}

      [[nodiscard]] auto Valid() const -> bool override {
        return currentFile_ < totalFileCnt_ && currentFile_ >= 0 &&
               currentI_ >= 0 &&
               currentI_ < threadSortDataList_[currentFile_].usefulIndexLen;
      }

      void SeekToFirst() override {
        currentFile_ = 0;
        currentI_ = 0;
      }

      void SeekToLast() override {
        currentFile_ = totalFileCnt_ - 1;
        currentI_ = (int)threadSortDataList_[currentFile_].usefulIndexLen - 1;
      }

      void Seek(const Slice &) override { assert(false); }

      void Next() override {
        currentI_++;
        assert(currentFile_ < totalFileCnt_);
        while(currentI_ >= threadSortDataList_[currentFile_].usefulIndexLen) {
          currentI_ = 0;
          currentFile_++;
        }
      }

      void Prev() override {
        currentI_--;
        if(currentI_ < 0) {
          currentFile_--;
          currentI_ = (int)threadSortDataList_[currentFile_].usefulIndexLen - 1;
        }
      }

      [[nodiscard]] auto key() const -> Slice override {
        return threadSortDataList_[currentFile_][currentI_]
          .GetData()
          ->GetInternalKeySlice();
      }

      [[nodiscard]] auto value() const -> Slice override {
        return threadSortDataList_[currentFile_][currentI_]
          .GetData()
          ->GetValueSlice();
      }

      [[nodiscard]] auto status() const -> Status override {
        return Status::OK();
      }

      auto IsInNvm() -> bool override { return true; }

      auto GetDataNodeInNvm() -> pmCache::PmSsTable::DataNode * override {
        return threadSortDataList_[currentFile_][currentI_].GetData();
      }

      pmCache::PmSsTable *WhichNvmNode() override {
        return threadSortDataList_[currentFile_]
          .buffer[threadSortDataList_[currentFile_].currentBuffer][currentI_]
          .second;
      }

     private:
      std::vector<ThreadSortData> &threadSortDataList_;
      int totalFileCnt_;
      int currentFile_ = 0;
      int currentI_ = 0;
    };


    /// \param threadSortDataList 
    /// \param totalFileCnt 
    /// \param compact compaction 
    /// \param pmCacheBgWorkThreadPool
    /// \param pmCacheBgSortThreadPool
    void DoParallelMergeSort(
      std::vector<parallelListCompaction::ThreadSortData> &threadSortDataList,
      int totalFileCnt, DBImpl::CompactionState *const compact,
      pmCache::ThreadPool *pmCacheBgSortThreadPool) {
      threadSortDataList.clear();
      threadSortDataList.reserve(totalFileCnt + 2);

      std::vector<std::unique_ptr<parallelListCompaction::WaitPreRoundMeta>>
        waitMetaList{};
      waitMetaList.reserve(totalFileCnt + 2);

      uint32_t maxIndexLen = 0;
      for(auto const &filesInputFromOneLevel : compact->compaction->inputs)
        for(auto const *inputFile : filesInputFromOneLevel)
          maxIndexLen =
            std::max(maxIndexLen,
                     inputFile->nvmSingleNode->GetPmSsTable()->GetIndexLen());

      for(auto const &filesInputFromOneLevel : compact->compaction->inputs)
        for(auto const *inputFile : filesInputFromOneLevel) {
          threadSortDataList.emplace_back(
            inputFile->nvmSingleNode->GetPmSsTable(), maxIndexLen);
          waitMetaList.emplace_back(
            std::make_unique<parallelListCompaction::WaitPreRoundMeta>());
        }

      int sampleCnt = 3;
      std::vector<pmCache::PmSsTable::Index> samples;
      samples.reserve((std::uint64_t)totalFileCnt * sampleCnt);
      for(auto i = 0; i < totalFileCnt; ++i) {
        auto &fileI = threadSortDataList[i];
        int sampleLen = (int)fileI.usefulIndexLen / sampleCnt;
        if(sampleLen == 0)
          sampleLen = 1;
        for(int j = (int)fileI.usefulIndexLen - 1; j > 0; j -= sampleLen)
          samples.emplace_back(fileI[j]);
      }

      std::sort(samples.begin(), samples.end());

      std::vector<pmCache::PmSsTable::Index> boundaries;
      auto sampleLen = (int)samples.size() / totalFileCnt;
      if(sampleLen == 0)
        sampleLen = 1;
      int k = totalFileCnt;
      boundaries.resize(totalFileCnt);
      for(int i = (int)samples.size() - 1; i >= 0; i -= sampleLen) {
        k--;
        if(0 <= k && k < int(boundaries.size()))
          boundaries[k] = (samples[i]);
        else
          break;
      }
      if(k > 0)
        for(int i = 0; i < k; ++i)
          boundaries[i] = samples[0];

      for(int i = 0; i < totalFileCnt; ++i) {
        pmCacheBgSortThreadPool->PostWork(
          {[&threadSortDataList, i, totalFileCnt, &waitMetaList,
            &boundaries]() {
             auto &thisThreadSortData = threadSortDataList[i];
             uint8_t currentBuffer = thisThreadSortData.currentBuffer;
             uint8_t otherBuffer = thisThreadSortData.currentBuffer ^ 1;
             auto thisBoundary = boundaries[i];
             auto lastBoundary = i > 0 ? boundaries[i - 1] : thisBoundary;
             auto &thisThreadNewBuffer =
               threadSortDataList[i].buffer[otherBuffer];

             for(int j = 0; j < totalFileCnt; ++j) {

               auto &otherThreadSortDataCurrentBuffer =
                 threadSortDataList[j].buffer[currentBuffer];
               auto otherBegin = otherThreadSortDataCurrentBuffer.begin();
               auto otherEnd = otherThreadSortDataCurrentBuffer.end();
               auto beginK =
                 (i == 0 ? otherBegin
                         : std::upper_bound(otherBegin, otherEnd,
                                            std::pair<pmCache::PmSsTable::Index,
                                                      pmCache::PmSsTable *>(
                                              lastBoundary, nullptr)));
               auto endK = std::upper_bound(
                 beginK, otherEnd,
                 std::pair<pmCache::PmSsTable::Index, pmCache::PmSsTable *>(
                   thisBoundary, nullptr));

               thisThreadNewBuffer.insert(thisThreadNewBuffer.end(), beginK,
                                          endK);
             }

             std::sort(thisThreadNewBuffer.begin(), thisThreadNewBuffer.end());
             thisThreadSortData.usefulIndexLen =
               (uint32_t)thisThreadNewBuffer.size();
             thisThreadSortData.SwitchBuffer();
             auto waitMeta = waitMetaList[i].get();
             {
               std::lock_guard<std::mutex> const waitMetaSecondGuard{
                 waitMeta->mutex};
               waitMeta->whichRound = 1;
             }
             waitMeta->cv.notify_all();
           },
           false}); 
      }

      for(auto &waiting : waitMetaList) {
        auto w = waiting.get();
        std::unique_lock<std::mutex> uniqueLock(w->mutex);
        w->cv.wait(uniqueLock, [w]() { return w->whichRound == 1; });
      }

    }

    void ScheduleBackgroundKvListLink(
      std::vector<std::unique_ptr<parallelListCompaction::WaitMeta>>
        &kvListLinkWaitMeta,
      std::vector<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *>
        &newNvmNodes,
      pmCache::ThreadPool *pmCacheBgWorkThreadPool) {
      auto newMeta = new parallelListCompaction::WaitMeta{};
      kvListLinkWaitMeta.emplace_back(newMeta);
      auto node = *newNvmNodes.rbegin();
      //      ParallelKeyValueDoubleListConnectWork(newMeta, node, pool);
      pmCacheBgWorkThreadPool->PostWork(new pmCache::Work{
        {[newMeta, node]() {
          ParallelKeyValueDoubleListConnectWork(newMeta, node);
        }},
        false});
    }

    void ScheduleBackgroundKvListLink(
      std::vector<std::unique_ptr<parallelListCompaction::WaitMeta>>
        &kvListLinkWaitMeta,
      pmCache::PmSsTable *node, pmCache::ThreadPool *pmCacheBgWorkThreadPool,
      std::vector<pmCache::PmSsTable::Index> &dummyIndexArray) {
      auto newMeta = new parallelListCompaction::WaitMeta{};
      kvListLinkWaitMeta.emplace_back(newMeta);
      //      ParallelKeyValueDoubleListConnectWork(newMeta, node, pool);
      pmCacheBgWorkThreadPool->PostWork(new pmCache::Work{
        {[newMeta, node, &dummyIndexArray]() {
          ParallelKeyValueDoubleListConnectWork(newMeta, node, dummyIndexArray);
        }},
        false});
    }

  } // namespace parallelListCompaction

  namespace hotKeyBuffer {
    enum WhichBuffer {
      UPPER_LEVEL [[maybe_unused]] = 0,
      LOWER_LEVE [[maybe_unused]] = 1,
      PROMOTE_LEVEL = 2
    };

    class TwoLevelIteratorForPromotion {
     public:
      inline auto Valid() { return it1 != bufferEnd_; }
      TwoLevelIteratorForPromotion() = default;
      void Init(std::vector<pmCache::BufferForEachFile *> *buffer1) {
        buffer = buffer1;
        it1 = buffer->begin();
        bufferEnd_ = buffer->end();
        while(Valid() && !(*it1)->GetSize())
          it1++;
        if(Valid())
          it2 = (*it1)->GetSetBegin();
      }
      auto LowerBound(const leveldb::Slice &key, bool allowEmptyTarget = false)
        -> std::pair<bool,
                     std::set<pmCache::BufferForEachFile::KeyValue>::iterator> {
        if(!Valid() || (!allowEmptyTarget && key.empty()))
          return {false, {}};
        else {
          bool moveIt1 = false;
          while(Valid() && (!(*it1)->GetSize() ||
                            leveldb::BytewiseComparator()->Compare(
                              (*it1)->GetMaxKey().GetUserKey(), key) < 0)) {
            it1++;
            moveIt1 = true;
          }
          if(!Valid())
            return {false, {}};
          else {
            
            if(moveIt1)
              it2 = (*it1)->GetSetBegin();
#ifndef NDEBUG
              //            std::string tmp;
//            if(it2 == (*it1)->GetSetEnd()) {
//              tmp.clear();
//            } else {
//              tmp.assign(it2->key.data.data(), it2->key.size);
//            }
//            if(it2 != (*it1)->GetSetBegin()) {
//              auto x1 = (*it1)->GetSetBegin()->key.GetUserKey().ToString();
//              assert(BytewiseComparator()->Compare(Slice{x1}, key) < 0);
//            }
#endif
            it2 = std::lower_bound(
              it2, (*it1)->GetSetEnd(), key,
              [](const pmCache::BufferForEachFile::KeyValue &kv,
                 const leveldb::Slice &key) {
                return leveldb::BytewiseComparator()->Compare(
                         kv.key.GetUserKey(), key) < 0;
              });
            if(__builtin_expect(it2 == (*it1)->GetSetEnd(), 0)) {
              return {false, {}};
            } else {
#ifndef NDEBUG
              //              assert(lastKey.empty() ||
              //              BytewiseComparator()->Compare(
//                                          {lastKey.data(), lastKey.size()},
//                                          it2->key.GetUserKey()) <= 0);
//
//              assert(lastSearch.empty() ||
//                     BytewiseComparator()->Compare(
//                       key, {lastSearch.data(), lastSearch.size()}) >= 0);
//              lastSearch.assign(key.data(), key.size());
//
//              lastKey.assign(it2->key.GetUserKey().data(),
//                             it2->key.GetUserKey().size());
//              std::string lastTmp;
//              for(auto &data : (*it1)->keyValueS) {
//                assert(lastTmp.empty() ||
//                       BytewiseComparator()->Compare(
//                         Slice{lastTmp}, data.key.GetUserKey()) <= 0);
//                lastSet.emplace(data.key.data, data.key.size,
//                data.value.data); lastTmp.assign(data.key.GetUserKey().data(),
//                               data.key.GetUserKey().size());
//              }
//              lastSearchBegin = tmp;
#endif
              return {true, it2};
            }
          }
        }
      }

      auto RemoveCurrentKey() {
        assert(Valid());
        assert((*it1)->GetSetEnd() != it2);
        auto nextIt2 = it2;
        nextIt2++;
        (*it1)->keyValueS.erase(it2);
        it2 = nextIt2;
        bool moveIt1 = false;
        if(it2 == (*it1)->GetSetEnd()) {
          moveIt1 = true;
          it1++;
        }
        while(Valid() && !(*it1)->GetSize()) {
          moveIt1 = true;
          it1++;
        }
        if(moveIt1 && Valid())
          it2 = (*it1)->GetSetBegin();
      }

      auto ExtractCurrentKey(
        //          std::_Node_handle<pmCache::BufferForEachFile::KeyValue,
        //                            pmCache::BufferForEachFile::KeyValue,
        //                            std::allocator<std::_Rb_tree_node<
        //                              pmCache::BufferForEachFile::KeyValue>>>
        std::set<pmCache::BufferForEachFile::KeyValue>::node_type &ret)
        -> void {
        assert(Valid());
        assert((*it1)->GetSetEnd() != it2);
        auto nextIt2 = it2;
        nextIt2++;
        ret = (*it1)->keyValueS.extract(it2);
        it2 = nextIt2;
        bool moveIt1 = false;
        if(it2 == (*it1)->GetSetEnd()) {
          moveIt1 = true;
          it1++;
        }
        while(Valid() && !(*it1)->GetSize()) {
          moveIt1 = true;
          it1++;
        }
        if(moveIt1 && Valid())
          it2 = (*it1)->GetSetBegin();

        
        //        std::stringstream ss;
        //        ss << "P " << ret.value().key.GetUserKey().ToString() << "\n";
        //        std::cout << ss.str() << std::flush;
      }

     public:
#ifndef NDEBUG
      std::string lastKey;
      std::string lastSearch;
      std::set<pmCache::BufferForEachFile::KeyValue> lastSet;
      std::string lastSearchBegin;
#endif

      std::vector<pmCache::BufferForEachFile *> *buffer{};

      std::vector<pmCache::BufferForEachFile *>::iterator it1{};
      std::set<pmCache::BufferForEachFile::KeyValue>::iterator it2{};

     private:
      std::vector<leveldb::pmCache::BufferForEachFile *>::iterator bufferEnd_;
    };
    auto WriteKeyValuePairToSuitableTable(Status &status, DBImpl *db,
                                          DBImpl::CompactionState *compact,
                                          bool isHotKey, const Slice &keySlice,
                                          const Slice &valueSlice) {


      //      isHotKey = false;

      TableBuilder *&tableBuilder =
        isHotKey ? compact->reserveHotKeyBuilder : compact->builder;
      if(tableBuilder == nullptr) {
        status = isHotKey
                   ? db->OpenCompactionOutputFileForReversedHotKey(compact)
                   : db->OpenCompactionOutputFile(compact);
        if(!status.ok())
          return false;
      }
      auto *output = isHotKey ? compact->current_reserveHotKeyOutputs()
                              : compact->current_output();
      if(!tableBuilder->NumEntries())
        output->smallest.DecodeFrom(keySlice);
      output->largest.DecodeFrom(keySlice);
      tableBuilder->Add(keySlice, valueSlice);

      if(tableBuilder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
        status = isHotKey ? db->FinishCompactionOutputReservedFile(compact)
                          : db->FinishCompactionOutputFile(compact);
        if(!status.ok())
          return false;
      }
      return true;
    }

    class [[maybe_unused]] DummyIteratorForCleanupBlockCache
        : public leveldb::Iterator {
      [[nodiscard]] auto Valid() const -> bool override { return false; }
      void SeekToFirst() override {}
      void SeekToLast() override {}
      void Seek(const Slice &target) override {}
      void Next() override {}
      void Prev() override {}
      [[nodiscard]] auto key() const -> Slice override { return {}; }
      [[nodiscard]] auto value() const -> Slice override { return {}; }
      [[nodiscard]] auto status() const -> Status override {
        return Status::OK();
      }
    };

  } // namespace hotKeyBuffer

  auto
  DBImpl::DoCompactionAndPromotionWork_(CompactionState *compact) -> Status {
  

    //    std::stringstream ss;
    //    ss << compact->compaction->level() << std::endl;
    //    std::cout << ss.str() << std::flush;

    // S
    //    {
    //      auto totalReadSize = 0;
    //      if(compact->compaction->level() < 4) {
    //        for(auto &input : compact->compaction->inputs) {
    //          for(auto &file : input) {
    //            totalReadSize +=
    //              file->nvmSingleNode->GetPmSsTable()->GetIndexLen() * 24;
    //          }
    //        }
    //      } else if(compact->compaction->level() == 4) {
    //        for(auto &file : compact->compaction->inputs[0]) {
    //          totalReadSize +=
    //            file->nvmSingleNode->GetPmSsTable()->GetIndexLen() * 1024;
    //        }
    //      }
    //      std::stringstream ss;
    //      ss << "r" << compact->compaction->level() << " " << totalReadSize
    //         << std::endl;
    //      std::cout << ss.str() << std::flush;
    //    }

    //    std::cerr << compact->compaction->DebugString() << std::flush;

    std::vector<pmCache::DramLruNode *> lruNodeToAdd;
    std::unordered_map<pmCache::PmSsTable *,
                       std::vector<pmCache::PmSsTable::Index>>
      nodesDropped;
    for(auto &input : compact->compaction->inputs) {
      for(auto &file : input) {
        auto pmSsTable = file->nvmSingleNode->GetPmSsTable();
        if(file->whereIsFile == FileMetaData::K_NVM) {
          nodesDropped[pmSsTable] = {};
          nodesDropped[pmSsTable].reserve(pmSsTable->indexLen);
        }
      }
    }
    bool isPromotion = compact->compaction->inputs[0].empty();
    std::string prevUserKeyFromSsd{};
    std::vector<FileMetaData *> sortedInput0 = compact->compaction->inputs[0];
    std::sort(sortedInput0.begin(), sortedInput0.end());
    Status status{};
    ParsedInternalKey ikey{};
    std::string currentUserKey{};
    bool hasCurrentUserKey = false;
    SequenceNumber lastSequenceForKey = kMaxSequenceNumber;

    std::vector<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *>
      newNvmNodes, nodesToDelete;

    std::vector<parallelListCompaction::ThreadSortData> threadSortDataList{};

    std::vector<std::unique_ptr<parallelListCompaction::WaitMeta>>
      kvListLinkWaitMeta{};


    compact->smallestSnapshot = snapshots_.empty()
                                  ? versions_->LastSequence()
                                  : snapshots_.oldest()->sequence_number();


    FileMetaData::WhereIsFile mainOutputTarget =
      0 <= compact->compaction->level() &&
          compact->compaction->level() < NVM_MAX_LEVEL
        ? FileMetaData::WhereIsFile::K_NVM
        : FileMetaData::WhereIsFile::K_SSD;

    auto totalFileCnt = compact->compaction->NumInputFiles(0) +
                        compact->compaction->NumInputFiles(1);
    bool fastSort = mainOutputTarget == FileMetaData::WhereIsFile::K_NVM &&
                    ((compact->compaction->level() == 0 && totalFileCnt >= 2) ||
                     totalFileCnt >= 4);

    //    fastSort = false;

 
    std::unique_ptr<Iterator> input{
      !fastSort ? versions_->MakeInputIterator(compact->compaction) : nullptr};

    assert(backGroundWorkCnt_.load(std::memory_order_acquire) > 0);
    // Release mutex while we're actually doing the compaction work

    mutex_.Unlock();

    if(fastSort) {
      parallelListCompaction::DoParallelMergeSort(
        threadSortDataList, totalFileCnt, compact,
        pmCacheBgSubCompactionThreadPool_);
      input = std::make_unique<parallelListCompaction::FastSortIterator>(
        threadSortDataList, totalFileCnt);
    }


    std::vector<std::unique_lock<std::shared_mutex>> fileBufferLockGuard{};
    bool shouldPromote{false},
      needReserveHotKey{!isPromotion &&
                        compact->compaction->level() > NVM_MAX_LEVEL};
    hotKeyBuffer::TwoLevelIteratorForPromotion promoteIterator{};
    std::array<std::vector<pmCache::BufferForEachFile *>, 3> buffers{};
    int levelToPromote;
    {
      auto level = compact->compaction->level(); // UPPER_LEVEL
      assert(level + 1 <= config::kNumLevels - 1);
      for(int i = 0; i < 2; ++i) {
        auto targetLevel = level + i;

        if(!isPromotion && targetLevel > NVM_MAX_LEVEL) {
          auto &currentBuffer = buffers[i];
          for(auto *file : compact->compaction->inputs[i]) {
            assert(file->whereIsFile == FileMetaData::K_SSD);
            file->compacting.store(true, std::memory_order_release);
            auto fileHotKeyBuffer = file->bufferForEachFile.get();
            fileBufferLockGuard.emplace_back(fileHotKeyBuffer->mutex);
            currentBuffer.emplace_back(fileHotKeyBuffer);
          }
        }
      }
      levelToPromote = level + 2;

      if(levelToPromote > NVM_MAX_LEVEL &&
         levelToPromote < config::kNumLevels) {
        auto &currentBuffer =
          buffers[hotKeyBuffer::WhichBuffer::PROMOTE_LEVEL /*2*/];
        for(auto *file : compact->compaction->grandparents) {
          assert(file->whereIsFile == FileMetaData::K_SSD);
          file->compacting.store(true, std::memory_order_release);
          auto fileBufferForEachFile = file->bufferForEachFile.get();
          fileBufferLockGuard.emplace_back(fileBufferForEachFile->mutex);
          currentBuffer.emplace_back(fileBufferForEachFile);
          shouldPromote |= (!fileBufferForEachFile->keyValueS.empty());
        }
        promoteIterator.Init(&currentBuffer);
      }
    } 

    auto inputRawPtr = input.get();

    inputRawPtr->SeekToFirst();

    std::string keyToPromote, valueToPromote;
    bool firstKey = true, isInNvm;
    Slice keySlice, valueSlice, userKeySlice, prevUserKeySlice,
      nextUserKeySlice;
    pmCache::PmSsTable::DataNode *kvNodeInNvm = nullptr;

    {
      //      pmem::obj::flat_transaction::manual const tx(pmemPool_);

      if(shouldPromote && !compact->compaction->inputs[1].empty() &&
         compact->compaction->level() > 0 &&
         compact->compaction->inputs[1][0] ==
           this->versions_->current()
             ->files[compact->compaction->level() + 1][0]) {
        auto emptySlice = Slice{};
        auto key = inputRawPtr->key();

        auto firstInputKey = Slice{key.data(), key.size() - 8};
        while(true) {
          auto keyValueToPromoteWrap =
            promoteIterator.LowerBound(emptySlice, true);
          if(keyValueToPromoteWrap.first &&
             BytewiseComparator()->Compare(
               keyValueToPromoteWrap.second->key.GetUserKey(), firstInputKey) <
               0) {

            //            std::_Node_handle<pmCache::BufferForEachFile::KeyValue,
            //                              pmCache::BufferForEachFile::KeyValue,
            //                              std::allocator<std::_Rb_tree_node<
            //                                pmCache::BufferForEachFile::KeyValue>>>
            //              nodeHandle;

            std::set<pmCache::BufferForEachFile::KeyValue>::node_type nodeHandle;

            promoteIterator.ExtractCurrentKey(nodeHandle);
            auto &value = nodeHandle.value();
            keyToPromote = std::move(value.key.data);
            valueToPromote = std::move(value.value.data);
            isInNvm = false;
            keySlice = {keyToPromote.data(), nodeHandle.value().key.size};

            // #ifndef NDEBUG
            //              if(compact->compaction->level() == 3) {
            //              std::cout <<
            //              std::to_string(compact->compaction->level()) + " "
            //              +
            //                             keySlice.ToString() + "\n"
            //                        << std::flush;
            //              }
            // #endif

            valueSlice = {valueToPromote.data(), valueToPromote.size()};
           
            *(char *)(keySlice.data() + keySlice.size() - 8) = 0x1;

            if(compact->compaction->ShouldStopBefore(keySlice)) {
              
              if(mainOutputTarget == FileMetaData::K_SSD &&
                 compact->builder != nullptr)
                status = FinishCompactionOutputFile(compact);
              else if(mainOutputTarget == FileMetaData::K_NVM &&
                      compact->pmSkiplistNvmSingleNodeBuilder != nullptr) {
                parallelListCompaction::FinishCompactionOutputNvmFile(
                  this, compact, newNvmNodes, lru_, &lruNodeToAdd);
                parallelListCompaction::ScheduleBackgroundKvListLink(
                  kvListLinkWaitMeta, newNvmNodes,
                  pmCacheBgSubCompactionThreadPool_);
              }
              if(!status.ok())
                break;
            }

            if(mainOutputTarget == FileMetaData::K_SSD) {


              bool shouldReserve = false;

              //              if(compact->compaction->level() >= 4) {
              //                std::stringstream ss;
              //                ss << "P ";
              //                Slice userKey = {keySlice.data(), keySlice.size()
              //                - 8}; ss << userKey.ToString() << std::endl;
              //                std::cout << ss.str() << std::flush;
              //              }

              if(!hotKeyBuffer::WriteKeyValuePairToSuitableTable(
                   status, this, compact, shouldReserve, keySlice, valueSlice))
                break;
            } else { // whereToOutPut == FileMetaData::kNVM

              if(compact->pmSkiplistNvmSingleNodeBuilder == nullptr) {
                assert(compact != nullptr);
                assert(compact->builder == nullptr);
                uint64_t fileNumber;
                {
                  MutexLock lock{&mutex_};

                  fileNumber = versions_->NewFileNumber();
                  pendingOutputs_.insert(fileNumber);
                  CompactionState::Output out;
                  out.number = fileNumber;
                  out.smallest.Clear();
                  out.largest.Clear();
                  compact->outputs.push_back(out);
                }
                compact->pmSkiplistNvmSingleNodeBuilder = std::make_unique<
                  pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>();
              }


              compact->pmSkiplistNvmSingleNodeBuilder
                ->PushNvmNodeToIndexWithoutChangeLinks(
                  __builtin_expect(isInNvm, 1)
                    ? kvNodeInNvm
                    : pmCache::PmSsTable::GenerateDataNode(keySlice,
                                                           valueSlice));


              if(compact->pmSkiplistNvmSingleNodeBuilder->sizeAllocated >=
                 (uint64_t)PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA) {
                parallelListCompaction::FinishCompactionOutputNvmFile(
                  this, compact, newNvmNodes, lru_, &lruNodeToAdd);
                parallelListCompaction::ScheduleBackgroundKvListLink(
                  kvListLinkWaitMeta, newNvmNodes,
                  pmCacheBgSubCompactionThreadPool_);
              }
            }
          } else {
            break;
          }
        }
      }
      auto cLevel = compact->compaction->level();
      while(inputRawPtr->Valid()) {
        assert(compact->compaction->level() <= NVM_MAX_LEVEL ||
               inputRawPtr->WhichFile());

        bool shouldMoveInputIteratorToNext = true;

        keySlice = inputRawPtr->key();
        //        valueSlice = inputRawPtr->value();

        //                std::stringstream ss;
        //        ss << "w2 " << keySlice.size() << std::endl;
        //        ss << "w2 " << valueSlice.size() << std::endl;
        //        ss << "r1 " << keySlice.size() << std::endl;
        //                std::cout << ss.str() << std::flush;

        isInNvm = inputRawPtr->IsInNvm();
        if(isInNvm) {
          //          std::stringstream ss;
          //          ss << "r12 " << sizeof(pmCache::PmSsTable::Index) <<
          //          std::endl; std::cout << ss.str() << std::flush;

          kvNodeInNvm = inputRawPtr->GetDataNodeInNvm();
        }

        userKeySlice = Slice{keySlice.data(), keySlice.size() - 8};


        if(shouldPromote) {
          assert(prevUserKeySlice.empty() || *prevUserKeySlice.data() == 'u');
          auto keyValueToPromoteWrap =
            promoteIterator.LowerBound(prevUserKeySlice);
          if(!keyValueToPromoteWrap.first) {
            if(!prevUserKeySlice.empty())
              shouldPromote = false;
          } else {
            auto compareKeyToPromoteWithNextKey = BytewiseComparator()->Compare(
              keyValueToPromoteWrap.second->key.GetUserKey(), nextUserKeySlice);
            if(compareKeyToPromoteWithNextKey < 0) {

              shouldMoveInputIteratorToNext = false;
              //              std::_Node_handle<pmCache::BufferForEachFile::KeyValue,
              //                                pmCache::BufferForEachFile::KeyValue,
              //                                std::allocator<std::_Rb_tree_node<
              //                                  pmCache::BufferForEachFile::KeyValue>>>
              //                nodeHandle;
              std::set<pmCache::BufferForEachFile::KeyValue>::node_type
                nodeHandle;
              promoteIterator.ExtractCurrentKey(nodeHandle);
              auto &value = nodeHandle.value();
              keyToPromote = std::move(value.key.data);
              valueToPromote = std::move(value.value.data);
              isInNvm = false;
              keySlice = {keyToPromote.data(), nodeHandle.value().key.size};

              // #ifndef NDEBUG
              //              if(compact->compaction->level() == 3) {
              //              std::cout <<
              //              std::to_string(compact->compaction->level()) + " "
              //              +
              //                             keySlice.ToString() + "\n"
              //                        << std::flush;
              //              }
              // #endif

              valueSlice = {valueToPromote.data(), valueToPromote.size()};
              assert(prevUserKeySlice.size() >= 18);
              assert(nextUserKeySlice.size() >= 18);
              assert(BytewiseComparator()->Compare(
                       prevUserKeySlice,
                       {keySlice.data(), keySlice.size() - 8}) <= 0);
              assert(BytewiseComparator()->Compare(
                       {keySlice.data(), keySlice.size() - 8},
                       nextUserKeySlice) < 0);

              *(char *)(keySlice.data() + keySlice.size() - 8) = 0x1;
            } else if(compareKeyToPromoteWithNextKey == 0)
              promoteIterator.RemoveCurrentKey();
          }
        }


        if(compact->compaction->ShouldStopBefore(keySlice)) {

          if(mainOutputTarget == FileMetaData::K_SSD &&
             compact->builder != nullptr)
            status = FinishCompactionOutputFile(compact);
          else if(mainOutputTarget == FileMetaData::K_NVM &&
                  compact->pmSkiplistNvmSingleNodeBuilder != nullptr) {
            parallelListCompaction::FinishCompactionOutputNvmFile(
              this, compact, newNvmNodes, lru_, &lruNodeToAdd);
            parallelListCompaction::ScheduleBackgroundKvListLink(
              kvListLinkWaitMeta, newNvmNodes,
              pmCacheBgSubCompactionThreadPool_);
          }
          if(!status.ok())
            break;
        }

        // Handle key/value, add to state, etc.

        bool drop = false;
        if(GSL_UNLIKELY(
             !ParseInternalKey(keySlice,
                               &ikey))) { 

          // Do not hide error keys
          currentUserKey.clear();
          hasCurrentUserKey = false;
          lastSequenceForKey = kMaxSequenceNumber;
        } else { 

          if(!hasCurrentUserKey ||
             UserComparator_()->Compare(ikey.userKey, Slice(currentUserKey)) !=
               0) {
            currentUserKey.assign(ikey.userKey.data(), ikey.userKey.size());
            hasCurrentUserKey = true;
            
            lastSequenceForKey = kMaxSequenceNumber;
          }


          if(lastSequenceForKey <=
             compact->smallestSnapshot) { // NOLINT(bugprone-branch-clone)
            // Hidden by a newer entry for same user key
            drop = true; // (A)
          } else if(ikey.type == kTypeDeletion &&
                    ikey.sequence <= compact->smallestSnapshot &&
                    compact->compaction->IsBaseLevelForKey(ikey.userKey)) {
 
            //
            // For this user key:
            // (1) there is no data in higher levels
            // (2) data in lower levels will have larger sequence numbers
            // (3) data in layers that are being compacted here and have
            //     smaller sequence numbers will be dropped in the next
            //     few iterations of this loop (by rule (A) above).
            // Therefore, this deletion marker is obsolete and can be
            // dropped.
            drop = true;
          }
          lastSequenceForKey = ikey.sequence;
        }           
        if(!drop) { 
          assert(!shouldPromote || nextUserKeySlice.empty() ||
                 (shouldPromote && BytewiseComparator()->Compare(
                                     {keySlice.data(), keySlice.size() - 8},
                                     nextUserKeySlice) <= 0));
          if(mainOutputTarget == FileMetaData::K_SSD) {

            bool shouldReserve =
              needReserveHotKey && shouldMoveInputIteratorToNext &&
              std::find(sortedInput0.begin(), sortedInput0.end(),
                        inputRawPtr->WhichFile()) != sortedInput0.end() &&
              inputRawPtr->WhichFile()->countMinSketch->CheckIfReserve(
                userKeySlice, hotKeyReserveThreshold[cLevel + 1]);

            //            if(compact->compaction->level() >= 4) {
            //              std::stringstream ss;
            //              if(keySlice.data() == keyToPromote.data()) {
            //                ss << "P ";
            //              } else {
            //                if(compact->compaction->level() >= 5) {
            //                  if(shouldReserve) {
            //                    ss << "R ";
            //                  } else {
            //                    ss << "C ";
            //                  }
            //                }
            //              }
            //              Slice userKey = {keySlice.data(), keySlice.size() -
            //              8}; if(!ss.str().empty()) {
            //                ss << userKey.ToString() << std::endl;
            //                std::cout << ss.str() << std::flush;
            //              }
            //            }
            valueSlice = inputRawPtr->value();
            if(!hotKeyBuffer::WriteKeyValuePairToSuitableTable(
                 status, this, compact, shouldReserve, keySlice, valueSlice))
              break;
          } else { // whereToOutPut == FileMetaData::kNVM


            if(compact->pmSkiplistNvmSingleNodeBuilder == nullptr) {
              assert(compact != nullptr);
              assert(compact->builder == nullptr);
              uint64_t fileNumber;
              {
                MutexLock lock{&mutex_};

                fileNumber = versions_->NewFileNumber();
                pendingOutputs_.insert(fileNumber);
                CompactionState::Output out;
                out.number = fileNumber;
                out.smallest.Clear();
                out.largest.Clear();
                compact->outputs.push_back(out);
              }
              compact->pmSkiplistNvmSingleNodeBuilder = std::make_unique<
                pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>();
            }
            if(isInNvm) {
              compact->pmSkiplistNvmSingleNodeBuilder
                ->PushNvmNodeToIndexWithoutChangeLinks(
                  kvNodeInNvm, keySlice.size(), inputRawPtr->GetValueLen());
            } else {
              valueSlice = inputRawPtr->value();
              compact->pmSkiplistNvmSingleNodeBuilder
                ->PushNvmNodeToIndexWithoutChangeLinks(
                  pmCache::PmSsTable::GenerateDataNode(keySlice, valueSlice));
            }


            if(compact->pmSkiplistNvmSingleNodeBuilder->sizeAllocated >=
               (uint64_t)PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA) {
              parallelListCompaction::FinishCompactionOutputNvmFile(
                this, compact, newNvmNodes, lru_, &lruNodeToAdd);
              parallelListCompaction::ScheduleBackgroundKvListLink(
                kvListLinkWaitMeta, newNvmNodes,
                pmCacheBgSubCompactionThreadPool_);
            }
          } // end if(whereToOutPut == FileMetaData::kNVM)
        }   // end if (!drop)

        if(drop) {
          if(shouldMoveInputIteratorToNext) {
            auto file = inputRawPtr->WhichNvmNode();
            if(file)
              nodesDropped[file].emplace_back(
                pmCache::GetOffset(inputRawPtr->GetDataNodeInNvm()));
            else {
              auto maybeFile = inputRawPtr->WhichFile();
              if(maybeFile && maybeFile->whereIsFile == FileMetaData::K_NVM)
                nodesDropped[maybeFile->nvmSingleNode->GetPmSsTable()]
                  .emplace_back(
                    pmCache::GetOffset(inputRawPtr->GetDataNodeInNvm()));
            }
          }
        }


        if(shouldMoveInputIteratorToNext) {

          if(shouldPromote) {
            if(firstKey) {
              if(inputRawPtr->IsInNvm()) {
                prevUserKeySlice = {keySlice.data(), keySlice.size() - 8};
              } else {

                //                auto cleanupNode =
                //                input->GetCleanupNodeForCompactionPrevNode();
                //                if(cleanupNode) {
                //                  if(!cleanupNode->IsEmpty()) {
                //                    dummyIteratorForCleanupBlockCache.RegisterCleanup(
                //                      cleanupNode->function,
                //                      cleanupNode->arg1, cleanupNode->arg2);
                //                    cleanupNode->function = nullptr;
                //                    bool first = true;
                //                    for(auto *node = cleanupNode->next; node
                //                    != nullptr;) {
                //                      if(first) {
                //                        first = false;
                //                        cleanupNode->next = nullptr;
                //                      }
                //                      dummyIteratorForCleanupBlockCache.RegisterCleanup(
                //                        node->function, node->arg1,
                //                        node->arg2);
                //                      auto *nextNode = node->next;
                //                      delete node;
                //                      node = nextNode;
                //                    }
                //                  }
                //                }

                prevUserKeyFromSsd.assign(keySlice.data(), keySlice.size() - 8);

                //                prevUserKeyFromSsd =
                //                std::move(*input->GetKeyString());
                prevUserKeySlice =
                  Slice{prevUserKeyFromSsd.data(), keySlice.size() - 8};
              }
            } else {
              if(inputRawPtr->IsInNvm())
                prevUserKeySlice = {std::move(nextUserKeySlice)};
              else {

                //                auto cleanupNode =
                //                input->GetCleanupNodeForCompactionPrevNode();
                //                if(cleanupNode) {
                //                  if(!cleanupNode->IsEmpty()) {
                //                    dummyIteratorForCleanupBlockCache.RegisterCleanup(
                //                      cleanupNode->function,
                //                      cleanupNode->arg1, cleanupNode->arg2);
                //                    cleanupNode->function = nullptr;
                //                    bool first = true;
                //                    for(auto *node = cleanupNode->next; node
                //                    != nullptr;) {
                //                      if(first) {
                //                        first = false;
                //                        cleanupNode->next = nullptr;
                //                      }
                //                      dummyIteratorForCleanupBlockCache.RegisterCleanup(
                //                        node->function, node->arg1,
                //                        node->arg2);
                //                      auto *nextNode = node->next;
                //                      delete node;
                //                      node = nextNode;
                //                    }
                //                  }
                //                }

                prevUserKeyFromSsd.assign(nextUserKeySlice.data(),
                                          nextUserKeySlice.size());
                prevUserKeySlice =
                  Slice{prevUserKeyFromSsd.data(), nextUserKeySlice.size()};
                //                nextUserKeySlice.clear();

                //                prevUserKeyFromSsd =
                //                std::move(*input->GetKeyString());
                //                prevUserKeySlice = nextUserKeySlice;
                //                nextUserKeySlice.clear();
              }
            }
            assert(prevUserKeySlice.empty() || *prevUserKeySlice.data() == 'u');
          }

          inputRawPtr->Next();


          if(shouldPromote && inputRawPtr->Valid())
            nextUserKeySlice = {inputRawPtr->key().data(),
                                inputRawPtr->key().size() - 8};

          if(firstKey)
            firstKey = false;
        }

      } 

      if(compact->compaction->level() < NVM_MAX_LEVEL) {
        for(auto &inputNvmNodes : nodesDropped) {
          parallelListCompaction::ScheduleBackgroundKvListLink(
            kvListLinkWaitMeta, inputNvmNodes.first,
            pmCacheBgSubCompactionThreadPool_, inputNvmNodes.second);
        }
      }

      auto lowerLevel = compact->compaction->level() + 1;
      auto &filesLowerLevel = this->versions_->current()->files[lowerLevel];
      auto &inputLowerLevel = compact->compaction->inputs[1];
      if(shouldPromote && !inputLowerLevel.empty() &&
         compact->compaction->level() > 0 &&
         inputLowerLevel.back() == filesLowerLevel.back()) {
        //        auto emptySlice = Slice{};
        //        auto firstInputKey =
        //          Slice{input->key().data(), input->key().size() - 8};
        while(true) {
          auto keyValueToPromoteWrap =
            promoteIterator.LowerBound(prevUserKeySlice);
          if(keyValueToPromoteWrap.first) {
            //            std::_Node_handle<pmCache::BufferForEachFile::KeyValue,
            //                              pmCache::BufferForEachFile::KeyValue,
            //                              std::allocator<std::_Rb_tree_node<
            //                                pmCache::BufferForEachFile::KeyValue>>>
            //              nodeHandle;
            std::set<pmCache::BufferForEachFile::KeyValue>::node_type nodeHandle;
            promoteIterator.ExtractCurrentKey(nodeHandle);
            auto &value = nodeHandle.value();
            keyToPromote = std::move(value.key.data);
            valueToPromote = std::move(value.value.data);
            assert(keyToPromote[0] == 'u');
            isInNvm = false;
            keySlice = {keyToPromote.data(), nodeHandle.value().key.size};

            // #ifndef NDEBUG
            //              if(compact->compaction->level() == 3) {
            //              std::cout <<
            //              std::to_string(compact->compaction->level()) + " "
            //              +
            //                             keySlice.ToString() + "\n"
            //                        << std::flush;
            //              }
            // #endif

            valueSlice = {valueToPromote.data(), valueToPromote.size()};

            *(char *)(keySlice.data() + keySlice.size() - 8) = 0x1;

            if(compact->compaction->ShouldStopBefore(keySlice)) {

              if(mainOutputTarget == FileMetaData::K_SSD &&
                 compact->builder != nullptr)
                status = FinishCompactionOutputFile(compact);
              else if(mainOutputTarget == FileMetaData::K_NVM &&
                      compact->pmSkiplistNvmSingleNodeBuilder != nullptr) {
                parallelListCompaction::FinishCompactionOutputNvmFile(
                  this, compact, newNvmNodes, lru_, &lruNodeToAdd);
                parallelListCompaction::ScheduleBackgroundKvListLink(
                  kvListLinkWaitMeta, newNvmNodes,
                  pmCacheBgSubCompactionThreadPool_);
              }
              if(!status.ok())
                break;
            }

            if(mainOutputTarget == FileMetaData::K_SSD) {

              bool shouldReserve = false;

              //              if(compact->compaction->level() >= 4) {
              //                std::stringstream ss;
              //                ss << "P ";
              //                Slice userKey = {keySlice.data(), keySlice.size()
              //                - 8}; ss << userKey.ToString() << std::endl;
              //                std::cout << ss.str() << std::flush;
              //              }

              if(!hotKeyBuffer::WriteKeyValuePairToSuitableTable(
                   status, this, compact, shouldReserve, keySlice, valueSlice))
                break;
            } else { // whereToOutPut == FileMetaData::kNVM

              if(compact->pmSkiplistNvmSingleNodeBuilder == nullptr) {
                assert(compact != nullptr);
                assert(compact->builder == nullptr);
                uint64_t fileNumber;
                {
                  MutexLock lock{&mutex_};

                  fileNumber = versions_->NewFileNumber();
                  pendingOutputs_.insert(fileNumber);
                  CompactionState::Output out;
                  out.number = fileNumber;
                  out.smallest.Clear();
                  out.largest.Clear();
                  compact->outputs.push_back(out);
                }
                compact->pmSkiplistNvmSingleNodeBuilder = std::make_unique<
                  pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>();
              }


              compact->pmSkiplistNvmSingleNodeBuilder
                ->PushNvmNodeToIndexWithoutChangeLinks(
                  __builtin_expect(isInNvm, 1)
                    ? kvNodeInNvm
                    : pmCache::PmSsTable::GenerateDataNode(keySlice,
                                                           valueSlice));


              if(compact->pmSkiplistNvmSingleNodeBuilder->sizeAllocated >=
                 (uint64_t)PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA) {
                parallelListCompaction::FinishCompactionOutputNvmFile(
                  this, compact, newNvmNodes, lru_, &lruNodeToAdd);
                parallelListCompaction::ScheduleBackgroundKvListLink(
                  kvListLinkWaitMeta, newNvmNodes,
                  pmCacheBgSubCompactionThreadPool_);
              }
            }
          } else {
            break;
          }
        }
      }

      if(levelToPromote > NVM_MAX_LEVEL && levelToPromote < config::kNumLevels)
        for(auto *file : compact->compaction->grandparents)
          file->compacting.store(false, std::memory_order_release);

      if(status.ok() && mainOutputTarget == FileMetaData::K_SSD &&
         compact->builder != nullptr)
        status = FinishCompactionOutputFile(compact);

      if(status.ok() && needReserveHotKey &&
         compact->reserveHotKeyBuilder != nullptr)
        status = FinishCompactionOutputReservedFile(compact);

      if(status.ok() && mainOutputTarget == FileMetaData::K_NVM &&
         compact->pmSkiplistNvmSingleNodeBuilder != nullptr) {
        parallelListCompaction::FinishCompactionOutputNvmFile(
          this, compact, newNvmNodes, lru_, &lruNodeToAdd);
        parallelListCompaction::ScheduleBackgroundKvListLink(
          kvListLinkWaitMeta, newNvmNodes, pmCacheBgSubCompactionThreadPool_);
      }

      if(!newNvmNodes.empty() || !nodesDropped.empty()) {
        parallelListCompaction::WaitMeta *waitMeta;
        for(auto &i : kvListLinkWaitMeta) {
          std::unique_lock<std::mutex> lk{(waitMeta = i.get())->mutex};
          waitMeta->cv.wait(lk, [&waitMeta] { return waitMeta->done; });
        }
      }
      //      pmemobj_tx_commit();
    } // finish compaction in PMDK TX

    if(status.ok())
      status = inputRawPtr->status();

    mutex_.Lock();

    auto const &compaction = compact->compaction;
    auto level = compaction->level();
    if(level <= NVM_MAX_LEVEL) {
      auto const &upperInputs = compaction->inputs[0];
      auto const &downInputs = compaction->inputs[1];
      if(level + 1 <= NVM_MAX_LEVEL) {

        for(auto const &nodeToDeleteDown : downInputs)
          nodesToDelete.push_back(nodeToDeleteDown->nvmSingleNode);
        //        mutex_.Lock();
        pmSkiplistDramEntrancePtrArray[level + 1]->ReplaceNodes(newNvmNodes,
                                                                nodesToDelete);
        //        mutex_.Unlock();
      }
      if(level <= NVM_MAX_LEVEL) {
        nodesToDelete.clear();
        for(auto const &nodeToDeleteUp : upperInputs)
          nodesToDelete.push_back(nodeToDeleteUp->nvmSingleNode);
        if(level == 0)
          std::sort(
            nodesToDelete.begin(), nodesToDelete.end(),
            [](pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *a,
               pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *b) {
              return a->fileNum < b->fileNum;
            });
        //        mutex_.Lock();
        pmSkiplistDramEntrancePtrArray[level]->ReplaceNodes({}, nodesToDelete);
        //        mutex_.Unlock();
      }
    }

    for(auto *i : lruNodeToAdd) {
      lru_.InsertNode(i);
    }


    if(status.ok()) {
      for(int i = 0; i <= 1; ++i)
        for(int j = 0; j < compact->compaction->NumInputFiles(i); ++j) {
          auto *f = compact->compaction->input(i, j);
          if((compact->compaction->level() > 0 || i == 1) &&
             f->whereIsFile == FileMetaData::K_NVM) {
            assert(f->nvmSingleNode);
            assert(f->nvmSingleNode->lruNode);
            auto lruNode = f->nvmSingleNode->lruNode;
            lru_.DeleteNodeWithoutFreeDramLruNode(lruNode);
            delete lruNode;
            f->nvmSingleNode->lruNode = nullptr;
          }
        }
      status = InstallCompactionResults_(
        compact, mainOutputTarget == FileMetaData::K_NVM,
        compact->compaction->level() >= NVM_MAX_LEVEL);
      assert(versions_->current()->t.thisTime);
    }
    if(!status.ok())
      RecordBackgroundError_(status);
    return status;
  }

  auto DBImpl::DoNvmMoveCompactionWork_(CompactionState *compact) -> Status {

    //    std::stringstream ss;
    //    ss << compact->compaction->level() << std::endl;
    //    std::cout << ss.str() << std::flush;

    std::vector<pmCache::DramLruNode *> lruNodeToAdd;
#ifndef NDEBUG
//    if(compact->compaction->inputs[0][0]->number == 55) {
//      std::cout << "DoNvmMoveCompactionWork_ 55" << std::endl;
//    }
#endif

    assert(compact->compaction->NumInputFiles(0) == 1 &&
           compact->compaction->NumInputFiles(1) == 0);
    assert(compact->compaction->input(0, 0)->whereIsFile ==
           FileMetaData::K_NVM);
    assert(versions_->NumLevelFiles(compact->compaction->level()) >
           0); 
    assert(compact->builder == nullptr);
    assert(compact->outfile == nullptr);

    // Release mutex while we're actually doing the compaction work

    mutex_.Unlock();
    Status status;
    std::vector<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *>
      newNvmNodes;
    {
      //      pmem::obj::flat_transaction::manual tx(pmemPool_);
      FileMetaData *compactInput = nullptr;

      if(compact->pmSkiplistNvmSingleNodeBuilder == nullptr) {
        assert(compact != nullptr);
        assert(compact->builder == nullptr);
        uint64_t fileNumber;
        {
          mutex_.Lock();
          fileNumber = versions_->NewFileNumber();
          pendingOutputs_.insert(fileNumber);
          CompactionState::Output out;
          out.number = fileNumber;
          out.smallest.Clear();
          out.largest.Clear();
          compact->outputs.push_back(out);
          mutex_.Unlock();
        }
        compact->pmSkiplistNvmSingleNodeBuilder = std::make_unique<
          pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNodeBuilder>();

        compactInput = compact->compaction->input(0, 0);
        compact->pmSkiplistNvmSingleNodeBuilder->PushNodeFromAnotherNvTable(
          compactInput);
      }
      if(status.ok() && compact->pmSkiplistNvmSingleNodeBuilder != nullptr) {
        parallelListCompaction::FinishCompactionOutputNvmFile(
          this, compact, newNvmNodes, lru_, &lruNodeToAdd);
        auto level = compact->compaction->level();
        //        mutex_.Lock();

        //        mutex_.Unlock();
      }
      //      pmem::obj::flat_transaction::commit();
    }

    mutex_.Lock();
    auto level = compact->compaction->level();
    pmSkiplistDramEntrancePtrArray[level + 1]->ReplaceNodes(newNvmNodes, {});
    pmSkiplistDramEntrancePtrArray[level]->ReplaceNodes(
      {}, {compact->compaction->inputs[0][0]->nvmSingleNode});
    for(auto *i : lruNodeToAdd) {
      lru_.InsertNode(i);
    }

    if(status.ok()) {
      for(int i = 0; i <= 1; ++i) {
        for(int j = 0; j < compact->compaction->NumInputFiles(i); ++j) {
          auto f = compact->compaction->input(i, j);
          if(compact->compaction->level() > 0 &&
             f->whereIsFile == FileMetaData::K_NVM) {
            assert(f->nvmSingleNode);
            assert(f->nvmSingleNode->lruNode);

            lru_.DeleteNodeWithoutFreeDramLruNode(f->nvmSingleNode->lruNode);
            delete f->nvmSingleNode->lruNode;
            f->nvmSingleNode->lruNode = nullptr;
          }
        }
      }
      status = InstallCompactionResults_(compact, true, false);
      assert(versions_->current()->t.thisTime);
    }
    if(!status.ok())
      RecordBackgroundError_(status);
    return status;
  }

  namespace {
    struct IterState {
      port::Mutex *const mu;
      Version *const version GUARDED_BY(mu);
      pmCache::ConcurrentMemtable *const cMem;
      pmCache::ConcurrentMemtable *const cImm;

      IterState(
        port::Mutex *mutex, Version *version,
        pmCache::ConcurrentMemtable // NOLINT(bugprone-easily-swappable-parameters)
          *cMem,
        pmCache::ConcurrentMemtable *cImm)
          : mu(mutex), version(version), cMem(cMem), cImm(cImm) {}
    };

    void CleanupIteratorState(void *arg1, void *arg2) {
      auto *state = reinterpret_cast<IterState *>(arg1);
      state->mu->Lock();
      //      state->mem->Unref();
      state->cMem->Unref();
      //      if(state->imm != nullptr)
      //        state->imm->Unref();
      if(state->cImm != nullptr)
        state->cImm->Unref();
      state->version->Unref();
      state->mu->Unlock();
      delete state;
    }
  } // anonymous namespace

  auto DBImpl::NewInternalIterator(const ReadOptions &options,
                                   SequenceNumber *latestSnapshot,
                                   uint32_t *seed) -> Iterator * {
    mutex_.Lock();
    *latestSnapshot = versions_->LastSequence();
    // Collect together all needed child iterators
    std::vector<Iterator *> list;
    //    list.push_back(mem_->NewIterator());
    list.push_back(cMem_->NewIterator());
    //    mem_->Ref();
    cMem_->Ref();
    //    if(imm_ != nullptr) {
    //      list.push_back(imm_->NewIterator());
    //      imm_->Ref();
    //    }
    if(cImm_ != nullptr) {
      list.push_back(cImm_->NewIterator());
      cImm_->Ref();
    }
    versions_->current()->AddIterators(options, &list);
    Iterator *internalIter =
      NewMergingIterator(&internalComparator_, &list[0], (int)list.size());
    versions_->current()->Ref();
    auto *cleanup = new IterState(&mutex_, versions_->current(), cMem_, cImm_);
    //    auto *cleanup = new IterState(&mutex_, cMem_, imm_,
    //    versions_->current());

    internalIter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);
    *seed = ++seed_;
    mutex_.Unlock();
    return internalIter;
  }

  auto DBImpl::TEST_NewInternalIterator() -> Iterator * {
    SequenceNumber ignored;
    uint32_t ignoredSeed;
    return NewInternalIterator(ReadOptions(), &ignored, &ignoredSeed);
  }

  auto DBImpl::TEST_MaxNextLevelOverlappingBytes() -> int64_t {
    MutexLock l(&mutex_);
    return versions_->MaxNextLevelOverlappingBytes();
  }
  static size_t veryLittle = 1024L * 36;

  thread_local int getcnt = 2;
#define TSC_KHZ 2194843
#define TSC2NS(tsc) (((tsc)*1000000) / TSC_KHZ)
  auto DBImpl::Get(const ReadOptions &options, const Slice &key,
                   std::string *value) -> Status {

    //    auto keyString = key.ToString();
    //    std::cout << keyString + "\n" << std::flush;
    //
    //    std::stringstream ss;
    //    ss << "X" << std::endl;
    //    std::cout << ss.str() << std::flush;

    if(!_pobj_cached_pool.pop)
      _pobj_cached_pool.pop = pmemPool_.handle();

    Status s;

    //    auto t1 = std::chrono::high_resolution_clock::now();

    MutexLock l(&mutex_); 

    //    auto t2 = std::chrono::high_resolution_clock::now();
    //    std::cout << "mt " +
    //                   std::to_string(
    //                     std::chrono::duration_cast<std::chrono::nanoseconds>(t2
    //                     -
    //                                                                          t1)
    //                       .count()) +
    //                   "\n"
    //              << std::flush;

    SequenceNumber snapshot; 
    bool readNewestVersion = false;

    if(options.snapshot != nullptr)
      snapshot =
        dynamic_cast<const SnapshotImpl *>(options.snapshot)->sequence_number();
    else {
      snapshot = versions_->LastSequence();
      readNewestVersion = true;
    }

    pmCache::ConcurrentMemtable *cMem = cMem_;
    pmCache::ConcurrentMemtable *cImm = cImm_;
    Version *current = versions_->current();
    cMem->Ref(); 
    if(cImm != nullptr)
      cImm->Ref();
    current->Ref();

    bool haveStatUpdate = false;
    Version::GetStats getStats{};

    // Unlock while reading from files and memtables
    {
      mutex_.Unlock();
      // First look in the memtable, then in the immutable memtable (if any).
      LookupKey lookupKey(key, snapshot);

      //      auto t3 = std::chrono::high_resolution_clock::now();

      if((cMem->Get(lookupKey, value, &s)) || // mem
         (cImm != nullptr &&                  // imm
          cImm->Get(lookupKey, value, &s))) { // imm

        getHitMemCnt_++;

        //        std::stringstream ss;
        //        ss << key.ToString() << " "
        //           << "D"
        //           << "\n";
        //        std::cout << ss.str() << std::flush;
        //        auto t4 = std::chrono::high_resolution_clock::now();
        //        std::cout << "dt " +
        //                       std::to_string(
        //                         std::chrono::duration_cast<std::chrono::nanoseconds>(
        //                           t4 - t3)
        //                           .count()) +
        //                       "\n"
        //                  << std::flush;

        //  Done
      } else {

        //        auto t4 = std::chrono::high_resolution_clock::now();
        //        std::cout << "dt " +
        //                       std::to_string(
        //                         std::chrono::duration_cast<std::chrono::nanoseconds>(
        //                           t4 - t3)
        //                           .count()) +
        //                       "\n"
        //                  << std::flush;

        assert(current->t.thisTime);
        s = current->Get(options, lookupKey, value, &getStats,
                         readNewestVersion, &getHitCnt_[0]);
        haveStatUpdate = true;
      }
      mutex_.Lock();
    }
    

    bool shouldPromote = false;
    
    if(getStats.hotKeyFile) {
#ifndef NDEBUG
      //      if(getStats.hotKeyFileLevel == 6) {
//        std::cout << "";
//      }
#endif
      auto buffer = getStats.hotKeyFile->bufferForEachFile.get();
      {
        std::shared_lock<std::shared_mutex> sharedLock(buffer->mutex,
                                                       std::try_to_lock);
        if(sharedLock.owns_lock()) {
          auto bufferSize = buffer->GetSize();
          if((getStats.hotKeyFile->fileSize > veryLittle) &&
             ((double)bufferSize >
              MAX_KEY_PRE_BUFFER *
                (double)((double)getStats.hotKeyFile->fileSize / 1024))) {
            shouldPromote = true;
          } else {
            shouldPromote = getStats.hotNessOfKeyFromBuffer >
                            hotKeyPromotionThreshold[getStats.hotKeyFileLevel];
          }
          //          auto xx = MAX_KEY_PRE_BUFFER;
          //          std::cout<<"";
#ifndef NDEBUG
          //          if(shouldPromote) {
//            std::cout << "";
//          }
#endif
        }
      }
    }


    if(haveStatUpdate && current->UpdateStats(getStats, shouldPromote))
      MaybeScheduleCompactionOrPromotion_();

    cMem->Unref();
    if(cImm != nullptr)
      cImm->Unref();

    current->Unref();

    if(getcnt++ % 10 == 0) {
      getcnt = 1;
      std::stringstream ss;
      unsigned int dummy;
      uint64_t current;
      current = __rdtscp(&dummy);
      ss << current << " " << getHitMemCnt_ << " " << getHitCnt_[0] << " "
         << getHitCnt_[1] << " " << getHitCnt_[2] << " " << getHitCnt_[3] << " "
         << getHitCnt_[4] << " " << getHitCnt_[5] << " " << getHitCnt_[6] << " "
         << 0 << std::endl;
      std::cout << ss.str() << std::flush;
    }

    return s;
  }

  auto DBImpl::NewIterator(const ReadOptions &options) -> Iterator * {
    if(!_pobj_cached_pool.pop)
      _pobj_cached_pool.pop = pmemPool_.handle();
    SequenceNumber latestSnapshot;
    uint32_t seed;
    Iterator *iter = NewInternalIterator(options, &latestSnapshot, &seed);
    return NewDBIterator(
      this, UserComparator_(), iter,
      (options.snapshot != nullptr
         ? dynamic_cast<const SnapshotImpl *>(options.snapshot)
             ->sequence_number()
         : latestSnapshot),
      seed);
  }

  void DBImpl::RecordReadSample(Slice key) {
    MutexLock l(&mutex_);
    if(versions_->current()->RecordReadSample(key)) {
      MaybeScheduleCompactionOrPromotion_();
    }
  }

  auto DBImpl::GetSnapshot() -> const Snapshot * {
    MutexLock l(&mutex_);
    return snapshots_.New(versions_->LastSequence());
  }

  void DBImpl::ReleaseSnapshot(const Snapshot *snapshot) {
    MutexLock l(&mutex_);
    snapshots_.Delete(dynamic_cast<const SnapshotImpl *>(snapshot));
  }

  // Convenience methods
  auto DBImpl::Put(const WriteOptions &o, const Slice &key, const Slice &val)
    -> Status {

    return ParallelWrite(o, key, val, kTypeValue);

    //        return DB::Put(o, key, val);
  }

  auto DBImpl::Delete(const WriteOptions &options, const Slice &key) -> Status {

    return ParallelWrite(options, key, Slice(), kTypeDeletion);
    //    return DB::Delete(options, key);
  }


  auto
  DBImpl::Write(const WriteOptions &options, WriteBatch *updates) -> Status {

    if(_pobj_cached_pool.pop == nullptr)
      _pobj_cached_pool.pop = pmemPool_.handle();

    Writer w(&mutex_); 
    w.batch = updates;
    w.sync = options.sync; 
    w.done = false;

    MutexLock l(&mutex_);
    
    writers_.push_back(&w);

    while(!w.done && &w != writers_.front())
      w.cv.Wait(); 


    if(w.done) 
      return w.status;



    // May temporarily unlock and wait.


    Status status = MakeRoomForWrite_(updates == nullptr);
    //    assert(status.ok());
    // last_sequence

    uint64_t lastSequence = versions_->LastSequence();
    Writer *lastWriter =
      &w; 

    if(status.ok() && updates != nullptr) { 
      WriteBatch *writeBatch = BuildBatchGroup_(&lastWriter);

      WriteBatchInternal::SetSequence(writeBatch, lastSequence + 1);
      lastSequence += WriteBatchInternal::Count(writeBatch);

      // Add to log and apply to memtable.  We can release the lock
      // during this phase since &w is currently responsible for logging
      // and protects against concurrent loggers and concurrent writes
      // into mem_.

      {
        mutex_.Unlock();


        std::vector<uint64_t> locates;
        locates.reserve(*((uint32_t *)(Slice(writeBatch->rep_).data() + 8)));

        assert(pmemobj_tx_stage() == TX_STAGE_NONE);
        {
          //          pmem::obj::flat_transaction::manual tx{pmemPool_};
          status =
            pmCachePoolRoot_->GetWalForMem()->AddRecord(writeBatch, locates);
          //          pmemobj_tx_commit();
        }
        bool syncError = false;
        if(!status.ok())
          syncError = true;
        if(status.ok()) 
  
          (void)1;
        //          status = WriteBatchInternal::InsertInto(writeBatch, mem_,
        //          locates);
        mutex_.Lock();
        if(syncError) {
          // The state of the log file is indeterminate: the log record we
          // just added may or may not show up when the DB is re-opened.
          // So we force the DB into a mode where all future writes fail.
          RecordBackgroundError_(status);
        }
      }
      if(writeBatch == tmpBatch_)
        tmpBatch_->Clear();

      versions_->SetLastSequence(lastSequence);
    }

    while(true) {
      Writer *ready = writers_.front();
      writers_.pop_front();
      if(ready != &w) {
        ready->status = status;
        ready->done = true;
        ready->cv.Signal();
      }
      if(ready == lastWriter)
        break;
    }

    // Notify new head of write queue
    if(!writers_.empty())
      writers_.front()->cv.Signal();

    return status;
  }

  // REQUIRES: Writer list must be non-empty
  // REQUIRES: First writer must have a non-null batch
  auto DBImpl::BuildBatchGroup_(Writer **lastWriter) -> WriteBatch * {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Writer *first = writers_.front();
    WriteBatch *result = first->batch;
    assert(result != nullptr);

    size_t size = WriteBatchInternal::ByteSize(
      first->batch); 

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth, so we do not slow
    // down the small write too much.
    //
   
    size_t maxSize = 1 << 20;
    if(size <= (256 << 10))
      maxSize = size + (256 << 10);

    *lastWriter =
      first; 
    auto iter = writers_.begin();
   
    ++iter; // Advance past "first"
    for(; iter != writers_.end(); ++iter) {
      Writer *w = *iter;
      if(w->sync &&
         !first->sync) { 
        break;
      }

      if(w->batch != nullptr) {
        size += WriteBatchInternal::ByteSize(w->batch);
        if(size > maxSize) {
          // Do not make batch too big
          break;
        }



        // Append to *result
        if(result == first->batch) {
          // Switch to temporary batch instead of disturbing caller's
          // batch
          result = tmpBatch_;
          assert(WriteBatchInternal::Count(result) == 0);
         
          WriteBatchInternal::Append(result, first->batch);
        }
        WriteBatchInternal::Append(result, w->batch);
      }
      *lastWriter = w; 
    }
    return result;
  }

#pragma clang diagnostic push
  // #pragma ide diagnostic ignored "UnreachableCode"

  // #pragma ide diagnostic ignored "bugprone-branch-clone"

  // REQUIRES: mutex_ is held
  // REQUIRES: this thread is currently at the front of the writer queue
  //
 
  auto DBImpl::MakeRoomForWrite_(bool force) -> Status {
    mutex_.AssertHeld();
    //    LruNvmCache();
    //    assert(!writers_.empty());
    bool allowDelay = !force; 
    Status s;
    while(true) {
      if(!bgError_.ok()) {
        // Yield previous error
        s = bgError_;
        break;
      } else if(allowDelay && (versions_->NumLevelFiles(0) >=
                               config::kL0_SlowdownWritesTrigger)) {


        // We are getting close to hitting a hard limit on the number of
        // L0 files.  Rather than delaying a single write by several
        // seconds when we hit the hard limit, start delaying each
        // individual write by 1ms to reduce latency variance.  Also,
        // this delay hands over some CPU to the compaction thread in
        // case it is sharing the same core as the writer.
        //

        mutex_.Unlock();
        //        slowCnt_ += 1;
        uint64_t delayTime = 1000;
        env_->SleepForMicroseconds((int)delayTime);
        allowDelay = false; // Do not delay a single write more than once
        mutex_.Lock();
      } else if(!force && (cMem_->ApproximateMemoryUsage() <=
                           options_.write_buffer_size)) {

        // There is room in current memtable
        break;
      } else if(cImm_ != nullptr) { // NOLINT(bugprone-branch-clone)

        // We have filled up the current memtable, but the previous one is
        // still being compacted, so we wait.
        //            Log(options_.info_log, "Current memtable full;
        //            waiting...\n");


        auto beginStallTime = std::chrono::high_resolution_clock ::now();
        backgroundWorkFinishedSignal_.Wait(); 
        
        auto endStallTime = std::chrono::high_resolution_clock ::now();
        //        delayNanoTimes_ +=
        //        std::chrono::duration_cast<std::chrono::nanoseconds>(
        //                             endStallTime - beginStallTime)
        //                             .count();

      } else if(versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {

        // There are too many level-0 files.
        //            Log(options_.info_log, "Too many L0 files;
        //            waiting...\n");


        auto beginStallTime = std::chrono::high_resolution_clock ::now();
        backgroundWorkFinishedSignal_.Wait();
        auto endStallTime = std::chrono::high_resolution_clock ::now();
        //        delayNanoTimes_ +=
        //        std::chrono::duration_cast<std::chrono::nanoseconds>(
        //                             endStallTime - beginStallTime)
        //                             .count();
      } else {
        
        // Attempt to switch to a new memtable and trigger
        // compaction of old
        assert(versions_->PrevLogNumber() == 0);
        cImm_ = cMem_;
        {
          assert(pmemobj_tx_stage() == TX_STAGE_NONE);
          //          pmem::obj::flat_transaction::manual tx(pmemPool_);
          pmCachePoolRoot_->TurnLogMemToImmAndCreateNewMem();
          //          pmemobj_tx_commit();
        }
        hasImm_.store(
          true,
          std::memory_order_release); 
        //        mem_ = new MemTable(internalComparator_); //

        cMem_ = new pmCache::ConcurrentMemtable(internalComparator_);
        //        mem_->Ref(); 
        cMem_->Ref();
        force = false; // Do not force another compaction if you have room

        MaybeScheduleCompactionOrPromotion_();
      }
    }
    return s;
  }

#pragma clang diagnostic pop

  auto DBImpl::GetProperty(const Slice &property, std::string *value) -> bool {
    value->clear();

    MutexLock l(&mutex_);
    Slice in = property;
    Slice prefix("leveldb.");
    if(!in.starts_with(prefix))
      return false;
    in.remove_prefix(prefix.size());

    if(in.starts_with(Slice{"num-files-at-level"})) {
      in.remove_prefix(strlen("num-files-at-level"));
      uint64_t level;
      bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
      if(!ok || level >= config::kNumLevels) {
        return false;
      } else {
        std::array<char, 100> buf{};
        std::snprintf(buf.data(), sizeof(buf), "%d",
                      versions_->NumLevelFiles(static_cast<int>(level)));
        *value = buf.data();
        return true;
      }
    } else if(in == Slice{"stats"}) {
      std::array<char, 200> buf{};
      std::snprintf(buf.data(), sizeof(buf),
                    "                               Compactions\n"
                    "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                    "--------------------------------------------------\n");
      value->append(buf.data());
      for(int level = 0; level < config::kNumLevels; level++) {
        int files = versions_->NumLevelFiles(level);
        if(stats_[level].micros > 0 || files > 0) {
          std::snprintf(buf.data(), sizeof(buf),
                        "%3d %8d %8.0f %9.0f %8.0f %9.0f\n", level, files,
                        (double)versions_->NumLevelBytes(level) / 1048576.0,
                        (double)stats_[level].micros / 1e6,
                        (double)stats_[level].bytes_read / 1048576.0,
                        (double)stats_[level].bytes_written / 1048576.0);
          value->append(buf.data());
        }
      }
      return true;
    } else if(in == Slice{"sstables"}) {
      *value = versions_->current()->DebugString();
      return true;
    } else if(in == Slice{"approximate-memory-usage"}) {
      size_t totalUsage = options_.block_cache->TotalCharge();
      if(cMem_)
        totalUsage += cMem_->ApproximateMemoryUsage();
      if(cImm_)
        totalUsage += cImm_->ApproximateMemoryUsage();
      std::array<char, 50> buf{};
      std::snprintf(buf.data(), sizeof(buf), "%llu",
                    static_cast<unsigned long long>(totalUsage));
      value->append(buf.data());
      return true;
    }

    return false;
  }

  void DBImpl::GetApproximateSizes(const Range *range, int n, uint64_t *sizes) {
    MutexLock l(&mutex_);
    Version *v = versions_->current();
    v->Ref();

    for(int i = 0; i < n; i++) {
      // Convert user_key into a corresponding internal key.
      InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
      uint64_t start = versions_->ApproximateOffsetOf(v, k1);
      uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
      sizes[i] = (limit >= start ? limit - start : 0);
    }

    v->Unref();
  }

  void DBImpl::TestPrint() {
    MutexLock l(&mutex_);
    auto v = versions_->current();
    v->Ref();

    for(int i = 0; i <= NVM_MAX_LEVEL; ++i) {
      std::cout << "i = " << i << std::endl << std::flush;
      auto dramEntrance = versions_->pmSkiplistDramEntrance[i];
      {
        auto &dummyHead = dramEntrance->dummyHead;
        auto &dummyTail = dramEntrance->dummyTail;
        auto currentNode = dummyHead.GetNext(0);
        while(currentNode != &dummyTail) {
          auto fanOutNodes = currentNode->GetFanOutNodes();
          auto fanOutArray = fanOutNodes->fanOutNodes.get();
          auto fanOutArraySize = fanOutNodes->fanOutNodesCount;
          for(uint32_t k = 0; k < fanOutArraySize; ++k)
            std::cout << fanOutArray[k].nvmNode->fileNum << " " << std::flush;
          currentNode = currentNode->GetNext(0);
        }
      }
      std::cout << std::endl << std::flush;

      auto nvmDummyHead = versions_->pmSkiplistDramEntrance[i]
                            ->dummyHead.GetFanOutNodes()
                            ->fanOutNodes[0]
                            .nvmNode;
      auto nvmDummyTail = versions_->pmSkiplistDramEntrance[i]
                            ->dummyTail.GetFanOutNodes()
                            ->fanOutNodes[0]
                            .nvmNode;
      auto currentNode = nvmDummyHead->nextNode.GetVPtr();
      while(currentNode != nvmDummyTail) {
        std::cout << currentNode->fileNum << " " << std::flush;
        currentNode = currentNode->nextNode.GetVPtr();
      }
      std::cout << std::endl << std::flush;
    }
    v->Unref();
  }
  static double x1 = 2;
  [[maybe_unused]] static std::array<int, 22> delayTime = {
    0,                   // 0 NOLINT(modernize-avoid-c-arrays)
    0,                   // 1
    0,                   // 2
    0,                   // 3
    0,                   // 4
    0,                   // 5
    0,                   // 6
    0,                   // 7
    int(500 * x1 * 1),   // 8
    int(2875 * x1 * 2),  // 9
    int(5250 * x1 * 3),  // 10
    int(7625 * x1 * 4),  // 11
    int(10000 * x1 * 5), // 12
    5000,                // 13
    6000,                // 14
    6000,                // 15
    6000,                // 16
    6000,                // 17
    6000,                // 18
    6000,                // 19
    6000,                // 20
    6000};               // 21

  auto DBImpl::ParallelWrite(const WriteOptions &options, const Slice &key,
                             const Slice &value, ValueType type) -> Status {

    //    assert(options.sync == true);

#ifndef NDEBUG
    //    auto s = key.ToString();
//    std::cout << s + "\n" << std::flush;
#endif

    //    auto size1 = this->lru_.totalSize.load(std::memory_order_relaxed);
    //    long long tt1 =
    //      (signed long long)this->lru_.maxSize - (signed long long)size1;
    //    int x11 = 0;

    //    if(tt1 <= 0) {
    //      env_->SleepForMicroseconds(250 - x11);
    //      //      std::cout << "!";
    //    } else {
    //      long long left1 = (long long)options_.write_buffer_size - tt1;
    //      if(left1 > 0) {
    //        env_->SleepForMicroseconds(
    //          (int)((250 - x11) *
    //                ((double)left1 / (double)options_.write_buffer_size)));
    //        //        std::cout << "@";
    //      }
    //    }

    //    if(tt1 <= (int64_t)((double)options_.write_buffer_size * 9)) {
    //      if(tt1 <= (int64_t)((double)options_.write_buffer_size * 0.5)) {
    //        env_->SleepForMicroseconds(250 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 1)) {
    //        env_->SleepForMicroseconds(200 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 2)) {
    //        env_->SleepForMicroseconds(180 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 3)) {
    //        env_->SleepForMicroseconds(160 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 4)) {
    //        env_->SleepForMicroseconds(150 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 5))
    //        env_->SleepForMicroseconds(140 - x11);
    //      else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 6)) {
    //        env_->SleepForMicroseconds(130 - x11);
    //      } else if(tt1 <= (int64_t)((double)options_.write_buffer_size * 7))
    //        env_->SleepForMicroseconds(125 - x11);
    //    }

    if(!_pobj_cached_pool.pop)
      _pobj_cached_pool.pop = pmemPool_.handle();

    if(isLeader_.load(std::memory_order_relaxed)) {
      std::unique_lock<std::mutex> shouldWait{nonLeaderWait_};
      shouldWaitCv_.wait(shouldWait, [this] { return !shouldWait_; });
    }

    bool isFull = false;
    {
      std::shared_lock<std::shared_mutex> lock{mutexForParallelWrite_};

      //      auto size = versions_->current()->files[0].size();
      //      if(size > config::kL0_SlowdownWritesTrigger)
      //        std::this_thread::sleep_for(std::chrono::microseconds(1000));

      uint64_t currentSeq = versions_->lastSequenceAtomic_.fetch_add(1) + 1,
               newNodeOff;
      pmCachePoolRoot_->GetWalForMem()->AddRecordParallel(
        key, value, currentSeq, type, newNodeOff, pmemPool_);
      cMem_->AddWithPmOff(currentSeq, type, key, value, newNodeOff);
      isFull = cMem_->ApproximateMemoryUsage() > options_.write_buffer_size;
    }


    bool expectedLeaderFlag = false;
    if(isFull && isLeader_.compare_exchange_strong(expectedLeaderFlag, true)) {

      {
        std::unique_lock<std::mutex> shouldWait{nonLeaderWait_};
        shouldWait_ = true;
      }

      {
        std::unique_lock<std::shared_mutex> lock{mutexForParallelWrite_};
        MutexLock l(&mutex_);
        Status status = MakeRoomForWrite_(false);
        if(!status.ok()) {
          std::cerr << status.ToString() + "\n" << std::flush;
        }
        assert(status.ok());
      }

      isLeader_.store(false, std::memory_order_release);

      {
        std::unique_lock<std::mutex> shouldWait{nonLeaderWait_};
        shouldWait_ = false;
      }

      shouldWaitCv_.notify_all();
    }
    return Status{};
  }

  // Default implementations of convenience methods that subclasses of DB
  // can call if they wish
  auto DB::Put(const WriteOptions &opt, const Slice &key, const Slice &value)
    -> Status {
    WriteBatch batch;
    batch.Put(
      key,
      value); 
    return Write(opt, &batch);
  }

  auto DB::Delete(const WriteOptions &opt, const Slice &key) -> Status {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
  }

  DB::~DB() = default;

  auto DB::Open(const Options &options, const std::string &dbname, DB **dbPtr)
    -> Status {
    *dbPtr = nullptr;

    auto *impl = new DBImpl(options, dbname);
    impl->mutex_.Lock();
    VersionEdit edit;
    // Recover handles create_if_missing, error_if_exists
    bool saveManifest = false;
    Status s = impl->Recover_(&edit, &saveManifest);

    if(s.ok() && impl->cMem_ == nullptr) {
      // Create new log and a corresponding memtable.
      //      auto const newLogNumber =
      impl->versions_->NewFileNumber();
      if(s.ok()) {
        impl->cMem_ =
          new pmCache::ConcurrentMemtable(impl->internalComparator_);
        impl->cMem_->Ref();
      }
    }


    if(s.ok() && saveManifest) {
      edit.SetPrevLogNumber(0); // No older logs needed after recovery.
      pmCache::GlobalTimestamp::TimestampToken time{};
      impl->timeStamp->ReferenceTimestamp(time);
      s = impl->versions_->LogAndApply(&edit, &impl->mutex_, &time,
                                       impl->timeStamp);
    }
    //    pmem::obj::flat_transaction::run(impl->pmemPool_, [&] {
    auto cacheRoot = impl->pmCachePoolRoot_;
    if(cacheRoot->GetWalForImm())
      cacheRoot->ClearImmWal(impl->pmemPool_);
    if(cacheRoot->GetWalForMem()) {
      cacheRoot->TurnLogMemToImmAndCreateNewMem();
      cacheRoot->ClearImmWal(impl->pmemPool_);
    }
    cacheRoot->SetMemWal(pmCache::PmWalBuilder::GenerateWal());
    //    });

    auto v = impl->versions_->current();


    for(int i = config::kNumLevels - 1; i >= 1; --i) {
      auto &fileInOneLevel = v->files[i];

      //      for(auto &file : fileInOneLevel) {
      //        if(file->whereIsFile == FileMetaData::K_NVM) {
      //          auto nvmSingleNode = file->nvmSingleNode;
      //          auto newNode = new pmCache::DramLruNode();
      //          nvmSingleNode->lruNode = newNode;
      //          newNode->whichLevel = i;
      //          newNode->nodeSize = file->fileSize;
      //          newNode->nvmNode = nvmSingleNode;
      //          impl->lru_.InsertNode(newNode);
      //        }
      //      }

      std::vector<FileMetaData *> fileInOneLevelShuffled;
      fileInOneLevelShuffled.reserve(fileInOneLevel.size());
      for(auto &file : fileInOneLevel)
        fileInOneLevelShuffled.push_back(file);
      std::shuffle(fileInOneLevelShuffled.begin(), fileInOneLevelShuffled.end(),
                   std::mt19937(std::random_device()()));
      for(auto &file : fileInOneLevelShuffled) {
        if(file->whereIsFile == FileMetaData::K_NVM) {
          auto nvmSingleNode = file->nvmSingleNode;
          auto newNode = new pmCache::DramLruNode();
          nvmSingleNode->lruNode = newNode;
          newNode->whichLevel = i;
          newNode->nodeSize = file->fileSize;
          //          newNode->nvmNode = nvmSingleNode;
          newNode->fileNum = file->number;
          impl->lru_.InsertNode(newNode);
        }
      }
    }

    if(s.ok()) {
      impl->RemoveObsoleteFiles_();
      impl->MaybeScheduleCompactionOrPromotion_();
    }
    impl->mutex_.Unlock();
    if(s.ok()) {
      assert(impl->cMem_ != nullptr);
      *dbPtr = impl;
    } else
      delete impl;

    
    return s;
  }

  Snapshot::~Snapshot() = default;

  auto DestroyDb(const std::string &dbname, const Options &options) -> Status {
    Env *env = options.env;
    std::vector<std::string> filenames;
    Status result = env->GetChildren(dbname, &filenames);
    if(!result.ok()) {
      // Ignore error in case directory does not exist
      return Status::OK();
    }

    FileLock *lock;
    const std::string lockName = LockFileName(dbname);
    result = env->LockFile(lockName, &lock);
    if(result.ok()) {
      uint64_t number;
      FileType type;
      auto dbNameWithSlash = dbname + "/";
      for(auto &filename : filenames) {
        if(ParseFileName(filename, &number, &type) &&
           type != kDBLockFile) { // Lock file will be deleted at end
          Status del = env->RemoveFile(dbNameWithSlash + filename);
          if(result.ok() && !del.ok())
            result = del;
        }
      }
      env->UnlockFile(lock); // Ignore error since state is already gone
      env->RemoveFile(lockName);
      env->RemoveDir(dbname); // Ignore error in case dir contains other files
    }
    return result;
  }

  DBImpl::Writer::Writer(port::Mutex *mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}
} // namespace leveldb
