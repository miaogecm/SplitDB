// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "../gsl/include/gsl/gsl"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "dbWithPmCache/count_min_sketch/count_min_sketch.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "pmCache/dbWithPmCache/dram_lru/dram_lru.h"
#include "pmCache/dbWithPmCache/help/dramSkipList/concurrentMemtable.h"
#include "pmCache/dbWithPmCache/pm_cache_pool_impl.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <atomic>
#include <deque>
#include <set>
#include <shared_mutex>
#include <string>

namespace leveldb {

  class MemTable;

  class TableCache;

  class Version;

  class VersionEdit;

  class VersionSet;

  class DBImpl : public DB {
   public:
    DBImpl(const Options &rawOptions, const std::string &dbname);

    DBImpl(const DBImpl &) = delete;

    auto operator=(const DBImpl &) -> DBImpl & = delete;

    ~DBImpl() override;

    void LruNvmCache();

    // Implementations of the DB interface

    auto Put(const WriteOptions &, const Slice &key, const Slice &value)
      -> Status override;

    auto Delete(const WriteOptions &, const Slice &key) -> Status override;

    auto
    Write(const WriteOptions &options, WriteBatch *updates) -> Status override;

    auto ParallelWrite(const WriteOptions &options, const Slice &key,
                       const Slice &value, ValueType type) -> Status;

    auto Get(const ReadOptions &options, const Slice &key, std::string *value)
      -> Status override;

    auto NewIterator(const ReadOptions &) -> Iterator * override;

    auto GetSnapshot() -> const Snapshot * override;

    void ReleaseSnapshot(const Snapshot *snapshot) override;

    auto
    GetProperty(const Slice &property, std::string *value) -> bool override;

    void
    GetApproximateSizes(const Range *range, int n, uint64_t *sizes) override;

    void CompactRange(const Slice *begin, const Slice *end) override;

    // Extra methods (for testing) that are not in the public DB interface

    // Compact any files in the named level that overlap [*begin,*end]
    void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

    // Force current memtable contents to be compacted.
    auto TEST_CompactMemTable() -> Status;

    // Return an internal iterator over the current state of the database.
    // The keys of this iterator are internal keys (see format.h).
    // The returned iterator should be deleted when no longer needed.
    auto TEST_NewInternalIterator() -> Iterator *;

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    auto TEST_MaxNextLevelOverlappingBytes() -> int64_t;

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.
    void RecordReadSample(Slice key);

    pmCache::GlobalTimestamp *timeStamp;

    struct CompactionState;

    auto WriteLevel0TableWithCertainMemtable(pmCache::PmWal *immWal,
                                             VersionEdit *edit, MemTable *mem)
      -> Status;

    auto OpenCompactionOutputFileForReversedHotKey(CompactionState *compact)
      -> Status;

    auto OpenCompactionOutputFile(CompactionState *compact) -> Status;

    static auto
    FinishCompactionOutputReservedFile(CompactionState *compact) -> Status;

    static auto FinishCompactionOutputFile(CompactionState *compact) -> Status;

   private:
    pmem::obj::pool<leveldb::pmCache::PmCachePoolRoot> pmemPool_;
    pmCache::PmCachePoolRoot *pmCachePoolRoot_ = nullptr;
    pmCache::ThreadPool *pmCacheBgGcWorkThreadPool_ = nullptr;
    pmCache::ThreadPool *pmCacheBgSubCompactionThreadPool_ = nullptr;
    pmCache::ThreadPool *pmCacheBgCompactionThreadPool_ = nullptr;

    /////////////////////////////////////new1
   public:
    std::array<pmCache::skiplistWithFanOut::PmSkiplistDramEntrance *,
               config::kNumLevels>
      pmSkiplistDramEntrancePtrArray{};
    /////////////////////////////////////new2
   private:
    friend class DB;

    // Information kept for every waiting writer
    struct Writer {
      explicit Writer(port::Mutex *mu);

      Status status;
      WriteBatch *batch;
      bool sync;
      bool done;
      port::CondVar cv;
    };

    // Information for a manual compaction
    struct ManualCompaction {
      int level{};
      bool done{};
      const InternalKey *begin{}; // null means beginning of key range
      const InternalKey *end{};   // null means end of key range
      InternalKey tmpStorage;     // Used to keep track of compaction progress
    };

    // Per level compaction stats.  stats_[level] stores the stats for
    // compactions that produced data for the specified "level".
    struct CompactionStats {
      CompactionStats() = default;

      void Add(const CompactionStats &c) {
        this->micros += c.micros;
        this->bytes_read += c.bytes_read;
        this->bytes_written += c.bytes_written;
      }

      int64_t micros{0};
      int64_t bytes_read{0};
      int64_t bytes_written{0};
    };

    auto
    NewInternalIterator(const ReadOptions &, SequenceNumber *latestSnapshot,
                        uint32_t *seed) -> Iterator *;

    auto NewDb_() -> Status;

    // Recover the descriptor from persistent storage.  May do a significant
    // amount of work to recover recently logged updates.  Any changes to
    // be made to the descriptor are added to *edit.
    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      Recover_(VersionEdit *edit, bool *saveManifest) -> Status;

    [[maybe_unused]] [[maybe_unused]] void MaybeIgnoreError_(Status *s) const;

    // Delete any unneeded files and stale in-memory entries.
    void RemoveObsoleteFiles_() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    // Compact the in-memory write buffer to disk.  Switches to a new
    // log-file/memtable and writes a new descriptor iff successful.
    // Errors are recorded in bg_error_.
    void CompactMemTable_() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    //        Status RecoverLogFile(uint64_t log_number, bool last_log, bool
    //        *save_manifest,
    //                              VersionEdit *edit, SequenceNumber
    //                              *max_sequence)
    //        EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    auto RecoverNvmLog_(bool *saveManifest, VersionEdit *edit,
                        SequenceNumber *maxSequence) -> Status;

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_) WriteLevel0Table_(
      pmCache::PmWal *immWal, VersionEdit *edit, Version *base,
      pmCache::GlobalTimestamp::TimestampToken *newVersionTime) -> Status;

    [[maybe_unused]] [[maybe_unused]] auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      WriteLevel0Table_(MemTable *mem, VersionEdit *edit, Version *base)
        -> Status;

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      MakeRoomForWrite_(bool force /* compact even if there is room? */)
        -> Status;

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_) BuildBatchGroup_(Writer **lastWriter)
      -> WriteBatch *;

    void RecordBackgroundError_(const Status &s);

    void MaybeScheduleCompactionOrPromotion_() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    static void BgWork_(void *db);

    void BackgroundCall_();

    void BackgroundCompactionOrPromotion_(bool enoughRoomForNvm)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    void CleanupCompaction_(gsl::not_null<CompactionState *> compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      DoCompactionAndPromotionWork_(CompactionState *compact) -> Status;

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      DoNvmMoveCompactionWork_(CompactionState *compact) -> Status;

    [[maybe_unused]] [[maybe_unused]] auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      InstallCompactionResults_(CompactionState *compact) -> Status;

    auto EXCLUSIVE_LOCKS_REQUIRED(mutex_)
      InstallCompactionResults_(CompactionState *compact, bool isInNvm,
                                bool sync = false) -> Status;

    [[nodiscard]] const Comparator *UserComparator_() const {
      return internalComparator_.user_comparator();
    }

    void TestPrint() override;

    pmCache::PmCachePoolRoot *pmCachePool_{};

    // Constant after construction
    Env *const env_;
    const InternalKeyComparator internalComparator_;
    const InternalFilterPolicy internalFilterPolicy_;
    const Options options_; // options_.comparator == &internal_comparator_
    const bool ownsInfoLog_;
    const bool ownsCache_;
    const std::string dbname_;

    // table_cache_ provides its own synchronization
    TableCache *const tableCache_;

    // Lock over the persistent DB state.  Non-null iff successfully acquired.
    FileLock *dbLock_;

    std::shared_mutex mutexForParallelWrite_;
    std::atomic<bool> isLeader_{false};

    std::mutex nonLeaderWait_;
    bool shouldWait_ = false;
    std::condition_variable shouldWaitCv_;

    port::Mutex mutex_;
    std::atomic<bool> shuttingDown_;
    port::CondVar backgroundWorkFinishedSignal_ GUARDED_BY(mutex_);

    pmCache::ConcurrentMemtable *cMem_;
    pmCache::ConcurrentMemtable *cImm_;

    std::atomic<bool> hasImm_; // So bg thread can detect non-null imm_

    uint32_t seed_ GUARDED_BY(mutex_); // For sampling.

    // Queue of writers.
    std::deque<Writer *> writers_ GUARDED_BY(mutex_);
    WriteBatch *tmpBatch_ GUARDED_BY(mutex_);

    SnapshotList snapshots_ GUARDED_BY(mutex_);

    // Set of table files to protect from deletion because they are
    // part of ongoing compactions.
    std::set<uint64_t> pendingOutputs_ GUARDED_BY(mutex_);

    // Has a background compaction been scheduled or is running?
    //    bool backgroundCompactionScheduled_{} GUARDED_BY(mutex_);

    std::atomic<int> backGroundWorkCnt_{0};

    ManualCompaction *manualCompaction_ GUARDED_BY(mutex_);

    VersionSet *const versions_;

    // Have we encountered a background error in paranoid mode?
    Status bgError_ GUARDED_BY(mutex_);

    std::array<CompactionStats, config::kNumLevels> stats_ GUARDED_BY(mutex_){};

    pmCache::DramLru lru_{LSM_NVM_TOTAL_LEVEL_SIZE};

    std::unique_ptr<std::thread> backGroundCompactionThread_;

    std::atomic<bool> doingFlush_{false}, doingLru_{false};
    //    std::mutex doingLruNvmCacheMutex_;

    //   public:
    //    std::array<pmCache::Buffer, config::kNumLevels> hotKeyBuffer{};

    // std::atomic<uint64_t> slowCnt_{}, delayNanoTimes_{};
   public:
    std::atomic<uint64_t> getHitCnt_[config::kNumLevels]{};
    std::atomic<uint64_t> getHitMemCnt_{};
  };

  // Sanitize db options.  The caller should delete result.info_log if
  // it is not equal to src.info_log.
  auto SanitizeOptions(const std::string &db, const InternalKeyComparator *icmp,
                       const InternalFilterPolicy *internalFilterPolicy,
                       const Options &src) -> Options;

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DB_IMPL_H_
