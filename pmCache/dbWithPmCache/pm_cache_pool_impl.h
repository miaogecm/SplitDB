//
// Created by jxz on 22-7-27.
//

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_PM_CACHE_POOL_IMPL_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_PM_CACHE_POOL_IMPL_H_

#include "db/dbformat.h"
#include "dbWithPmCache/help/pm_timestamp/pm_time_stamp.h"
#include "dbWithPmCache/pm_write_impl/pm_wal_log.h"
#include "pmCache/dbWithPmCache/help/pmem.h"
#include "pmCache/dbWithPmCache/help/thread_pool/thread_pool.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level_opt/pm_file_in_one_level_opt.h"
#include "pmCache/dbWithPmCache/pmem_sstable/pm_sstable_old.h"
#include <queue>
#include <set>

namespace leveldb::pmCache {

  class PmCachePoolRoot {
   public:
    PmCachePoolRoot(const PmCachePoolRoot &) = delete;
    PmCachePoolRoot(PmCachePoolRoot &&) = delete;
    auto operator=(const PmCachePoolRoot &) -> PmCachePoolRoot & = delete;
    PmCachePoolRoot();

   public:
    [[nodiscard]] inline auto GetWalForMem() -> PmWal *;
    [[nodiscard]] inline auto GetWalForImm() -> PmWal *;
    inline void SetMemWal(PmWal *wal);
    void ClearImmWal(pmem::obj::pool_base &pool);

    void InitWhenOpen();
    void TurnLogMemToImmAndCreateNewMem();
    //    inline auto GetFileInOneLevel(uint8_t i) -> PmFileInOneLevel *;

    inline auto GetFileInOneLevelOptDramEntrance(uint8_t i)
      -> skiplistWithFanOut::PmSkiplistDramEntrance *;

   private:
    void Prepare_();

   public:
    ThreadPool *threadPool{};

   private:
    skiplistWithFanOut::PersistentPtrLight<PmWal> memtableWriteAheadLog_{};
    skiplistWithFanOut::PersistentPtrLight<PmWal> immutableWriteAheadLog_{};

    std::array<skiplistWithFanOut::PersistentPtrLight<
                 skiplistWithFanOut::PmSkiplistNvmEntrance>,
               config::kNumLevels>
      nvmSkiplistEntrance_{};
    std::array<skiplistWithFanOut::PmSkiplistDramEntrance *, config::kNumLevels>
      nvmSkiplistDramEntrance_{};
    std::array<uint64_t, config::kNumLevels> filesOff_{};
    NvGlobalTimestamp pmTimeStamp_;
  };

  leveldb::pmCache::PmWal *leveldb::pmCache::PmCachePoolRoot::GetWalForMem() {
    return memtableWriteAheadLog_.GetVPtr();
  }

  PmWal *PmCachePoolRoot::GetWalForImm() {
    return immutableWriteAheadLog_.GetVPtr();
  }

  void PmCachePoolRoot::SetMemWal(PmWal *wal) {
    //    memtableWriteAheadLogOff_ = getOffset(wal);
    memtableWriteAheadLog_.SetWithoutBoundaryOrFlush(wal);
  }

  //  PmFileInOneLevel *PmCachePoolRoot::GetFileInOneLevel(uint8_t i) {
  //    assert(0 <= i && i < config::kNumLevels);
  //    return GetVPtr<PmFileInOneLevel>(filesOff_[i]);
  //    //    return files_[i];
  //  }

  skiplistWithFanOut::PmSkiplistDramEntrance *
  PmCachePoolRoot::GetFileInOneLevelOptDramEntrance(uint8_t i) {
    assert(0 <= i && i < config::kNumLevels);
    return nvmSkiplistDramEntrance_[i];
  }
} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_PM_CACHE_POOL_IMPL_H_
