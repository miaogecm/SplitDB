//
// Created by jxz on 22-7-25.
//

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_PM_WRITE_IMPL_PM_WAL_LOG_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_PM_WRITE_IMPL_PM_WAL_LOG_H_

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "pm_write_batch.h"
#include "pmCache/dbWithPmCache/help/pmem.h"
#include "pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.h"
#include <cstdint>

namespace leveldb::pmCache {
  class PmWal { 
   public:
    PmWal(const PmWal &) = delete;
    auto operator=(const PmWal &) -> PmWal & = delete;
    PmWal(PmWal &&) = delete;
    PmWal() = delete;

    inline auto GetDummyHead() -> PmSsTable::DataNode *;
    inline auto GetDummyTail() -> PmSsTable::DataNode *;

    Status AddRecord(WriteBatch *batch, std::vector<uint64_t> &);

    void AddRecordParallel(const Slice &key, const Slice &value, uint64_t sq,
                           unsigned char type, uint64_t &off,
                           pmem::obj::pool_base &pool);

    inline auto IsEmpty() -> bool;
    ~PmWal();

   private:
    /// followed by dummyHead and dummyTail
  };

  class PmWalBuilder {
   public:
    PmWalBuilder(const PmSsTableBuilder &) = delete;
    PmWalBuilder(PmWalBuilder &&) = delete;
    bool operator=(const PmWalBuilder &) = delete;

    PmWalBuilder() = default;

    /// \return
    [[nodiscard]] static PmWal *GenerateWal();

   private:
  };

  bool leveldb::pmCache::PmWal::IsEmpty() {
    return GetDummyHead()->NextNode() == GetDummyTail();
  }

  leveldb::pmCache::PmSsTable::DataNode *
  leveldb::pmCache::PmWal::GetDummyHead() {
    return (PmSsTable::DataNode *)this;
  }

  leveldb::pmCache::PmSsTable::DataNode *
  leveldb::pmCache::PmWal::GetDummyTail() {
    return GetDummyHead() + 1;
  }
} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_PM_WRITE_IMPL_PM_WAL_LOG_H_
