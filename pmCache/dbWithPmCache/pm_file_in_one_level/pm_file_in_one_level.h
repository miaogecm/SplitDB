////
//// Created by jxz on 22-8-3.
////

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_PM_FILE_IN_ONE_LEVEL_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_PM_FILE_IN_ONE_LEVEL_H_

#include "dbWithPmCache/pm_write_impl/pm_wal_log.h"
#include "pmCache/dbWithPmCache/dram_lru/dram_lru.h"
#include "pmCache/dbWithPmCache/help/height_generator.h"
#include "pmCache/dbWithPmCache/help/pm_timestamp/pm_time_stamp.h"
#include "pmCache/dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.h"
#include "pmCache/dbWithPmCache/help/thread_pool/thread_pool.h"
#include "pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.h"
#include "port/port_stdcxx.h"
#include "port/thread_annotations.h"
#include <cstdint>
#include <queue>
#include <set>
//
namespace leveldb::pmCache {

  class PmSsTableIterator : public leveldb::Iterator {
   public:
    explicit PmSsTableIterator(PmSsTable *pmSsTable);

    PmSsTableIterator(const PmSsTableIterator &) = delete;

    auto operator=(const PmSsTableIterator &) -> PmSsTableIterator & = delete;

    PmSsTableIterator(PmSsTableIterator &&) = delete;

    ~PmSsTableIterator() override = default;

    inline void Seek(const Slice &target) override;

    inline void SeekToFirst() override;

    inline void SeekToLast() override;

    inline void Next() override;

    inline void Prev() override;

    [[nodiscard]] inline auto Valid() const -> bool override;

    [[nodiscard]] inline auto key() const -> Slice override;

    [[nodiscard]] inline auto value() const -> Slice override;

    [[nodiscard]] inline auto status() const -> Status override;

    [[nodiscard]] inline auto
    GetDataNodeInNvm() -> PmSsTable::DataNode * override;

    [[nodiscard]] inline auto IsInNvm() -> bool override { return true; };

    inline int GetValueLen() { return valueLens_[i_]; }

   private:
    std::vector<std::string> keys_;
    std::vector<uint32_t> internalKeyLens_;
    std::vector<uint32_t> valueLens_;
    PmSsTable::Index *indexS_ = nullptr;
    PmSsTable *table_;
    int entryCount_ = 0;
    int i_ = 0;
  };

  auto leveldb::pmCache::PmSsTableIterator::GetDataNodeInNvm()
    -> leveldb::pmCache::PmSsTable::DataNode * {
    assert(indexS_);
    return indexS_[i_].GetData();
  }

  void leveldb::pmCache::PmSsTableIterator::Seek(const Slice &target) {
    i_ = table_->BinarySearch(target);
  }

  void leveldb::pmCache::PmSsTableIterator::SeekToFirst() { i_ = 0; }

  void leveldb::pmCache::PmSsTableIterator::SeekToLast() {
    i_ = (int)table_->GetIndexLen() - 1;
  }

  void leveldb::pmCache::PmSsTableIterator::Next() { ++i_; }

  void leveldb::pmCache::PmSsTableIterator::Prev() { --i_; }

  auto leveldb::pmCache::PmSsTableIterator::Valid() const -> bool {
    return i_ >= 0 && i_ < entryCount_;
  }

  auto leveldb::pmCache::PmSsTableIterator::key() const -> leveldb::Slice {
    return {keys_[i_].data(), internalKeyLens_[i_]};
  }

  auto leveldb::pmCache::PmSsTableIterator::value() const -> leveldb::Slice {
    return {indexS_[i_].GetData()->GetValue(), valueLens_[i_]};
  }

  auto leveldb::pmCache::PmSsTableIterator::status() const -> leveldb::Status {
    if(Valid() || i_ == entryCount_)
      return Status::OK();
    else
      return Status::NotFound(Slice{"read nvm sstable error"});
  }

} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_PM_FILE_IN_ONE_LEVEL_H_
