////
//// Created by jxz on 22-8-3.
////
//
#include "pm_file_in_one_level.h"
#include "dbWithPmCache/dram_lru/dram_lru.h"
#include "dbWithPmCache/help/pmem.h"
#include "pmCache/dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.h"

leveldb::pmCache::PmSsTableIterator::PmSsTableIterator(
  leveldb::pmCache::PmSsTable *pmSsTable) {
  table_ = pmSsTable;
  entryCount_ = (int)pmSsTable->GetIndexLen();
  indexS_ = table_->GetIndexS();

  internalKeyLens_.reserve(entryCount_);
  valueLens_.reserve(entryCount_);
  keys_.reserve(entryCount_);
  for(int i = 0; i < entryCount_; ++i) {
    auto data = indexS_[i].GetData();
    internalKeyLens_.push_back(data->internalKeyLen);
    valueLens_.push_back(data->valueLen);
    keys_.emplace_back(data->GetInternalKeySlice().ToString());
  }
}