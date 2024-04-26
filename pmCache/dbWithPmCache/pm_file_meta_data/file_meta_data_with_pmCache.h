//
// Created by jxz on 22-7-27.
//

#ifndef LEVELDB_FILE_META_DATA_WITH_PMCACHE_H
#define LEVELDB_FILE_META_DATA_WITH_PMCACHE_H

#include "db/dbformat.h"
#include <cstdint>

namespace leveldb::pmCache {
  enum WhereIsFile { kInNVM = 0, kInSSD = 1, kUnknown = 2 };

} // namespace leveldb::pmCache

#endif // LEVELDB_FILE_META_DATA_WITH_PMCACHE_H
