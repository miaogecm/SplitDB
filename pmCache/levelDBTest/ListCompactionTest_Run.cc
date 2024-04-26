//

//

#include "db/db_impl.h"
#include "leveldb/db.h"
#include <iostream>

auto main() -> int {
  leveldb::DB *db;
  auto status = leveldb::DB::Open(leveldb::Options(),
                                  "/media/disk/TestDbSplitDB_80G_tmp1", &db);
  if(!status.ok()) {
    std::cout << "open db failed" << std::endl;
    return 0;
  }
  auto *dbImpl = reinterpret_cast<leveldb::DBImpl *>(db);
  dbImpl->TEST_CompactRange(0, nullptr, nullptr);
  while(true) {}

  delete db;
}