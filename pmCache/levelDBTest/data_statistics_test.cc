////

////
//
//// hello world
//
// #include "pmCache/dbWithPmCache/help/pmem.h"
// #include
// "pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h"
// #include <iostream>
// using leveldb::pmCache::skiplistWithFanOut::PersistentPtrLight;
// class Root {
// public:
//  PersistentPtrLight<char> str;
//};
//
// #pragma clang diagnostic push
// #pragma ide diagnostic ignored "bugprone-exception-escape"
// int a;
// auto main() -> int {
//
//  std::cin >> a;
//
//  auto path = "/mnt/pmem/test";
//  auto layout = "test";
//  auto myPool = leveldb::pmCache::pool<Root>::create(
//    path, layout, 1024L * 1024 * 1024, S_IWUSR | S_IRUSR);
//  auto strSize = 1000L * 1000 * 1000;
//  pmem::obj::flat_transaction::run(myPool, [&] {
//    auto root = myPool.root();
//    root->str.AddTxRangeWithoutSnapshot();
//    root->str.SetWithoutBoundaryOrFlush(
//      pmem::obj::make_persistent<char>(strSize));
//    pmemobj_tx_xadd_range_direct(root->str.GetVPtr(), strSize,
//                                 POBJ_XADD_NO_SNAPSHOT);
//    memset(root->str.GetVPtr(), 'a', strSize);
//  });
//  myPool.close();
//  myPool = leveldb::pmCache::pool<Root>::open(path, layout);
//  myPool.root()->str.GetVPtr();
//  for(int i = 0; i < strSize; i++)
//    std::cout << myPool.root()->str.GetVPtr()[i];
//  std::cout << std::endl;
//  myPool.close();
//
//  std::cin >> a;
//
//  return 0;
//}
// #pragma clang diagnostic pop