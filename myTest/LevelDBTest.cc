////
//// Created by jxz on 22-7-7.
////
//
// #include "leveldb/cache.h"
// #include "leveldb/db.h"
// #include <cassert>
// #include <iostream>
// #include <libpmemobj++/persistent_ptr.hpp>
// #include <libpmemobj++/pool.hpp>
//
// using namespace std;
// using namespace leveldb;
// using namespace pmem;
// using namespace pmCache;
//
// class root {
// public:
//  char test[100];
//};
//
// std::string nvmPoolPath = "/home/jxz/pmem0/testPool";
//
// class TestCacheData {
// public:
//  void init(){};
//};
//
// class TestPool {
// public:
//  PmTimeStamp timeStamp{};
//  ThreadPool threadPool{10};
//  PmSkipList<TestCacheData, PmSkipListDataNode<TestCacheData>> pmSkipList{
//    5, &timeStamp, &threadPool};
//};
//
// int main() {
//  DB *db;
//  Options options;
//  options.create_if_missing = true;
//  options.compression = leveldb::kNoCompression;
//  options.block_cache = leveldb::NewLRUCache(8 << 20);
//  Status status = DB::Open(options, "/home/jxz/pmem0/testDB", &db);
//  assert(status.ok());
//  auto writeOptions = WriteOptions();
//  status = db->Put(WriteOptions(), "foo", "bar");
//  assert(status.ok());
//  string res;
//  status = db->Get(ReadOptions(), "foo", &res);
//  assert(status.ok());
//  cout << res << endl;
//  delete db;
//
//  pmem::obj::pool<TestPool> testPool;
//  std::string testPoolName = TEST_POOL_NAME;
//  remove(testPoolName.c_str());
//  testPool = pmem::obj::pool<TestPool>::create(testPoolName, "test");
//  auto s = &testPool.root()->pmSkipList;
//  testPool.close();
//
//  //    obj::pool<root> p;
//  //    p = obj::pool<root>::create(nvmPoolPath, "test");
//  //    auto rootPtr = p.root().get();
//  //    p.memcpy_persist(rootPtr->test, "hello world", 11);
//  //    p.close();
//  //    p = obj::pool<root>::open(nvmPoolPath, "test");
//  //    cout << p.root()->test << endl;
//  //    p.close();
//  return 0;
//}