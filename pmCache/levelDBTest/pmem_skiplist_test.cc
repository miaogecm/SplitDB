////
//// Created by jxz on 22-7-19.
////
// #include "iostream"
// #include "pmCache/dbWithPmCache/pmemSkiplist/pmem_skiplist.h"
// #include <cstdio>
// #include <filesystem>
// #include <gtest/gtest.h>
//
// using std::cout;
// using std::endl;
// std::string testPoolName = TEST_POOL_NAME;
// const int MaxIndexHeight = 7;
//
// class TestRoot {
//  public:
//   pmem::obj::persistent_ptr<leveldb::pmCache::PmemSkiplist<int>> skiplist;
//   pmem::obj::persistent_ptr<leveldb::pmCache::PmTimeStamp> timestamp;
// };
//
// pmem::obj::pool<TestRoot> pop;
// leveldb::pmCache::PmemSkiplist<int> *skiplist;
// leveldb::pmCache::ThreadPool *threadPool;
// TEST(t, create) {
//   if(std::filesystem::exists(testPoolName)) {
//     std::filesystem::remove(testPoolName);
//   }
//   pop = pmem::obj::pool<TestRoot>::create(testPoolName, "test",
//                                           1L * 1024 * 1024 * 1024);
//   auto root = pop.root();
//   auto allocator = pmem::obj::allocator<void>();
//   pmem::obj::flat_transaction::run(pop, [&] {
//     root->timestamp =
//       pmem::obj::make_persistent<leveldb::pmCache::PmTimeStamp>();
//     leveldb::pmCache::PmemSkiplist<int> *newSkiplist =
//       (leveldb::pmCache::PmemSkiplist<int> *)allocator
//         .allocate(leveldb::pmCache::PmemSkiplist<int>::getSkipListMetaSize(
//           MaxIndexHeight))
//         .get();
//     printPmAllocate(
//       "ll1", newSkiplist,
//       leveldb::pmCache::PmemSkiplist<int>::getSkipListMetaSize(MaxIndexHeight));
//     threadPool = new leveldb::pmCache::ThreadPool(20, pop.handle());
//     new(newSkiplist) leveldb::pmCache::PmemSkiplist<int>(
//       MaxIndexHeight, root->timestamp.get(), threadPool);
//     root->skiplist = newSkiplist;
//   });
//   skiplist = root->skiplist.get();
// }
//// TEST(t, insertOneThread) {
////          int a = 99;
////          std::string key = "1";
////          skiplist->InsertNode(key.data(), 1, &a, sizeof(int));
////          for (int i = 3; i < 100; ++i) {
////              key = std::to_string(i);
////              skiplist->InsertNode(key.data(), key.length(), &i,
/// sizeof(int)); /          } /      } /      TEST(t, insertMultiThread) { /
/// std::vector<std::thread> testThreads; /         for (int i = 0; i < 10; i++)
///{ /             testThreads.emplace_back( /                 [&](int c) { /
///_pobj_cached_pool.pop = pop.handle(); /                     for (int i = 100
///* c; i < 100 * (c + 1); i++) { /                         std::string key =
/// std::to_string(i); /                         key = key + "t"; /
/// skiplist->InsertNode(key.data(), key.length(), &i, / sizeof(int)); / } / },
////                 i);
////         }
////         for (auto &t : testThreads) {
////             t.join();
////         }
////             auto node = skiplist->head;
////             while (node != nullptr) {
////                 if (node == skiplist->head)
////                     cout << "head" << endl;
////                 else if (node == skiplist->tail)
////                     cout << "tail" << endl;
////                 else
////                     cout << node->ToString() << endl;
////                 node = node->getNextVPtr(0);
////             }
//// }
// TEST(t, insertSame) {
//   std::string key = "1";
//   for(int i = 3; i < 100; ++i) {
//     key = std::to_string(1);
//     skiplist->InsertNode(key.data(), key.length(), &i, sizeof(int));
//   }
//   //    std::this_thread::sleep_for(std::chrono::seconds(1));
//   bool once = false;
//   while(skiplist->gcQueue->sizeWithLock() > 2) {
//     //        if (!once) {
//     skiplist->ScheduleBackgroundGcWithNewGcTime();
//     //            once = true;
//     //        }
//   }
//   std::stringstream s;
//   s << "\n";
//   for(int i = 0; i < MaxIndexHeight; ++i) {
//     auto node = skiplist->head;
//     while(node != nullptr) {
//       if(node == skiplist->head)
//         s << "head -> ";
//       else if(node == skiplist->tail)
//         s << "tail ";
//       else
//         s << node->ToString() << "(" << (int)node->indexHeight << ")"
//           << "-> ";
//       node = node->getNextVPtr(i);
//     }
//     s << "\n";
//   }
//   cout << s.str() << endl;
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }