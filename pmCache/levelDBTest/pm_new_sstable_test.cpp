////
//// Created by jxz on 22-8-2.
////
// #include "pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.h"
// #include <cstdio>
// #include <filesystem>
// #include <gtest/gtest.h>
// #include <libpmemobj/iterator.h>
// #include <set>
// #include <string>
//
// using std::cout;
// using std::endl;
// using namespace leveldb::pmCache;
// namespace {
//   class RootClass {
//    public:
//     PmSSTable *sstable;
//   };
// } // namespace
// pool<RootClass> pop;
// RootClass *root;
// TEST(PmSSTable, test1) {
//   std::string testPoolName = TEST_POOL_NAME;
//   if(std::filesystem::exists(testPoolName))
//     std::filesystem::remove(testPoolName);
//
//   pop = pmem::obj::pool<RootClass>::create(testPoolName, "test",
//                                            1L * 1024 * 1024 * 1024);
//   root = pop.root().get();
//   _pobj_cached_pool.pop = pop.handle();
//   PmSSTableBuilder pmSsTableBuilder{};
//   uint64_t sum = 0;
//   std::set<std::string> ss;
//   for(int i = 0; i < 180000; i++) {
//     ss.insert(std::to_string(i));
//   }
//   auto c = 0;
//   PMEMoid raw_obj;
//   POBJ_FOREACH(pop.handle(), raw_obj) { c++; }
//   cout << "c = " << c << endl;
//   {
//     auto tx = pmSsTableBuilder.PrepareWithTXBegin();
//     for(int i = 1799; i >= 0; i--) {
//       std::string st = "1";
//       char buffer[10];
//       memcpy(buffer, st.c_str(), st.size());
//       *(uint64_t *)&buffer[1] = i;
//       pmSsTableBuilder.PushNodeWithoutFlush({buffer, 9}, {buffer, 9});
//     }
//     root->sstable = pmSsTableBuilder.GeneratePmSSTable();
//     pmSsTableBuilder.FlushPmSSTableWithFence();
//     pmemobj_flush(_pobj_cached_pool.pop, &root->sstable, sizeof(size_t));
//     pmem::obj::flat_transaction::commit();
//   }
//
//   //    root->sstable->TEST_Print();
//   cout << sum << endl;
//   for(int i = 0; i < 1800; i++) {
//     std::string st = "1";
//     char buffer[10];
//     memcpy(buffer, st.c_str(), st.size());
//     *(uint64_t *)&buffer[1] = i;
//     root->sstable->lower_bound({buffer, 9}) /*.TEST_Print()*/;
//   }
//   c = 0;
//   POBJ_FOREACH(pop.handle(), raw_obj) { c++; }
//   cout << "c = " << c << endl;
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }