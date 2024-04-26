////
//// Created by jxz on 22-8-2.
////
// #include "pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.h"
// #include <cstdio>
// #include <filesystem>
// #include <gtest/gtest.h>
// #include <string>
//
// using std::cout;
// using std::endl;
// using namespace leveldb::pmCache;
// namespace {
//   class RootClass {
//    public:
//     int a = 1;
//     int b = 2;
//     int c = 3;
//
//     void test(int i) const {
//       cout << i << ":" << a << " " << b << " " << c << endl;
//     }
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
//   try {
//     pmem::obj::flat_transaction::run(pop, [&] {
//       root->test(1);
//       root->a = 1;
//       root->b = 2;
//       root->c = 3;
//       root->test(2);
//       pmem::obj::flat_transaction::snapshot(&root->a);
//       pmem::obj::flat_transaction::snapshot(&root->b);
//       pmem::obj::flat_transaction::snapshot(&root->c);
//       root->a = 11;
//       root->b = 22;
//       root->c = 33;
//       root->test(3);
//       pmem::obj::flat_transaction::run(pop, [&] {
//         pmem::obj::flat_transaction::snapshot(&root->a);
//         pmem::obj::flat_transaction::snapshot(&root->b);
//         pmem::obj::flat_transaction::snapshot(&root->c);
//         root->a = 111;
//         root->b = 222;
//         root->c = 333;
//         root->test(4);
//         pmem::obj::flat_transaction::register_callback(
//           pmem::obj::flat_transaction::stage::oncommit,
//           [&] { cout << "in t c" << endl; });
//       });
//       //            pmemobj_tx_abort(0);
//       pmem::obj::flat_transaction::register_callback(
//         pmem::obj::flat_transaction::stage::oncommit, [&] {
//           root->test(5);
//           cout << "out t c" << endl;
//         });
//     });
//   }
//   catch(std::exception &e) {
//     cout << e.what() << endl;
//   }
//   root->test(6);
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }