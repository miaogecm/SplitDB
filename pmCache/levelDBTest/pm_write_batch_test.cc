////
//// Created by jxz on 22-7-24.
////
// #include "dbWithPmCache/pm_write_impl/pm_write_batch.h"
// #include "pmCache/dbWithPmCache/pm_write_impl/pm_wal_log.h"
// #include "pmCache/dbWithPmCache/pmem_sstable/pm_sstable_old.h"
// #include <cstdio>
// #include <filesystem>
// #include <gtest/gtest.h>
// #include <string>
//
// using leveldb::Slice;
// using std::cout;
// using std::endl;
// TEST(walTest, append) {
//   using namespace leveldb::pmCache;
//   WriteBatchForPmWal batch1;
//   WriteBatchForPmWal batch2;
//   for(int i = 0; i < 10; i++) {
//     batch1.Put(Slice{std::to_string(i)}, Slice{std::to_string(i)});
//   }
//   std::cout << "batch1" << std::endl;
//   for(auto &i : batch1.getBatch()) {
//     i.PrintNode();
//   }
//   std::cout << std::endl;
//   for(int i = 11; i < 20; i++) {
//     batch2.Put(Slice{std::to_string(i)}, Slice{std::to_string(i)});
//   }
//   std::cout << "batch2" << std::endl;
//   for(auto &i : batch2.getBatch()) {
//     i.PrintNode();
//   }
//   std::cout << std::endl;
//   batch1.Append(&batch2);
//   std::cout << "batch1 = batch1+move(batch2)" << std::endl;
//   for(auto &i : batch1.getBatch()) {
//     i.PrintNode();
//   }
//
//   std::cout << std::endl;
//   std::cout << "batch2" << std::endl;
//   for(auto &i : batch2.getBatch()) {
//     i.PrintNode();
//   }
// }
//
// namespace {
//   using namespace leveldb::pmCache;
//
//   class Pool {
//    public:
//     pmem::obj::persistent_ptr<PmWAL> pm_wal;
//     pmem::obj::persistent_ptr<PmemSstable> pm_sstable;
//   };
// } // namespace
// TEST(pmwalTest, insert) {
//   auto pool =
//     pmem::obj::pool<Pool>::create(TEST_POOL_NAME, "test", PMEM_CACHE_SIZE);
//   auto root = pool.root();
//   MemtableWriteAheadLog *wal;
//   PmemSstable *sstable;
//   pmem::obj::flat_transaction::run(pool, [&] {
//     root->pm_wal = pmem::obj::make_persistent<MemtableWriteAheadLog>();
//     root->pm_wal->PrepareMemtableWriteAheadLogWithoutFlush();
//     wal = root->pm_wal.get();
//     root->pm_sstable = pmem::obj::make_persistent<PmemSstable>();
//     sstable = root->pm_sstable.get();
//     mfence();
//     root->pm_wal->FlushAfterPrepare();
//   });
//   WriteBatchForPmWal batch1;
//   for(int i = 0; i <= 100; i++) {
//     batch1.Put(std::to_string(i), std::to_string(i));
//   }
//   batch1.setSequence(1);
//   wal->AddRecord(&batch1);
//   PmKVNode *node = nullptr;
//
//   cout << endl << "before build sstable in WAL" << endl;
//   node = wal->getFirstNode();
//   std::basic_string<char> toPrint;
//   while(node) {
//     assert(node->getInternalKeySize());
//     toPrint =
//       std::string(node->getInternalKey(), node->getInternalKeySize() - 8);
//     cout << toPrint << " ";
//     node = node->getNextVPtr(0);
//   }
//   cout << endl;
//   pmem::obj::flat_transaction::run(pool,
//                                    [&] { sstable->BuildFromWalLog(*wal); });
//   cout << endl << "after build sstable in WAL" << endl;

//   node = wal->getFirstNode();
//   while(node) {
//     assert(node->getInternalKeySize());
//     toPrint =
//       std::string(node->getInternalKey(), node->getInternalKeySize() - 8);
//     cout << toPrint << " ";
//     node = node->getNextVPtr(0);
//   }
//   cout << endl;
//
//   std::set<int> testSet;
//   for(int i = 0; i <= 100; i++) {
//     testSet.insert(i);
//   }
//
//   cout << endl << "after build sstable in pmem_sstable" << endl;
//   for(int i = PM_SSTABLE_MAX_INDEX_HEIGHT - 1; i >= 0; --i) {
//     node = sstable->getFistNode(i);
//     while(node) {
//       assert(node->getInternalKeySize());
//       toPrint =
//         std::string(node->getInternalKey(), node->getInternalKeySize() - 8);
//       int num = std::stoi(toPrint);
//       if(i == 0) {
//         ASSERT_TRUE(testSet.contains(num));
//         testSet.erase(num);
//       }
//       cout << toPrint << " ";
//       node = node->getNextVPtr(i);
//     }
//     cout << endl;
//   }
//   ASSERT_TRUE(testSet.empty());
//
//   cout << "max key = "
//        << std::string(sstable->getLargestKey(), sstable->getLargestKeyLen())
//        << endl;
//   cout << "min key = "
//        << std::string(sstable->getSmallestKey(), sstable->getSmallestKeyLen())
//        << endl;
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }