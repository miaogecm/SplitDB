////
//// Created by jxz on 22-8-4.
////
// #include "dbWithPmCache/help/pmem.h"
// #include "pmCache/dbWithPmCache/help/pm_timestamp/pm_time_stamp.h"
// #include "pmCache/dbWithPmCache/help/thread_pool/thread_pool.h"
// #include "pmCache/dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
// #include <cstdio>
// #include <filesystem>
// #include <gtest/gtest.h>
// #include <libpmemobj/iterator.h>
// #include <string>
//
// using std::cout;
// using std::endl;
// using namespace leveldb::pmCache;
// using leveldb::Slice;
//
// namespace {
//   class TestRoot {
//    public:
//     PmSsTableNodeInSkiplist *ssTableNodeInSkiplist;
//     PmFileInOneLevel *fileInOneLevel;
//     PmTimeStamp *timeStamp;
//   };
// } // namespace
// TestRoot *root = nullptr;
// TEST(TEST_SSTABLE, create) {
//   if(std::filesystem::exists(TEST_POOL_NAME))
//     std::filesystem::remove_all(TEST_POOL_NAME);
//   auto pool =
//     pmem::obj::pool<TestRoot>::create(TEST_POOL_NAME, "test",
//     PMEM_CACHE_SIZE);
//   root = pool.root().get();
//   cout << "1 :" << getNumOfPmPieces() << endl;
//   PmSSTableNodeBuilder builder{};
//   {
//     std::unique_ptr<pmem::obj::flat_transaction::manual> tx =
//       static_cast<std::unique_ptr<flat_transaction::manual>>(
//         builder.PrepareWithTxBegin());
//     root->ssTableNodeInSkiplist = nullptr;
//     pmem::obj::flat_transaction::snapshot(&root->ssTableNodeInSkiplist);
//     for(int i = 0; i < 10; i++) {
//       auto buffer = std::to_string(i);
//       int len = buffer.length();
//       buffer.reserve(buffer.length() + 8);
//       *((uint64_t *)(buffer.c_str() + len)) = i;
//       builder.PushNodeWithoutFlush(Slice{buffer.c_str(), (uint64_t)(len +
//       8)},
//                                    Slice{std::to_string(i)});
//     }
//     root->ssTableNodeInSkiplist = builder.GenerateSsTableNode();
//     builder.FlushNodeWithFence();
//     pmemobj_flush(_pobj_cached_pool.pop, &root->ssTableNodeInSkiplist,
//                   sizeof(size_t));
//     pmem::obj::flat_transaction::commit();
//   }
//   root->ssTableNodeInSkiplist->GetPmSstable()->TEST_Print();
//   cout << "total :" << getNumOfPmPieces() << endl;
// }
//
// PmFileInOneLevel *pmFileInOneLevel = nullptr;
// leveldb::pmCache::ThreadPool *threadPool = nullptr;
// TEST(TEST_SSTABLE, skipList) {
//   PmFileInOneLevelBuilder pmFileInOneLevelBuilder{};
//   pmem::obj::pool_base pool{_pobj_cached_pool.pop};
//   pmem::obj::flat_transaction::run(pool, [&] {
//     pmem::obj::flat_transaction::snapshot(&root->timeStamp);
//     root->timeStamp = pmem::obj::make_persistent<PmTimeStamp>().get();
//     printPmAllocate("b", root->timeStamp, root->timeStamp,
//     sizeof(PmTimeStamp)); root->timeStamp->initWhenOpen(); sfence();
//     pmemobj_flush(_pobj_cached_pool.pop, &root->timeStamp, sizeof(size_t));
//   });
//   cout << "pieces1 = " << getNumOfPmPieces() << endl;
//   threadPool = new leveldb::pmCache::ThreadPool(10, pool.handle());
//
//   assert(pmemobj_tx_stage() == TX_STAGE_NONE);
//   pmem::obj::flat_transaction::run(pool, [&] {
//     root->fileInOneLevel = pmFileInOneLevelBuilder.GeneratePmFileInOneLevel(
//       root->timeStamp, threadPool, false);
//     pmFileInOneLevelBuilder.FlushFileInOneLevelMeta();
//   });
//
//   pmFileInOneLevel = root->fileInOneLevel;
//   cout << "currentSize = " << pmFileInOneLevel->currentSize << endl;
//   cout << "pieces2 = " << getNumOfPmPieces() << endl;
// }
//
// TEST(TEST_SSTABLE, insertNode) {
//   assert(pmemobj_tx_stage() == TX_STAGE_NONE);
//   pmem::obj::pool_base pool(_pobj_cached_pool.pop);
//   root->ssTableNodeInSkiplist->Lock();
//   root->ssTableNodeInSkiplist->UnLock();

//   PmSsTableNodeInSkiplist *newNode;
//   cout << "before count = " << getNumOfPmPieces() << endl;
//   {
//     auto nodeBuilder = PmSSTableNodeBuilder{0};
//     auto tx = nodeBuilder.PrepareWithTxBegin();
//     auto otherSstable = root->ssTableNodeInSkiplist->GetPmSstable();
//     auto nodeCnt = otherSstable->GetIndexLen();
//     auto indexS = otherSstable->GetIndexS();
//     for(int i = 0; i < nodeCnt; ++i) {
//       auto node = indexS[i].getData();
//       nodeBuilder.PushNodeWithoutFlush(node);
//     }
//     newNode = nodeBuilder.GenerateSsTableNode();
//     newNode->Lock();
//     newNode->UnLock();
//     root->fileInOneLevel->InsertNode(newNode);
//     nodeBuilder.FlushNodeWithFence();
//     pmem::obj::flat_transaction::commit();
//   }
//   cout << "after count = " << getNumOfPmPieces() << endl;
//
//   pmem::obj::flat_transaction::run(pool, [&] {
//     root->ssTableNodeInSkiplist->Clear();
//     root->ssTableNodeInSkiplist->Clear();
//   });
//   cout << "after clear = " << getNumOfPmPieces() << endl;
//
//   pmem::obj::flat_transaction::run(pool, [&] {
//     printPmAllocate("d10", root->ssTableNodeInSkiplist, 0);
//     pmem::obj::delete_persistent<PmSsTableNodeInSkiplist>(
//       root->ssTableNodeInSkiplist);
//     root->ssTableNodeInSkiplist = nullptr;
//   });
//   cout << "after delete old = " << getNumOfPmPieces() << endl;
//
//   pmem::obj::flat_transaction::run(
//     pool, [&] { root->fileInOneLevel->DeleteNode(newNode); });
//
//   cout << "after delete new = " << getNumOfPmPieces() << endl;
// }
//
//[[nodiscard]] int PackKeyWithUint64(std::string &key, uint64_t sq) {
//   auto len = key.length() + 8;
//   key.reserve(len);
//   char *key0 = key.data();
//   *((uint64_t *)(key0 + key.length())) = sq;
//   return (int)len;
// }
//
// TEST(TEST_SSTABLE, insertMore) {
//   pmem::obj::pool_base pool{_pobj_cached_pool.pop};
//   PmSsTableNodeInSkiplist *newNode;
//   for(int i = 10; i < 2000; i++) {
//     {
//       auto nodeBuilder = PmSSTableNodeBuilder{};
//       auto key = std::to_string(i);
//       int len = PackKeyWithUint64(key, i);
//       auto tx = nodeBuilder.PrepareWithTxBegin();
//       nodeBuilder.PushNodeWithoutFlush(Slice{key.c_str(), (uint64_t)len},
//                                        Slice{std::to_string(i)});
//       newNode = nodeBuilder.GenerateSsTableNode();
//       root->fileInOneLevel->InsertNode(newNode);
//       nodeBuilder.FlushNodeWithFence();
//       pmem::obj::flat_transaction::commit();
//     }
//     {
//       pmem::obj::flat_transaction::run(
//         pool, [&] { root->fileInOneLevel->DeleteNode(newNode); });
//     }
//     cout << "i = " << i << " p = " << getNumOfPmPieces() << endl;
//   }
//   cout << endl;
//   root->fileInOneLevel->TEST_Print();
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }