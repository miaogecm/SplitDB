////

////
//
//#include "pm_file_in_one_level_fan_out.h"
//#include "pmCache/dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.h"
//#include <iostream>
//using namespace leveldb::pmCache::skiplistWithFanOut;
//using namespace leveldb::pmCache;
//static std::string testPath = // NOLINT(cert-err58-cpp)
//  "/mnt/ext4/disk/jiang/leveldb-y/cmake-build-debug/testDB/testPool";
//
//class TestPool {
// public:
//  PmSkiplistNvmEntrance nvmEntrance{};
//  NvGlobalTimestamp nvGlobalTimestamp{};
//};
//
//int main() {
//
//  auto pool =
//    pmem::obj::pool<TestPool>::create(testPath, "test", PMEMOBJ_MIN_POOL);
//  {
//    {
//      //      auto tx = pmem::obj::flat_transaction::manual(pool);
//      new(pool.root().get()) TestPool();
//      //      pmemobj_tx_commit();
//    }
//
//    auto threadPool = new ThreadPool{10, pool.handle()};
//
//    GlobalTimestamp globalTimestamp{&pool.root()->nvGlobalTimestamp};
//    PmSkiplistDramEntrance pmSkiplist{7, &(pool.root()->nvmEntrance), false,
//                                      &globalTimestamp, threadPool};
//
//    std::vector<PmSkiplistNvmSingleNode *> newNodes;
//
//    char buffer[2][100];
//    {
//      auto tx = pmem::obj::flat_transaction::manual{pool};
//      int k = 0;
//      for(int i = 1; i < 11; ++i) {
//        PmSkiplistNvmSingleNodeBuilder nvmNodeBuilder{};
//
//        std::stringstream s;
//        s << k << 'a';
//        auto str0 = s.str();
//        memcpy(buffer[0], str0.data(), str0.size());
//        *(uint64_t *)(buffer[0] + str0.size()) = (k << 8) | 0x1;
//
//        std::stringstream s1;
//        s1 << k++ << 'b';
//        auto str1 = s1.str();
//        memcpy(buffer[1], str1.data(), str1.size());
//        *(uint64_t *)(buffer[1] + str1.size()) = (k << 8) | 0x1;
//
//        auto newNode = nvmNodeBuilder.TestGenerate(
//          UINT64_MAX, i, leveldb::Slice{buffer[0], str0.size() + 8},
//          leveldb::Slice{buffer[1], str1.size() + 8});
//        newNodes.emplace_back(newNode);
//      }
//
//      pmSkiplist.ReplaceNodes(newNodes, {});
//
//      for(int i = 0; i < 10; ++i)
//        std::cout << newNodes[i]->GetPmSsTable()->GetMinUserKey().ToString()
//                  << " "
//                  << newNodes[i]->GetPmSsTable()->GetMaxUserKey().ToString()
//                  << "(" << newNodes[i]->bornSq << ")" << std::endl;
//
//      pmemobj_tx_commit();
//    }
//
//    std::cout << std::endl;
//
//    {
//      newNodes.clear();
//      auto tx = pmem::obj::flat_transaction::manual{pool};
//      int k = 0;
//      for(int i = 1; i < 11; ++i) {
//        PmSkiplistNvmSingleNodeBuilder nvmNodeBuilder{};
//
//        std::stringstream s;
//        s << k << "c";
//        auto str0 = s.str();
//        memcpy(buffer[0], str0.data(), str0.size());
//        *(uint64_t *)(buffer[0] + str0.size()) = (k << 8) | 0x1;
//
//        std::stringstream s1;
//        s1 << k++ << "d";
//        auto str1 = s1.str();
//        memcpy(buffer[1], str1.data(), str1.size());
//        *(uint64_t *)(buffer[1] + str1.size()) = (k << 8) | 0x1;
//
//        auto newNode = nvmNodeBuilder.TestGenerate(
//          UINT64_MAX, i + 100, leveldb::Slice{buffer[0], str0.size() + 8},
//          leveldb::Slice{buffer[1], str1.size() + 8});
//        newNodes.emplace_back(newNode);
//      }
//
//      pmSkiplist.ReplaceNodes(newNodes, {});
//
//      for(int i = 0; i < 10; ++i)
//        std::cout << newNodes[i]->GetPmSsTable()->GetMinUserKey().ToString()
//                  << " "
//                  << newNodes[i]->GetPmSsTable()->GetMaxUserKey().ToString()
//                  << "(" << newNodes[i]->bornSq << ")" << std::endl;
//
//      pmemobj_tx_commit();
//
//      MFence();
//    }
//
//    {
//      {
//        auto currentNode = &pmSkiplist.dummyHead;
//        currentNode = currentNode->GetNext(0);
//        while(currentNode != &pmSkiplist.dummyTail) {
//          currentNode->TestPrint();
//          std::cout << std::endl;
//          currentNode = currentNode->GetNext(0);
//        }
//      }
//
//      std::cout << std::endl << "globalTimestamp: ";
//      globalTimestamp.TestPrint();
//      std::cout << "currentTime = " << globalTimestamp.GetCurrentTimeFromDram()
//                << std::endl;
//      std::cout << "deadTime = " << globalTimestamp.GetDeadTime() << std::endl;
//
//      auto tx = pmem::obj::flat_transaction::manual{pool};
//      pmSkiplist.ReplaceNodes({}, newNodes);
//      pmemobj_tx_commit();
//    }
//
//    {
//      {
//        auto currentNode = &pmSkiplist.dummyHead;
//        currentNode = currentNode->GetNext(0);
//        while(currentNode != &pmSkiplist.dummyTail) {
//          currentNode->TestPrint();
//          std::cout << std::endl;
//          currentNode = currentNode->GetNext(0);
//        }
//      }
//
//      std::cout << std::endl << "globalTimestamp: ";
//      globalTimestamp.TestPrint();
//      std::cout << "currentTime = " << globalTimestamp.GetCurrentTimeFromDram()
//                << std::endl;
//      std::cout << "deadTime = " << globalTimestamp.GetDeadTime() << std::endl;
//    }
//
//    pmSkiplist.ScheduleBackgroundGcAndSmr();
//    std::this_thread::sleep_for(std::chrono::seconds{1});
//    globalTimestamp.CurrentTimeAddOne();
//    globalTimestamp.CurrentTimeAddOne();
//    globalTimestamp.CurrentTimeAddOne();
//    pmSkiplist.ScheduleBackgroundGcAndSmr();
//
//    std::cout << std::endl;
//
//    delete threadPool;
//
//    {
//      {
//        auto currentNode = &pmSkiplist.dummyHead;
//        currentNode = currentNode->GetNext(0);
//        while(currentNode != &pmSkiplist.dummyTail) {
//          currentNode->TestPrint();
//          std::cout << std::endl;
//          currentNode = currentNode->GetNext(0);
//        }
//      }
//
//      std::cout << std::endl << "globalTimestamp: ";
//      globalTimestamp.TestPrint();
//      std::cout << "currentTime = " << globalTimestamp.GetCurrentTimeFromDram()
//                << std::endl;
//      std::cout << "deadTime = " << globalTimestamp.GetDeadTime() << std::endl;
//    }
//  }
//  pool.close();
//  return 0;
//}