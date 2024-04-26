////
//// Created by jxz on 22-7-12.
////
// #include "pmCache/dbWithPmCache/help/pmem.h"
// #include "pmCache/dbWithPmCache/help/thread_pool/thread_pool.h"
// #include <filesystem>
// #include <gtest/gtest.h>
//
// TEST(threadPoolTest, test1) {
//   auto pool = new leveldb::pmCache::ThreadPool(10, nullptr);
//   for(auto i = 0; i < 10; i++)
//     pool->postWork({[i] { printf("%d\n", i); }, false});
//   for(auto i = 0; i < 10; i++)
//     pool->postWork({nullptr, true});
//   while(pool->haveWorker())
//     continue;
//   delete pool;
// }
//
// TEST(threadPoolTest, test2) {
//   auto pool = new leveldb::pmCache::ThreadPool(16, nullptr);
//   int ans = 0;
//   for(auto i = 0; i < 10000; i++)
//     pool->postWork({[&ans, i] { i % 2 == 0 ? ans++ : ans--; }, false});
//   while(pool->haveWorker())
//     continue;
//   std::cout << ans << std::endl;
//   delete pool;
// }
//
// TEST(threadPoolTest, test3) {
//   auto pool = new leveldb::pmCache::ThreadPool(16, nullptr);
//   int ans = 0;
//   for(auto i = 0; i < 16; i++)
//     pool->postWork({[&ans] {
//                       for(long long i = 0; i <= 100000; i++)
//                         i % 2 == 0 ? ans++ : ans--;
//                     },
//                     false});
//   while(pool->haveWorker())
//     continue;
//   std::cout << ans << std::endl;
//   delete pool;
// }
//
// int main(int argc, char **argv) {
//   printf("Running main() from %s\n", __FILE__);
//   testing::InitGoogleTest(&argc, argv);
//   return RUN_ALL_TESTS();
// }