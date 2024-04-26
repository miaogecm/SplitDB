////
//// Created by jxz on 22-7-12.
////
//
// #include "pm_time_stamp.h"
//
// void leveldb::pmCache::PmTimeStamp::initDramTimeStamp() {
//  DramCacheTimestampNode *dramTimeStamp;
//  dramTimeStamp = new DramCacheTimestampNode;
//  dramTimeStamp->timestamp = currentTime_;
//  dramTimeStamp->referenceCount = 0;
//  dramTimeStamp->prevNode = &registerHead_;
//  dramTimeStamp->nextNode = &registerTail_;
//  registerHead_.nextNode = dramTimeStamp;
//  registerTail_.prevNode = dramTimeStamp;
//  meta = new TimeStampDramMeta{};
//  // #ifndef NDEBUG
//  //     meta->toBeWorkerMutex.lock();
//  //     meta->toBeWorkerMutex.unlock();
//  // #endif
//}
//
// leveldb::pmCache::PmTimeStamp::PmTimeStamp() {
//  std::lock_guard<std::mutex> lockGuard{mutex_};
//  pmem::detail::transaction_base<true>::snapshot(&currentTime_);
//  currentTime_ = 2;
//  _mm_clflush(&currentTime_);
//  //    initDramTimeStamp();
//}
//
// uint64_t leveldb::pmCache::PmTimeStamp::initWhenOpen() {
//  uint64_t ret;
//  pthread_mutex_init(mutex_.native_handle(), nullptr);
//  std::lock_guard<std::mutex> lockGuard{mutex_};
//  auto ct = currentTime_;
//  __atomic_store_n(&currentTime_, ct + 2, __ATOMIC_RELAXED);
//  _mm_sfence();
//  _mm_clflush(&currentTime_);
//  ret = currentTime_;
//  initDramTimeStamp();
//  return ret;
//}
//
// leveldb::pmCache::Timestamp::Timestamp(uint64_t t,
//                                       pmCache::PmTimeStamp::DramCacheTimestampNode
//                                       *nodePtr)
//    : thisTime(t), whichNode(nodePtr) {}
//
// leveldb::pmCache::Timestamp::Timestamp(uint64_t t) : thisTime(t),
// whichNode(nullptr) {}
