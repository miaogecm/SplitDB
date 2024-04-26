#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_PM_TIMESTAMP_OPT_NVTIMESTAMP_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_PM_TIMESTAMP_OPT_NVTIMESTAMP_H_

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <condition_variable>
#include <cstdint>
#include <mutex>

namespace leveldb::pmCache {
  class NvGlobalTimestamp {
   public:
    uint64_t currentTime = 2;

    inline uint64_t GetCurrentTime() {
      return __atomic_load_n(&currentTime, __ATOMIC_RELAXED);
    }
  };

  class GlobalTimestamp {
   public:
    class DramTimestampNode {
     public:
      DramTimestampNode() = delete;

      explicit DramTimestampNode(uint32_t referenceCount);

      DramTimestampNode(uint64_t thisTime, uint32_t referenceCount);

      DramTimestampNode(uint64_t thisTime, uint32_t referenceCount,
                        DramTimestampNode *nextNode,
                        DramTimestampNode *prevNode);

      uint64_t thisTime{};
      uint32_t referenceCount{};
      DramTimestampNode *nextNode{}, *prevNode{};
    };

    class TimestampToken {
     public:
      uint64_t thisTime;
      DramTimestampNode *whichNode;
    };

    class GlobalTimestampDramHandle {
     public:
      class TimestampWorkTask {
       public:
        enum TimestampWorkType : char {
          DEREFERENCE,
          REFERENCE,
          ADD_ONE,
          UNKNOWN
        };

        TimestampToken *timestampToken;
        bool done = false;
        TimestampWorkType type = UNKNOWN;

        TimestampWorkTask(TimestampToken *t, TimestampWorkType type);
      };

      GlobalTimestampDramHandle() {
        registerHead.nextNode = &registerTail;
        registerTail.prevNode = &registerHead;
      }

      ~GlobalTimestampDramHandle();

      std::atomic<bool> working{};
      std::atomic<int64_t> pendingTaskCount{};
      boost::lockfree::queue<TimestampWorkTask *> tasks{0};
      std::mutex toBeWorkerMutex{};
      std::condition_variable toBeWorkerCondition{};

      DramTimestampNode registerHead{UINT32_MAX}, registerTail{UINT32_MAX};
    };

    void InitWhenOpen() {}

    explicit GlobalTimestamp(NvGlobalTimestamp *nvCurrentTime);

    GlobalTimestamp() = delete;

    [[nodiscard]] inline uint64_t GetCurrentTimeFromNvm() const {
      return nvCurrentTime_->GetCurrentTime();
    }

    [[nodiscard]] inline uint64_t GetDeadTime() const {
      return deadTime_.load(std::memory_order_relaxed);
    }

    void TestPrint();

    inline void DereferenceTimestamp(TimestampToken &t) {
      HandleReferenceOrDereferenceOrAdd_(
        t, GlobalTimestampDramHandle::TimestampWorkTask::TimestampWorkType::
             DEREFERENCE);
    }

    inline void ReferenceTimestamp(TimestampToken &t) {
      HandleReferenceOrDereferenceOrAdd_(
        t, GlobalTimestampDramHandle::TimestampWorkTask::REFERENCE);
    }

    inline void CurrentTimeAddOne() {
      HandleReferenceOrDereferenceOrAdd_(
        dummyT_,
        GlobalTimestampDramHandle::TimestampWorkTask::TimestampWorkType::ADD_ONE);
    }

    [[nodiscard]] inline uint64_t GetCurrentTimeFromDram() {
      return currentTime_.load(std::memory_order_acquire);
    }

    GlobalTimestampDramHandle globalTimestampDramHandle{};

   private:
    [[nodiscard]] inline uint64_t GetCurrentTimeFromDram_() const {
      return globalTimestampDramHandle.registerHead.nextNode->thisTime;
    }

    void CurrentTimeAddOneWithOutLock_();

    void DereferenceTimestampWithoutLock_(TimestampToken *timestampToken);

    static void
    ReferenceTimestampWithoutLock_(DramTimestampNode *timeToReference,
                                   TimestampToken *timestampToken);

    inline void HandleSelfAndAllTasksInQueue_(
      const GlobalTimestampDramHandle::TimestampWorkTask &selfTask) {
      HandleSelfTask_(selfTask);
      HandleRestOfTasks_();
    }

    void HandleRestOfTasks_();

    inline void HandleAllTasksInQueue_() { HandleRestOfTasks_(); }

    void
    HandleSelfTask_(const GlobalTimestampDramHandle::TimestampWorkTask &task);

    void HandleReferenceOrDereferenceOrAdd_(
      TimestampToken &t,
      GlobalTimestampDramHandle::TimestampWorkTask::TimestampWorkType taskType);

    TimestampToken dummyT_{};
    NvGlobalTimestamp *nvCurrentTime_{};
    std::atomic<uint64_t> deadTime_{};
    std::atomic<uint64_t> currentTime_{};
  };

  class TimestampRefGuard {
   public:
    explicit TimestampRefGuard(GlobalTimestamp *globalTimestamp);

    TimestampRefGuard() = delete;

    TimestampRefGuard(const TimestampRefGuard &) = delete;

    TimestampRefGuard &operator=(const TimestampRefGuard &) = delete;

    ~TimestampRefGuard();

    GlobalTimestamp::TimestampToken t{};

   private:
    GlobalTimestamp *globalTimestamp_{};
  };
} // namespace leveldb::pmCache

#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_PM_TIMESTAMP_OPT_NVTIMESTAMP_H_
