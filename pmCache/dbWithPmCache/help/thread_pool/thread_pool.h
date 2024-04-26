//
// Created by jxz on 22-7-12.
//

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_THREAD_POOL_THREAD_POOL_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_THREAD_POOL_THREAD_POOL_H_

#include "boost/lockfree/queue.hpp"
#include "condition_variable"
#include "functional"
#include "thread"
#include "vector"
#include <iostream>
#include <mutex>
#include <sstream>

namespace leveldb::pmCache {
  class Work {
   public:
    std::function<void()> func;
    bool isTerminal = false;

    void operator()() const { func(); }
  };

  class ThreadPool {
   public:
    explicit ThreadPool(uint32_t workerCount, void *pool);
    ~ThreadPool();

   public:
    void PostWork(Work &&);

    inline void PostWork(Work *work) { Push_(work); }

    auto GetWorkerCnt() const { return workerCount_; }

   private:
    std::vector<std::thread> workers_;
    uint32_t workerCount_{0};
    mutable std::mutex workSignalMutex_;
    std::condition_variable workSignal_;
    boost::lockfree::queue<Work *> workQueue_{0};
    std::atomic<long long> workQueueSize_{0};

    void Push_(std::unique_ptr<Work> &&work);

    void Push_(Work *work);

    void WorkerMain_(void *pool);
  };
} // namespace leveldb::pmCache

#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_THREAD_POOL_THREAD_POOL_H_
