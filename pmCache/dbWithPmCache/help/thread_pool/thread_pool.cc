//
// Created by jxz on 22-7-12.
//
#include "thread_pool.h"
#include "iostream"
#include "pmCache/dbWithPmCache/help/pmem.h"
#include <sys/syscall.h>

leveldb::pmCache::ThreadPool::ThreadPool(uint32_t workerCount, void *pool) {
  assert(pool != nullptr);
  workerCount_ = workerCount;
  while(workerCount-- > 0) {
    workers_.emplace_back([this](void *p) { WorkerMain_(p); }, pool);
  }
}

leveldb::pmCache::ThreadPool::~ThreadPool() {
  int n = (int)workerCount_;
  while(n-- > 0)
    PostWork({[] {}, true});
  for(auto &w : workers_)
    w.join();
}

void leveldb::pmCache::ThreadPool::PostWork(pmCache::Work &&work) {
  auto work1 = std::make_unique<pmCache::Work>(work);
  Push_(std::move(work1));
}

void leveldb::pmCache::ThreadPool::Push_(std::unique_ptr<pmCache::Work> &&work) {
  workQueue_.push(work.release());
  {
    std::lock_guard<std::mutex> lock(workSignalMutex_);
    workQueueSize_.fetch_add(1, std::memory_order_release);
  }
  workSignal_.notify_one();
}

#pragma clang diagnostic push
// #pragma ide diagnostic ignored "DanglingPointer"

void leveldb::pmCache::ThreadPool::WorkerMain_(void *pool) {
  Work *workPtr = nullptr;
  _pobj_cached_pool.pop = (pmemobjpool *)pool;
  while(true) {
    while(workQueue_.pop(workPtr)) {
      workQueueSize_.fetch_sub(1, std::memory_order_release);
      if(workPtr->isTerminal) {
        delete workPtr;
        return;
      }
      (*workPtr)();
      delete workPtr;
    }
    std::unique_lock<std::mutex> lock(workSignalMutex_);
    workSignal_.wait(lock, [this]() {
      return workQueueSize_.load(std::memory_order_acquire) > 0;
    });
  }
}

void leveldb::pmCache::ThreadPool::Push_(leveldb::pmCache::Work *work) {
  workQueue_.push(work);
  {
    std::lock_guard<std::mutex> lock(workSignalMutex_);
    workQueueSize_.fetch_add(1, std::memory_order_release);
  }
  workSignal_.notify_one();
}

#pragma clang diagnostic pop
