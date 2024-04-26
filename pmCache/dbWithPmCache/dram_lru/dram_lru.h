//
// Created by jxz on 22-7-12.
//

#ifndef LEVELDB_DRAM_LRU_H
#define LEVELDB_DRAM_LRU_H

#include "atomic"
#include "port/port_stdcxx.h"
#include <mutex>

namespace leveldb::pmCache::skiplistWithFanOut {
  class PmSkiplistNvmSingleNode;
}

namespace leveldb::pmCache {
  //  class PmSsTableNodeInSkiplist;

  class DramLruShard;

  class DramLruNode {
   public:
    friend class DramLru;
    friend class DramLruShard;

    DramLruNode *nextNode = nullptr;
    DramLruNode *prevNode = nullptr;

   public:
    uint8_t whichShard = UINT8_MAX;
    uint8_t whichLevel = 0;
    //    skiplistWithFanOut::PmSkiplistNvmSingleNode *nvmNode = nullptr;
    size_t nodeSize = 0;
    uint64_t fileNum;
  };

  class DramLruShard {
   public:
    void
    PromoteNodeWithoutLock(DramLruNode *node) EXCLUSIVE_LOCKS_REQUIRED(mutex);

    void
    DeleteNodeWithoutLock(DramLruNode *node) EXCLUSIVE_LOCKS_REQUIRED(mutex);

    void
    InsertNodeWithoutLock(DramLruNode *node) EXCLUSIVE_LOCKS_REQUIRED(mutex);

    auto GetColdestNode() -> DramLruNode *;

    void Prepare(uint64_t size);

    void Clear();

    //    size_t maxSize = 0;
    std::atomic<size_t> totalSize = 0;
    //    size_t maxSize GUARDED_BY(mutex) = 0;
    port::Mutex mutex{};
    DramLruNode dummyHead{};
    DramLruNode dummyTail{};
  };

  class DramLru {
   public:
    class MakeSpaceIterator {
     public:
      MakeSpaceIterator(int64_t neededSpace, DramLru *whichLru);

      [[nodiscard]] auto Valid() const -> bool;

      void SeekFirst();

      inline void Next() { SeekFirst(); }

      DramLru *lru;
      DramLruNode *currentNode = nullptr;
      int64_t needSpace = 0;
      int64_t madeSpace = 0;
      int currentShard = PM_LRU_SHARD_COUNT - 1;
    };

    explicit DramLru(uint64_t totalSize);

    ~DramLru();

    void InsertNode(DramLruNode *newNode);

    void DeleteNodeWithoutFreeDramLruNode(DramLruNode *delNode);

    void Clear();

    DramLruShard shards[PM_LRU_SHARD_COUNT];
    std::atomic<size_t> totalSize = 0;
    const size_t maxSize;
  };

  ///////////////////////////////////////////////////////////////////////////////////////////////////
} // namespace leveldb::pmCache
#endif // LEVELDB_DRAM_LRU_H
