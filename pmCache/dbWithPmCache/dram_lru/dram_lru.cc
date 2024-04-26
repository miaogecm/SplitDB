//
// Created by jxz on 22-7-12.
//

#include "dram_lru.h"
#include "dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include <cstdint>

namespace leveldb::pmCache {
  void DramLruShard::DeleteNodeWithoutLock(DramLruNode *node)
    EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    assert(node);
    auto prevNode = node->prevNode;
    auto nextNode = node->nextNode;
    prevNode->nextNode = nextNode;
    nextNode->prevNode = prevNode;
    totalSize -= node->nodeSize;
  }

  void DramLruShard::InsertNodeWithoutLock(DramLruNode *node)
    EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    auto prevNode = &dummyHead;
    auto nextNode = prevNode->nextNode;
    prevNode->nextNode = node;
    node->nextNode = nextNode;
    node->prevNode = prevNode;
    nextNode->prevNode = node;
    totalSize += node->nodeSize;
  }

  void DramLruShard::PromoteNodeWithoutLock(DramLruNode *node)
    EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    auto prevNode = node->prevNode;
    auto nextNode = node->nextNode;
    prevNode->nextNode = nextNode;
    nextNode->prevNode = prevNode;

    prevNode = &dummyHead;
    nextNode = prevNode->nextNode;
    prevNode->nextNode = node;
    node->nextNode = nextNode;
    node->prevNode = prevNode;
    nextNode->prevNode = prevNode;
  }

  auto DramLruShard::GetColdestNode() -> DramLruNode * {
    auto ret = dummyTail.prevNode;
    if(ret != &dummyHead)
      return ret;
    else
      return nullptr;
  }

  void DramLruShard::Prepare(uint64_t size) {
    mutex.Lock();
    dummyHead.nextNode = &dummyTail;
    dummyTail.prevNode = &dummyHead;
    totalSize = 0;
    //    maxSize = size;
    mutex.Unlock();
  }

  void DramLruShard::Clear() {
    auto node = dummyHead.nextNode;
    while(node != &dummyTail) {
      auto nextNode = node->nextNode;
      delete node;
      node = nextNode;
    }
  }

  DramLru::DramLru(uint64_t totalSize) : maxSize(totalSize) {
    int64_t sizeBaseShard = LSM_NVM_BASE_LEVEL_SIZE;
    int64_t k = 1;
    for(auto &shard : shards) {
      shard.Prepare(sizeBaseShard * k);
      k *= LSM_NVM_LEVEL_SIZE_MULTIPLE;
    }
  }

  void DramLru::InsertNode(DramLruNode *newNode) {
    if(newNode->whichShard > (PM_LRU_SHARD_COUNT - 1)) {
      if(PM_LRU_SHARD_COUNT == 1)
        newNode->whichShard = 0;
      else
        newNode->whichShard = rand() % PM_LRU_SHARD_COUNT;
    }
    // NOLINT(cert-msc50-cpp)

    {
      shards[newNode->whichShard].mutex.Lock();
      shards[newNode->whichShard].InsertNodeWithoutLock(newNode);
      shards[newNode->whichShard].mutex.Unlock();
    }
    totalSize.fetch_add(newNode->nodeSize);
  }

  void DramLru::DeleteNodeWithoutFreeDramLruNode(DramLruNode *delNode) {
    auto whichShard = delNode->whichShard;
    assert(0 <= whichShard && whichShard <= PM_LRU_SHARD_COUNT);
    {
      shards[whichShard].mutex.Lock();
      shards[whichShard].DeleteNodeWithoutLock(delNode);
      shards[whichShard].mutex.Unlock();
    }
    totalSize.fetch_add(-delNode->nodeSize);
  }

  void DramLru::Clear() {
    for(auto &shard : shards)
      shard.Clear();
  }

  DramLru::~DramLru() { Clear(); }

  void DramLru::MakeSpaceIterator::SeekFirst() {
    currentNode = nullptr;
    if(madeSpace < needSpace &&
       lru->totalSize.load(std::memory_order_relaxed) > 0) {
      DramLruShard *lruShard;
      while(!currentNode) {
        if(PM_LRU_SHARD_COUNT == 1)
          lruShard = &lru->shards[0];
        else
          lruShard = &lru->shards[(rand() % PM_LRU_SHARD_COUNT)];
        lruShard->mutex.Lock();
        if(lruShard->totalSize > 0)
          currentNode = lruShard->GetColdestNode();
        lruShard->mutex.Unlock();
      }
    }
    assert(madeSpace >= 0);
  }

  bool DramLru::MakeSpaceIterator::Valid() const {
    return currentNode != nullptr;
  }

  DramLru::MakeSpaceIterator::MakeSpaceIterator(int64_t neededSpace,
                                                DramLru *whichLru)
      : lru(whichLru),
        //        needSpace(
        //          (int64_t)(neededSpace > lru->maxSize ? lru->maxSize :
        //          neededSpace) -
        //          ((int64_t)lru->maxSize -
        //           (int64_t)lru->totalSize.load(std::memory_order_relaxed)))
        needSpace((long long)neededSpace +
                  (long long)lru->totalSize.load(std::memory_order_relaxed) -
                  (long long)lru->maxSize) {
    if(needSpace > (long long)lru->maxSize)
      needSpace = (long long)lru->maxSize;
    assert(madeSpace == 0);
  }
} // namespace leveldb::pmCache