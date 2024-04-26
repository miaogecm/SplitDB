//

//

#include "NVTimestamp.h"
#include "dbWithPmCache/help/pmem.h"

#include <iostream>
#include <libpmemobj/base.h>

leveldb::pmCache::GlobalTimestamp::GlobalTimestamp(
  leveldb::pmCache::NvGlobalTimestamp *nvCurrentTime)
    : nvCurrentTime_(nvCurrentTime) {
  globalTimestampDramHandle.registerHead.nextNode->thisTime =
    nvCurrentTime_->currentTime;
  CurrentTimeAddOneWithOutLock_();
  deadTime_.store(globalTimestampDramHandle.registerHead.nextNode->thisTime -
                  1);
}

void leveldb::pmCache::GlobalTimestamp::DereferenceTimestampWithoutLock_(
  leveldb::pmCache::GlobalTimestamp::TimestampToken *timestampToken) {
  if(timestampToken->thisTime) {
    auto whichNode = timestampToken->whichNode;

    if((--whichNode->referenceCount) == 0) {
      auto currentTime = GetCurrentTimeFromDram_();
      if(whichNode->thisTime != currentTime) {
        auto prevNode = whichNode->prevNode;
        auto nextNode = whichNode->nextNode;
        prevNode->nextNode = nextNode;
        nextNode->prevNode = prevNode;
        delete whichNode;
        if(nextNode == &globalTimestampDramHandle.registerTail)
          deadTime_.store(
            globalTimestampDramHandle.registerTail.prevNode->thisTime - 1);
      }
    }
    timestampToken->thisTime = 0;
  }
}

void leveldb::pmCache::GlobalTimestamp::ReferenceTimestampWithoutLock_(
  DramTimestampNode *timeToReference,
  leveldb::pmCache::GlobalTimestamp::TimestampToken *timestampToken) {
  ++(timeToReference->referenceCount);
  timestampToken->whichNode = timeToReference;
  timestampToken->thisTime = timeToReference->thisTime;
}

void leveldb::pmCache::GlobalTimestamp::HandleSelfTask_(
  const leveldb::pmCache::GlobalTimestamp::GlobalTimestampDramHandle::
    TimestampWorkTask &task) {
  auto firstNode = globalTimestampDramHandle.registerHead.nextNode;
  switch(task.type) {
  case GlobalTimestampDramHandle::TimestampWorkTask::DEREFERENCE:
    DereferenceTimestampWithoutLock_(task.timestampToken);
    break;
  case GlobalTimestampDramHandle::TimestampWorkTask::REFERENCE:
    ReferenceTimestampWithoutLock_(firstNode, task.timestampToken);
    break;
  case GlobalTimestampDramHandle::TimestampWorkTask::UNKNOWN: break;
  case GlobalTimestampDramHandle::TimestampWorkTask::ADD_ONE:
    CurrentTimeAddOneWithOutLock_();
    break;
  }
}

void leveldb::pmCache::GlobalTimestamp::HandleRestOfTasks_() {
  int64_t currentPendingTaskCount = 0;
  while(!globalTimestampDramHandle.pendingTaskCount.compare_exchange_strong(
    currentPendingTaskCount, 0)) {}
  auto firstNode = globalTimestampDramHandle.registerHead.nextNode;
  if(currentPendingTaskCount > 0) {
    GlobalTimestampDramHandle::TimestampWorkTask *task;
    for(auto i = 0; i < currentPendingTaskCount; ++i) {
      globalTimestampDramHandle.tasks.pop(task);

      switch(task->type) {
      case GlobalTimestampDramHandle::TimestampWorkTask::DEREFERENCE:
        DereferenceTimestampWithoutLock_(task->timestampToken);
        break;
      case GlobalTimestampDramHandle::TimestampWorkTask::REFERENCE:
        ReferenceTimestampWithoutLock_(firstNode, task->timestampToken);
        break;
      case GlobalTimestampDramHandle::TimestampWorkTask::ADD_ONE:
        CurrentTimeAddOneWithOutLock_();
        firstNode = globalTimestampDramHandle.registerHead.nextNode;
        break;
      case GlobalTimestampDramHandle::TimestampWorkTask::UNKNOWN: break;
      }
      task->done = true;
    }
  }
}

void leveldb::pmCache::GlobalTimestamp::CurrentTimeAddOneWithOutLock_() {

  auto newDramTimeStampNode =
    new DramTimestampNode{GetCurrentTimeFromDram_() + 1, 0,
                          globalTimestampDramHandle.registerHead.nextNode,
                          &globalTimestampDramHandle.registerHead};
  globalTimestampDramHandle.registerHead.nextNode->prevNode =
    newDramTimeStampNode;
  globalTimestampDramHandle.registerHead.nextNode = newDramTimeStampNode;
  auto nodeAfterNewNode = newDramTimeStampNode->nextNode;
  if(nodeAfterNewNode->referenceCount == 0) {
    auto prevNode = nodeAfterNewNode->prevNode;
    auto nextNode = nodeAfterNewNode->nextNode;
    prevNode->nextNode = nextNode;
    nextNode->prevNode = prevNode;
    delete nodeAfterNewNode;
    deadTime_.store(newDramTimeStampNode->thisTime - 1);
  }
  __atomic_store_n(&nvCurrentTime_->currentTime, newDramTimeStampNode->thisTime,
                   __ATOMIC_RELAXED);
  currentTime_.store(newDramTimeStampNode->thisTime, std::memory_order_release);
  assert(_pobj_cached_pool.pop);
  pmemobj_flush(_pobj_cached_pool.pop, &nvCurrentTime_->currentTime,
                sizeof(nvCurrentTime_->currentTime));
}

void leveldb::pmCache::GlobalTimestamp::HandleReferenceOrDereferenceOrAdd_(
  leveldb::pmCache::GlobalTimestamp::TimestampToken &t,
  leveldb::pmCache::GlobalTimestamp::GlobalTimestampDramHandle::
    TimestampWorkTask::TimestampWorkType taskType) {
  bool someOneWorking = false;
  if(globalTimestampDramHandle.working.compare_exchange_strong(someOneWorking,
                                                               true))
    HandleSelfAndAllTasksInQueue_({&t, taskType});
  else {
    GlobalTimestampDramHandle::TimestampWorkTask newTask{&t, taskType};
    globalTimestampDramHandle.tasks.push(&newTask);
    SFence();
    globalTimestampDramHandle.pendingTaskCount.fetch_add(1);
    std::unique_lock<std::mutex> toBeWorkerMutex(
      globalTimestampDramHandle.toBeWorkerMutex);
    globalTimestampDramHandle.toBeWorkerCondition.wait(
      toBeWorkerMutex, [&newTask, this] {
        if(newTask.done)
          return true;
        else {
          bool someOneWorking = false;
          if(this->globalTimestampDramHandle.working.compare_exchange_strong(
               someOneWorking, true))
            return true;
          return false;
        }
      });
    if(newTask.done)
      return;
    else
      while(!newTask.done)
        HandleAllTasksInQueue_();
  }
  globalTimestampDramHandle.toBeWorkerMutex.lock();
  globalTimestampDramHandle.working.store(false);
  globalTimestampDramHandle.toBeWorkerMutex.unlock();
  globalTimestampDramHandle.toBeWorkerCondition.notify_all();
}

void leveldb::pmCache::GlobalTimestamp::TestPrint() {
  auto currentNode = globalTimestampDramHandle.registerHead.nextNode;
  while(currentNode != &globalTimestampDramHandle.registerTail) {
    std::cout << currentNode->thisTime << " " << currentNode->referenceCount
              << "; ";
    currentNode = currentNode->nextNode;
  }
  std::cout << std::endl;
}

leveldb::pmCache::GlobalTimestamp::GlobalTimestampDramHandle::
  TimestampWorkTask::TimestampWorkTask(
    leveldb::pmCache::GlobalTimestamp::TimestampToken *t,
    leveldb::pmCache::GlobalTimestamp::GlobalTimestampDramHandle::
      TimestampWorkTask::TimestampWorkType type)
    : timestampToken(t), type(type) {}

leveldb::pmCache::GlobalTimestamp::DramTimestampNode::DramTimestampNode(
  uint64_t thisTime, uint32_t referenceCount)
    : thisTime(thisTime), referenceCount(referenceCount), nextNode(nullptr),
      prevNode(nullptr) {}

leveldb::pmCache::GlobalTimestamp::DramTimestampNode::DramTimestampNode(
  uint64_t thisTime, uint32_t referenceCount,
  leveldb::pmCache::GlobalTimestamp::DramTimestampNode *nextNode,
  leveldb::pmCache::GlobalTimestamp::DramTimestampNode *prevNode)
    : thisTime(thisTime), referenceCount(referenceCount), nextNode(nextNode),
      prevNode(prevNode) {}

leveldb::pmCache::GlobalTimestamp::DramTimestampNode::DramTimestampNode(
  uint32_t referenceCount)
    : referenceCount(referenceCount) {}

leveldb::pmCache::GlobalTimestamp::GlobalTimestampDramHandle::
  ~GlobalTimestampDramHandle() {
  auto currentNode = registerHead.nextNode;
  while(currentNode != &registerTail) {
    auto nextNode = currentNode->nextNode;
    delete currentNode;
    currentNode = nextNode;
  }
}

leveldb::pmCache::TimestampRefGuard::TimestampRefGuard(
  leveldb::pmCache::GlobalTimestamp *globalTimestamp) {
  globalTimestamp->ReferenceTimestamp(t);
  globalTimestamp_ = globalTimestamp;
}

leveldb::pmCache::TimestampRefGuard::~TimestampRefGuard() {
  globalTimestamp_->DereferenceTimestamp(t);
}
