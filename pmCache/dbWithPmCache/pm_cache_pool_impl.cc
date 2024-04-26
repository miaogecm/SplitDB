//
// Created by jxz on 22-7-27.
//

#include "pm_cache_pool_impl.h"

#include "dbWithPmCache/help/pmem.h"
#include <memory>

leveldb::pmCache::PmCachePoolRoot::PmCachePoolRoot() {
  for(unsigned long &i : filesOff_)
    i = 0;
  Prepare_();
}

void leveldb::pmCache::PmCachePoolRoot::InitWhenOpen() {
  auto globalTimestamp = new GlobalTimestamp(&pmTimeStamp_);
  globalTimestamp->InitWhenOpen();
  threadPool = new ThreadPool(PMCACHE_GC_THREAD_NUM, _pobj_cached_pool.pop);

  for(int i = 0; i < config::kNumLevels; ++i) {
    nvmSkiplistDramEntrance_.at(i) =
      new pmCache::skiplistWithFanOut::PmSkiplistDramEntrance(
        config::kMaxIndexHeights[i], nvmSkiplistEntrance_.at(i).GetVPtr(),
        i == 0, globalTimestamp, threadPool);
    auto currentNode =
      nvmSkiplistEntrance_.at(i).GetVPtr()->dummyHead.nextNode.GetVPtr();

    auto dramEntrance = nvmSkiplistDramEntrance_.at(i);
    auto &nvmHead = dramEntrance->nvmEntrance->dummyHeadOfNodesToFree;


    auto dummyHeadToSmr = &dramEntrance->nvmEntrance->dummyHeadOfNodesToFree;
    while(dummyHeadToSmr != nullptr) {
      dramEntrance->nvmEntrance
        ->nextPersistentPointerOfLastNodeInListOfNvmNodesToFree =
        &dummyHeadToSmr->nextNode;
      dummyHeadToSmr = dummyHeadToSmr->nextNode.GetVPtr();
    }

    while(!dramEntrance->IsNvmNodeToSmrEmptyWithoutLock()) {
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *nodeToSmr = nullptr;
      {
        skiplistWithFanOut::SpinLockGuard const spinLockGuard{
          dramEntrance->spinLockForSmr};
        if(!dramEntrance->IsNvmNodeToSmrEmptyWithoutLock()) {
          nodeToSmr = nvmHead.nextNode.GetVPtr();
          //          assert(pmemobj_tx_stage() == TX_STAGE_WORK);
          dramEntrance->nvmEntrance->PopNodeToSmrWithoutLock();

          if(nodeToSmr) {
            pmem::detail::destroy<skiplistWithFanOut::PmSkiplistNvmSingleNode>(
              *nodeToSmr);
            auto oidToFree = pmemobj_oid(nodeToSmr);
            pmemobj_free(&oidToFree);
          }
          //          pmem::obj::delete_persistent<
          //            skiplistWithFanOut::PmSkiplistNvmSingleNode>(nodeToSmr);
          //          PrintPmAllocate("-1_init", nodeToSmr, 0);
        } else {
          break;
        }
      }
    }

    std::vector<pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *>
      validNodes;
    auto endNode = &nvmSkiplistEntrance_[i].GetVPtr()->dummyTail;
    pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *nextNode = nullptr;
    auto currentTime = globalTimestamp->GetCurrentTimeFromDram();
    while(currentNode != endNode) {
      nextNode = currentNode->nextNode.GetVPtr();
      assert(nextNode);
      if(currentNode->bornSq <= currentTime &&
         currentNode->deadSq == UINT64_MAX) {
        validNodes.emplace_back(currentNode);
#ifndef NDEBUG
        auto firstEntry = currentNode->GetPmSsTable()->GetDataNodeHead();
        auto lastEntry = firstEntry + 1;
        auto currentEntry = firstEntry->NextNode();
        while(currentEntry != lastEntry) {
          auto p = currentEntry->PrevNode();
          auto n = currentEntry->NextNode();
          assert(p->NextNode() == currentEntry);
          assert(n->PrevNode() == currentEntry);
          currentEntry = currentEntry->NextNode();
        }
#endif
      } else {
        if(currentNode) {
          pmem::detail::destroy<skiplistWithFanOut::PmSkiplistNvmSingleNode>(
            *currentNode);
          auto oid = pmemobj_oid(currentNode);
          pmemobj_free(&oid);
        }
        //        pmem::obj::delete_persistent<skiplistWithFanOut::PmSkiplistNvmSingleNode>(
        //          currentNode);
        //        PrintPmAllocate("-2_init", currentNode, 0);
      }
      currentNode = nextNode;
    }
    auto &nextNodeOfDummyHead =
      nvmSkiplistEntrance_[i].GetVPtr()->dummyHead.nextNode;
    //    nextNodeOfDummyHead.AddTxRangeWithoutSnapshot();
    nextNodeOfDummyHead.SetWithoutBoundaryOrFlush(
      &nvmSkiplistEntrance_[i].GetVPtr()->dummyTail);
    nvmSkiplistDramEntrance_[i]->ReplaceNodes(validNodes, {});
  }
}

void leveldb::pmCache::PmCachePoolRoot::Prepare_() {
  //  assert(pmemobj_tx_stage() == TX_STAGE_WORK);
  //  pmemobj_tx_xadd_range_direct(this, sizeof(PmCachePoolRoot),
  //                               POBJ_XADD_NO_SNAPSHOT);

  threadPool = new ThreadPool(PMCACHE_GC_THREAD_NUM, _pobj_cached_pool.pop);

  auto globalTimestamp = new GlobalTimestamp(&pmTimeStamp_);
  globalTimestamp->InitWhenOpen();

  //  /////////////////////////new1 x
  //  for(int i = 0; i < config::kNumLevels; ++i) {
  //    filesOpt_[i].SetWithoutBoundaryOrFlush(
  //      PmFileInOneLevelOptNvmRootBuilder::Generate());
  //    filesOptDramEntrance_[i] = new PmFileInOneLevelOptDramEntrance(
  //      globalTimestamp, threadPool, config::kMaxIndexHeights[i], i == 0,
  //      filesOpt_[i].GetVPtr(), true);
  //  }
  //  /////////////////////////new2 x

  /////////////////////////new3
  for(int i = 0; i < config::kNumLevels; ++i) {
    auto size = sizeof(skiplistWithFanOut::PmSkiplistNvmEntrance);
    PMEMoid oid;
    pmemobj_alloc(_pobj_cached_pool.pop, &oid, size, 0, nullptr, nullptr);
    auto newNvmEntrance = pmemobj_direct_inline(oid);
    PrintPmAllocate("2_prepare", newNvmEntrance, size);
    new(newNvmEntrance) skiplistWithFanOut::PmSkiplistNvmEntrance();
    nvmSkiplistEntrance_.at(i).SetWithoutBoundaryOrFlush(
      (skiplistWithFanOut::PmSkiplistNvmEntrance *)newNvmEntrance);
    nvmSkiplistDramEntrance_.at(i) =
      new skiplistWithFanOut::PmSkiplistDramEntrance(
        config::kMaxIndexHeights[i], nvmSkiplistEntrance_.at(i).GetVPtr(),
        i == 0, globalTimestamp, threadPool);
  }
  /////////////////////////new4
}

void leveldb::pmCache::PmCachePoolRoot::TurnLogMemToImmAndCreateNewMem() {
  immutableWriteAheadLog_.SetWithoutBoundaryOrFlush(
    memtableWriteAheadLog_.GetOffset());
  SetMemWal(PmWalBuilder::GenerateWal());
}

void leveldb::pmCache::PmCachePoolRoot::ClearImmWal(pmem::obj::pool_base &pool) {
  //  std::unique_ptr<pmem::obj::flat_transaction::manual> tx;
  //  if(pmemobj_tx_stage() == TX_STAGE_NONE)
  //    tx = std::make_unique<pmem::obj::flat_transaction::manual>(pool);
  immutableWriteAheadLog_.SetWithoutBoundaryOrFlush((uint64_t)0);
  auto imm = GetWalForImm();
  if(imm) {
    pmem::detail::destroy<PmWal>(*imm);
    auto oid = pmemobj_oid(imm);
    pmemobj_free(&oid);
  }
  //    pmem::obj::delete_persistent<PmWal>(imm);
  //  PrintPmAllocate("-3_clearImm", imm, 0);
  //  if(tx)
  //    pmemobj_tx_commit();
}