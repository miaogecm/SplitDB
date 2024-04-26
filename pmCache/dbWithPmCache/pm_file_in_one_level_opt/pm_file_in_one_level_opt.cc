////

////
//
// #include "pm_file_in_one_level_opt.h"
// #include "pmCache/dbWithPmCache/pm_write_impl/pm_wal_log.h"
// leveldb::pmCache::PmFileInOneLevelOptDramSingleNode::
//  ~PmFileInOneLevelOptDramSingleNode() {
//  SetNextLevelHintWithoutHeldLock(nullptr);
//}
//
// void leveldb::pmCache::PmFileInOneLevelOptDramSingleNode::
//  SetNextLevelHintWithoutHeldLock(
//    leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *newRefNode) {
//  if(nextLevelHint != newRefNode) {
//    if(nextLevelHint != nullptr) {
//      nextLevelHint->mutex.Lock();
//      assert(nextLevelHint->refCount > 0);
//      auto refCnt = --nextLevelHint->refCount;
//      auto isDisappearNode = nextLevelHint->disappearNode;
//      nextLevelHint->mutex.Unlock();
//      if(refCnt == 0 && isDisappearNode)
//        delete nextLevelHint;
//    }
//  } else if(newRefNode != nullptr && newRefNode->mutex.TryLock()) {
//    if(newRefNode->deadSq == UINT64_MAX) {
//      newRefNode->refCount++;
//      nextLevelHint = newRefNode;
//    } else
//      nextLevelHint = nullptr;
//    newRefNode->mutex.Unlock();
//  }
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramSingleNode::
//  PmFileInOneLevelOptDramSingleNode(
//    uint8_t indexHeight,
//    leveldb::pmCache::PmFileInOneLevelOptNvmNode *nvNvTableIndexNode)
//    : indexHeight(indexHeight), pmFileInOneLevelOptNvmNode(nvNvTableIndexNode)
//    {
//  InitFenceKeyAndFileNum_();
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramSingleNode::
//  PmFileInOneLevelOptDramSingleNode(uint8_t indexHeight)
//    : indexHeight(indexHeight), pmFileInOneLevelOptNvmNode(nullptr) {}
//
// leveldb::pmCache::PmFileInOneLevelOptNvmNode::~PmFileInOneLevelOptNvmNode() {
//  GetInlinePmSsTable()->ClearData();
//}
// leveldb::pmCache::PmFileInOneLevelOptNvmNode::PmFileInOneLevelOptNvmNode() {
//  fileNum = 0;
//}
//
// leveldb::pmCache::LogicalNodes::LogicalNodes(
//  const std::vector<PmFileInOneLevelOptDramSingleNode *> *nodes, bool init)
//    : nodes(nodes) {
//  if(init) {
//    Init_();
//  }
//}
//
// void leveldb::pmCache::LogicalNodes::ConnectNodes() {
//  assert(inited);
//  if(nodes->size() > 1 && !connected) {
//    connected = true;
//    PmFileInOneLevelOptDramSingleNode *lastNodes[PM_CACHE_MAX_INDEX_HEIGHT]{};
//    for(auto node : *nodes) {
//      auto indexHeight = node->indexHeight;
//      for(int i = 0; i < indexHeight; i++) {
//        if(lastNodes[i] != nullptr)
//          lastNodes[i]->SetNextAtomic(i, node);
//        lastNodes[i] = node;
//      }
//      if(lastNodes[0])
//        lastNodes[0]
//          ->pmFileInOneLevelOptNvmNode->nextNode.SetWithoutBoundaryOrFlush(
//            node->pmFileInOneLevelOptNvmNode);
//    }
//  }
//}
//
// void leveldb::pmCache::LogicalNodes::InitAndConnectNodes() {
//  assert(!inited && !connected);
//  inited = connected = true;
//  PmFileInOneLevelOptDramSingleNode *lastNodes[PM_CACHE_MAX_INDEX_HEIGHT]{};
//  for(auto node : *nodes) {
//    auto indexHeight = node->indexHeight;
//    maxHeight = std::max(maxHeight, indexHeight);
//    for(int i = 0; i < indexHeight; i++) {
//      if(lastNodes[i] != nullptr)
//        lastNodes[i]->SetNextAtomic(i, node);
//      lastNodes[i] = node;
//      nextInterface[i] = node;
//      if(!prevInterface[i])
//        prevInterface[i] = node;
//    }
//    if(lastNodes[0])
//      lastNodes[0]
//        ->pmFileInOneLevelOptNvmNode->nextNode.SetWithoutBoundaryOrFlush(
//          node->pmFileInOneLevelOptNvmNode);
//  }
//}
//
// void leveldb::pmCache::LogicalNodes::Init_() {
//  if(!inited) {
//    inited = true;
//    for(auto node : *nodes) {
//      auto indexHeight = node->indexHeight;
//      for(int i = 0; i < indexHeight; i++) {
//        nextInterface[i] = node;
//        if(!prevInterface[i])
//          prevInterface[i] = node;
//      }
//      maxHeight = std::max(maxHeight, indexHeight);
//    }
//  }
//}
//
// int leveldb::pmCache::PmFileInOneLevelOptDramEntrance::Compare_(
//  leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *a,
//  leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *b) const {
//  if(a == b)
//    return 0;
//  else if(a == dummyHead_ || b == dummyTail_)
//    return -1;
//  else if(a == dummyTail_ || b == dummyHead_)
//    return 1;
//  else {
//

//    auto cmpResult =
//      isLevel0_
//        ? (int)b->fileNum - (int)a->fileNum
//        : a->GetMaxInternalKeySlice().compare(b->GetMaxInternalKeySlice());
//
//    if(cmpResult == 0) {

//      auto aBornSq = a->GetBornSq();
//      auto bBornSq = b->GetBornSq();
//      if(aBornSq < bBornSq)
//        return 1;
//      else if(aBornSq > bBornSq)
//        return -1;
//      else
//        exit(-1);
//    } else
//      return cmpResult;
//  }
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramEntrance::PmFileInOneLevelOptDramEntrance(
//  leveldb::pmCache::GlobalTimestamp *globalTimestamp,
//  leveldb::pmCache::ThreadPool *threadPool, uint8_t maxIndexHeight,
//  bool isLevel0, PmFileInOneLevelOptNvmRoot *indexInNvm, bool isFirstInit)
//    : globalTimestamp(globalTimestamp), bgThreadPool(threadPool),
//      maxIndexHeight_(maxIndexHeight), isLevel0_(isLevel0) {
//  upperDummyHead_ =
//    PmFileInOneLevelOptDramUpperNodeBuilder::Generate(maxIndexHeight);
//  upperDummyTail_ =
//    PmFileInOneLevelOptDramUpperNodeBuilder::Generate(maxIndexHeight);
//
//  upperDummyHead_->SetFanOutNodesWhenInit({indexInNvm->GetDummyHead()}, true);
//  upperDummyTail_->SetFanOutNodesWhenInit({indexInNvm->GetDummyTail()}, true);
//
//  InitDummyNodePointers_();
//  if(!isFirstInit)
//    InitWhenOpen();
//}
//
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::
//  ScheduleBackGroundGcAndSmr() {
//  if(bgThreadPool)
//    bgThreadPool->PostWork(new Work{[this] {
//                                      assert(pmemobj_tx_stage() ==
//                                             TX_STAGE_NONE);
//                                      assert(_pobj_cached_pool.pop);
//                                      DoGc_();
//                                      DoSmr_();
//                                    },
//                                    false});
//}
//
// #pragma clang diagnostic push
// #pragma ide diagnostic ignored "LocalValueEscapesScope"
// #pragma clang diagnostic ignored "-Wthread-safety-analysis"
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::DoGc_() {
//  while(!nodesToGc_.IsEmptyWithoutLock()) {
//    auto guard = TimestampRefGuard{globalTimestamp};
//    uint64_t currentDeadTime = globalTimestamp->GetDeadTime();
//    GcMeta gcMeta;
//    {
//      ConcurrentQueueLockGuard<GcMeta> guard1{&nodesToGc_};
//      if(!nodesToGc_.IsEmptyWithoutLock() &&
//         (gcMeta = nodesToGc_.FrontWithoutLock()).time <= currentDeadTime)
//        nodesToGc_.PopWithoutLock();
//      else
//        break;
//    }

//    auto nodeToGc = gcMeta.node;

//
//    bool gcOk = false;
//    std::vector<PmFileInOneLevelOptDramSingleNode *> nodesToDelete{};
//    std::vector<PmFileInOneLevelOptDramSingleNode *> leftBoundary(
//      PM_CACHE_MAX_INDEX_HEIGHT);
//
//    while(!gcOk) {
//      gcOk = true;
//      nodesToDelete.clear();

//      if(nodeToGc->InLogicalDeleteGroup()) {
//        auto currentNode = nodeToGc;
//        auto headNode = currentNode->FirstNodeInThisDeleteGroup();
//        while(currentNode->FirstNodeInThisDeleteGroup() == headNode) {
//          nodesToDelete.push_back(currentNode);
//          assert(currentNode->GetDeadSq() <= currentDeadTime);
//          currentNode = currentNode->GetNextAtomic(0);
//        }
//        ///

//        if(currentNode->InLogicalDeleteGroup() &&
//           headNode->LastNodeInThisDeleteGroup()->GetNextAtomic(0) !=
//             currentNode) {
//          gcOk = false;
//          continue;
//        }
//      } else 
//        nodesToDelete.push_back(nodeToGc);
//      auto nodesToGcGroup = LogicalNodes{&nodesToDelete};
//
//      
//      std::set<PmFileInOneLevelOptDramSingleNode *> lockedNodesToDelete{};
//      std::set<PmFileInOneLevelOptDramSingleNode *> lockedLeftBoundaries{};
//      bool lockOk = false;
//      while(!lockOk) {
//        lockedNodesToDelete.clear();
//        lockedLeftBoundaries.clear();
//        lockOk = true;
//
//        if(!LockAllNodesWithCheck_(nodesToDelete, lockedNodesToDelete)) {
//          lockOk = false;
//          UnLockAllNodesWithoutCheck_(lockedNodesToDelete);
//          continue;
//        }
//      
//        for(auto &i : leftBoundary)
//          i = nullptr;
//        GetLeftNodeBoundary_(leftBoundary, nodeToGc,
//        nodesToGcGroup.maxHeight);
//  
//        PmFileInOneLevelOptDramSingleNode *lastNode{};
//        for(int i = 0; i < nodesToGcGroup.maxHeight; ++i) {
//          PmFileInOneLevelOptDramSingleNode *node = leftBoundary[i];
//          if(node != lastNode) {
//            if(!(lockedLeftBoundaries.find(node) !=
//                 lockedLeftBoundaries.end())) {
//              if(node->mutex.TryLock())
//                lockedLeftBoundaries.insert(node);
//              else {
//                lockOk = false;
//                UnLockAllNodesWithoutCheck_(lockedNodesToDelete);
//                UnLockAllNodesWithoutCheck_(lockedLeftBoundaries);
//                break;
//              }
//            }
//            lastNode = node;
//          }
//        }
//        if(!lockOk)
//          continue;
// 
//        lastNode = nullptr;
//        for(int i = 0; i < nodesToGcGroup.maxHeight; i++) {
//          PmFileInOneLevelOptDramSingleNode *node = leftBoundary[i];
//          if(lastNode != node &&
//          !(lockedNodesToDelete.find(node->GetNextAtomic(
//                                     i)) != lockedNodesToDelete.end())) {
//            lockOk = false;
//            UnLockAllNodesWithoutCheck_(lockedNodesToDelete);
//            UnLockAllNodesWithoutCheck_(lockedLeftBoundaries);
//            break;
//          }
//          lastNode = node;
//        }
//        if(!lockOk)
//          continue;
//        /// 
//      }
//      /// 
//      /// DRAM
//      for(int i = nodesToGcGroup.maxHeight - 1; i >= 0; --i)
//        leftBoundary[i]->SetNextAtomic(
//          i, nodesToGcGroup.nextInterface[i]->GetNextAtomic(i));
//      /// NVM
//      assert(pmemobj_tx_stage() == TX_STAGE_NONE);
//      /// 
//      {
//        auto pool = pmem::obj::pool_base{_pobj_cached_pool.pop};
//        pmem::obj::flat_transaction::manual tx(pool);
//
//        ///
//        auto leftBottomNodeNvm = leftBoundary[0]->pmFileInOneLevelOptNvmNode;
//        assert(leftBottomNodeNvm->deadSq);
//        {
//          auto node = &leftBottomNodeNvm->nextNode;
//          node->AddTxRange();
//          node->SetWithoutBoundaryOrFlush(
//            nodesToGcGroup.nextInterface[0]
//              ->pmFileInOneLevelOptNvmNode->nextNode.GetOffset());
//        }
//        _mm_sfence();
//
//        /// 
//        auto currentTime = this->globalTimestamp->GetCurrentTimeFromNvm();
//        for(auto &node : nodesToDelete) {
//          node->SetDisappearedSq(currentTime);
//          _mm_sfence();
//          pmem::obj::delete_persistent<PmFileInOneLevelOptNvmNode>(
//            node->pmFileInOneLevelOptNvmNode);
//          nodesToFree_.PushWithLock({node, currentTime});
//        }
//        pmemobj_tx_commit();
//      }
//
//      UnLockAllNodesWithoutCheck_(lockedNodesToDelete);
//      UnLockAllNodesWithoutCheck_(lockedLeftBoundaries);
//    }
//  }
//}
// #pragma clang diagnostic pop
//
// #pragma clang diagnostic push
// #pragma clang diagnostic ignored "-Wthread-safety-analysis"
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::DoSmr_() {
//  while(!nodesToFree_.IsEmptyWithoutLock()) {
//    uint64_t currentDeadTime = globalTimestamp->GetDeadTime();
//    GcMeta gcMeta;
//    {
//      ConcurrentQueueLockGuard<GcMeta> guard1{&nodesToFree_};
//      if(!nodesToFree_.IsEmptyWithoutLock() &&
//         (gcMeta = nodesToFree_.FrontWithoutLock()).time <= currentDeadTime)
//        nodesToFree_.PopWithoutLock();
//      else
//        break;
//    }
//    auto nodeToFree = gcMeta.node;
//    nodeToFree->mutex.Lock();
//    if(nodeToFree->refCount == 0)
//      delete nodeToFree;
//    else {
//      //
//     
//      nodeToFree->disappearNode = true;
//      nodeToFree->mutex.Unlock();
//    }
//  }
//}
// #pragma clang diagnostic pop
//
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::GetLeftNodeBoundary_(
//  std::vector<leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *>
//  &leftNodes, leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *centerNode,
//  uint8_t maxHeight) {
//  assert(centerNode != nullptr);
//  assert(leftNodes.size() == this->maxIndexHeight_);
//
//  auto currentNode = dummyHead_;
//  PmFileInOneLevelOptDramSingleNode *nextNode, *compareNode;
//  for(int i = maxHeight - 1; i >= 0; --i) {
//    while(true) {
//      nextNode = currentNode->GetNextAtomic(i);
//      if(nextNode->InLogicalDeleteGroup())
//        compareNode = nextNode->FirstNodeInThisDeleteGroup();
//      else
//        compareNode = nextNode;
//      if(Compare_(compareNode, centerNode) >= 0) { // compareNode>=centerNode
//        leftNodes[i] = currentNode;
//        break;
//      } else
//        currentNode = nextNode;
//    }
//  }
//}
// #pragma clang diagnostic push
// #pragma clang diagnostic ignored "-Wthread-safety-analysis"
//
// void
// leveldb::pmCache::PmFileInOneLevelOptDramEntrance::ReplaceGroupOfNodesAtomic(
//  const std::vector<leveldb::pmCache::PmFileInOneLevelOptNvmNode *> &newNodes,
//  const std::vector<leveldb::pmCache::PmFileInOneLevelOptNvmNode *> &oldNodes)
//  {
//
//  /// 
//  ///
//  
//
//  assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//  if(newNodes.empty() && oldNodes.empty())
//    return;
//
//  TimestampRefGuard timestampGuard(globalTimestamp);
//  if(!oldNodes.empty()) {
//
// #ifndef NDEBUG
//    for(auto &node : oldNodes)
//      assert(node->deadSq == UINT64_MAX);
// #endif
//  }
//
//  ////////////////////////////////////////////////////////////////////////////
//  //  PersistentPtrLight<PmFileInOneLevelOptNvmNode> *nextNode;
//  //  PmFileInOneLevelOptDramSingleNode *testingNode, *minNode,
//  //  *lowestRightNode; int maxHeight; bool lockOk;
//  //
//  //  auto lockedLeftBoundaryNodes =
//  //    std::vector<PmFileInOneLevelOptDramSingleNode *>{};
//  //
//  //  /// 
//  //  auto oldNodesGroup = LogicalNodes{&oldNodes};
//  //
//  //  /// 
//  //  if(oldNodesGroup.maxHeight) {
//  //    for(auto node : oldNodes)
//  //      node->mutex.Lock();
//  //    auto firstNodeInOldNodes = oldNodes.front();
//  //    auto lastNodeInOldNodes = oldNodes.back();
//  //    auto deadSq = timestampGuard.t.thisTime;
//  //    for(auto node : oldNodes) {
//  //      node->SetFirstNodeInLogicalDeleteGroup(firstNodeInOldNodes);
//  //      node->SetLastNodeInLogicalDeleteGroup(lastNodeInOldNodes);
//  //      node->SetDeadSq(deadSq);
//  //      /// 
//  //      node->pmFileInOneLevelOptNvmNode->SetDeadSqInTx(deadSq);
//  //    }
//  //    nodesToGc_.PushWithLock({firstNodeInOldNodes, deadSq});
//  //    for(auto node : oldNodes)
//  //      node->mutex.Unlock();
//  //  }
//  //
//  //  std::vector<PmFileInOneLevelOptDramSingleNode *> leftBoundary(
//  //    maxIndexHeight_);
//  //  std::vector<int> newNodesMaxHeight(newNodes.size());
//  //  auto currentMaxHeight1 = 0;
//  //  for(int i = (int)newNodes.size() - 1; i >= 0; --i)
//  //    newNodesMaxHeight[i] = currentMaxHeight1 =
//  //      std::max(currentMaxHeight1, (int)newNodes[i]->indexHeight);
//  //
//  //  /// 
//  //  auto bornSq = timestampGuard.t.thisTime + 1;
//  //  for(auto node : newNodes) {
//  //    node->SetBornSq(bornSq);
//  //    /// 
//  //    node->pmFileInOneLevelOptNvmNode->SetBornSqAtomic(bornSq);
//  //  }
//  //
//  //  _mm_sfence();
//  //
//  //  int currentI = 0;
//  //  std::vector<PmFileInOneLevelOptDramSingleNode *>
//  nodesToInsertThisRound{};
//  //  while(currentI < newNodes.size()) {
//  //    ///
//  // 
//  //
//  //    ///
//  //    maxHeight = newNodesMaxHeight[currentI];
//  //    minNode = newNodes[currentI];
//  //    lockOk = false;
//  //    while(!lockOk) {
//  //      lockOk = true;
//  //      GetLeftNodeBoundary_(leftBoundary, minNode, maxHeight);
//  //      lockedLeftBoundaryNodes.clear();
//  //      /// 
//  //      if(!LockAllNodesWithCheck_(leftBoundary, lockedLeftBoundaryNodes,
//  //                                 maxHeight))
//  //        lockOk = false;
//  //      else /// 
//  //        ///
//  //
//  
//  //        for(int i = 0; i < maxHeight && lockOk; ++i) {
//  //          testingNode = leftBoundary[i]->GetNextAtomic(i);
//  //          if(Compare_(testingNode, minNode) <= 0)
//  //            lockOk = false;
//  //        }
//  //
//  //      if(!lockOk)
//  //        UnLockAllNodesWithoutCheck_(lockedLeftBoundaryNodes);
//  //    }
//  //
//  //    nodesToInsertThisRound.clear();
//  //
//  //    lowestRightNode = leftBoundary[0]->GetNextAtomic(0);
//  //    // 
//  //    auto firstNode = newNodes[currentI++];
//  //    nodesToInsertThisRound.push_back(firstNode);
//  //    firstNode->mutex.Lock();
//  //
//  //    for(; currentI < newNodes.size(); ++currentI) {
//  //      testingNode = newNodes[currentI];
//  //      if(Compare_(testingNode, lowestRightNode) < 0) {
//  //        nodesToInsertThisRound.push_back(testingNode);
//  //        testingNode->mutex
//  //          .Lock(); //
//  //      } else
//  //        break;
//  //    }
//  //
//  //    auto nodesToInsertThisRoundGroup =
//  //      LogicalNodes{&nodesToInsertThisRound, false};
//  //    nodesToInsertThisRoundGroup.InitAndConnectNodes();
//  //
//  //    assert(nodesToInsertThisRoundGroup.maxHeight > 0);
//  //
//  //    PmFileInOneLevelOptDramSingleNode *lowestNextInDram = nullptr;
//  //    auto minHeight = nodesToInsertThisRoundGroup.maxHeight;
//  //    for(int i = 0; i < minHeight; ++i)
//  //      nodesToInsertThisRoundGroup.nextInterface[i]->SetNextAtomic(
//  //        i, (!lowestNextInDram && i == 0)
//  //             ? (lowestNextInDram = leftBoundary[i]->GetNextAtomic(i))
//  //             : (leftBoundary[i]->GetNextAtomic(i)));
//  //    if(lowestNextInDram)
//  //      nodesToInsertThisRoundGroup.nextInterface[0]
//  //        ->pmFileInOneLevelOptNvmNode->nextNode.SetWithoutBoundaryOrFlush(
//  //          lowestNextInDram->pmFileInOneLevelOptNvmNode);
//  //
//  //    lowestNextInDram = nullptr;
//  //    minHeight = nodesToInsertThisRoundGroup.maxHeight;
//  //    for(int i = 0; i < minHeight; ++i)
//  //      leftBoundary[i]->SetNextAtomic(
//  //        i, (!lowestNextInDram && i == 0)
//  //             ? (lowestNextInDram =
//  //             nodesToInsertThisRoundGroup.prevInterface[i]) :
//  //             (nodesToInsertThisRoundGroup.prevInterface[i]));
//  //    if(lowestNextInDram) {
//  //      nextNode = &leftBoundary[0]->pmFileInOneLevelOptNvmNode->nextNode;
//  //      nextNode->AddTxRange();
//  //      nextNode->SetWithoutBoundaryOrFlush(
//  //        lowestNextInDram->pmFileInOneLevelOptNvmNode);
//  //    }
//  //
//  //    UnLockAllNodesWithoutCheck_(lockedLeftBoundaryNodes);
//  //    UnLockAllNodesWithoutCheck_(nodesToInsertThisRound);
//  //  }
//  //
//  //  globalTimestamp->CurrentTimeAddOne();
//  //  retTs = timestampGuard.t.thisTime + 1;
//  //
//  //  return retTs;
//}
// bool
// leveldb::pmCache::PmFileInOneLevelOptDramEntrance::LockAllNodesWithCheck_(
//  const std::vector<PmFileInOneLevelOptDramSingleNode *> &nodes,
//  std::vector<PmFileInOneLevelOptDramSingleNode *> &lockedNodes,
//  uint8_t height) {
//  PmFileInOneLevelOptDramSingleNode *lastLockedNode = nullptr;
//  for(int i = 0; i < height; ++i) {
//    auto node = nodes[i];
//    if(lastLockedNode != node) {
//      lastLockedNode = node;
//      if(node->mutex.TryLock())
//        lockedNodes.push_back(node);
//      else
//        return false;
//    }
//  }
//  return true;
//}
// bool
// leveldb::pmCache::PmFileInOneLevelOptDramEntrance::LockAllNodesWithCheck_(
//  const std::vector<PmFileInOneLevelOptDramSingleNode *> &nodes,
//  std::set<PmFileInOneLevelOptDramSingleNode *> &lockedNodes) {
//  PmFileInOneLevelOptDramSingleNode *lastLockedNode = nullptr;
//  for(auto node : nodes)
//    if(lastLockedNode != node &&
//       !(lockedNodes.find(node) != lockedNodes.end())) {
//      lastLockedNode = node;
//      if(node->mutex.TryLock())
//        lockedNodes.insert(node);
//      else
//        return false;
//    }
//  return true;
//}
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::
//  UnLockAllNodesWithoutCheck_(
//    const std::set<PmFileInOneLevelOptDramSingleNode *> &nodes) {
//  for(auto node : nodes)
//    node->mutex.Unlock();
//}
// void leveldb::pmCache::PmFileInOneLevelOptDramEntrance::
//  UnLockAllNodesWithoutCheck_(
//    const std::vector<PmFileInOneLevelOptDramSingleNode *> &nodes) {
//  for(auto node : nodes)
//    node->mutex.Unlock();
//}
// leveldb::pmCache::PmFileInOneLevelOptDramEntrance::
//  ~PmFileInOneLevelOptDramEntrance() {
//  delete dummyTail_;
//  delete dummyHead_;
//  delete upperDummyTail_;
//  delete upperDummyHead_;
//}
// #pragma clang diagnostic pop
//
// leveldb::pmCache::PmFileInOneLevelOptNvmRoot *
// leveldb::pmCache::PmFileInOneLevelOptNvmRootBuilder::Generate() {
//  assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//  size_t size =
//    sizeof(PmFileInOneLevelOptNvmRoot) + sizeof(PmFileInOneLevelOptNvmNode) *
//    2;
//  auto data =
//    OffsetToVPtr<PmFileInOneLevelOptNvmRoot>(pmemobj_tx_alloc(size, 0).off);
//  return new(data) PmFileInOneLevelOptNvmRoot();
//  //
//  
// 
//}
//
// leveldb::pmCache::PmFileInOneLevelOptNvmNode *leveldb::pmCache::
//  PmFileInOneLevelOptNvmNodeBuilder::GeneratePmFileInOneLevelOptNvmNode() {
//  assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//  auto size = sizeof(PmFileInOneLevelOptNvmNode) + sizeof(PmSsTable) +
//              pmSsTableBuilder.CalculateExtraSizeForIndex();
//  auto newNode =
//    OffsetToVPtr<PmFileInOneLevelOptNvmNode>(pmemobj_tx_alloc(size, 0).off);
//  sizeAllocated_ += size;
//  new(newNode)
//    PmFileInOneLevelOptNvmNode(fileNum_, pmSsTableBuilder.offsetIndex.size());
//  auto pmSsTable = newNode->GetInlinePmSsTable();
//  pmSsTable->SetFirstDataNodeOff(getOffset(pmSsTableBuilder.dummyHead_));
//  pmSsTable->SetIndexWithoutFlush(pmSsTableBuilder.offsetIndex);
//  return newNode;
//}
//
// void leveldb::pmCache::PmFileInOneLevelOptDramSingleNode::
//  InitFenceKeyAndFileNum_() {
//  auto pmSsTable = pmFileInOneLevelOptNvmNode->GetInlinePmSsTable();
//  fileNum = pmFileInOneLevelOptNvmNode->fileNum;
//  SetUserKey(pmSsTable->GetMaxUserKey(), pmSsTable->GetMaxUserKey());
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramFanOut::PmFileInOneLevelOptDramFanOut(
//  uint64_t lastSplitTime, uint32_t nodeCount)
//    : lastSplitTime(lastSplitTime), nodeCount(nodeCount) {
//  memset(nodes, 0, sizeof(PmFileInOneLevelOptDramSingleNode) * nodeCount);
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramUpperNode::
//  PmFileInOneLevelOptDramUpperNode(uint8_t indexHeight)
//    : indexHeight(indexHeight) {}
//
// leveldb::pmCache::PmFileInOneLevelOptDramFanOut *
// leveldb::pmCache::PmFileInOneLevelOptDramFanOutBuilder::Generate_(
//  uint64_t lastSplitTime, uint32_t nodeCount,
//  leveldb::pmCache::PmFileInOneLevelOptDramSingleNode *nodes, uint32_t from) {
//  auto ret = (PmFileInOneLevelOptDramFanOut *)malloc(
//    PmFileInOneLevelOptDramFanOut::CalculateSize(nodeCount));
//  new(ret) PmFileInOneLevelOptDramFanOut(lastSplitTime, nodeCount);
//  auto end = from + nodeCount;
//  auto retNodes = &ret->nodes[0];
//  auto k = 0;
//  for(int i = (int)from; i < end; ++i)
//    retNodes[k++] = nodes[i];
//  return ret;
//}
//
// leveldb::pmCache::PmFileInOneLevelOptDramFanOut *
// leveldb::pmCache::PmFileInOneLevelOptDramFanOutBuilder::Generate_(
//  uint64_t lastSplitTime,
//  const std::vector<PmFileInOneLevelOptNvmNode *> &nvmNodes, uint64_t bornSq,
//  uint64_t deadSq, bool isDummyNode) {
//  auto nodeCnt = nvmNodes.size();
//  auto ret = (PmFileInOneLevelOptDramFanOut *)malloc(
//    PmFileInOneLevelOptDramFanOut::CalculateSize(nodeCnt));
//  new(ret) PmFileInOneLevelOptDramFanOut(lastSplitTime, nodeCnt);
//  auto retNodes = &ret->nodes[0];
//  for(int i = 0; i < nodeCnt; ++i) {
//    retNodes[i].bornSq = bornSq;
//    retNodes[i].deadSq = deadSq;
//    retNodes[i].disappearSq = UINT64_MAX;
//    retNodes[i].pmFileInOneLevelOptNvmNode = nvmNodes[i];
//    if(!isDummyNode)
//      retNodes[i].InitFenceKeyAndFileNum_();
//  }
//  return ret;
//}
//
// void
// leveldb::pmCache::PmFileInOneLevelOptDramUpperNode::SetFanOutNodesWhenInit(
//  const std::vector<PmFileInOneLevelOptNvmNode *> &nodesInNvm,
//  bool isDummyNode) {
//  assert(fanOutNodes.get() == nullptr);
//  fanOutNodes.reset(PmFileInOneLevelOptDramFanOutBuilder::Generate_(
//    0, nodesInNvm, 0, UINT64_MAX, isDummyNode));
//}
//
// leveldb::pmCache::LogicalUpperNodes::LogicalUpperNodes(
//  const std::vector<PmFileInOneLevelOptDramUpperNode *> *nodes, bool init)
//    : nodes(nodes) {
//  if(init) {
//    Init_();
//  }
//}
// void leveldb::pmCache::LogicalUpperNodes::ConnectNodes() {
//  assert(inited);
//  if(nodes->size() > 1 && !connected) {
//    connected = true;
//    PmFileInOneLevelOptDramUpperNode *lastNodes[PM_CACHE_MAX_INDEX_HEIGHT]{};
//    for(auto node : *nodes) {
//      auto indexHeight = node->indexHeight;
//      for(int i = 0; i < indexHeight; i++) {
//        if(lastNodes[i] != nullptr)
//          lastNodes[i]->SetNextAtomic(i, node);
//        lastNodes[i] = node;
//      }
//      if(lastNodes[0]) {
//        auto lastFanOutNodes = lastNodes[0]->fanOutNodes.get();
//        lastFanOutNodes->nodes[lastFanOutNodes->nodeCount - 1]
//          .pmFileInOneLevelOptNvmNode->nextNode.SetWithoutBoundaryOrFlush(
//            node->fanOutNodes->nodes[0].pmFileInOneLevelOptNvmNode);
//      }
//    }
//  }
//}
// void leveldb::pmCache::LogicalUpperNodes::InitAndConnectNodes() {
//  assert(!inited && !connected);
//  inited = connected = true;
//  PmFileInOneLevelOptDramUpperNode *lastNodes[PM_CACHE_MAX_INDEX_HEIGHT]{};
//  for(auto node : *nodes) {
//    auto indexHeight = node->indexHeight;
//    maxHeight = std::max(maxHeight, indexHeight);
//    for(int i = 0; i < indexHeight; i++) {
//      if(lastNodes[i] != nullptr)
//        lastNodes[i]->SetNextAtomic(i, node);
//      lastNodes[i] = node;
//      nextInterface[i] = node;
//      if(!prevInterface[i])
//        prevInterface[i] = node;
//    }
//    if(lastNodes[0]) {
//      auto lastFanOutNodes = lastNodes[0]->fanOutNodes.get();
//      lastFanOutNodes->nodes[lastFanOutNodes->nodeCount - 1]
//        .pmFileInOneLevelOptNvmNode->nextNode.SetWithoutBoundaryOrFlush(
//          node->fanOutNodes->nodes[0].pmFileInOneLevelOptNvmNode);
//    }
//  }
//}
// void leveldb::pmCache::LogicalUpperNodes::Init_() {
//  if(!inited) {
//    inited = true;
//    for(auto node : *nodes) {
//      auto indexHeight = node->indexHeight;
//      for(int i = 0; i < indexHeight; i++) {
//        nextInterface[i] = node;
//        if(!prevInterface[i])
//          prevInterface[i] = node;
//      }
//      maxHeight = std::max(maxHeight, indexHeight);
//    }
//  }
//}
