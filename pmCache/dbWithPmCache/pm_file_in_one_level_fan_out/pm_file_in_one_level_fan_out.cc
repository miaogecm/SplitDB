//

//

#include "pm_file_in_one_level_fan_out.h"
#include "db/version_edit.h"
#include <set>
#include <sys/syscall.h>

namespace leveldb::pmCache::skiplistWithFanOut {
  using leveldb::Slice;

  namespace {

    InternalKeyComparator internalKeyComparator{// NOLINT(cert-err58-cpp)
                                                BytewiseComparator()};

  }

  auto PmSkiplistInternalKey::Compare(const PmSkiplistInternalKey &key1) const
    -> int {
    return internalKeyComparator.Compare(this->GetKey(), key1.GetKey());
  }
  auto PmSkiplistInternalKey::operator=(const PmSkiplistInternalKey &other)
    -> PmSkiplistInternalKey & {
    if(this == &other)
      return *this;
    keyLen_ = other.keyLen_;
    if(EnoughSize(keyLen_)) {
      memcpy(keyData_, other.keyData_, keyLen_);
    } else {
      longKeyData_ = std::make_unique<char[]>(keyLen_);
      memcpy(longKeyData_.get(), other.longKeyData_.get(), keyLen_);
    }
    return *this;
  }

  auto PmSkiplistDramEntrance::Compare(PmSkiplistUpperNode &a,
                                       PmSkiplistUpperNode &b) const -> int {
    return Compare(&a, &b);
  }
  auto PmSkiplistDramEntrance::Compare(PmSkiplistUpperNode *a,
                                       PmSkiplistUpperNode *b) const -> int {
    return a == b                               ? 0
           : a == &dummyHead || b == &dummyTail ? -1
           : b == &dummyHead || a == &dummyTail ? 1
           : isLevel0_ ? Compare(a->GetFileNunOfKey(), b->GetFileNunOfKey(),
                                 a->GetBornSqOfKey(), b->GetBornSqOfKey())
                       : Compare(a->GetKey().GetKey(), b->GetKey().GetKey(),
                                 a->GetBornSqOfKey(), b->GetBornSqOfKey());
  }

  auto PmSkiplistDramEntrance::Compare(PmSkiplistUpperNode *a,
                                       PmSkiplistNvmSingleNode *b,
                                       Slice bMaxInternalKey) const -> int {
    return a == &dummyHead   ? -1
           : a == &dummyTail ? 1
           : isLevel0_       ? Compare(a->GetFileNunOfKey(), b->fileNum,
                                       a->GetBornSqOfKey(), b->bornSq)
                             : Compare(a->GetKey().GetKey(), bMaxInternalKey,
                                       a->GetBornSqOfKey(), b->bornSq);
  }

  PmSkiplistDramEntrance::PmSkiplistDramEntrance(
    uint8_t indexHeight, PmSkiplistNvmEntrance *nvmEntrance, bool isLevel0,
    GlobalTimestamp *t, ThreadPool *threadPool)
      : maxIndexHeight(indexHeight),
        dummyHead(indexHeight, {&nvmEntrance->dummyHead}, true),
        dummyTail(indexHeight, {&nvmEntrance->dummyTail}, true),
        nvmEntrance(nvmEntrance), pool(pmem::obj::pool_by_vptr(nvmEntrance)),
        threadPool_(threadPool), isLevel0_(isLevel0) {
    auto dummyTailPtr = &dummyTail;
    for(int i = 0; i < indexHeight; ++i)
      dummyHead.SetNext(i, dummyTailPtr);
    timestamp = t;
  }

  void PmSkiplistDramEntrance::ReplaceNodes(
    const std::vector<PmSkiplistNvmSingleNode *> &newNodes,
    const std::vector<PmSkiplistNvmSingleNode *> &oldNodes) {

    std::vector<std::string> newNodesKeys, oldNodesKeys;
    std::vector<uint32_t> newNodesKeysLen, oldNodesKeysLen;
    if(!newNodes.empty()) {
      newNodesKeys.reserve(newNodes.size());
      newNodesKeysLen.reserve(newNodes.size());
      for(auto node : newNodes) {
        auto maxInternalKey = node->GetPmSsTable()->GetMaxInternalKey();
        newNodesKeys.emplace_back(maxInternalKey.ToString());
        newNodesKeysLen.emplace_back(maxInternalKey.size());
      }
    }
    if(!oldNodes.empty()) {
      oldNodesKeys.reserve(oldNodes.size());
      oldNodesKeysLen.reserve(oldNodes.size());
      for(auto node : oldNodes) {
        auto maxInternalKey = node->GetPmSsTable()->GetMaxInternalKey();
        oldNodesKeys.emplace_back(maxInternalKey.ToString());
        oldNodesKeysLen.emplace_back(maxInternalKey.size());
      }
    }

    std::set<PmSkiplistUpperNode *> lockedNodes{};

    TimestampRefGuard timestampRefGuard{timestamp};

    uint64_t bNum{}; 
    Slice
      bMaxInternalKey; 


    uint64_t bBornSq;
    auto currentI = 0; 
    PmSkiplistUpperNode *currentUpperNode;
    std::string tmpKey;

    if(!oldNodes.empty()) {
      auto deadTime = timestampRefGuard.t.thisTime;


      currentUpperNode = &dummyHead;
      while(currentI < oldNodes.size()) {

        auto currentDeletingNode = oldNodes[currentI];

        bBornSq = currentDeletingNode->bornSq;
        if(isLevel0_) {
          bNum = currentDeletingNode->fileNum;
        } else {
          bMaxInternalKey = {oldNodesKeys[currentI].data(),
                             oldNodesKeysLen[currentI]};
        }

        int maxHeightToTry = maxIndexHeight; 
        PmSkiplistUpperNode *nextNodeI;

        while(maxHeightToTry > 0) {
          for(int i = std::min(currentUpperNode->GetIndexHeight() - 1,
                               maxHeightToTry - 1);
              i >= 0; --i) {
            nextNodeI = currentUpperNode->GetNext(i);

            if((isLevel0_ ? Compare(nextNodeI, bNum, bBornSq)
                          : Compare(nextNodeI, bMaxInternalKey, bBornSq)) < 0 &&
               (currentUpperNode = nextNodeI))
              break;
            else
              maxHeightToTry = i;
          }
        }

        if(isLevel0_ ? Compare(currentUpperNode, bNum, bBornSq)
                     : Compare(currentUpperNode, bMaxInternalKey, bBornSq) < 0)
          currentUpperNode = currentUpperNode->GetNext(0);


        std::unique_lock<std::mutex> lockGuard;

        bool lockOk = false;
        while(!lockOk) {
          lockOk = true;

          lockGuard = std::unique_lock<std::mutex>{currentUpperNode->mutex};
          if(currentUpperNode->disappearNode.load(std::memory_order_acquire)) {
            lockOk = false;
            currentUpperNode = currentUpperNode->GetNext(0);
            lockGuard.unlock();
          }
        }


        auto fanOutNodes = currentUpperNode->GetFanOutNodes().get();
        auto fanOutArray = fanOutNodes->fanOutNodes.get();
        auto fanOutNodesCnt = fanOutNodes->fanOutNodesCount;

        auto targetNode =
          isLevel0_
            ? std::lower_bound(fanOutArray, fanOutArray + fanOutNodesCnt,
                               CompareNodeForLevel0{bNum, bBornSq},
                               [this](const PmSkiplistFanOutSingleNode &a,
                                      const CompareNodeForLevel0 &b) {
                                 return Compare(a, b) < 0;
                               })
            : std::lower_bound(fanOutArray, fanOutArray + fanOutNodesCnt,
                               CompareNodeForLevel1P{bMaxInternalKey, bBornSq},
                               [this](const PmSkiplistFanOutSingleNode &a,
                                      const CompareNodeForLevel1P &b) {
                                 return Compare(a, b) < 0;
                               });

        if(targetNode->nvmNode != currentDeletingNode) {
          currentUpperNode = &dummyHead;
          std::cerr << "!\n" << std::flush;
          continue;
        }

        auto endAddr = fanOutArray + fanOutNodesCnt;
        while(targetNode != endAddr && currentI < oldNodes.size()) {
          if(targetNode->nvmNode == oldNodes[currentI]) {
            targetNode->SetDeadSq(deadTime);
            targetNode->nvmNode->deadSq = deadTime;

            nvmNodesToGc.PushWithLock({targetNode->nvmNode, deadTime});
            currentI++;
          }
          targetNode++;
        }
      }
    }

    if(!newNodes.empty()) {
      currentI = 0;
      bBornSq = timestampRefGuard.t.thisTime + 1;

      for(auto *node : newNodes)
        node->bornSq = bBornSq;

      PmSkiplistNvmSingleNode *nodeToInsert;

      auto newNodesCnt = newNodes.size();
      while(currentI < newNodesCnt) {
        currentUpperNode = &dummyHead;
        nodeToInsert = newNodes[currentI];
        if(isLevel0_) {
          bNum = nodeToInsert->fileNum;
        } else {
          bMaxInternalKey = {newNodesKeys[currentI].data(),
                             newNodesKeysLen[currentI]};
        }


        int maxHeightToTry = maxIndexHeight;

        while(maxHeightToTry > 0 && currentUpperNode != &dummyTail)
          for(int i = std::min(currentUpperNode->GetIndexHeight() - 1,
                               maxHeightToTry - 1);
              i >= 0; --i) {
            auto nextNodeI = currentUpperNode->GetNext(i);
            if((isLevel0_ ? Compare(nextNodeI, bNum, bBornSq)
                          : Compare(nextNodeI, bMaxInternalKey, bBornSq)) < 0 &&
               (currentUpperNode = nextNodeI))
              break;
            else
              maxHeightToTry = i;
          }

        if((isLevel0_
              ? Compare(currentUpperNode, bNum, bBornSq)
              : Compare(currentUpperNode, bMaxInternalKey, bBornSq)) < 0)
          currentUpperNode = currentUpperNode->GetNext(0);

        assert(currentUpperNode);

        std::vector<PmSkiplistUpperNode *> leftBoundary{maxIndexHeight};

        if(currentUpperNode == &dummyTail) {
          bool lockOk = false;

          while(!lockOk) {
            lockOk = true;
            GetLeftBoundary(leftBoundary, nodeToInsert,
                            timestampRefGuard.t.thisTime + 1);

            for(auto it = leftBoundary.rbegin(); it != leftBoundary.rend();
                it++) {
              auto *node = *it;
              if(!lockedNodes.count(node)) {
                node->mutex.lock();
                lockedNodes.insert(node);
              }
              if(node->disappearNode.load(std::memory_order_relaxed)) {
                lockOk = false;
                break;
              }
            }

            if(!lockOk) {
              for(auto *node : lockedNodes)
                node->mutex.unlock();
              lockedNodes.clear();
            }
          }


          std::vector<PmSkiplistNvmSingleNode *> nodesToInsertThisRound{};

          for(int i = 0;
              i < PM_SKIPLIST_MAX_FAN_OUT && currentI < newNodes.size(); ++i)
            nodesToInsertThisRound.push_back(newNodes[currentI++]);

          auto newUpperNode = new PmSkiplistUpperNode{
            HeightGenerator::GenerateHeight(maxIndexHeight),
            nodesToInsertThisRound, false};

          auto fanOutOfNextUpperNode =
            leftBoundary[0]->GetNext(0)->GetFanOutNodes();
          auto fanOutOfPrevUpperNode = leftBoundary[0]->GetFanOutNodes();
          auto fanOutOfCurrUpperNode = newUpperNode->GetFanOutNodes().get();

          fanOutOfCurrUpperNode
            ->fanOutNodes[fanOutOfCurrUpperNode->fanOutNodesCount - 1]
            .nvmNode->nextNode.SetWithoutBoundaryOrFlush(
              fanOutOfNextUpperNode
                ->fanOutNodes[fanOutOfNextUpperNode->fanOutNodesCount - 1]
                .nvmNode);
          auto &node =
            fanOutOfPrevUpperNode
              ->fanOutNodes[fanOutOfPrevUpperNode->fanOutNodesCount - 1]
              .nvmNode->nextNode;
          //          node.AddTxRangeWithoutSnapshot();
          node.SetWithoutBoundaryOrFlush(
            fanOutOfCurrUpperNode->fanOutNodes[0].nvmNode);

          for(int i = 0; i < newUpperNode->GetIndexHeight(); ++i)
            newUpperNode->SetNext(i, leftBoundary[i]->GetNext(i));
          for(int i = 0; i < newUpperNode->GetIndexHeight(); ++i)
            leftBoundary[i]->SetNext(i, newUpperNode);
        } else {

          auto oldCurrentI = currentI;

          std::vector<PmSkiplistNvmSingleNode *> nodesToInsertThisRound{};
          std::vector<Slice> nodesToInsertThisRoundKeys{};
          nodesToInsertThisRound.push_back(nodeToInsert);
          nodesToInsertThisRoundKeys.emplace_back(newNodesKeys[currentI].data(),
                                                  newNodesKeysLen[currentI]);
          currentI++;

          newNodesCnt = newNodes.size();
          while(currentI < newNodesCnt) {
            auto testingNode = newNodes[currentI];
            if(Compare(currentUpperNode, testingNode,
                       {newNodesKeys[currentI].data(),
                        newNodesKeysLen[currentI]}) >= 0) {
              nodesToInsertThisRound.push_back(testingNode);
              nodesToInsertThisRoundKeys.emplace_back(
                newNodesKeys[currentI].data(), newNodesKeysLen[currentI]);
              currentI++;
            } else
              break;
          }


          bool lockOk = false;
          bool targetCorrectness = true;
          while(!lockOk) {
            lockOk = true;
            GetLeftBoundary(leftBoundary, nodeToInsert,
                            timestampRefGuard.t.thisTime + 1);


            for(auto it = leftBoundary.rbegin(); it != leftBoundary.rend();
                it++) {
              auto *node = *it;
              if(!lockedNodes.count(node)) {
                lockedNodes.insert(node);
                node->mutex.lock();
              }
              if(node->disappearNode.load(std::memory_order_relaxed)) {
                lockOk = false;
                break;
              }
            }
            currentUpperNode->mutex.lock();
            lockedNodes.insert(currentUpperNode);
            if(currentUpperNode->disappearNode.load(
                 std::memory_order_relaxed)) {
              targetCorrectness = false;
              lockOk = false;
            }

            if(!lockOk) {
              for(auto *node : lockedNodes)
                node->mutex.unlock();
              lockedNodes.clear();
            }
            if(!targetCorrectness)
              break;
          }
          if(targetCorrectness)
            currentUpperNode->UpdateFanOutNodes(
              nodesToInsertThisRound, leftBoundary, isLevel0_, timestamp,
              maxIndexHeight, lockedNodes, nodesToInsertThisRoundKeys);
          else
            currentI = oldCurrentI;
        }
        for(auto *node : lockedNodes)
          node->mutex.unlock();
      }
    }
    timestamp->CurrentTimeAddOne();
    // #ifndef NDEBUG
    //     CheckNvm();
    // #endif
  }
  auto
  PmSkiplistDramEntrance::Compare(const Slice &a, const Slice &b,
                                  uint64_t bornSqA, uint64_t bornSqB) -> int {
    auto compareAns = internalKeyComparator.Compare(a, b);
    return compareAns > 0      ? 1
           : compareAns < 0    ? -1
           : bornSqA < bornSqB ? 1
           : bornSqA > bornSqB ? -1
                               : 0;
  }
  auto
  PmSkiplistDramEntrance::Compare(uint64_t aNum, uint64_t bNum,
                                  uint64_t bornSqA, uint64_t bornSqB) -> int {
    return aNum > bNum         ? 1
           : aNum < bNum       ? -1
           : bornSqA < bornSqB ? 1
           : bornSqA > bornSqB ? -1
                               : 0;
  }
  auto PmSkiplistDramEntrance::Compare(PmSkiplistUpperNode *a, leveldb::Slice b,
                                       uint64_t bornSqB) -> int {
    assert(!isLevel0_);
    return a == &dummyHead ? -1
           : a == &dummyTail
             ? 1
             : Compare(a->GetKey().GetKey(), b, a->GetBornSqOfKey(), bornSqB);
  }
  auto PmSkiplistDramEntrance::Compare(PmSkiplistUpperNode *a, uint64_t bNum,
                                       uint64_t bornSqB) -> int {
    assert(isLevel0_);
    return a == &dummyHead   ? -1
           : a == &dummyTail ? 1
                             : Compare(a->GetFileNunOfKey(), bNum,
                                       a->GetBornSqOfKey(), bornSqB);
  }
  auto PmSkiplistDramEntrance::Compare(
    const PmSkiplistFanOutSingleNode &a,
    const PmSkiplistDramEntrance::CompareNodeForLevel1P &b) const -> int {
    assert(!isLevel0_);
    return Compare(a.key.GetKey(), b.key, a.GetBornSq(), b.bornSq);
  }
  auto PmSkiplistDramEntrance::Compare(
    const PmSkiplistFanOutSingleNode &a,
    const PmSkiplistDramEntrance::CompareNodeForLevel0 &b) const -> int {
    assert(isLevel0_);
    return Compare(a.nvmNode->fileNum, b.fileNum, a.GetBornSq(), b.bornSq);
  }
  void PmSkiplistDramEntrance::GetLeftBoundary(
    std::vector<PmSkiplistUpperNode *> &leftBoundary,
    PmSkiplistNvmSingleNode *nvNode, uint64_t bornSq) {
    assert(leftBoundary.size() >= maxIndexHeight);

    Slice bInternalKey;
    uint64_t bNum{};
    std::string tmpString;
    if(isLevel0_) {
      bNum = nvNode->fileNum;
    } else {
      bInternalKey = nvNode->GetPmSsTable()->GetMaxInternalKey();
      tmpString = bInternalKey.ToString();
      bInternalKey = Slice(tmpString);
    }

    auto currentNode = &dummyHead;
    for(int i = maxIndexHeight - 1; i >= 0; --i) {
      auto nextNodeI = currentNode->GetNext(i);
      while((isLevel0_ ? (Compare(nextNodeI, bNum, bornSq))
                       : (Compare(nextNodeI, bInternalKey, bornSq))) < 0 &&
            (currentNode = nextNodeI))
        nextNodeI = currentNode->GetNext(i);
      leftBoundary[i] = currentNode;
    }
  }

  void PmSkiplistFanOutSingleNode::InitWithNodeInNvm(
    PmSkiplistNvmSingleNode *nodeInNvm, bool isDummyNode) {
    nvmNode = nodeInNvm;
    if(!isDummyNode) {
      key.SetKey(nvmNode->GetPmSsTable()->GetMaxInternalKey());
      bornSqOfKey_ = nodeInNvm->bornSq;
      deadSqOfKey_ = nodeInNvm->deadSq;
    }
  }

  auto
  PmSkiplistNvmSingleNodeBuilder::Generate(uint64_t bornSq, uint64_t deadSq,
                                           uint64_t fileNum)
    -> PmSkiplistNvmSingleNode * {
    //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
    auto size = sizeof(PmSkiplistNvmSingleNode) + sizeof(PmSsTable) +
                ssTableBuilder.CalculateExtraSizeForIndex();

    // S
    //    std::stringstream ss;
    //    ss << "w " << sizeof(PmSkiplistNvmSingleNode) + sizeof(PmSsTable)
    //       << std::endl;
    //    ss << "w " << ssTableBuilder.CalculateExtraSizeForIndex() <<
    //    std::endl; std::cout << ss.str() << std::flush;

    //    auto newNode =
    //      GetVPtr<PmSkiplistNvmSingleNode>(pmemobj_tx_alloc(size, 0).off);

    PMEMoid oid;
    pmemobj_alloc(_pobj_cached_pool.pop, &oid, size, 0, nullptr, nullptr);
    auto newNode = (PmSkiplistNvmSingleNode *)pmemobj_direct(oid);

    sizeAllocated += size;
    new(newNode) PmSkiplistNvmSingleNode(ssTableBuilder.offsetIndex.size(),
                                         bornSq, deadSq, fileNum);
    auto pmSsTable = newNode->GetPmSsTable();
    pmSsTable->SetFirstDataNodeOff(GetOffset(ssTableBuilder.dummyHead));
    pmSsTable->SetIndexWithoutFlush(ssTableBuilder.offsetIndex);
    ssTableBuilder.SetPmSsTable(pmSsTable);
    pmemobj_flush(_pobj_cached_pool.pop, newNode, size);
    return newNode;
  }
  //  auto PmSkiplistNvmSingleNodeBuilder::TestGenerate(uint64_t deadSq,
  //                                                    uint64_t fileNum,
  //                                                    const Slice
  //                                                    &internalKey1, const
  //                                                    Slice &internalKey2)
  //    -> PmSkiplistNvmSingleNode * {
  //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
  //    uint64_t newNodeOff;
  //    PmSsTable::GenerateDataNodeWithoutFlush(
  //      {internalKey1.data(), internalKey1.size() - 8}, Slice{"123"},
  //      *(uint64_t *)(internalKey1.data() + internalKey1.size() - 8),
  //      newNodeOff);
  //    ssTableBuilder.AddNodeToIndex(newNodeOff);
  //    PmSsTable::GenerateDataNodeWithoutFlush(
  //      {internalKey2.data(), internalKey2.size() - 8}, Slice{"123"},
  //      *(uint64_t *)(internalKey2.data() + internalKey2.size() - 8),
  //      newNodeOff);
  //    ssTableBuilder.AddNodeToIndex(newNodeOff);
  //    return Generate(0, deadSq, fileNum);
  //  }
  void PmSkiplistNvmSingleNodeBuilder::PushNodeFromAnotherNvTable(
    FileMetaData *fileMeta) {
    ssTableBuilder.PushNodeFromAnotherSsTableWithoutFlush(
      fileMeta->nvmSingleNode->GetPmSsTable());
    sizeAllocated = fileMeta->fileSize;
    smallest = fileMeta->smallest;
    largest = fileMeta->largest;
  }
  PmSkiplistUpperNode::PmSkiplistUpperNode(
    uint8_t indexHeight, const std::vector<PmSkiplistNvmSingleNode *> &nodes,
    bool isDummyNode)
      : indexHeight_(indexHeight) {
    auto newFanOutGroupNodes = new PmSkiplistFanOutGroupNodes();
    newFanOutGroupNodes->fanOutNodes =
      std::unique_ptr<PmSkiplistFanOutSingleNode[]>(
        new PmSkiplistFanOutSingleNode[nodes.size()]);
    newFanOutGroupNodes->fanOutNodesCount = nodes.size();
    int i = 0;
    PmSkiplistNvmSingleNode *lastNode = nullptr;
    for(auto *node : nodes) {
      if(lastNode)
        lastNode->nextNode.SetWithoutBoundaryOrFlush(node);
      lastNode = node;
      newFanOutGroupNodes->fanOutNodes[i].InitWithNodeInNvm(node, isDummyNode);
      ++i;
    }
    if(!isDummyNode) {
      auto &maxNode =
        newFanOutGroupNodes
          ->fanOutNodes[newFanOutGroupNodes->fanOutNodesCount - 1];
      auto nvmNode = maxNode.nvmNode;
      fileNumOfKey_ = nvmNode->fileNum;
      internalKey_.SetKey(nvmNode->GetPmSsTable()->GetMaxInternalKey());
      bornSqOfKey_ = maxNode.GetBornSq();
    }
    std::atomic_store(
      &fanOutGroupNodes_,
      std::shared_ptr<PmSkiplistFanOutGroupNodes>(newFanOutGroupNodes));
  }

  void PmSkiplistUpperNode::UpdateFanOutNodes(
    const std::vector<PmSkiplistNvmSingleNode *> &nodes,
    std::vector<PmSkiplistUpperNode *> leftBoundary, bool isLevel0,
    GlobalTimestamp *time, uint8_t maxIndexHeight,
    std::set<PmSkiplistUpperNode *> &lockedNodes, std::vector<Slice> &keys) {

    auto fanOutGroupNodes = fanOutGroupNodes_.get();
    auto currentFanOutNodes = fanOutGroupNodes->fanOutNodes.get();
    auto currentFanOutNodesCount = fanOutGroupNodes->fanOutNodesCount;

    std::vector<PmSkiplistNvmSingleNode *> totalNodes;

    auto totalNodeCount = nodes.size() + currentFanOutNodesCount;
    int k = 0, newNodesI = 0, oldNodesI = 0;
    while(k < totalNodeCount) {
      if(newNodesI >= nodes.size()) {
        totalNodes.push_back(currentFanOutNodes[oldNodesI].nvmNode);
        ++oldNodesI;
      } else if(oldNodesI >= currentFanOutNodesCount) {
        totalNodes.push_back(nodes[newNodesI]);
        ++newNodesI;
      } else {
        auto &nodesNewNodesI = nodes[newNodesI];
        auto &currentFanOutNodesOldNodesI = currentFanOutNodes[oldNodesI];
        if((isLevel0
              ? PmSkiplistDramEntrance::Compare(
                  currentFanOutNodesOldNodesI.nvmNode->fileNum,
                  nodesNewNodesI->fileNum,
                  currentFanOutNodesOldNodesI.GetBornSq(),
                  nodesNewNodesI->bornSq)
              : PmSkiplistDramEntrance::Compare(
                  currentFanOutNodesOldNodesI.key.GetKey(), keys[newNodesI],
                  currentFanOutNodesOldNodesI.GetBornSq(),
                  nodesNewNodesI->bornSq)) <= 0) {
          totalNodes.push_back(currentFanOutNodesOldNodesI.nvmNode);
          oldNodesI++;
        } else {
          totalNodes.push_back(nodesNewNodesI);
          newNodesI++;
        }
      }
      k++;
    }
    auto leftBoundaryNodes = leftBoundary[0]->GetFanOutNodes().get();

    auto *rightBoundary =
      fanOutGroupNodes_->fanOutNodes[currentFanOutNodesCount - 1]
        .nvmNode->nextNode.GetVPtr();
    auto lastNode =
      leftBoundaryNodes->fanOutNodes[leftBoundaryNodes->fanOutNodesCount - 1]
        .nvmNode;
    for(auto *node : totalNodes) {
      //      lastNode->nextNode.AddTxRangeWithoutSnapshot();
      lastNode->nextNode.SetWithoutBoundaryOrFlush(node);
      lastNode = node;
    }
    //    lastNode->nextNode.AddTxRangeWithoutSnapshot();
    lastNode->nextNode.SetWithoutBoundaryOrFlush(rightBoundary);

    uint64_t nodeCountInNewNodes = (totalNodeCount > PM_SKIPLIST_MAX_FAN_OUT)
                                     ? totalNodeCount - PM_SKIPLIST_MAX_FAN_OUT
                                     : 0;

    k = 0;
    while(k < nodeCountInNewNodes) {
      std::vector<PmSkiplistNvmSingleNode *> nodesToInsertThisRound;
      for(int i = 0; i < PM_SKIPLIST_MAX_FAN_OUT; ++i) {
        nodesToInsertThisRound.push_back(totalNodes[k]);
        k++;
        if(k >= nodeCountInNewNodes) {
          break;
        }
      }
      auto newUpperNode =
        new PmSkiplistUpperNode(HeightGenerator::GenerateHeight(maxIndexHeight),
                                nodesToInsertThisRound, false);
      newUpperNode->mutex.lock();
      lockedNodes.insert(newUpperNode);
      auto newUpperNodeIndexHeight = newUpperNode->GetIndexHeight();
      for(int i = 0; i < newUpperNodeIndexHeight; ++i) {
        newUpperNode->SetNext(i, leftBoundary[i]->GetNext(i));
      }
      for(int i = 0; i < newUpperNodeIndexHeight; ++i) {
        leftBoundary[i]->SetNext(i, newUpperNode);
        leftBoundary[i] = newUpperNode;
      }
    }

    auto nodeCountAfterSplit = totalNodeCount - nodeCountInNewNodes;
    auto newFanOutGroupNodes = new PmSkiplistFanOutGroupNodes();
    auto newFanOutNodes = new PmSkiplistFanOutSingleNode[nodeCountAfterSplit];
    k = 0;
    for(auto i = nodeCountInNewNodes; i < totalNodeCount; ++i) {
      newFanOutNodes[k].InitWithNodeInNvm(totalNodes[i], false);
      k++;
    }
    newFanOutGroupNodes->fanOutNodes.reset(newFanOutNodes);
    newFanOutGroupNodes->fanOutNodesCount = nodeCountAfterSplit;
    newFanOutGroupNodes->lastSplitTime = time->GetCurrentTimeFromDram();

    std::atomic_store(
      &fanOutGroupNodes_,
      std::shared_ptr<PmSkiplistFanOutGroupNodes>(newFanOutGroupNodes));
  }
  //  void PmSkiplistUpperNode::TestPrint() {
  //    auto fanOutNodes = GetFanOutNodes();
  //    auto fanOutNodesDirect = fanOutNodes->fanOutNodes.get();
  //    auto fanOutNodesCount = fanOutNodes->fanOutNodesCount;
  //    for(uint32_t i = 0; i < fanOutNodesCount; ++i) {
  //      std::cout << fanOutNodesDirect[i]
  //                     .nvmNode->GetPmSsTable()
  //                     ->GetMaxUserKey()
  //                     .ToString()
  //                << "(" << fanOutNodesDirect[i].nvmNode->bornSq << "~"
  //                << fanOutNodesDirect[i].nvmNode->deadSq << ")"
  //                << " ";
  //    }
  //  }
  PmSkiplistUpperNode::~PmSkiplistUpperNode() { ClearNextLevelHint(); }

  namespace {
    class WriteLockGuard {
     public:
      WriteLockGuard(std::shared_mutex &mutex, bool tryLock) : mutex(mutex) {
        if(tryLock) {
          lockOk = mutex.try_lock();
        } else {
          mutex.lock();
          lockOk = true;
        }
      }
      ~WriteLockGuard() {
        if(lockOk) {
          mutex.unlock();
        }
      }
      std::shared_mutex &mutex;
      bool lockOk;
    };
  } // namespace

  auto
  PmSkiplistUpperNode::SetNextLevelHint(PmSkiplistUpperNode *nextLevelHintNode)
    -> bool {
    WriteLockGuard guard(nextLevelHintMutex, false);
    std::lock_guard<std::mutex> lock(mutex);
    bool shouldReturnFalse = false;
    if(disappearNode.load(std::memory_order_relaxed)) {
      shouldReturnFalse = true;
      nextLevelHintNode = nullptr;
    }

    if(nextLevelHint_.load(std::memory_order_relaxed) == nextLevelHintNode)
      return !shouldReturnFalse;

    if(nextLevelHintNode) {
      std::lock_guard<std::mutex> lock1(nextLevelHintNode->mutex);
      if(nextLevelHintNode->disappearNode.load(std::memory_order_relaxed))
        return true;
      else {
        nextLevelHintNode->refCount++;
        nextLevelHint_.store(nextLevelHintNode, std::memory_order_release);
      }
    } else {
      auto currentHint = nextLevelHint_.load(std::memory_order_relaxed);
      if(currentHint) {
        currentHint->mutex.lock();
        currentHint->refCount--;
        if(currentHint->refCount == 0 && currentHint->lostNode) {
          currentHint->mutex.unlock();
          delete currentHint;
        } else {
          currentHint->mutex.unlock();
        }
      }
      nextLevelHint_.store(nullptr, std::memory_order_release);
    }
    return !shouldReturnFalse;
  }

  void PmSkiplistUpperNode::ClearNextLevelHint() { SetNextLevelHint(nullptr); }

  auto PmSkiplistUpperNode::GetNextLevelHint() -> PmSkiplistUpperNode * {
    return nextLevelHint_.load(std::memory_order_acquire);
  }

  void PmSkiplistDramEntrance::ScheduleBackgroundGcAndSmr() {
    //    bool c = false;
    if(dummyHead.GetNext(0) != &dummyTail) {
      threadPool_->PostWork(new Work{[this] {
                                       //                   assert(pmemobj_tx_stage()
                                       //                   == TX_STAGE_NONE);
                                       //                   assert(_pobj_cached_pool.pop);
                                       //                   pmem::obj::flat_transaction::manual
                                       //                   tx{pool};
                                       Gc_();
                                       Smr_();
                                       //                   pmemobj_tx_commit();
                                     },
                                     false});
      if(rand() % 5 == 0)
        threadPool_->PostWork(new Work{
          [this] {
            while(!upperNodesToSmr.IsEmptyWithoutLock()) {
              uint64_t currentDeadTime = timestamp->GetDeadTime();
              PmSkiplistUpperNodeGcMeta smrMeta;
              {
                ConcurrentQueueLockGuard<PmSkiplistUpperNodeGcMeta> lockGuard{
                  &upperNodesToSmr};
                if(!upperNodesToSmr.IsEmptyWithoutLock() &&
                   (smrMeta = upperNodesToSmr.FrontWithoutLock()).time <=
                     currentDeadTime) {
                  upperNodesToSmr.PopWithoutLock();

                  auto upperNodeToFree = smrMeta.upperNode;
                  upperNodeToFree->mutex.lock();
                  if(upperNodeToFree->refCount > 0) {
                    upperNodeToFree->lostNode = true;
                    upperNodeToFree->mutex.unlock();
                  } else {
                    upperNodeToFree->mutex.unlock();
                    delete upperNodeToFree;
                  }
                } else
                  break;
              }
            }
          },
          false});
    }
  }

  static void UnlockAllNodes(std::set<PmSkiplistUpperNode *> &lockedNodes) {
    for(auto *node : lockedNodes)
      node->mutex.unlock();
  }

  void PmSkiplistDramEntrance::Gc_() {

    while(!nvmNodesToGc.IsEmptyWithoutLock()) {
      TimestampRefGuard timestampRefGuard{timestamp};
      uint64_t currentDeadTime = timestamp->GetDeadTime();
      PmSkiplistNvmSingleNodeGcMeta gcMeta;
      {
        ConcurrentQueueLockGuard<PmSkiplistNvmSingleNodeGcMeta>
          concurrentQueueLockGuard{&nvmNodesToGc};
        if(!nvmNodesToGc.IsEmptyWithoutLock() &&
           (gcMeta = nvmNodesToGc.FrontWithoutLock()).time <= currentDeadTime)
          nvmNodesToGc.PopWithoutLock();
        else
          break;
      }

      auto nodeToGcNvmNode = gcMeta.nvmNode;
      bool findAndLockOk = false;
      auto findLocation = -1;
      PmSkiplistUpperNode *targetUpperNode = nullptr;
      PmSkiplistFanOutGroupNodes *fanOutNodesOfTargetNodeRawPtr;
      PmSkiplistFanOutSingleNode *fanOutArrayOfTargetNode;
      uint32_t fanOutNodesCntOfTargetNode;

      std::shared_ptr<PmSkiplistFanOutGroupNodes> oldFanOutNodes;

      std::vector<PmSkiplistUpperNode *> leftBoundary{maxIndexHeight};
      std::vector<PmSkiplistFanOutSingleNode *> newFanOutNodesPtr{};
      std::set<PmSkiplistUpperNode *> lockedNodes;

      bool quickEnd = false;

      while(!findAndLockOk) {
        findAndLockOk = true;

        GetLeftBoundary(leftBoundary, nodeToGcNvmNode, nodeToGcNvmNode->bornSq);
        targetUpperNode = leftBoundary[0]->GetNext(0);
        if(!targetUpperNode) {
          findAndLockOk = false;
          continue;
        }

        {
          std::lock_guard<std::mutex> targetUpperNodeLockGuard{
            targetUpperNode->mutex};
          if(!targetUpperNode->disappearNode.load(std::memory_order_acquire)) {
            PrepareNodeToDelete_(oldFanOutNodes, fanOutNodesOfTargetNodeRawPtr,
                                 fanOutArrayOfTargetNode,
                                 fanOutNodesCntOfTargetNode, targetUpperNode);
            if(fanOutNodesCntOfTargetNode > 1) {
              TryFindNodeAndBuildNewFanOut_(
                findLocation, newFanOutNodesPtr, fanOutNodesCntOfTargetNode,
                fanOutArrayOfTargetNode, nodeToGcNvmNode);
              if(findLocation > 0) {
                targetUpperNode->DeleteOneNode_(
                  newFanOutNodesPtr, fanOutNodesCntOfTargetNode,
                  fanOutNodesOfTargetNodeRawPtr, nodeToGcNvmNode, this,
                  fanOutArrayOfTargetNode[findLocation - 1].nvmNode);
                quickEnd = true;
                break;
              }
            }
          } else {
            findAndLockOk = false;
            continue;
          }
        }

        for(int i = targetUpperNode->GetIndexHeight() - 1; i >= 0; --i) {
          auto &node = leftBoundary[i];
          if(lockedNodes.count(node) == 0) {
            node->mutex.lock();
            lockedNodes.insert(node);
            if(node->disappearNode.load(std::memory_order_relaxed) ||
               node->GetNext(i) != targetUpperNode) {
              findAndLockOk = false;
              break;
            }
          }
        }
        if(findAndLockOk) {
          targetUpperNode->mutex.lock();
          lockedNodes.insert(targetUpperNode);
          if(targetUpperNode->disappearNode.load(std::memory_order_relaxed))
            findAndLockOk = false;
        }

        if(findAndLockOk) {
          PrepareNodeToDelete_(oldFanOutNodes, fanOutNodesOfTargetNodeRawPtr,
                               fanOutArrayOfTargetNode,
                               fanOutNodesCntOfTargetNode, targetUpperNode);
          TryFindNodeAndBuildNewFanOut_(
            findLocation, newFanOutNodesPtr, fanOutNodesCntOfTargetNode,
            fanOutArrayOfTargetNode, nodeToGcNvmNode);

          if(findLocation < 0)
            findAndLockOk = false;
        }

        if(!findAndLockOk) {
          UnlockAllNodes(lockedNodes);
          lockedNodes.clear();
        }
      } // while(!findAndLockOk)

      if(!quickEnd) {
        PmSkiplistNvmSingleNode *prevNodeInNvm;
        if(findLocation == 0) {
          auto leftBoundary0FanOutNodes =
            leftBoundary[0]->GetFanOutNodes().get();
          prevNodeInNvm =
            leftBoundary0FanOutNodes
              ->fanOutNodes[leftBoundary0FanOutNodes->fanOutNodesCount - 1]
              .nvmNode;
        } else
          prevNodeInNvm = fanOutArrayOfTargetNode[findLocation - 1].nvmNode;
        targetUpperNode->DeleteOneNode_(
          newFanOutNodesPtr, fanOutNodesCntOfTargetNode,
          fanOutNodesOfTargetNodeRawPtr, nodeToGcNvmNode, this, prevNodeInNvm);
        if(fanOutNodesCntOfTargetNode == 1) {
          targetUpperNode->disappearNode.store(true, std::memory_order_release);
          SFence();
          for(int i = targetUpperNode->GetIndexHeight() - 1; i >= 0; --i) {
            assert(targetUpperNode->GetNext(i)->disappearNode.load() != true);
            leftBoundary[i]->SetNext(i, targetUpperNode->GetNext(i));
          }
          upperNodesToSmr.PushWithLock(
            {targetUpperNode, timestamp->GetCurrentTimeFromDram()});
        }
        UnlockAllNodes(lockedNodes);
      }
    }
  }

  void PmSkiplistDramEntrance::Smr_() {
    auto &nvmHead = nvmEntrance->dummyHeadOfNodesToFree;
    while(!IsNvmNodeToSmrEmptyWithoutLock()) {
      uint64_t currentDeadTime = timestamp->GetDeadTime();
      PmSkiplistNvmSingleNode *nodeToSmr;
      {
        SpinLockGuard spinLockGuard{spinLockForSmr};
        if(!IsNvmNodeToSmrEmptyWithoutLock() &&
           (nodeToSmr = nvmHead.nextNode.GetVPtr())->disappearSq <=
             currentDeadTime) {
          //          assert(pmemobj_tx_stage() == TX_STAGE_WORK);
          nvmEntrance->PopNodeToSmrWithoutLock();

          if(nodeToSmr) {
            pmem::detail::destroy<PmSkiplistNvmSingleNode>(*nodeToSmr);
            auto oidToFree = pmemobj_oid(nodeToSmr);
            pmemobj_free(&oidToFree);
          }
          //          pmem::obj::delete_persistent<PmSkiplistNvmSingleNode>(nodeToSmr);
        } else
          break;
      }
    }
  }
  //  void PmSkiplistDramEntrance::CheckNvm() {
  //    auto nvmDummyHead = dummyHead.GetFanOutNodes()->fanOutNodes[0].nvmNode;
  //    auto nvmDummyTail = dummyTail.GetFanOutNodes()->fanOutNodes[0].nvmNode;
  //    auto currentNode = nvmDummyHead->nextNode.GetVPtr();
  //    while(currentNode != nvmDummyTail) {
  //      std::cout << currentNode->fileNum << " " << std::flush;
  //      currentNode = currentNode->nextNode.GetVPtr();
  //    }
  //    std::cout << std::endl << std::flush;
  //  }

  ///
  /// \param startNode 
  /// \param internalKey 
  /// \param prevNode 
  /// \param currentTime 
  /// \return 
  auto PmSkiplistDramEntrance::LowerBoundSearchUpperNode(
    PmSkiplistUpperNode *startNode, const Slice &internalKey,
    PmSkiplistUpperNode *&prevNode, uint64_t currentTime)
    -> PmSkiplistUpperNode * {
    assert(!isLevel0_);

    //    if(startNode && startNode != &dummyHead) {
    //      std::cout << "1\n" << std::flush;
    //    }


    if(startNode && startNode != &dummyHead &&
       Compare(startNode, internalKey, currentTime) >= 0 &&
       !startNode->disappearNode.load()) {
      prevNode = &dummyHead;
      return startNode;
    }

    if(!startNode)
      startNode = &dummyHead;

    auto currentNode = startNode;
    int currentHeight = maxIndexHeight; 
    int currentNodeHeight =
      currentNode->GetIndexHeight(); 
    PmSkiplistUpperNode *testNextNode;
    while(currentNodeHeight > 0 && currentHeight > 0) {
      testNextNode =
        currentNode->GetNext(std::min(currentHeight, currentNodeHeight) - 1);

      //      if(testNextNode != nullptr) {
      //        std::cout << "2\n" << std::flush;
      //      }

      if(testNextNode != nullptr &&
         Compare(testNextNode, internalKey, currentTime) < 0) {
        currentNode = testNextNode;
        assert(currentNode);
        currentNodeHeight = currentNode->GetIndexHeight();
      } else
        currentHeight = std::min(currentHeight, currentNodeHeight) - 1;
    }
    prevNode = currentNode;
    currentNode = currentNode->GetNext(0);
#ifndef NDEBUG
    if(!((startNode && startNode->disappearNode.load()) ||
         Compare(currentNode, internalKey, currentTime) >= 0)) {
      std::cout << "";
    }
#endif
    assert((startNode && startNode->disappearNode.load()) ||
           Compare(prevNode, internalKey, currentTime) < 0);
    return currentNode;
  }

  auto PmSkiplistNvmSingleNode::NewIterator() -> leveldb::Iterator * {
    return new leveldb::pmCache::PmSsTableIterator(this->GetPmSsTable());
  }

  auto PmSkiplistNvmSingleNode::Get(Slice &iKey, std::string *value,
                                    PmSkiplistNvmSingleNode::GetKind *getKind)
    -> bool {
    auto sstable = GetPmSsTable();
    auto begin = sstable->index;
    auto end = sstable->index + sstable->indexLen;
    auto targetIndex = std::lower_bound(begin, end, iKey);

    PmSsTable::DataNode *data;
    if(targetIndex != end &&
       targetIndex->HaveSameUserKey((data = targetIndex->GetData()), iKey)) {
      auto valueData = data->GetValue();
      auto valueLen = data->GetValueLen();
      auto keyData = data->GetUserKey();
      auto keyLen = data->GetUserKeyLen();
      char kind = *(keyData + keyLen);
      switch(kind) {
      case kTypeValue: {
        assert(valueLen > 0);
        value->assign(valueData, valueLen);
        *getKind = K_FOUND;
        return true;
      }
      case kTypeDeletion: {
        *getKind = K_DELETED;
        return true;
      }
      default: *getKind = K_CORRUPTED; return false;
      }
    } else {
      *getKind = K_NOT_FOUND;
      return true;
    }
  }
} // namespace leveldb::pmCache::skiplistWithFanOut