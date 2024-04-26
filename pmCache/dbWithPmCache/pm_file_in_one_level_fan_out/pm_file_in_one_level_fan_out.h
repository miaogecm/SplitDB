//
//

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_FAN_OUT_PM_FILE_IN_ONE_LEVEL_FAN_OUT_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_FAN_OUT_PM_FILE_IN_ONE_LEVEL_FAN_OUT_H_

#include "../help/pmem.h"
#include "dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.h"
#include "dbWithPmCache/help/thread_pool/thread_pool.h"
#include "dbWithPmCache/pmem_sstable/pm_SSTable.h"
#include "leveldb/slice.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h"
#include <cstdint>
#include <memory>
#include <queue>
#include <set>
#include <shared_mutex>

namespace leveldb {
  class FileMetaData;
}
namespace leveldb::pmCache {
  class DramLruNode;
}
namespace leveldb::pmCache::skiplistWithFanOut {
  class PmSkiplistFanOutSingleNode;
  class PmSkiplistNvmSingleNode;
  class PmSkiplistFanOutGroupNodes;
  class PmSkiplistUpperNode;
  class PmSkiplistDramEntrance;
  class PmSkiplistNvmSingleNodeGcMeta {
   public:
    PmSkiplistNvmSingleNode *nvmNode{};
    uint64_t time = 0;
  };
  class PmSkiplistUpperNodeGcMeta {
   public:
    PmSkiplistUpperNode *upperNode{};
    uint64_t time = 0;
  };

  class SpinLock {
   public:
    inline void Lock() {
      for(;;) {
        if(!lock_.exchange(true, std::memory_order_acquire))
          break;
        while(lock_.load(std::memory_order_relaxed))
          std::this_thread::yield();
      }
    }
    inline void UnLock() { lock_.store(false, std::memory_order_release); }

   private:
    std::atomic<bool> lock_{false};
  };

  class SpinLockGuard {
   public:
    explicit SpinLockGuard(SpinLock &lock) {
      myLock_ = &lock;
      lock.Lock();
    }
    ~SpinLockGuard() { myLock_->UnLock(); }

   private:
    SpinLock *myLock_;
  };

  template <class T> class ConcurrentQueue {
   public:
    ~ConcurrentQueue() {
#ifndef NDEGBU
      std::cout << "";
#endif
    }

    inline void SpinLock() { lock_.Lock(); }

    inline void UnLock() { lock_.UnLock(); }

    inline void PushWithLock(T node) {
      SpinLock();
      queue_.push(node);
      UnLock();
    }

    inline auto FrontWithoutLock() -> T { return queue_.front(); }

    inline void PopWithoutLock() { queue_.pop(); }

    inline auto IsEmptyWithoutLock() -> bool { return queue_.empty(); }

   private:
    class SpinLock lock_ {};
    std::queue<T> queue_{};
  };

  template <class T> class ConcurrentQueueLockGuard {
   public:
    explicit ConcurrentQueueLockGuard(ConcurrentQueue<T> *queue)
        : queue_(queue) {
      queue_->SpinLock();
    }

    ConcurrentQueueLockGuard() = delete;

    ConcurrentQueueLockGuard(const ConcurrentQueueLockGuard &) = delete;

    ConcurrentQueueLockGuard(ConcurrentQueueLockGuard &&) = delete;

    auto operator=(const ConcurrentQueueLockGuard &)
      -> ConcurrentQueueLockGuard & = delete;

    ~ConcurrentQueueLockGuard() { queue_->UnLock(); }

   private:
    ConcurrentQueue<T> *queue_;
  };

  template <class T> class PersistentPtrLight {
   public:
    auto operator=(const PersistentPtrLight<T> &other)
      -> const PersistentPtrLight<T> & = delete;
    //    inline void AddTxRangeAndSnapshot();
    //    inline void AddTxRangeWithoutSnapshot();
    inline auto GetVPtr() -> T *;
    inline auto GetOffset() -> uint64_t;
    inline void SetWithoutBoundaryOrFlush(T *data);
    inline void SetWithoutBoundaryOrFlush(uint64_t off);
    //    inline void SetWithoutBoundaryOrFlush(PersistentPtrLight<T> *ptr);
    //    inline void SetWithoutBoundaryOrFlush(PersistentPtrLight<T> &ptr);
    //    inline void SetWithoutBoundaryOrFlush(pmem::obj::persistent_ptr<T> ptr);
    inline auto GetPtrDataAddr() -> uint64_t *;
    inline void FlushSelf();

   private:
    uint64_t offset_ = 0;
  };

  class PmSkiplistInternalKey {
   public:
    PmSkiplistInternalKey() = default;

    auto
    operator=(const PmSkiplistInternalKey &other) -> PmSkiplistInternalKey &;

   public:
    [[nodiscard]] inline auto GetKey() const -> Slice;
    inline void SetKey(Slice internalKey);
    [[nodiscard]] inline auto EnoughSize(uint32_t keyLen) const -> bool;
    [[nodiscard]] auto Compare(const PmSkiplistInternalKey &key1) const -> int;

   private:
    uint32_t keyLen_ = 0;
    char keyData_[PM_CACHE_RECOMMEND_KEY_LEN + 8]{};
    std::unique_ptr<char[]> longKeyData_{};
  };

  class PmSkiplistUpperNode {
   private:
    friend class PmSkiplistDramEntrance;

   public:
    PmSkiplistUpperNode(uint8_t indexHeight,
                        const std::vector<PmSkiplistNvmSingleNode *> &nodes,
                        bool isDummyNode);
    ~PmSkiplistUpperNode();

   public:
    auto SetNextLevelHint(PmSkiplistUpperNode *nextLevelHintNode) -> bool;
    void ClearNextLevelHint();
    auto GetNextLevelHint() -> PmSkiplistUpperNode *;

    [[nodiscard]] inline auto GetIndexHeight() const -> uint8_t {
      return indexHeight_;
    }
    [[nodiscard]] inline auto
    GetFanOutNodes() -> std::shared_ptr<PmSkiplistFanOutGroupNodes>;
    inline void SetFanOutNodes(PmSkiplistFanOutGroupNodes *newFanOutNodes) {
      std::atomic_store(
        &fanOutGroupNodes_,
        std::shared_ptr<PmSkiplistFanOutGroupNodes>(newFanOutNodes));
    }
    [[nodiscard]] inline auto GetFileNunOfKey() const -> uint64_t {
      return fileNumOfKey_;
    }
    [[nodiscard]] inline auto GetKey() -> PmSkiplistInternalKey & {
      return internalKey_;
    }
    [[nodiscard]] inline auto GetBornSqOfKey() const -> uint64_t {
      return bornSqOfKey_;
    }
    void SetNext(int i, PmSkiplistUpperNode *nextNode) {
      __atomic_store_n(nextNodes_ + i, nextNode, __ATOMIC_RELEASE);
    }
    auto GetNext(int i) -> PmSkiplistUpperNode * {
      return __atomic_load_n(nextNodes_ + i, __ATOMIC_ACQUIRE);
    }

    void UpdateFanOutNodes(const std::vector<PmSkiplistNvmSingleNode *> &nodes,
                           std::vector<PmSkiplistUpperNode *> leftBoundary,
                           bool isLevel0, GlobalTimestamp *time,
                           uint8_t maxIndexHeight,
                           std::set<PmSkiplistUpperNode *> &lockedNodes,
                           std::vector<Slice> &keys);
    //    void TestPrint();

   private:
    inline void
    DeleteOneNode_(const std::vector<PmSkiplistFanOutSingleNode *> &newNodes,
                   uint32_t oldCnt,
                   PmSkiplistFanOutGroupNodes *oldFanOutGroupNodes,
                   PmSkiplistNvmSingleNode *nodeToGcNvmNode,
                   PmSkiplistDramEntrance *dramEntrance,
                   PmSkiplistNvmSingleNode *prevNodeInNvm);

   public:
    std::shared_mutex nextLevelHintMutex{};
    std::mutex mutex{};
    uint32_t refCount = 0;
    std::atomic<bool> disappearNode = false;
    bool lostNode = false;

   private:
    std::atomic<PmSkiplistUpperNode *> nextLevelHint_ = nullptr;
    uint64_t fileNumOfKey_ = 0;
    uint64_t bornSqOfKey_{UINT64_MAX};
    PmSkiplistInternalKey internalKey_{};
    std::shared_ptr<PmSkiplistFanOutGroupNodes> fanOutGroupNodes_{};
    uint8_t indexHeight_;
    PmSkiplistUpperNode *nextNodes_[PM_CACHE_MAX_INDEX_HEIGHT]{};
  };

  class PmSkiplistFanOutSingleNode {
   public:
    void
    InitWithNodeInNvm(PmSkiplistNvmSingleNode *nodeInNvm, bool isDummyNode);
    [[nodiscard]] inline uint64_t GetBornSq() const {
      return __atomic_load_n(&bornSqOfKey_, __ATOMIC_RELAXED);
    }
    [[nodiscard]] inline uint64_t GetDeadSq() const {
      return __atomic_load_n(&deadSqOfKey_, __ATOMIC_RELAXED);
    }
    inline void SetDeadSq(uint64_t deadSq) {
      __atomic_store_n(&deadSqOfKey_, deadSq, __ATOMIC_RELAXED);
    }

   public:
    PmSkiplistNvmSingleNode *nvmNode{};
    PmSkiplistInternalKey key{};

   private:
    uint64_t bornSqOfKey_{UINT64_MAX};
    uint64_t deadSqOfKey_{UINT64_MAX};
  };

  class PmSkiplistFanOutGroupNodes {
   public:
    uint64_t lastSplitTime = 0;
    uint32_t fanOutNodesCount = 0;
    std::unique_ptr<PmSkiplistFanOutSingleNode[]> fanOutNodes;
  };

  class PmSkiplistNvmSingleNode {
   public:
    PmSkiplistNvmSingleNode() = default;
    explicit PmSkiplistNvmSingleNode(uint32_t kvNodeCount, uint64_t bornSq,
                                     uint64_t deadSq, uint64_t fileNum)
        : bornSq(bornSq), deadSq(deadSq), fileNum(fileNum) {
      new(GetPmSsTable()) PmSsTable(kvNodeCount);
    }
    ~PmSkiplistNvmSingleNode() { GetPmSsTable()->ClearData(); }

   public:
    enum GetKind {
      K_FOUND = 0,
      K_NOT_FOUND = 1,
      K_DELETED = 2,
      K_CORRUPTED = 3
    };
    inline auto GetPmSsTable() -> PmSsTable * {
      return (PmSsTable *)(this + 1);
    }
    auto NewIterator() -> leveldb::Iterator *;
    auto Get(Slice &iKey, std::string *value, GetKind *getKind) -> bool;

   public:
    uint64_t bornSq = 0, deadSq = UINT64_MAX, fileNum = 0, disappearSq = 0;
    PersistentPtrLight<PmSkiplistNvmSingleNode> nextNode{};
    DramLruNode *lruNode = nullptr;

   private:
    /// followed by PmSsTable
  };

  class PmSkiplistNvmSingleNodeBuilder {
   public:
    PmSkiplistNvmSingleNodeBuilder() {
      ssTableBuilder.AllocateRootOfKvListInTxAndAddRange();
      sizeAllocated += sizeof(PmSsTable::DataNode) * 2;
    }

    auto Generate(uint64_t bornSq, uint64_t deadSq,
                  uint64_t fileNum) -> PmSkiplistNvmSingleNode *;

    //    auto
    //    TestGenerate(uint64_t deadSq, uint64_t fileNum, const Slice
    //    &internalKey1,
    //                 const Slice &internalKey2) -> PmSkiplistNvmSingleNode *;

    inline void
    AddNodeToIndexOfSsTableAndUpdateSizeAllocated(uint64_t offsetInNvm,
                                                  uint64_t keySize,
                                                  uint64_t valueSize) {
      ssTableBuilder.AddNodeToIndex(offsetInNvm);
      sizeAllocated +=
        pmCache::PmSsTable::DataNode::CalculateSize(keySize, valueSize);
    }

    /// \param fileMeta
    void PushNodeFromAnotherNvTable(FileMetaData *fileMeta);

    inline void
    PushNvmNodeToIndexWithoutChangeLinks(PmSsTable::DataNode *node) {
      assert(node);
      ssTableBuilder.PushNodeToIndexWithoutChangeLinksOrFlush(node);
      sizeAllocated += node->GetThisSize();
    }

    inline void PushNvmNodeToIndexWithoutChangeLinks(PmSsTable::DataNode *node,
                                                     uint64_t internalKeyLen,
                                                     uint64_t valueLen) {
      assert(node);
      ssTableBuilder.PushNodeToIndexWithoutChangeLinksOrFlush(node);
      sizeAllocated +=
        PmSsTable::DataNode::CalculateSize(internalKeyLen, valueLen);
    }

   public:
    InternalKey smallest, largest;
    PmSsTableBuilder ssTableBuilder{}; // TODO add kv node to it
    uint64_t sizeAllocated = 0;
  };

  class PmSkiplistNvmEntrance {
   public:
    PmSkiplistNvmEntrance() {
      dummyHead.nextNode.SetWithoutBoundaryOrFlush(&dummyTail);
      pmemobj_flush(_pobj_cached_pool.pop, &dummyHead.nextNode,
                    sizeof(dummyHead.nextNode));
      nextPersistentPointerOfLastNodeInListOfNvmNodesToFree =
        &dummyHeadOfNodesToFree.nextNode;
    }

    inline void PushNodeToSmrWithoutLock(PmSkiplistNvmSingleNode *nvmNode) {
      //      assert(pmemobj_tx_stage() == TX_STAGE_WORK);
      nextPersistentPointerOfLastNodeInListOfNvmNodesToFree
        ->SetWithoutBoundaryOrFlush(nvmNode);
      nvmNode->nextNode.SetWithoutBoundaryOrFlush((uint64_t)0);
      nextPersistentPointerOfLastNodeInListOfNvmNodesToFree =
        &nvmNode->nextNode;
    }

    inline void PopNodeToSmrWithoutLock() {
      //      assert(pmemobj_tx_stage() == TX_STAGE_WORK);
      auto currentNode = dummyHeadOfNodesToFree.nextNode.GetVPtr();
      //      dummyHeadOfNodesToFree.nextNode.AddTxRangeAndSnapshot();
      dummyHeadOfNodesToFree.nextNode.SetWithoutBoundaryOrFlush(
        currentNode->nextNode.GetVPtr());
      if(nextPersistentPointerOfLastNodeInListOfNvmNodesToFree ==
         &currentNode->nextNode)
        nextPersistentPointerOfLastNodeInListOfNvmNodesToFree =
          &dummyHeadOfNodesToFree.nextNode;
    }

   public:
    PmSkiplistNvmSingleNode dummyHead{}, dummyTail{};

    PmSkiplistNvmSingleNode dummyHeadOfNodesToFree{};
    PersistentPtrLight<PmSkiplistNvmSingleNode>
      *nextPersistentPointerOfLastNodeInListOfNvmNodesToFree{};
  };

  class PmSkiplistDramEntrance {
   private:
    friend class PmSkiplistUpperNode;

   public:
    class CompareNodeForLevel1P {
     public:
      leveldb::Slice key;
      uint64_t bornSq;
    };
    class CompareNodeForLevel0 {
     public:
      uint64_t fileNum;
      uint64_t bornSq;
    };

   public:
    PmSkiplistDramEntrance(uint8_t indexHeight,
                           PmSkiplistNvmEntrance *nvmEntrance, bool isLevel0,
                           GlobalTimestamp *t, ThreadPool *threadPool);

   public:
    //    void CheckNvm();
    void ReplaceNodes(const std::vector<PmSkiplistNvmSingleNode *> &newNodes,
                      const std::vector<PmSkiplistNvmSingleNode *> &oldNodes);
    void GetLeftBoundary(std::vector<PmSkiplistUpperNode *> &leftBoundary,
                         PmSkiplistNvmSingleNode *nvNode, uint64_t bornSq);
    void ScheduleBackgroundGcAndSmr();
    inline void
    PushNvmNodeToSmr(PmSkiplistNvmSingleNode *deletedNvmNode) const {
      nvmEntrance->PushNodeToSmrWithoutLock(deletedNvmNode);
    }

    [[nodiscard]] inline auto IsNvmNodeToSmrEmptyWithoutLock() const -> bool {
      return nvmEntrance
               ->nextPersistentPointerOfLastNodeInListOfNvmNodesToFree ==
             &nvmEntrance->dummyHeadOfNodesToFree.nextNode;
    }

    auto
    LowerBoundSearchUpperNode(PmSkiplistUpperNode *startNode,
                              const Slice &internalKey,
                              PmSkiplistUpperNode *&prevNode,
                              uint64_t currentTime) -> PmSkiplistUpperNode *;

   private:
    static void PrepareNodeToDelete_(
      std::shared_ptr<PmSkiplistFanOutGroupNodes> &oldFanOutNodes,
      PmSkiplistFanOutGroupNodes *&fanOutNodesOfTargetNodeRawPtr,
      PmSkiplistFanOutSingleNode *&fanOutArrayOfTargetNode,
      uint32_t &fanOutNodesCntOfTargetNode,
      PmSkiplistUpperNode *targetUpperNode) {
      oldFanOutNodes = targetUpperNode->GetFanOutNodes();
      fanOutNodesOfTargetNodeRawPtr = oldFanOutNodes.get();
      fanOutArrayOfTargetNode =
        fanOutNodesOfTargetNodeRawPtr->fanOutNodes.get();
      fanOutNodesCntOfTargetNode =
        fanOutNodesOfTargetNodeRawPtr->fanOutNodesCount;
    }

    static void TryFindNodeAndBuildNewFanOut_(
      int &findLocation,
      std::vector<PmSkiplistFanOutSingleNode *> &newFanOutNodesPtr,
      uint32_t fanOutNodesCntOfTargetNode,
      PmSkiplistFanOutSingleNode *fanOutArrayOfTargetNode,
      PmSkiplistNvmSingleNode *nodeToGcNvmNode) {

      findLocation = -1;
      newFanOutNodesPtr.resize(fanOutNodesCntOfTargetNode - 1);
      int k = 0;
      for(uint32_t i = 0; i < fanOutNodesCntOfTargetNode; ++i) {
        if(fanOutArrayOfTargetNode[i].nvmNode == nodeToGcNvmNode)
          findLocation = i;
        else
          newFanOutNodesPtr[k++] = &fanOutArrayOfTargetNode[i];
      }
    }

    void Gc_();
    void Smr_();

   public:
    auto Compare(PmSkiplistUpperNode &a, PmSkiplistUpperNode &b) const -> int;
    auto Compare(PmSkiplistUpperNode *a, PmSkiplistUpperNode *b) const -> int;
    //    auto Compare(PmSkiplistFanOutSingleNode *a,
    //                 PmSkiplistNvmSingleNode *b) const -> int;
    [[nodiscard]] auto Compare(const PmSkiplistFanOutSingleNode &a,
                               const CompareNodeForLevel1P &b) const -> int;
    [[nodiscard]] auto Compare(const PmSkiplistFanOutSingleNode &a,
                               const CompareNodeForLevel0 &b) const -> int;
    //    auto
    //    Compare(PmSkiplistUpperNode *a, PmSkiplistNvmSingleNode *b) const ->
    //    int;
    auto Compare(PmSkiplistUpperNode *a, PmSkiplistNvmSingleNode *b,
                 Slice bMaxInternalKey) const -> int;

    [[nodiscard]] static auto
    Compare(const Slice &a, const Slice &b, uint64_t bornSqA,
            uint64_t bornSqB) -> int;
    [[nodiscard]] static auto
    Compare(uint64_t aNum, uint64_t bNum, uint64_t bornSqA,
            uint64_t bornSqB) -> int;
    auto
    Compare(PmSkiplistUpperNode *a, leveldb::Slice b, uint64_t bornSqB) -> int;
    auto
    Compare(PmSkiplistUpperNode *a, uint64_t bNum, uint64_t bornSqB) -> int;

   private:
   public:
    uint8_t maxIndexHeight = PM_CACHE_MAX_INDEX_HEIGHT;
    PmSkiplistUpperNode dummyHead, dummyTail;

    PmSkiplistNvmEntrance *nvmEntrance;
    SpinLock spinLockForSmr{};

    GlobalTimestamp *timestamp{};
    ConcurrentQueue<PmSkiplistNvmSingleNodeGcMeta> nvmNodesToGc;
    ConcurrentQueue<PmSkiplistUpperNodeGcMeta> upperNodesToSmr;

    pmem::obj::pool_base pool{};

   private:
    ThreadPool *threadPool_{};
    bool isLevel0_ = false;
  };

  auto PmSkiplistInternalKey::GetKey() const -> Slice {
    return Slice{keyLen_ > PM_CACHE_RECOMMEND_KEY_LEN + 8 ? longKeyData_.get()
                                                          : keyData_,
                 keyLen_};
  }
  auto PmSkiplistInternalKey::EnoughSize(uint32_t keyLen) const -> bool {
    return keyLen_ <= sizeof(keyData_);
  }
  void PmSkiplistInternalKey::SetKey(Slice internalKey) {
    keyLen_ = internalKey.size();
    memcpy(EnoughSize(keyLen_)
             ? keyData_
             : (longKeyData_ = std::make_unique<char[]>(keyLen_)).get(),
           internalKey.data(), keyLen_);
  }
  auto PmSkiplistUpperNode::GetFanOutNodes()
    -> std::shared_ptr<PmSkiplistFanOutGroupNodes> {
    return std::atomic_load(&fanOutGroupNodes_);
  }

  //  template <class T> void PersistentPtrLight<T>::AddTxRangeAndSnapshot() {
  //    pmemobj_tx_add_range_direct(GetPtrDataAddr(),
  //                                sizeof(PersistentPtrLight<T>));
  //  }
  //  template <class T> void PersistentPtrLight<T>::AddTxRangeWithoutSnapshot()
  //  {
  //    pmemobj_tx_xadd_range_direct(
  //      GetPtrDataAddr(), sizeof(PersistentPtrLight<T>),
  //      POBJ_XADD_NO_SNAPSHOT);
  //  }
  template <class T> auto PersistentPtrLight<T>::GetVPtr() -> T * {
    if(offset_ == 0)
      return nullptr;
    return ::leveldb::pmCache::GetVPtr<T>(offset_);
  }
  template <class T> auto PersistentPtrLight<T>::GetOffset() -> uint64_t {
    return offset_;
  }
  template <class T>
  void PersistentPtrLight<T>::SetWithoutBoundaryOrFlush(T *data) {
    if(data == nullptr) {
      offset_ = 0;
      return;
    }
    assert(_pobj_cached_pool.pop);
    offset_ = (uint64_t)data - (uint64_t)(_pobj_cached_pool.pop);
  }
  template <class T>
  void PersistentPtrLight<T>::SetWithoutBoundaryOrFlush(uint64_t off) {
    offset_ = off;
  }
  //  template <class T>
  //  void
  //  PersistentPtrLight<T>::SetWithoutBoundaryOrFlush(PersistentPtrLight<T>
  //  *ptr) {
  //    offset_ = ptr->offset_;
  //  }
  //  template <class T>
  //  void
  //  PersistentPtrLight<T>::SetWithoutBoundaryOrFlush(PersistentPtrLight<T>
  //  &ptr) {
  //    offset_ = ptr.offset_;
  //  }
  //  template <class T>
  //  void PersistentPtrLight<T>::SetWithoutBoundaryOrFlush(persistent_ptr<T>
  //  ptr) {
  //    offset_ = ptr.raw().off;
  //  }
  template <class T>
  auto PersistentPtrLight<T>::GetPtrDataAddr() -> uint64_t * {
    return &offset_;
  }
  template <class T> void PersistentPtrLight<T>::FlushSelf() {
    assert(_pobj_cached_pool.pop);
    pmemobj_flush(_pobj_cached_pool.pop, &offset_, sizeof(uint64_t));
  }

  void PmSkiplistUpperNode::DeleteOneNode_(
    const std::vector<PmSkiplistFanOutSingleNode *> &newNodes, uint32_t oldCnt,
    PmSkiplistFanOutGroupNodes *oldFanOutGroupNodes,
    PmSkiplistNvmSingleNode *nodeToGcNvmNode,
    PmSkiplistDramEntrance *dramEntrance,
    PmSkiplistNvmSingleNode *prevNodeInNvm) {
    assert(oldCnt >= 1);
    auto newFanOutNodes =
      oldCnt > 1 ? new PmSkiplistFanOutSingleNode[oldCnt - 1]{} : nullptr;
    if(newFanOutNodes)
      for(int i = 0; i < int(oldCnt - 1); ++i)
        newFanOutNodes[i] = *newNodes[i];

    auto newFanOutGroupNodes = new PmSkiplistFanOutGroupNodes{};
    newFanOutGroupNodes->fanOutNodes.reset(newFanOutNodes);
    newFanOutGroupNodes->fanOutNodesCount = oldCnt - 1;
    newFanOutGroupNodes->lastSplitTime = oldFanOutGroupNodes->lastSplitTime;

    SetFanOutNodes(newFanOutGroupNodes);
    {
      auto &next = prevNodeInNvm->nextNode;
      __atomic_store_n(next.GetPtrDataAddr(),
                       nodeToGcNvmNode->nextNode.GetOffset(), __ATOMIC_RELEASE);
      //      next.AddTxRangeAndSnapshot();
      //      next.SetWithoutBoundaryOrFlush(nodeToGcNvmNode->nextNode);
      next.FlushSelf();
      {
        SpinLockGuard smrLockGuard{dramEntrance->spinLockForSmr};
        nodeToGcNvmNode->disappearSq =
          dramEntrance->timestamp->GetCurrentTimeFromDram();
        SFence();
        dramEntrance->PushNvmNodeToSmr(nodeToGcNvmNode);
      }
    }
  }

} // namespace leveldb::pmCache::skiplistWithFanOut

#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_PM_FILE_IN_ONE_LEVEL_FAN_OUT_PM_FILE_IN_ONE_LEVEL_FAN_OUT_H_
