//
// Created by jxz on 22-8-2.
//

#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_PMEM_SSTABLE_PM_SSTABLE_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_PMEM_SSTABLE_PM_SSTABLE_H_

#include <memory>

#include "leveldb/slice.h"
#include "pmCache/dbWithPmCache/help/pmem.h"

namespace leveldb::pmCache {
  class PmWal;
  class PmSsTable {
   public:
    class DataNode;

    class Index {
     public:
      Index() = default;

      inline explicit Index(uint64_t off);

      inline auto operator=(const Index &otherIndex) -> Index & = default;

      inline auto operator<(const Index &i) const -> bool;

      inline auto operator<(const Slice &internalKey) const -> bool;

      [[nodiscard]] inline auto GetData() const -> PmSsTable::DataNode *;

      [[maybe_unused]] [[nodiscard]] inline auto GetOff() const -> uint64_t;

      //      inline void TestPrint() const;

      static inline auto HaveSameUserKey(PmSsTable::DataNode *node,
                                         const leveldb::Slice &iKey) -> bool;

     private:
      uint64_t dataNodeOff_ = 0;
    };

    PmSsTable(const PmSsTable &) = delete;

    PmSsTable(PmSsTable &&) = delete;

    auto operator=(const PmSsTable &) -> PmSsTable & = delete;

    PmSsTable() = delete;

    inline ~PmSsTable(); // NOLINT(bugprone-exception-escape)

    static inline auto GenerateDataNode(const Slice &internalKey,
                                        const Slice &value) -> DataNode *;

    //    [[maybe_unused]] static inline auto
    //    GenerateDataNodeWithoutFlush(const Slice &userKey, const Slice &value,
    //                                 uint64_t sqPackedWithType) -> DataNode *;

    static inline auto
    GenerateDataNodeWithoutFlush(const Slice &userKey, const Slice &value,
                                 uint64_t sqPackedWithType,
                                 uint64_t &newNodeOff) -> DataNode *;

    inline void SetFirstDataNodeOff(uint64_t off);

    inline void SetIndexWithoutFlush(const std::vector<Index> &index);

    [[maybe_unused]] inline auto
    LowerBound(const Slice &targetInternalKey) -> Index;

    inline auto BinarySearch(const Slice &targetInternalKey) -> int;

    //    [[maybe_unused]] [[maybe_unused]] inline auto
    //    BinarySearchReturnIndex(const Slice &targetInternalKey) -> Index *;

    inline auto GetMaxInternalKey() -> Slice;

    inline auto GetMinInternalKey() -> Slice;

    //    inline auto GetMaxUserKey() -> Slice;

    [[nodiscard]] inline auto GetDataNodeHead() const -> PmSsTable::DataNode *;

    inline void ClearData();

    [[nodiscard]] inline auto GetIndexS() -> Index *;

    [[nodiscard]] inline auto GetIndexLen() const -> uint32_t;

    //    [[maybe_unused]] inline auto GetIndex(int i) -> Index;

    void Init() const;

    //    inline void TestPrint();

    explicit PmSsTable(uint32_t indexLen) : indexLen(indexLen) {}

    class Iterator {
     public:
      Index *ptr = nullptr;
      auto operator++() -> Iterator & {
        ptr++;
        return *this;
      }
      auto operator!=(const Iterator &it) const -> bool {
        return ptr != it.ptr;
      }
      auto operator*() const -> Index & { return *ptr; }
    };

    auto begin() -> Iterator {
      Iterator it;
      it.ptr = &index[0];
      return it;
    }

    auto end() -> Iterator {
      Iterator it;
      it.ptr = &index[indexLen];
      return it;
    }

   public:
    uint64_t firstDataNodeOff; 
    uint32_t indexLen;
    Index index[1]; // NOLINT(modernize-avoid-c-arrays)
    // follow by index_[indexLen-1]
  };

  class PmSsTableBuilder {
   public:
    PmSsTableBuilder(const PmSsTableBuilder &) = delete;

    PmSsTableBuilder(PmSsTableBuilder &&) = delete;

    auto operator=(const PmSsTableBuilder &) -> PmSsTableBuilder & = delete;

    PmSsTableBuilder() { offsetIndex.reserve(60000); }

    inline void AllocateRootOfKvList();

    inline void AllocateRootOfKvListInTxAndAddRange();

    inline void PushNodeWithoutFlush(PmSsTable::DataNode *node, bool aloneNode);

    //    [[maybe_unused]] inline void
    //    PushNodeWithoutAddRange(PmSsTable::DataNode *node);

    inline void AddNodeToIndex(uint64_t nodeOff);

    inline void PushNodeFromAnotherSsTableWithoutFlush(PmSsTable *node);

    //    [[maybe_unused]] void
    //    PushNodeFromAnotherSsTableAndAddRange(PmSsTable *node);

    void GetControlFromPmWalAndAddRangeWithoutAddIndex(PmWal *walNode) const;

    inline void
    PushNodeToIndexWithoutChangeLinksOrFlush(PmSsTable::DataNode *node);

    [[nodiscard]] inline auto CalculateExtraSizeForIndex() const -> size_t {
      return sizeof(PmSsTable::Index) * (offsetIndex.size() - 1);
    }

    PmSsTable::DataNode *dummyHead = nullptr;
    PmSsTable::DataNode *dummyTail = nullptr;

    auto GetPmSsTable() -> PmSsTable * { return pmSsTable_; }
    void SetPmSsTable(PmSsTable *pmSsTable) { pmSsTable_ = pmSsTable; }

   private:
    [[maybe_unused]] inline void FlushNodeLinks_() const;

   public:
    uint64_t sizeAllocated = 0;
    std::vector<PmSsTable::Index> offsetIndex;

   private:
    PmSsTable *pmSsTable_ = nullptr;
    std::vector<PmSsTable::DataNode *> nodeToFlushData_;
  };

  class PmSsTable::DataNode {
   public:
    enum CompareResult {
      K_NODE_1_LESS_NODE_2 = -1,
      K_NODE_1_EQUAL_NODE_2 = 0,
      K_NODE_1_GREATER_NODE_2 = 1
    };

    DataNode() = default;
    DataNode(const DataNode &) = delete;
    DataNode(DataNode &&) = delete;
    auto operator=(const DataNode &) -> DataNode & = delete;
    ~DataNode() = default;
    static inline auto
    CalculateSize(uint64_t internalKeyLen, uint64_t valueLen) -> uint32_t;
    inline auto GetInternalKey() -> char *;
    inline auto GetInternalKeySlice() -> Slice;
    inline auto GetValueSlice() -> Slice;
    inline auto GetValue() -> char *;
    inline auto GetUserKey() -> char *;
    inline auto GetSqAndKind() -> uint64_t;
    [[nodiscard]] inline auto GetInternalKeyLen() const -> uint32_t;
    [[nodiscard]] inline auto GetValueLen() const -> uint32_t;
    [[nodiscard]] inline auto GetUserKeyLen() const -> uint32_t;
    inline void SetInternalKeyAndValueWithoutFlush(const Slice &internalKey,
                                                   const Slice &value);
    inline void
    SetInternalKeyAndValueWithoutFlush(const Slice &userKey, const Slice &value,
                                       uint64_t sqPackedWithType);
    inline auto Compare(DataNode &d) -> int;
    inline auto Compare(const Slice &s) -> int;
    inline auto Compare(DataNode *d) -> int;
    inline auto operator<(DataNode &d2) -> bool;
    [[nodiscard]] inline auto PrevNode() const -> DataNode *;
    inline void SetPrevNodeOff(DataNode *prevDataNode);
    inline void SetPrevNodeOff(uint64_t prevDataNodeOff);
    [[nodiscard]] inline auto NextNode() const -> DataNode *;
    inline void SetNextNodeOff(DataNode *nextDataNode);
    inline void SetNextNodeOff(uint64_t nextDataNodeOff);
    //    inline void FlushNextNodeOff() {
    //      pmemobj_flush(_pobj_cached_pool.pop, &nextNodeOff, sizeof(uint64_t));
    //    }
    //    inline void FlushPrevNodeOff() {
    //      pmemobj_flush(_pobj_cached_pool.pop, &prevNodeOff, sizeof(uint64_t));
    //    }
    [[nodiscard]] inline auto GetThisSize() const -> int {
      return (int)CalculateSize(internalKeyLen, valueLen);
    }
    inline auto GetNextOffAddr() -> const uint64_t * { return &nextNodeOff; }
    inline auto GetPrevOffAddr() -> const uint64_t * { return &prevNodeOff; }

   public:
    uint64_t nextNodeOff = 0;
    uint64_t prevNodeOff = 0;
    uint32_t valueLen = 0;
    uint32_t internalKeyLen = 0;
    uint8_t off = 0;

   private:
    char data_[1]{}; // NOLINT(modernize-avoid-c-arrays)
  };

  auto
  PmSsTable::Index::operator<(const leveldb::pmCache::PmSsTable::Index &i) const
    -> bool {
    return GetVPtr<DataNode>(dataNodeOff_)
             ->Compare(GetVPtr<DataNode>(i.dataNodeOff_)) < 0;
  }

  auto PmSsTable::Index::operator<(const Slice &internalKey) const -> bool {
    auto node = GetData();
    return node->Compare(internalKey) < 0;
  }

  auto PmSsTable::Index::HaveSameUserKey(PmSsTable::DataNode *node,
                                         const leveldb::Slice &iKey) -> bool {
    auto interKeyLenThisNode = node->GetInternalKeyLen();
    return (interKeyLenThisNode == iKey.size()) &&
           memcmp(node->GetUserKey(), iKey.data(), interKeyLenThisNode - 8) ==
             0;
  }

  PmSsTable::Index::Index(uint64_t off) { dataNodeOff_ = off; }

  //  void PmSsTable::Index::TestPrint() const {
  //    auto node = GetVPtr<PmSsTable::DataNode>(dataNodeOff_);
  //    std::cout << "k:"
  //              << std::string(node->GetInternalKey(),
  //                             node->GetInternalKeyLen() - 8)
  //              << ", v:" << std::string(node->GetValue(), node->GetValueLen())
  //              << std::endl;
  //  }

  auto PmSsTable::Index::GetData() const -> PmSsTable::DataNode * {
    return GetVPtr<PmSsTable::DataNode>(dataNodeOff_);
  }

  [[maybe_unused]] auto PmSsTable::Index::GetOff() const -> uint64_t {
    return dataNodeOff_;
  }

  auto leveldb::pmCache::PmSsTable::GenerateDataNode(
    const leveldb::Slice &internalKey, const leveldb::Slice &value)
    -> leveldb::pmCache::PmSsTable::DataNode * {

    //    std::cout<<"promoting\n"<<std::flush;

    auto newDataLen = DataNode::CalculateSize(internalKey.size(), value.size());

    //    auto node = GetVPtr<PmSsTable::DataNode>(
    //      pmemobj_tx_xalloc(newDataLen, 0, POBJ_XALLOC_NO_FLUSH).off);

    PMEMoid oid;
    pmemobj_alloc(_pobj_cached_pool.pop, &oid, newDataLen, 0, nullptr, nullptr);
    auto node = GetVPtr<PmSsTable::DataNode>(oid.off);

    auto mod = (uint64_t)node % L1_CACHE_BYTES;
    node =
      (PmSsTable::DataNode *)(mod == 0 ? (char *)node
                                       : (char *)node + (L1_CACHE_BYTES - mod));

    // S
    //    std::stringstream ss;
    //    ss << "w " << newDataLen << std::endl;
    //    std::cout << ss.str() << std::flush;

    new(node) PmSsTable::DataNode();
    if(mod != 0)
      node->off = L1_CACHE_BYTES - mod;
    else
      node->off = 0;
    node->SetInternalKeyAndValueWithoutFlush(internalKey, value);

    pmemobj_flush(_pobj_cached_pool.pop, node,
                  sizeof(DataNode) - 7 + internalKey.size());
    return node;
  }

  auto leveldb::pmCache::PmSsTable::GenerateDataNodeWithoutFlush(
    const Slice &userKey, const Slice &value, uint64_t sqPackedWithType,
    uint64_t &newNodeOff) -> leveldb::pmCache::PmSsTable::DataNode * {
    auto size = DataNode::CalculateSize(userKey.size() + 8, value.size());
    //    auto node = GetVPtr<PmSsTable::DataNode>(
    //      newNodeOff = pmemobj_tx_xalloc(size, 0, POBJ_XALLOC_NO_FLUSH).off);
    PMEMoid oid;
    pmemobj_alloc(_pobj_cached_pool.pop, &oid, size, 0, nullptr, nullptr);
    auto node = GetVPtr<PmSsTable::DataNode>(oid.off);

    auto mod = (uint64_t)node % L1_CACHE_BYTES;
    node =
      (PmSsTable::DataNode *)(mod == 0 ? (char *)node
                                       : (char *)node + (L1_CACHE_BYTES - mod));
    newNodeOff = GetOffset(node);

    // S
    //    std::stringstream ss;
    //    ss << "f " << size << std::endl;
    //    std::cout << ss.str();

    new(node) PmSsTable::DataNode();
    if(mod != 0)
      node->off = L1_CACHE_BYTES - mod;
    else
      node->off = 0;
    node->SetInternalKeyAndValueWithoutFlush(userKey, value, sqPackedWithType);
    return node;
  }

  void PmSsTable::SetFirstDataNodeOff(uint64_t off) { firstDataNodeOff = off; }

  //  [[maybe_unused]] void PmSsTable::FlushMeta() {
  //    auto len =
  //      sizeof(PmSsTable) + (indexLen > 1 ? (indexLen - 1) * sizeof(Index) :
  //      0);
  //    pmemobj_flush(_pobj_cached_pool.pop, this, len);
  //  }

  void PmSsTable::SetIndexWithoutFlush(const std::vector<Index> &index1) {
    indexLen = index1.size();
    memcpy(index, index1.data(), indexLen * sizeof(Index));
  }

  //  void PmSsTable::TestPrint() {
  //    for(uint32_t i = 0; i < indexLen; ++i)
  //      index[i].TestPrint();
  //  }

  [[maybe_unused]] auto
  PmSsTable::LowerBound(const Slice &targetInternalKey) -> PmSsTable::Index {
    return *std::lower_bound(index, index + indexLen, targetInternalKey);
  }

  auto PmSsTable::GetMaxInternalKey() -> Slice {
    auto node = index[indexLen - 1].GetData();
    return {node->GetInternalKey(), node->GetInternalKeyLen()};
  }

  void PmSsTable::ClearData() {
    //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
    auto dummyHead = GetDataNodeHead();
    bool headDeleted = false;
    if(dummyHead) {
      auto dummyTail = dummyHead + 1;
      auto currentNode = dummyHead;
      auto nextNode = currentNode->NextNode();
      while(currentNode != dummyTail) {
        headDeleted = true;

        auto oid = pmemobj_oid((char *)currentNode - currentNode->off);
        pmemobj_free(&oid);
        //        pmemobj_tx_free(pmemobj_oid((char *)currentNode -
        //        currentNode->off));
        //        pmem::obj::delete_persistent<PmSsTable::DataNode>(currentNode);

        currentNode = nextNode;
        nextNode = currentNode->NextNode();
        assert(nextNode || currentNode == dummyTail);
      }
      this->firstDataNodeOff = 0;
      if(!headDeleted) {

        if(dummyHead) {
          auto oid = pmemobj_oid((char *)dummyHead - dummyHead->off);
          pmemobj_free(&oid);
        }

        //        pmem::obj::delete_persistent<PmSsTable::DataNode>(dummyHead);
      }
    }
  }

  PmSsTable::~PmSsTable() { ClearData(); } // NOLINT(bugprone-exception-escape)

  auto PmSsTable::GetDataNodeHead() const -> PmSsTable::DataNode * {
    return GetVPtr<PmSsTable::DataNode>(firstDataNodeOff);
  }

  auto PmSsTable::GetIndexS() -> PmSsTable::Index * { return &index[0]; }

  auto PmSsTable::GetIndexLen() const -> uint32_t { return indexLen; }

  auto PmSsTable::GetMinInternalKey() -> Slice {
    auto node = index[0].GetData();
    return {node->GetInternalKey(), node->GetInternalKeyLen()};
  }

  //  auto PmSsTable::GetMaxUserKey() -> Slice {
  //    auto node = index[indexLen - 1].GetData();
  //    return {node->GetInternalKey(), node->GetUserKeyLen()};
  //  }

  auto PmSsTable::BinarySearch(const Slice &targetInternalKey) -> int {
    Index *index1 =
      std::lower_bound(index, index + indexLen, targetInternalKey);
    return (int)(index1 - index) / (int)sizeof(Index);
  }

  //  [[maybe_unused]] auto
  //  PmSsTable::BinarySearchReturnIndex(const Slice &targetInternalKey)
  //    -> pmCache::PmSsTable::Index * {
  //    return std::lower_bound(index, index + indexLen, targetInternalKey);
  //  }

  //  [[maybe_unused]] auto PmSsTable::GetIndex(int i) -> PmSsTable::Index {
  //    return index[i];
  //  }

  auto
  leveldb::pmCache::PmSsTable::DataNode::CalculateSize(uint64_t internalKeyLen,
                                                       uint64_t valueLen)
    -> uint32_t {
    //    std::cout << sizeof(DataNode) << std::endl;
    return sizeof(DataNode) - 7 + internalKeyLen + valueLen + L1_CACHE_BYTES;
  }

  auto PmSsTable::DataNode::GetValue() -> char * {
    return &data_[internalKeyLen];
  }

  auto PmSsTable::DataNode::GetInternalKey() -> char * { return data_; }

  void PmSsTable::DataNode::SetInternalKeyAndValueWithoutFlush(
    const Slice &internalKey, const Slice &value) {
    internalKeyLen = internalKey.size();
    valueLen = value.size();
    auto internalKeyPtr = GetInternalKey();
    memcpy(internalKeyPtr, internalKey.data(), internalKey.size());

    char *valuePtr = GetValue();
    auto mod = (uint64_t)valuePtr % L1_CACHE_BYTES;
    uint64_t copyUnaligned = 0;
    if(mod != 0) {
      copyUnaligned = L1_CACHE_BYTES - mod;
      memcpy(valuePtr, value.data(), copyUnaligned);
    }

    pmCache::memcpy_nt(valuePtr + copyUnaligned,
                       (char *)value.data() + copyUnaligned,
                       value.size() - copyUnaligned);
  }

  void PmSsTable::DataNode::SetInternalKeyAndValueWithoutFlush(
    const Slice &userKey, const Slice &value, uint64_t sqPackedWithType) {
    internalKeyLen = userKey.size() + 8;
    valueLen = value.size();
    auto keyPtr = GetInternalKey();
    memcpy(keyPtr, userKey.data(), userKey.size());
    *(uint64_t *)(keyPtr + userKey.size()) = sqPackedWithType;

    char *valuePtr = GetValue();
    auto mod = (uint64_t)valuePtr % L1_CACHE_BYTES;
    uint64_t copyUnaligned = 0;
    if(mod != 0) {
      copyUnaligned = L1_CACHE_BYTES - mod;
      memcpy(valuePtr, value.data(), copyUnaligned);
    }

    pmCache::memcpy_nt(valuePtr + copyUnaligned,
                       (char *)value.data() + copyUnaligned,
                       value.size() - copyUnaligned);
  }

  auto PmSsTable::DataNode::GetUserKey() -> char * { return GetInternalKey(); }

  auto PmSsTable::DataNode::GetSqAndKind() -> uint64_t {
    return (*(uint64_t *)&data_[internalKeyLen - 8]);
  }

  auto PmSsTable::DataNode::GetInternalKeyLen() const -> uint32_t {
    return internalKeyLen;
  }

  auto PmSsTable::DataNode::GetValueLen() const -> uint32_t { return valueLen; }

  auto PmSsTable::DataNode::GetUserKeyLen() const -> uint32_t {
    return internalKeyLen - 8;
  }

  auto PmSsTable::DataNode::Compare(PmSsTable::DataNode &d) -> int {
    auto node1 = this;
    auto node2 = &d;
    auto node1InternalKey = node1->GetInternalKey();
    auto node2InternalKey = node2->GetInternalKey();
    auto node1UserKeyLen = node1->GetUserKeyLen();
    auto node2UserKeyLen = node2->GetUserKeyLen();
    auto compareLen =
      node1UserKeyLen < node2UserKeyLen ? node1UserKeyLen : node2UserKeyLen;
    auto compareResult = memcmp(node1InternalKey, node2InternalKey, compareLen);
    if(compareResult == 0)
      if(node1UserKeyLen < node2UserKeyLen)
        return CompareResult::K_NODE_1_LESS_NODE_2;
      else if(node1UserKeyLen > node2UserKeyLen)
        return CompareResult::K_NODE_1_GREATER_NODE_2;
      else { // node1UserKeyLen == node2UserKeyLen
        auto node1Sq = node1->GetSqAndKind();
        auto node2Sq = node2->GetSqAndKind();
        if(node1Sq < node2Sq)
          return K_NODE_1_GREATER_NODE_2;
        else if(node1Sq > node2Sq)
          return K_NODE_1_LESS_NODE_2;
        else // node1Sq == node2Sq
          return K_NODE_1_EQUAL_NODE_2;
      }
    else // compareResult != 0
      return compareResult;
  }

  auto PmSsTable::DataNode::Compare(PmSsTable::DataNode *d) -> int {
    auto node1 = this;
    auto node2 = d;
    auto node1InternalKey = node1->GetInternalKey();
    auto node2InternalKey = node2->GetInternalKey();
    auto node1UserKeyLen = node1->GetUserKeyLen();
    auto node2UserKeyLen = node2->GetUserKeyLen();
    auto compareLen =
      node1UserKeyLen < node2UserKeyLen ? node1UserKeyLen : node2UserKeyLen;
    auto compareResult = memcmp(node1InternalKey, node2InternalKey, compareLen);
    if(compareResult == 0)
      if(node1UserKeyLen < node2UserKeyLen)
        return CompareResult::K_NODE_1_LESS_NODE_2;
      else if(node1UserKeyLen > node2UserKeyLen)
        return CompareResult::K_NODE_1_GREATER_NODE_2;
      else { // node1UserKeyLen == node2UserKeyLen
        auto node1Sq = node1->GetSqAndKind();
        auto node2Sq = node2->GetSqAndKind();
        if(node1Sq < node2Sq)
          return K_NODE_1_GREATER_NODE_2;
        else if(node1Sq > node2Sq)
          return K_NODE_1_LESS_NODE_2;
        else // node1Sq == node2Sq
          return K_NODE_1_EQUAL_NODE_2;
      }
    else // compareResult != 0
      return compareResult;
  }

  auto PmSsTable::DataNode::Compare(const Slice &s) -> int {
    auto node1 = this;
    auto node1InternalKey = node1->GetInternalKey();
    auto node2InternalKey = s.data();
    auto node1UserKeyLen = node1->GetUserKeyLen();
    auto node2UserKeyLen = s.size() - 8;
    auto compareLen =
      node1UserKeyLen < node2UserKeyLen ? node1UserKeyLen : node2UserKeyLen;
    auto compareResult = memcmp(node1InternalKey, node2InternalKey, compareLen);
    if(compareResult == 0)
      if(node1UserKeyLen < node2UserKeyLen)
        return CompareResult::K_NODE_1_LESS_NODE_2;
      else if(node1UserKeyLen > node2UserKeyLen)
        return CompareResult::K_NODE_1_GREATER_NODE_2;
      else { // node1UserKeyLen == node2UserKeyLen
        auto node1Sq = node1->GetSqAndKind();
        auto node2Sq = *(uint64_t *)(s.data() + s.size() - 8);
        if(node1Sq < node2Sq)
          return K_NODE_1_GREATER_NODE_2;
        else if(node1Sq > node2Sq)
          return K_NODE_1_LESS_NODE_2;
        else // node1Sq == node2Sq
          return K_NODE_1_EQUAL_NODE_2;
      }
    else // compareResult != 0
      return compareResult;
  }

  auto PmSsTable::DataNode::PrevNode() const -> PmSsTable::DataNode * {
    return GetVPtr<DataNode>(prevNodeOff);
  }

  void PmSsTable::DataNode::SetPrevNodeOff(PmSsTable::DataNode *prevDataNode) {
    __atomic_store_n(&prevNodeOff, GetOffset(prevDataNode), __ATOMIC_RELAXED);
  }

  void PmSsTable::DataNode::SetPrevNodeOff(uint64_t prevDataNodeOff) {
    __atomic_store_n(&prevNodeOff, prevDataNodeOff, __ATOMIC_RELAXED);
  }

  auto PmSsTable::DataNode::NextNode() const -> PmSsTable::DataNode * {
    return GetVPtr<DataNode>(nextNodeOff);
  }

  void PmSsTable::DataNode::SetNextNodeOff(PmSsTable::DataNode *nextDataNode) {
    __atomic_store_n(&nextNodeOff, GetOffset(nextDataNode), __ATOMIC_RELAXED);
  }

  void PmSsTable::DataNode::SetNextNodeOff(uint64_t nextDataNodeOff) {
    __atomic_store_n(&nextNodeOff, nextDataNodeOff, __ATOMIC_RELAXED);
  }

  auto PmSsTable::DataNode::operator<(PmSsTable::DataNode &d2) -> bool {
    return this->Compare(d2) < 0;
  }

  auto PmSsTable::DataNode::GetInternalKeySlice() -> Slice {
    return {data_, internalKeyLen};
  }

  auto PmSsTable::DataNode::GetValueSlice() -> Slice {
    return {GetValue(), GetValueLen()};
  }

  //  [[maybe_unused]] auto PmSsTableBuilder::AllocateRootOfKvListWithTxBegin()
  //    -> pmem::obj::flat_transaction::manual * {
  //    assert(pmemobj_tx_stage() == TX_STAGE_NONE);
  //    pmem::obj::pool_base pool{_pobj_cached_pool.pop};
  //    auto tx = std::make_unique<pmem::obj::flat_transaction::manual>(pool);
  //    AllocateRootOfKvList();
  //    return tx.release();
  //  }

  void PmSsTableBuilder::AllocateRootOfKvList() {
    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
    auto allocator = pmem::obj::allocator<void>{};
    dummyHead =
      (PmSsTable::DataNode *)allocator.allocate(sizeof(PmSsTable::DataNode) * 2)
        .get();
    sizeAllocated += sizeof(PmSsTable::DataNode) * 2;
    dummyTail = dummyHead + 1;

    new(dummyHead) PmSsTable::DataNode();
    new(dummyTail) PmSsTable::DataNode();

    dummyHead->SetNextNodeOff(dummyTail);
    dummyTail->SetPrevNodeOff(dummyHead);
  }
  void PmSsTableBuilder::AllocateRootOfKvListInTxAndAddRange() {

    PMEMoid oid;
    pmemobj_alloc(_pobj_cached_pool.pop, &oid, sizeof(PmSsTable::DataNode) * 2,
                  0, nullptr, nullptr);
    dummyHead = (PmSsTable::DataNode *)pmemobj_direct_inline(oid);

    // S
    //    std::stringstream ss;
    //    ss << "f " << sizeof(PmSsTable::DataNode) * 2 << std::endl;
    //    std::cout << ss.str() << std::flush;

    dummyTail = dummyHead + 1;
    new(dummyHead) PmSsTable::DataNode();
    new(dummyTail) PmSsTable::DataNode();
    dummyHead->SetNextNodeOff(dummyTail);
    dummyTail->SetPrevNodeOff(dummyHead);
  }

  void PmSsTableBuilder::PushNodeWithoutFlush(PmSsTable::DataNode *node,
                                              bool aloneNode) {
    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
    assert(_pobj_cached_pool.pop);
    PmSsTable::DataNode *prevNode;
    PmSsTable::DataNode *nextNode;
    if(!aloneNode) {
      prevNode = node->PrevNode();
      nextNode = node->NextNode();
      prevNode->SetNextNodeOff(nextNode);
      nextNode->SetPrevNodeOff(prevNode);
    }
    prevNode = dummyHead;
    nextNode = dummyHead->NextNode();
    auto nodeOff = GetOffset(node);
    dummyHead->SetNextNodeOff(nodeOff);
    node->SetNextNodeOff(nextNode);
    nextNode->SetPrevNodeOff(nodeOff);
    node->SetPrevNodeOff(prevNode);
    offsetIndex.emplace_back(nodeOff);
  }

  void
  PmSsTableBuilder::PushNodeFromAnotherSsTableWithoutFlush(PmSsTable *node) {
    //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
    PmSsTable::DataNode *prevNode = node->GetDataNodeHead();
    PmSsTable::DataNode *nextNode = prevNode + 1;
    auto firstNode = prevNode->NextNode();
    auto lastNode = nextNode->PrevNode();
    prevNode->SetNextNodeOff(nextNode);
    nextNode->SetPrevNodeOff(prevNode);
    prevNode = dummyHead;
    nextNode = dummyHead->NextNode();
    dummyHead->SetNextNodeOff(firstNode);
    lastNode->SetNextNodeOff(nextNode);
    nextNode->SetPrevNodeOff(lastNode);
    firstNode->SetPrevNodeOff(prevNode);
    uint64_t indexLen = node->GetIndexLen();
    offsetIndex.reserve(offsetIndex.size() + indexLen);
    offsetIndex.insert(offsetIndex.end(), &node->index[0],
                       &node->index[indexLen]);
  }

  void PmSsTableBuilder::PushNodeToIndexWithoutChangeLinksOrFlush(
    PmSsTable::DataNode *node) {
    offsetIndex.emplace_back(GetOffset(node));
  }

  //  [[maybe_unused]] void
  //  PmSsTableBuilder::PushNodeWithoutFlush(const Slice &internalKey,
  //                                         const Slice &value) {
  //    auto newNode = PmSsTable::GenerateDataNodeWithoutFlush(internalKey,
  //    value); sizeAllocated += newNode->GetThisSize();
  //    PushNodeWithoutFlush(newNode, true);
  //    nodeToFlushData_.emplace_back(newNode);
  //  }

  //  [[maybe_unused]] auto PmSsTableBuilder::GeneratePmSsTable() -> PmSsTable *
  //  {
  //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
  //    auto allocator = pmem::obj::allocator<void>();
  //    auto pmSstable = (PmSsTable *)allocator
  //                       .allocate(sizeof(PmSsTable) + (offsetIndex.size() -
  //                       1) *
  //                                                       sizeof(PmSsTable::Index))
  //                       .get();
  //    sizeAllocated +=
  //      sizeof(PmSsTable) + (offsetIndex.size() - 1) *
  //      sizeof(PmSsTable::Index);
  //    pmSstable->SetFirstDataNodeOff(GetOffset(dummyHead));
  //    pmSstable->SetIndexWithoutFlush(offsetIndex);
  //    this->pmSsTable_ = pmSstable;
  //    return pmSstable;
  //  }

  [[maybe_unused]] void PmSsTableBuilder::FlushNodeLinks_() const {
    auto node = dummyHead;
    auto pool = _pobj_cached_pool.pop;
    assert(pool);
    while(node != nullptr) {
      auto nextNode = node->NextNode();
      pmemobj_flush(pool, &node->nextNodeOff, 2 * sizeof(uint64_t));
      node = nextNode;
    }
  }

  //  [[maybe_unused]] void PmSsTableBuilder::SortIndex() {
  //    std::sort(offsetIndex.begin(), offsetIndex.end());
  //  }
  //  [[maybe_unused]] void
  //  PmSsTableBuilder::PushNodeWithoutAddRange(PmSsTable::DataNode *node) {
  //    assert(pmemobj_tx_stage() == TX_STAGE_WORK);
  //    assert(_pobj_cached_pool.pop);
  //    PmSsTable::DataNode *prevNode;
  //    PmSsTable::DataNode *nextNode;
  //
  //    prevNode = node->PrevNode();
  //    nextNode = node->NextNode();
  //    // prevNode node nextNode
  //    prevNode->SetNextNodeOff(nextNode);
  //    nextNode->SetPrevNodeOff(prevNode);
  //
  //    // prevNode nextNode
  //    prevNode = dummyHead;
  //    nextNode = dummyHead->NextNode();
  //    auto nodeOff = GetOffset(node);
  //
  //    prevNode->SetNextNodeOff(nodeOff);
  //    node->SetNextNodeOff(nextNode);
  //    nextNode->SetPrevNodeOff(nodeOff);
  //    node->SetPrevNodeOff(prevNode);
  //
  //    AddNodeToIndex(nodeOff);
  //  }
  void PmSsTableBuilder::AddNodeToIndex(uint64_t nodeOff) {
    offsetIndex.emplace_back(nodeOff);
  }
  //  void PmSsTableBuilder::PushNodeWithoutFlush(
  //    const Slice &internalKey, const Slice &value,
  //    PmSsTable::DataNode *&pmSsTableDataNode) {
  //    auto newNode = PmSsTable::GenerateDataNodeWithoutFlush(internalKey,
  //    value); sizeAllocated += newNode->GetThisSize();
  //    PushNodeWithoutFlush(newNode, true);
  //    pmSsTableDataNode = newNode;
  //    nodeToFlushData_.emplace_back(newNode); 
  //  }

} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_PMEM_SSTABLE_PM_SSTABLE_H_
