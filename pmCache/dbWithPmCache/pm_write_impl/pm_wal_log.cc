//
// Created by jxz on 22-7-25.
//

#include "pm_wal_log.h"
#include "db/write_batch_internal.h"
#include "dbWithPmCache/help/pmem.h"
#include "leveldb/write_batch.h"
#include "pmCache/dbWithPmCache/help/height_generator.h"

auto leveldb::pmCache::PmWalBuilder::GenerateWal()
  -> leveldb::pmCache::PmWal * {
  //  assert(pmemobj_tx_stage() == TX_STAGE_WORK);

  auto size = sizeof(leveldb::pmCache::PmSsTable::DataNode) * 2;
  PMEMoid oid;
  pmemobj_xalloc(_pobj_cached_pool.pop, &oid, size, 0, POBJ_XALLOC_NO_FLUSH,
                 nullptr, nullptr);
  auto wal = pmCache::GetVPtr<PmWal>(oid.off);

  // S
  //  std::stringstream ss;
  //  ss << "f " << size << std::endl;
  //  std::cout << ss.str() << std::flush;

  auto dummyHead = wal->GetDummyHead();
  auto dummyTail = wal->GetDummyTail();
  new(dummyHead) PmSsTable::DataNode();
  new(dummyTail) PmSsTable::DataNode();
  dummyHead->SetNextNodeOff(dummyTail);
  dummyTail->SetPrevNodeOff(dummyHead);
  return wal;
}

leveldb::pmCache::PmWal::~PmWal() { assert(IsEmpty()); }

namespace {
  class WalInserter : public leveldb::WriteBatch::Handler {
   public:
    leveldb::SequenceNumber baseSqInsert, baseSqDelete;
    uint64_t dummyHeadOff = 0;
    leveldb::pmCache::PmSsTable::DataNode *dummyHead = nullptr;
    leveldb::pmCache::PmSsTable::DataNode *nextNode = nullptr;
    leveldb::pmCache::PmSsTable::DataNode *newNode = nullptr;

    WalInserter(uint64_t insertSq, uint64_t deleteSq,
                leveldb::pmCache::PmSsTable::DataNode *dummyHead)
        : baseSqInsert(insertSq), baseSqDelete(deleteSq), dummyHead(dummyHead) {
      dummyHeadOff = GetOffset(dummyHead);
    }


    void Insert(const leveldb::Slice &key, const leveldb::Slice &value,
                uint64_t sq) {
      //      assert(pmemobj_tx_stage() == TX_STAGE_WORK);


      uint64_t nextNodeOff = dummyHead->nextNodeOff, newNodeOff;


      newNode = leveldb::pmCache::PmSsTable::GenerateDataNodeWithoutFlush(
        key, value, sq, newNodeOff);
      nextNode =
        leveldb::pmCache::GetVPtr<leveldb::pmCache::PmSsTable::DataNode>(
          nextNodeOff);
      dummyHead->nextNodeOff = newNodeOff;
      newNode->nextNodeOff = nextNodeOff;
      nextNode->prevNodeOff = newNodeOff;
      newNode->prevNodeOff = dummyHeadOff;
    }

    void Put(const leveldb::Slice &key, const leveldb::Slice &value) override {
      Insert(key, value, baseSqInsert);
    }

    void Delete(const leveldb::Slice &key) override {
      Insert(key, leveldb::Slice{}, baseSqDelete);
    }
  };
} // namespace
auto leveldb::pmCache::PmWal::AddRecord(leveldb::WriteBatch *batch,
                                        std::vector<uint64_t> &offS)
  -> leveldb::Status {
  //  assert(pmemobj_tx_stage() == TX_STAGE_WORK);
  uint64_t baseSq = leveldb::WriteBatchInternal::Sequence(batch) << 8;
  WalInserter inserter{baseSq | kTypeValue, baseSq | kTypeDeletion,
                       GetDummyHead()};
  Status ret;
  const int kHeader = 12;
  int i = 0;
  Slice input(batch->rep_);
  input.remove_prefix(kHeader); 
  Slice key, value;
  int found = 0; 
  char tag;

  try {
    while(!input.empty()) {
      found++; 
      tag = input[0];        
      input.remove_prefix(1); 
      switch(tag) { 
      case kTypeValue:
        if(GetLengthPrefixedSlice(&input, &key) &&
           GetLengthPrefixedSlice(&input, &value)) {
          inserter.Put(key, value);
          offS[i++] = GetOffset(inserter.newNode);
        } else
          ret = Status::Corruption(Slice{"bad WriteBatch Put"});
        break;
      case kTypeDeletion:
        if(GetLengthPrefixedSlice(&input, &key)) {
          inserter.Delete(key);
          offS[i++] = GetOffset(inserter.newNode);
        } else
          ret = Status::Corruption(Slice{"bad WriteBatch Delete"});
        break;
      default: ret = Status::Corruption(Slice{"unknown WriteBatch tag"});
      }
    }
    if(found != WriteBatchInternal::Count(batch))
      ret = Status::Corruption(Slice{"WriteBatch has wrong count"});
    else
      ret = Status::OK();
  }
  catch(std::exception &e) {
    std::cout << e.what() << std::endl;
    ret = Status::IOError(Slice{"PmWAL::AddRecord"});
  }
  return ret;
}
void leveldb::pmCache::PmWal::AddRecordParallel(const leveldb::Slice &key,
                                                const leveldb::Slice &value,
                                                uint64_t sq, unsigned char type,
                                                uint64_t &off,
                                                pmem::obj::pool_base &pool) {
  sq = (sq << 8) | type;
  PmSsTable::DataNode *newNode;

  newNode = leveldb::pmCache::PmSsTable::GenerateDataNodeWithoutFlush(
    key, value, sq, off);

  auto dummyHead = GetDummyHead();
  newNode->nextNodeOff =
    __atomic_load_n(&dummyHead->nextNodeOff, __ATOMIC_ACQUIRE);
  while(!__sync_bool_compare_and_swap(&(dummyHead->nextNodeOff),
                                      newNode->nextNodeOff, off)) {
    newNode->nextNodeOff =
      __atomic_load_n(&dummyHead->nextNodeOff, __ATOMIC_ACQUIRE);
  }
  auto nextNode = newNode->NextNode();
  pool.flush(&dummyHead->nextNodeOff, sizeof(uint64_t));


  pool.flush(newNode, sizeof(PmSsTable::DataNode) - 7 + key.size() + 8);
  nextNode->prevNodeOff = off;
}
