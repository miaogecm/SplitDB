//
// Created by jxz on 22-8-2.
//

#include "pm_SSTable.h"
#include "dbWithPmCache/pm_write_impl/pm_wal_log.h"

void leveldb::pmCache::PmSsTable::Init() const {
  auto dummyHead = GetDataNodeHead();
  auto lastNode = dummyHead;
  auto currentNode = dummyHead->NextNode();
  while(currentNode != nullptr) {
    currentNode->SetPrevNodeOff(lastNode);
    lastNode = currentNode;
    currentNode = currentNode->NextNode();
  }
}

// void
// leveldb::pmCache::PmSsTableBuilder::PushNodeFromAnotherSsTableAndAddRange(
//   PmSsTable *node) {
//   assert(pmemobj_tx_stage() == TX_STAGE_WORK);
//
//   auto *prevNode = node->GetDataNodeHead();
//   auto *nextNode = prevNode + 1;
//   auto *firstNode = prevNode->NextNode();
//   auto *lastNode = nextNode->PrevNode();
//
//   // prevNode firstNode lastNode nextNode
//
//   pmemobj_tx_add_range_direct(prevNode->GetNextOffAddr(), sizeof(uint64_t));
//   pmemobj_tx_add_range_direct(firstNode->GetPrevOffAddr(), sizeof(uint64_t));
//   pmemobj_tx_add_range_direct(lastNode->GetNextOffAddr(), sizeof(uint64_t));
//   pmemobj_tx_add_range_direct(nextNode->GetPrevOffAddr(), sizeof(uint64_t));
//
//   prevNode->SetNextNodeOff(nextNode);
//   nextNode->SetPrevNodeOff(prevNode);
//
//   prevNode = dummyHead;
//   nextNode = dummyHead->NextNode();
//
//   pmemobj_tx_xadd_range_direct(prevNode->GetNextOffAddr(), sizeof(uint64_t),
//                                POBJ_XADD_NO_SNAPSHOT);
//   pmemobj_tx_xadd_range_direct(nextNode->GetPrevOffAddr(), sizeof(uint64_t),
//                                POBJ_XADD_NO_SNAPSHOT);
//
//   prevNode->SetNextNodeOff(firstNode);
//   lastNode->SetNextNodeOff(nextNode);
//   nextNode->SetPrevNodeOff(lastNode);
//   firstNode->SetPrevNodeOff(prevNode);
//   offsetIndex.insert(offsetIndex.end(), &node->index[0],
//                      &node->index[node->GetIndexLen()]);
// }

void leveldb::pmCache::PmSsTableBuilder::
  GetControlFromPmWalAndAddRangeWithoutAddIndex(PmWal *walNode) const {

  auto firstNode = walNode->GetDummyHead();
  auto nextNode = firstNode + 1;
  auto firstDataNode = firstNode->NextNode();
  auto lastDataNode = nextNode->PrevNode();
  auto newDummyHead = dummyHead;
  auto newDummyTail = dummyHead + 1;

  // firstNode firstDataNode lastDataNode nextNode
  // newDummyHead ... newDummyTail

  firstNode->SetNextNodeOff(nextNode);
  nextNode->SetPrevNodeOff(firstNode);
  firstDataNode->SetPrevNodeOff(newDummyHead);
  lastDataNode->SetNextNodeOff(newDummyTail);
  newDummyHead->SetNextNodeOff(firstDataNode);
  newDummyTail->SetPrevNodeOff(lastDataNode);

  // S
  //  std::stringstream ss;
  //  ss << "f " << 6 * 64 * 2 << std::endl;
  //  ss << "X" << std::endl;
  //  std::cout << ss.str() << std::flush;
}