//
// Created by jxz on 22-7-26.
//

#include "pm_write_batch.h"

leveldb::pmCache::KVNodeInWriteBatch::~KVNodeInWriteBatch() {
  delete[] userKey_;
  delete[] value_;
}

leveldb::pmCache::KVNodeInWriteBatch::KVNodeInWriteBatch(char kind,
                                                         uint32_t userKeySize,
                                                         const char *userKey,
                                                         uint32_t valueSize,
                                                         const char *value)
    : kind_(kind), userKeySize_(userKeySize), valueSize_(valueSize) {
  if(userKeySize_ > 0) {
    userKey_ = new char[userKeySize_];
    memcpy(userKey_, userKey, userKeySize_);
  }
  if(kind == (char)kTypeValue && valueSize > 0) {
    value_ = new char[valueSize_];
    memcpy(value_, value, valueSize_);
  }
}

leveldb::pmCache::KVNodeInWriteBatch::KVNodeInWriteBatch(
  char kind, const leveldb::Slice &userKey, const leveldb::Slice &value)
    : kind_(kind), userKeySize_(userKey.size()), valueSize_(value.size()) {
  if(userKeySize_ > 0) {
    userKey_ = new char[userKeySize_];
    memcpy(userKey_, userKey.data(), userKeySize_);
  }
  if(kind == (char)kTypeValue && valueSize_ > 0) {
    value_ = new char[valueSize_];
    memcpy(value_, value.data(), valueSize_);
  }
}

leveldb::pmCache::KVNodeInWriteBatch::KVNodeInWriteBatch(
  leveldb::pmCache::KVNodeInWriteBatch &&node) noexcept
    : kind_(node.kind_), userKeySize_(node.userKeySize_),
      valueSize_(node.valueSize_), userKey_(node.userKey_),
      value_(node.value_) {
  node.value_ = nullptr;
  node.userKey_ = nullptr;
  node.valueSize_ = 0;
  node.userKeySize_ = 0;
}

void leveldb::pmCache::WriteBatchForPmWal::AddCount_(int c) { count_ += c; }

void leveldb::pmCache::WriteBatchForPmWal::Put(const leveldb::Slice &userKey,
                                               const leveldb::Slice &value) {
  AddCount_();
  rep_.emplace_back((char)kTypeValue, userKey, value);
  currentSize_ += userKey.size() + value.size() + sizeof(uint32_t) * 2 + 1;
}

void leveldb::pmCache::WriteBatchForPmWal::Delete(
  const leveldb::Slice &userKey) {
  AddCount_();
  rep_.emplace_back((char)kTypeDeletion, (uint32_t)userKey.size(),
                    userKey.data());
  currentSize_ += userKey.size() + sizeof(uint32_t) * 2 + 1;
}

uint64_t leveldb::pmCache::WriteBatchForPmWal::ApproximateSize() const {
  return currentSize_;
}

namespace {
  class MemTableInserter
      : public leveldb::pmCache::WriteBatchForPmWal::Handler {
   public:
    uint64_t sequence_;
    leveldb::MemTable *mem_;

    void Put(const leveldb::Slice &key, const leveldb::Slice &value) override {
      mem_->Add(sequence_, leveldb::kTypeValue, key, value);
      sequence_++;
    }

    void Delete(const leveldb::Slice &key) override {
      mem_->Add(sequence_, leveldb::kTypeDeletion, key, leveldb::Slice());
      sequence_++;
    }
  };
} // namespace

leveldb::Status leveldb::pmCache::WriteBatchForPmWal::InsertIntoMemtable(
  leveldb::MemTable *memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = getSeqNumber();
  inserter.mem_ = memtable;
  return Iterate(&inserter);
}

leveldb::Status
leveldb::pmCache::WriteBatchForPmWal::Iterate(Handler *handler) {
  for(auto &node : rep_) {
    char kind = node.kind_;
    switch(kind) {
    case leveldb::kTypeValue:
      if(node.userKeySize_ > 0 && node.valueSize_ > 0)
        handler->Put({node.userKey_, node.userKeySize_},
                     {node.value_, node.valueSize_});
      break;
    case leveldb::kTypeDeletion:
      if(node.userKeySize_ > 0)
        handler->Delete({node.userKey_, node.userKeySize_});
      break;
    default: return Status::Corruption(Slice{"unknown WriteBatch tag"});
    }
  }
  return Status::OK();
}

void leveldb::pmCache::WriteBatchForPmWal::Append(
  leveldb::pmCache::WriteBatchForPmWal *source) {
  currentSize_ += source->currentSize_; 
  AddCount_(source->count_);            
  rep_.reserve(rep_.size() + source->rep_.size());
  for(auto &i : source->rep_)
    rep_.emplace_back(std::move(i));
}

leveldb::pmCache::WriteBatchForPmWal::WriteBatchForPmWal() { rep_.reserve(10); }
