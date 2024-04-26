//
// Created by jxz on 22-7-26.
//

#ifndef LEVELDB_PM_WRITE_BATCH_H
#define LEVELDB_PM_WRITE_BATCH_H

#include "db/dbformat.h"
#include "db/memtable.h"
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>

namespace leveldb::pmCache {
  class WriteBatchForPmWal;

  class KVNodeInWriteBatch {
   public:
    friend class WriteBatchForPmWal;

    KVNodeInWriteBatch() = delete;

    KVNodeInWriteBatch(char kind, uint32_t userKeySize, const char *userKey,
                       uint32_t valueSize = 0, const char *value = nullptr);

    KVNodeInWriteBatch(char kind, const Slice &userKey, const Slice &value);

    KVNodeInWriteBatch(const KVNodeInWriteBatch &node) = delete;

    KVNodeInWriteBatch(KVNodeInWriteBatch &&node) noexcept;

    [[nodiscard]] inline char getKind() const;

    [[nodiscard]] inline uint32_t getUserKeyLen() const;

    [[nodiscard]] inline uint32_t getValueLen() const;

    [[nodiscard]] inline const char *getUserKey() const;

    [[nodiscard]] inline const char *getValue() const;

    inline void PrintNode() const;

    ~KVNodeInWriteBatch();

   private:
    char kind_;
    uint32_t userKeySize_;
    uint32_t valueSize_;
    char *userKey_ = nullptr;
    char *value_ = nullptr;
  };

  class WriteBatchForPmWal {
   public:
    friend class PmWal;

    class Handler {
     public:
      virtual ~Handler() = default;

      virtual void Put(const Slice &key, const Slice &value) = 0;

      virtual void Delete(const Slice &key) = 0;
    };

    WriteBatchForPmWal();

    void Put(const Slice &userKey, const Slice &value);

    void Delete(const Slice &userKey);

    [[nodiscard]] uint64_t ApproximateSize() const;

    void Append(WriteBatchForPmWal *source);

    inline const std::vector<KVNodeInWriteBatch> &getBatch();

    [[nodiscard]] inline int getCount() const;

    inline void setSequence(uint64_t sequenceNumber);

    [[nodiscard]] inline uint64_t getSeqNumber() const;

    Status InsertIntoMemtable(MemTable *memtable);

    Status Iterate(Handler *handler);

    inline void Clear();

   private:
    std::vector<KVNodeInWriteBatch> rep_; 
    void AddCount_(int c = 1);

    int count_ = 0;
    uint64_t currentSize_ = 0;
    uint64_t sequenceNumber_ = 0;
  };

  void leveldb::pmCache::WriteBatchForPmWal::Clear() {
    rep_.clear();
    currentSize_ = 0;
    count_ = 0;
    sequenceNumber_ = 0;
  }

  char leveldb::pmCache::KVNodeInWriteBatch::getKind() const { return kind_; }

  uint32_t leveldb::pmCache::KVNodeInWriteBatch::getUserKeyLen() const {
    return userKeySize_;
  }

  uint32_t leveldb::pmCache::KVNodeInWriteBatch::getValueLen() const {
    return valueSize_;
  }

  const char *leveldb::pmCache::KVNodeInWriteBatch::getUserKey() const {
    return userKey_;
  }

  void KVNodeInWriteBatch::PrintNode() const {
    std::cout << (((char)getKind() == (char)1) ? "delete" : "insert") << " "
              << getUserKeyLen() << " "
              << (getUserKeyLen() > 0
                    ? std::string(getUserKey(), getUserKeyLen())
                    : std::string("null"))
              << " " << getValueLen() << " "
              << (getValueLen() > 0 ? std::string(getValue(), getValueLen())
                                    : std::string("null"))
              << std::endl;
  }

  const char *KVNodeInWriteBatch::getValue() const { return value_; }

  const std::vector<KVNodeInWriteBatch> &
  leveldb::pmCache::WriteBatchForPmWal::getBatch() {
    return rep_;
  }

  int WriteBatchForPmWal::getCount() const { return count_; }

  void WriteBatchForPmWal::setSequence(uint64_t sequenceNumber) {
    sequenceNumber_ = sequenceNumber;
  }

  uint64_t WriteBatchForPmWal::getSeqNumber() const { return sequenceNumber_; }
} // namespace leveldb::pmCache
#endif // LEVELDB_PM_WRITE_BATCH_H
