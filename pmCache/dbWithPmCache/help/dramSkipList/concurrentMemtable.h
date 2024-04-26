#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTMEMTABLE_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTMEMTABLE_H_
#include <utility>

#include "concurrentDramSkiplist.h"
#include "db/dbformat.h"
namespace leveldb::pmCache {
  class ConcurrentMemtable {
   public:
    explicit ConcurrentMemtable(InternalKeyComparator comparator)
        : comparator_(std::move(comparator)), table_(&comparator_, &arena_) {}
    ConcurrentMemtable(const ConcurrentMemtable &) = delete;
    ConcurrentMemtable &operator=(const ConcurrentMemtable &) = delete;
    void Ref() { ++refs_; }
    void Unref() {
      --refs_;
      assert(refs_ >= 0);
      if(refs_ <= 0)
        delete this;
    }
    size_t ApproximateMemoryUsage() { return arena_.MemoryAllocatedBytes(); }
    Iterator *NewIterator();
    void AddWithPmOff(SequenceNumber s, ValueType type, const Slice &key,
                      const Slice &value, uint64_t pmOff) {
      size_t keySize = key.size();
      size_t valSize = value.size();
      std::size_t internalKeySize = keySize + 8;
      const size_t encodedLen = VarintLength(internalKeySize) +
                                internalKeySize + VarintLength(valSize) +
                                valSize;

      auto height = table_.RandomHeight();
      auto prefix =
        sizeof(std::atomic<ConcurrentDramSkiplist::Node *>) * (height - 1);

      char *buf =
        arena_.Allocate(prefix + sizeof(ConcurrentDramSkiplist::Node) +
                        encodedLen + sizeof(uint64_t));
      auto *x = reinterpret_cast<ConcurrentDramSkiplist::Node *>(buf + prefix);
      x->StashHeight(height);

      char *p = EncodeVarint32(
        buf + prefix + sizeof(ConcurrentDramSkiplist::Node), internalKeySize);
      std::memcpy(p, key.data(), keySize);
      p += keySize;
      EncodeFixed64(p, ((s << 8) | type));
      p += 8;
      p = EncodeVarint32(p, valSize);
      std::memcpy(p, value.data(), valSize);
      EncodeFixed64(p + valSize, pmOff);

      while(!table_.InsertConcurrently(x->Key())) {}

      // #ifndef NDEBUG
      //       auto it = ConcurrentDramSkiplist::Iterator(&table_);
      //       it.SeekToFirst();
      //       while(it.Valid()) {
      //         std::cout << GetLengthPrefixedSlice(it.key()).ToString() << " "
      //                   << std::flush;
      //         it.Next();
      //       }
      //       std::cout << std::endl << std::flush;
      // #endif
    }
    void Add(SequenceNumber s, ValueType type, const Slice &key,
             const Slice &value) {
      AddWithPmOff(s, type, key, value, 0);
    }
    bool Get(const LookupKey &key, std::string *value, Status *s) {
      Slice memKey = key.MemtableKey();
      ConcurrentDramSkiplist::Iterator iter(&table_);
      iter.Seek(memKey.data());

      if(iter.Valid()) {
        const char *entry = iter.key();
        uint32_t key_length;
        const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
        if(BytewiseComparator()->Compare(Slice(key_ptr, key_length - 8),
                                         key.UserKey()) == 0) {
          // Correct user key
          const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
          switch(
            static_cast<ValueType>(tag & 0xff)) { 
          case kTypeValue: {
            Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
            value->assign(v.data(), v.size());
            return true;
          }
          case kTypeDeletion: *s = Status::NotFound(Slice()); return true;
          }
        }
      }
      return false;
    }
    ~ConcurrentMemtable() { assert(refs_ == 0); };

   private:
    InternalKeyComparator comparator_;
    int refs_{};
    ConcurrentArena arena_{};
    ConcurrentDramSkiplist table_;
  };
} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTMEMTABLE_H_
