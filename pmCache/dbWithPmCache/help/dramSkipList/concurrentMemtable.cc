
//

#include "concurrentMemtable.h"
#include "leveldb/iterator.h"
namespace leveldb::pmCache {

  thread_local size_t ConcurrentArena::tlsCpuid_ = 0;

  static const char *EncodeKey(std::string *scratch, const Slice &target) {
    scratch->clear();
    PutVarint32(scratch, target.size());
    scratch->append(target.data(), target.size());
    return scratch->data();
  }

  class ConcurrentMemTableIterator : public Iterator {
   public:
    explicit ConcurrentMemTableIterator(ConcurrentDramSkiplist *table)
        : iter_(table) {}
    ConcurrentMemTableIterator(const ConcurrentMemTableIterator &) = delete;
    ConcurrentMemTableIterator &
    operator=(const ConcurrentMemTableIterator &) = delete;
    ~ConcurrentMemTableIterator() override = default;

    [[nodiscard]] bool Valid() const override { return iter_.Valid(); }
    void Seek(const Slice &k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
    void SeekToFirst() override { iter_.SeekToFirst(); }
    void SeekToLast() override { iter_.SeekToLast(); }
    void Next() override { iter_.Next(); }
    void Prev() override { iter_.Prev(); }
    [[nodiscard]] auto key() const -> Slice override {
      assert(Valid());
      return GetLengthPrefixedSlice(iter_.key());
    }
    [[nodiscard]] auto value() const -> Slice override {
      Slice keySlice = GetLengthPrefixedSlice(iter_.key());
      return GetLengthPrefixedSlice(keySlice.data() + keySlice.size());
    }
    [[nodiscard]] auto status() const -> Status override {
      return Status::OK();
    }

   private:
    ConcurrentDramSkiplist::Iterator iter_;
    std::string tmp_;
  };

  leveldb::Iterator *leveldb::pmCache::ConcurrentMemtable::NewIterator() {
    return new ConcurrentMemTableIterator(&table_);
  }
} // namespace leveldb::pmCache
