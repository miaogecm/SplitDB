#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_COUNT_MIN_SKETCH_COUNT_MIN_SKETCH_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_COUNT_MIN_SKETCH_COUNT_MIN_SKETCH_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <set>
#include <shared_mutex>
#include <utility>

namespace leveldb::pmCache {

  // Number of hash functions
  static const uint32_t kHashCount = 2;

  // element count in each column
  static const uint32_t kItemCount = 500;

  // The key-value pairs that are read more than this many times are added
  static const uint32_t kHotCount = 1;

  // The maximum number of records for calculating the hotness, each count-min
  // sketch is cleared after being accumulated so many times
  static const uint64_t kMaxAdd = 100000000L;

  class CountMinData {
   private:
    friend class CountMinSketch;

   public:
    struct Data {
      uint16_t count;
    } __attribute__((packed));

   public:
    auto Clear() { data_.store({}); }
    auto Update() -> bool {
      auto updateOk = false;
      Data currentData{};
      Data newData{};
      currentData = GetCurrentData_();
      while(!updateOk) {
        newData.count = currentData.count + 1;

        updateOk = data_.compare_exchange_strong(currentData, newData,
                                                 std::memory_order_relaxed);
      }

      return newData.count > kHotCount;
    }

   private:
    auto GetCurrentData_() -> Data {
      return data_.load(std::memory_order_relaxed);
    }

   private:
    std::atomic<Data> data_{};
  };

  class CountMinSketch {
   public:
    [[nodiscard]] inline auto Hash(const std::string &userKey) const {
      return hashFunction_(userKey);
    }

    static inline auto Delta(uint64_t hashValue) {
      return (hashValue >> 17) | (hashValue << 15);
    }

    auto Add(const leveldb::Slice &userKey) -> auto {
      auto addCount = addCount_.fetch_add(1, std::memory_order_relaxed);
      if(addCount > kMaxAdd) {
        addCount_.store(0);
        clearMutex_.lock();
        for(auto &line : data_)
          for(auto &item : line)
            item.Clear();
        clearMutex_.unlock();
        return false;
      } else {
        if(clearMutex_.try_lock_shared()) {
          auto hashValue =
            std::_Hash_impl::hash(userKey.data(), userKey.size());
          uint64_t delta = Delta(hashValue);
          bool ret = true;
          for(uint32_t i = 0; i < kHashCount; ++i) {
            ret &= data_[i][hashValue % kItemCount].Update();
            hashValue += delta;
          }
          clearMutex_.unlock_shared();
          return ret;
        } else
          return false;
      }
    }

    auto CheckIfHot(uint64_t hashValue, uint32_t hotCount) {
      uint64_t delta = Delta(hashValue);
      bool ret = true;
      for(uint32_t i = 0; i < kHashCount; ++i) {
        ret &=
          data_[i][hashValue % kItemCount].GetCurrentData_().count > hotCount;
        hashValue += delta;
      }
      return ret;
    }

    auto CheckIfHot(const std::string &userKey,
                    uint32_t hotCount = kHotCount) -> auto {
      return CheckIfHot(Hash(userKey), hotCount);
    }

    auto CheckIfReserve(const leveldb::Slice &userKey,
                        uint64_t reserveCount) -> auto {
      auto ret = CheckIfHot(
        std::_Hash_impl::hash(userKey.data(), userKey.size()), reserveCount);
      //      if(ret) {
      //        std::cout << "reserve: " << userKey.ToString() << std::endl;
      //      }
      return ret;
    }
    auto GetAddCount() { return addCount_.load(); }

   private:
    __attribute__((aligned(L1_CACHE_BYTES)))
    std::array<std::array<CountMinData, kItemCount>, kHashCount>
      data_{};
    __attribute__((aligned(L1_CACHE_BYTES))) std::shared_mutex clearMutex_{};
    __attribute__((aligned(L1_CACHE_BYTES))) std::hash<std::string>
      hashFunction_{};
    __attribute__((aligned(L1_CACHE_BYTES))) std::atomic<uint64_t> addCount_{0};
  };

  class BufferForEachFile {
   public:
    class Key {
     public:
      Key(std::string data, uint64_t size)
          : data(std::move(data)), size(size) {}
      Key() = default;
      [[nodiscard]] auto GetUserKey() const {
        return leveldb::Slice{data.data(), size - 8};
      }
      [[nodiscard]] auto GetInternalKey() const {
        return leveldb::Slice{data.data(), size};
      }
      auto operator<(const Key &otherKey) const {
        return leveldb::BytewiseComparator()->Compare(
                 GetUserKey(), otherKey.GetUserKey()) < 0;
      }
      std::string data{};
      uint64_t size{};
    };
    class Value {
     public:
      explicit Value(std::string &&data) : data(std::move(data)) {}
      Value() = default;
      std::string data;
    };
    class KeyValue {
     public:
      KeyValue(std::string keyData, uint64_t keySize, std::string valueData)
          : key(std::forward<std::string>(keyData), keySize),
            value(std::move(valueData)) {}

      auto operator<(const KeyValue &otherKeyValue) const -> bool {
        return key < otherKeyValue.key;
      }

     public:
      Key key;
      Value value;
      std::atomic<uint16_t> hitCnt = 0;
    };

    [[nodiscard]] inline auto
    Search(const leveldb::Slice &userKey, uint16_t &hotNess) -> std::string {
      auto it =
        std::lower_bound(keyValueS.begin(), keyValueS.end(), userKey,
                         [](const KeyValue &kv, const leveldb::Slice &userKey) {
                           return leveldb::BytewiseComparator()->Compare(
                                    kv.key.GetUserKey(), userKey) < 0;
                         });
      if(it != keyValueS.end() && leveldb::BytewiseComparator()->Compare(
                                    it->key.GetUserKey(), userKey) == 0) {
        //        auto a = it->hitCnt.load(std::memory_order_relaxed);
        hotNess = const_cast<std::atomic<uint16_t> *>(&(it->hitCnt))
                    ->fetch_add(1, std::memory_order_relaxed);
        //        auto b = it->hitCnt.load(std::memory_order_relaxed);
        //        std::cout<<a<<" "<<b<<std::endl;
        return it->value.data;
      } else
        return {};
    }

    [[nodiscard]] auto GetSize() const -> uint64_t { return keyValueS.size(); }
    [[nodiscard]] auto GetMinKey() const -> const BufferForEachFile::Key & {
      return keyValueS.begin()->key;
    }
    [[nodiscard]] auto GetMaxKey() const -> const BufferForEachFile::Key & {
      auto it = keyValueS.end();
      --it;
      return it->key;
    }
    [[nodiscard]] auto GetSetBegin() const { return keyValueS.begin(); }
    [[nodiscard]] auto GetSetEnd() const { return keyValueS.end(); }

    std::set<KeyValue> keyValueS;
    std::shared_mutex mutex;
  };
} // namespace leveldb::pmCache

#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_COUNT_MIN_SKETCH_COUNT_MIN_SKETCH_H_
