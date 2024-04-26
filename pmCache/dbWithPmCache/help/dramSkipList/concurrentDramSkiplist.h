#ifndef LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTDRAMSKIPLIST_H_
#define LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTDRAMSKIPLIST_H_

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
#define UNLIKELY(x) (__builtin_expect((x), 0))

#include "leveldb/slice.h"
#include "util/coding.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <db/dbformat.h>
#include <deque>
#include <malloc.h>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <sys/mman.h>
#include <thread>
#define STORAGE_DECL static thread_local
namespace leveldb::pmCache {

  //  static InternalKeyComparator internalKeyComparator{//
  //  NOLINT(cert-err58-cpp)
  //                                                     BytewiseComparator()};

  static auto GetLengthPrefixedSlice(const char *data) -> Slice {
    uint32_t len;
    const char *p = data;
    p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
    return {p, len};
  }

  // A very simple random number generator.  Not especially good at
  // generating truly random bits, but good enough for our needs in this
  // package.
  class Random {
   private:
    enum : uint32_t {
      M = 2147483647L // 2^31-1
    };
    enum : uint64_t {
      A = 16807 // bits 14, 8, 7, 5, 2, 1, 0
    };

    uint32_t seed_;

    static auto GoodSeed_(uint32_t s) -> uint32_t {
      return (s & M) != 0 ? (s & M) : 1;
    }

   public:
    // This is the largest value that can be returned from Next()
    enum : uint32_t { kMaxNext = M };

    explicit Random(uint32_t s) : seed_(GoodSeed_(s)) {}

    void Reset(uint32_t s) { seed_ = GoodSeed_(s); }

    auto Next() -> uint32_t {
      // We are computing
      //       seed_ = (seed_ * A) % M,    where M = 2^31-1
      //
      // seed_ must not be zero or M, or else all subsequent computed values
      // will be zero or M respectively.  For all other values, seed_ will end
      // up cycling through every number in [1,M-1]
      uint64_t product = seed_ * A;

      // Compute (product % M) using the fact that ((x << 31) % M) == x.
      seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
      // The first reduction may overflow by 1 bit, so we may need to
      // repeat.  mod == M is not possible; using > allows the faster
      // sign-bit-based test.
      if(seed_ > M) {
        seed_ -= M;
      }
      return seed_;
    }

    auto Next64() -> uint64_t { return (uint64_t{Next()} << 32) | Next(); }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    auto Uniform(int n) -> uint32_t { return Next() % n; }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    auto OneIn(int n) -> bool { return Uniform(n) == 0; }

    // "Optional" one-in-n, where 0 or negative always returns false
    // (may or may not consume a random value)
    auto OneInOpt(int n) -> bool { return n > 0 && OneIn(n); }

    // Returns random bool that is true for the given percentage of
    // calls on average. Zero or less is always false and 100 or more
    // is always true (may or may not consume a random value)
    auto PercentTrue(int percentage) -> bool {
      return static_cast<int>(Uniform(100)) < percentage;
    }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    auto Skewed(int max_log) -> uint32_t {
      return Uniform(1 << Uniform(max_log + 1));
    }

    // Returns a random string of length "len"
    auto RandomString(int len) -> std::string;

    // Generates a random string of len bytes using human-readable characters
    auto HumanReadableString(int len) -> std::string;

    // Generates a random binary data
    auto RandomBinaryString(int len) -> std::string;

    // Returns a Random instance for use by the current thread without
    // additional locking
    static auto GetTlsInstance() -> Random * {
      STORAGE_DECL Random *tlsInstance;
      STORAGE_DECL std::aligned_storage<sizeof(Random)>::type tlsInstanceBytes;

      auto rv = tlsInstance;
      if(UNLIKELY(rv == nullptr)) {
        size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
        rv = new(&tlsInstanceBytes) Random((uint32_t)seed);
        tlsInstance = rv;
      }
      return rv;
    }
  };

  // A good 32-bit random number generator based on std::mt19937.
  // This exists in part to avoid compiler variance in warning about coercing
  // uint_fast32_t from mt19937 to uint32_t.
  class Random32 {
   private:
    std::mt19937 generator_;

   public:
    explicit Random32(uint32_t s) : generator_(s) {}

    // Generates the next random number
    auto Next() -> uint32_t { return static_cast<uint32_t>(generator_()); }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    auto Uniform(uint32_t n) -> uint32_t {
      return static_cast<uint32_t>(
        std::uniform_int_distribution<std::mt19937::result_type>(0, n - 1)(
          generator_));
    }

    // Returns an *almost* uniformly distributed value in the range [0..n-1].
    // Much faster than Uniform().
    // REQUIRES: n > 0
    auto Uniformish(uint32_t n) -> uint32_t {
      // fastrange (without the header)
      return static_cast<uint32_t>((uint64_t(generator_()) * uint64_t(n)) >>
                                   32);
    }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    auto OneIn(uint32_t n) -> bool { return Uniform(n) == 0; }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    auto Skewed(int max_log) -> uint32_t {
      return Uniform(uint32_t{1} << Uniform(max_log + 1));
    }

    // Reset the seed of the generator to the given value
    void Seed(uint32_t new_seed) { generator_.seed(new_seed); }
  };

  // A good 64-bit random number generator based on std::mt19937_64
  class Random64 {
   private:
    std::mt19937_64 generator_;

   public:
    explicit Random64(uint64_t s) : generator_(s) {}

    // Generates the next random number
    auto Next() -> uint64_t { return generator_(); }

    // Returns a uniformly distributed value in the range [0..n-1]
    // REQUIRES: n > 0
    auto Uniform(uint64_t n) -> uint64_t {
      return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
    }

    // Randomly returns true ~"1/n" of the time, and false otherwise.
    // REQUIRES: n > 0
    auto OneIn(uint64_t n) -> bool { return Uniform(n) == 0; }

    // Skewed: pick "base" uniformly from range [0,max_log] and then
    // return "base" random bits.  The effect is to pick a number in the
    // range [0,2^max_log-1] with exponential bias towards smaller numbers.
    auto Skewed(int max_log) -> uint64_t {
      return Uniform(uint64_t(1) << Uniform(max_log + 1));
    }
  };

  // A seeded replacement for removed std::random_shuffle
  template <class RandomIt>
  void RandomShuffle(RandomIt first, RandomIt last, uint32_t seed) {
    std::mt19937 rng(seed);
    std::shuffle(first, last, rng);
  }

  // A replacement for removed std::random_shuffle
  template <class RandomIt> void RandomShuffle(RandomIt first, RandomIt last) {
    RandomShuffle(first, last, std::random_device{}());
  }

  // An RAII wrapper for mmaped memory
  class MemMapping {
   public:
    static constexpr bool kHugePageSupported = false;

    // Allocate memory requesting to be backed by huge pages
    static auto AllocateHuge(size_t length) -> MemMapping {
      return AllocateAnonymous_(length, /*huge*/ true);
    }

    // Allocate memory that is only lazily mapped to resident memory and
    // guaranteed to be zero-initialized. Note that some platforms like
    // Linux allow memory over-commit, where only the used portion of memory
    // matters, while other platforms require enough swap space (page file) to
    // back the full mapping.
    static auto AllocateLazyZeroed(size_t length) -> MemMapping {
      return AllocateAnonymous_(length, /*huge*/ false);
    }

    // No copies
    MemMapping(const MemMapping &) = delete;
    auto operator=(const MemMapping &) -> MemMapping & = delete;
    // Move
    MemMapping(MemMapping &&other) noexcept { *this = std::move(other); }
    auto operator=(MemMapping &&other) noexcept -> MemMapping & {
      if(&other == this) {
        return *this;
      }
      this->~MemMapping();
      std::memcpy((void *)this, (void *)&other, sizeof(*this));
      new(&other) MemMapping();
      return *this;
    }
    // Releases the mapping
    ~MemMapping() {
      if(addr_ != nullptr) {
        auto status = munmap(addr_, length_);
        assert(status == 0);
        if(status != 0) {}
      }
    }

    [[nodiscard]] inline auto Get() const -> void * { return addr_; }
    [[nodiscard]] inline auto Length() const -> size_t { return length_; }

   private:
    MemMapping() = default;

    // The mapped memory, or nullptr on failure / not supported
    void *addr_ = nullptr;
    // The known usable number of bytes starting at that address
    size_t length_ = 0;

    static auto AllocateAnonymous_(size_t length, bool huge) -> MemMapping {
      MemMapping mm;
      mm.length_ = length;
      assert(mm.addr_ == nullptr);
      if(length == 0) {
        // OK to leave addr as nullptr
        return mm;
      }
      int huge_flag = 0;
      if(huge) {
#ifdef MAP_HUGETLB
        huge_flag = MAP_HUGETLB;
#endif // MAP_HUGETLB
      }
      mm.addr_ = mmap(nullptr, length, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | huge_flag, -1, 0);
      if(mm.addr_ == MAP_FAILED) {
        mm.addr_ = nullptr;
      }
      return mm;
    }
  };

  class Allocator {
   public:
    virtual ~Allocator() = default;

    virtual auto Allocate(size_t bytes) -> char * = 0;
    virtual auto
    AllocateAligned(size_t bytes, size_t hugePageSize) -> char * = 0;

    [[nodiscard]] virtual auto BlockSize() const -> size_t = 0;
  };

  class Arena : public Allocator {
   public:
    // No copying allowed
    Arena(const Arena &) = delete;
    void operator=(const Arena &) = delete;

    static constexpr size_t kInlineSize = 2048;
    static constexpr size_t kMinBlockSize = 4096;
    static constexpr size_t kMaxBlockSize = 2u << 30;

    static constexpr unsigned kAlignUnit = alignof(std::max_align_t);
    static_assert((kAlignUnit & (kAlignUnit - 1)) == 0,
                  "Pointer size should be power of 2");

    // huge_page_size: if 0, don't use huge page TLB. If > 0 (should set to the
    // supported hugepage size of the system), block allocation will try huge
    // page TLB first. If allocation fails, will fall back to normal case.
    explicit Arena(
      size_t blockSize = // NOLINT(bugprone-easily-swappable-parameters)
      kMinBlockSize,
      size_t hugePageSize = 0) // NOLINT(bugprone-easily-swappable-parameters)
        : kBlockSize(OptimizeBlockSize(blockSize)) {
      assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
             kBlockSize % kAlignUnit == 0);
      allocBytesRemaining_ = sizeof(inline_block_);
      blocksMemory_ += allocBytesRemaining_;
      alignedAllocPtr_ = inline_block_;
      unalignedAllocPtr_ = inline_block_ + allocBytesRemaining_;
    }
    ~Arena() override = default;

    auto Allocate(size_t bytes) -> char * override {
      // The semantics of what to return are a bit messy if we allow
      // 0-byte allocations, so we disallow them here (we don't need
      // them for our internal use).
      assert(bytes > 0);
      if(bytes <= allocBytesRemaining_) {
        unalignedAllocPtr_ -= bytes;
        allocBytesRemaining_ -= bytes;
        return unalignedAllocPtr_;
      }
      return AllocateFallback_(bytes, false /* unaligned */);
    }

    // huge_page_size: if >0, will try to allocate from huage page TLB.
    // The argument will be the size of the page size for huge page TLB. Bytes
    // will be rounded up to multiple of the page size to allocate through mmap
    // anonymous option with huge page on. The extra  space allocated will be
    // wasted. If allocation fails, will fall back to normal case. To enable it,
    // need to reserve huge pages for it to be allocated, like:
    //     sysctl -w vm.nr_hugepages=20
    // See linux doc Documentation/vm/hugetlbpage.txt for details.
    // huge page allocation can fail. In this case it will fail back to
    // normal cases. The messages will be logged to logger. So when calling with
    // huge_page_tlb_size > 0, we highly recommend a logger is passed in.
    // Otherwise, the error message will be printed out to stderr directly.
    auto AllocateAligned(size_t bytes, size_t hugePageSize) -> char * override {
      size_t currentMod =
        reinterpret_cast<uintptr_t>(alignedAllocPtr_) & (kAlignUnit - 1);
      size_t slop = (currentMod == 0 ? 0 : kAlignUnit - currentMod);
      size_t needed = bytes + slop;
      char *result;
      if(needed <= allocBytesRemaining_) {
        result = alignedAllocPtr_ + slop;
        alignedAllocPtr_ += needed;
        allocBytesRemaining_ -= needed;
      } else {
        // AllocateFallback always returns aligned memory
        result = AllocateFallback_(bytes, true /* aligned */);
      }
      assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
      return result;
    }

    // Returns an estimate of the total memory usage of data allocated
    // by the arena (exclude the space allocated but not yet used for future
    // allocations).
    [[nodiscard]] auto ApproximateMemoryUsage() const -> size_t {
      return blocksMemory_ + blocks_.size() * sizeof(char *) -
             allocBytesRemaining_;
    }

    [[nodiscard]] auto MemoryAllocatedBytes() const -> size_t {
      return blocksMemory_;
    }

    [[nodiscard]] auto AllocatedAndUnused() const -> size_t {
      return allocBytesRemaining_;
    }

    // If an allocation is too big, we'll allocate an irregular block with the
    // same size of that allocation.
    [[nodiscard]] auto IrregularBlockNum() const -> size_t {
      return irregular_block_num;
    }

    [[nodiscard]] auto BlockSize() const -> size_t override {
      return kBlockSize;
    }

    [[nodiscard]] auto IsInInlineBlock() const -> bool {
      return blocks_.empty() && huge_blocks_.empty();
    }

    // check and adjust the block_size so that the return value is
    //  1. in the range of [kMinBlockSize, kMaxBlockSize].
    //  2. the multiple of align unit.
    static auto OptimizeBlockSize(size_t block_size) -> size_t {
      // Make sure block_size is in optimal range
      block_size = std::max(Arena::kMinBlockSize, block_size);
      block_size = std::min(Arena::kMaxBlockSize, block_size);

      // make sure block_size is the multiple of kAlignUnit
      if(block_size % kAlignUnit != 0) {
        block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
      }

      return block_size;
    }

   private:
    alignas(std::max_align_t) char inline_block_
      [kInlineSize]{}; // NOLINT(modernize-avoid-c-arrays)
    // Number of bytes allocated in one block
    const size_t kBlockSize;
    // Allocated memory blocks
    std::deque<std::unique_ptr<char[]>>
      blocks_{}; // NOLINT(modernize-avoid-c-arrays)
    // Huge page allocations
    std::deque<MemMapping> huge_blocks_{};
    size_t irregular_block_num = 0;

    // Stats for current active block.
    // For each block, we allocate aligned memory chucks from one end and
    // allocate unaligned memory chucks from the other end. Otherwise the
    // memory waste for alignment will be higher if we allocate both types of
    // memory from one direction.
    char *unalignedAllocPtr_ = nullptr;
    char *alignedAllocPtr_ = nullptr;
    // How many bytes left in currently active block?
    size_t allocBytesRemaining_ = 0;

    [[maybe_unused]] size_t hugetlbSize_ = 0;

    auto AllocateFromHugePage_(size_t bytes) -> char * {
      MemMapping mm = MemMapping::AllocateHuge(bytes);
      auto addr = static_cast<char *>(mm.Get());
      if(addr) {
        huge_blocks_.push_back(std::move(mm));
        blocksMemory_ += bytes;
      }
      return addr;
    }
    auto AllocateFallback_(size_t bytes, bool aligned) -> char * {
      if(bytes > kBlockSize / 4) {
        ++irregular_block_num;
        // Object is more than a quarter of our block size.  Allocate it
        // separately to avoid wasting too much space in leftover bytes.
        return AllocateNewBlock_(bytes);
      }

      // We waste the remaining space in the current block.
      size_t size = 0;
      char *blockHead = nullptr;
      size = kBlockSize;
      blockHead = AllocateNewBlock_(size);
      allocBytesRemaining_ = size - bytes;

      if(aligned) {
        alignedAllocPtr_ = blockHead + bytes;
        unalignedAllocPtr_ = blockHead + size;
        return blockHead;
      } else {
        alignedAllocPtr_ = blockHead;
        unalignedAllocPtr_ = blockHead + size - bytes;
        return unalignedAllocPtr_;
      }
    }
    auto AllocateNewBlock_(size_t block_bytes) -> char * {
      auto uniq = std::make_unique<char[]>( // NOLINT(modernize-avoid-c-arrays)
        block_bytes);
      char *block = uniq.get();
      blocks_.push_back(std::move(uniq));

      size_t allocated_size;
      allocated_size = malloc_usable_size(block);
      blocksMemory_ += allocated_size;
      return block;
    }

    // Bytes of memory in blocks allocated so far
    size_t blocksMemory_ = 0;
  };

  class SpinMutex {
   public:
    SpinMutex() : locked_(false) {}

    auto try_lock() -> bool {
      auto currently_locked = locked_.load(std::memory_order_relaxed);
      return !currently_locked &&
             locked_.compare_exchange_weak(currently_locked, true,
                                           std::memory_order_acquire,
                                           std::memory_order_relaxed);
    }

    void lock() {
      for(size_t tries = 0;; ++tries) {
        if(try_lock()) {
          // success
          break;
        }
        asm volatile("pause");
        if(tries > 100) {
          std::this_thread::yield();
        }
      }
    }

    void unlock() { locked_.store(false, std::memory_order_release); }

   private:
    std::atomic<bool> locked_;
  };

  // An array of core-local values. Ideally the value type, T, is cache aligned
  // to prevent false sharing.
  template <typename T> class CoreLocalArray {
   public:
    CoreLocalArray() {
      int numCpus = static_cast<int>(std::thread::hardware_concurrency());
      // find a power of two >= num_cpus and >= 8
      sizeShift_ = 3;
      while(1 << sizeShift_ < numCpus) {
        ++sizeShift_;
      }
      data_.reset(new T[static_cast<size_t>(1) << sizeShift_]);
    }

    [[nodiscard]] auto Size() const -> size_t {
      return static_cast<size_t>(1) << sizeShift_;
    }
    // returns pointer to the element corresponding to the core that the thread
    // currently runs on.
    [[nodiscard]] auto Access() const -> T * {
      return AccessElementAndIndex().first;
    }
    // same as above, but also returns the core index, which the client can cache
    // to reduce how often core ID needs to be retrieved. Only do this if some
    // inaccuracy is tolerable, as the thread may migrate to a different core.
    [[nodiscard]] auto AccessElementAndIndex() const -> std::pair<T *, size_t> {
      int cpuid;

      int cpuno = sched_getcpu();
      if(cpuno < 0) {
        cpuid = -1;
      } else {
        cpuid = cpuno;
      }

      size_t core_idx;
      if((__builtin_expect(((cpuid < 0)), 0))) {
        // cpu id unavailable, just pick randomly
        core_idx = Random::GetTlsInstance()->Uniform(1 << sizeShift_);
      } else {
        core_idx = static_cast<size_t>(cpuid & ((1 << sizeShift_) - 1));
      }
      return {AccessAtCore(core_idx), core_idx};
    }
    // returns pointer to element for the specified core index. This can be
    // used, e.g., for aggregation, or if the client caches core index.
    [[nodiscard]] T *AccessAtCore(size_t core_idx) const {
      assert(core_idx < static_cast<size_t>(1) << sizeShift_);
      return &data_[core_idx];
    }

   private:
    std::unique_ptr<T[]> data_; // NOLINT(modernize-avoid-c-arrays)
    int sizeShift_;
  };

  class ConcurrentArena : public Allocator {
   public:
    // block_size and huge_page_size are the same as for Arena (and are
    // in fact just passed to the constructor of arena_.  The core-local
    // shards compute their shard_block_size as a fraction of block_size
    // that varies according to the hardware concurrency level.
    explicit ConcurrentArena(size_t blockSize = Arena::kMinBlockSize,
                             size_t hugePageSize = 0);

    auto Allocate(size_t bytes) -> char * override {
      return AllocateImpl_(bytes, false /*force_arena*/,
                           [this, bytes]() { return arena_.Allocate(bytes); });
    }

    auto
    AllocateAligned(size_t bytes, // NOLINT(bugprone-easily-swappable-parameters)
                    size_t hugePageSize) -> char * override {
      size_t rounded_up = ((bytes - 1) | (sizeof(void *) - 1)) + 1;
      assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void *) &&
             (rounded_up % sizeof(void *)) == 0);

      return AllocateImpl_(rounded_up, hugePageSize != 0 /*force_arena*/,
                           [this, rounded_up, hugePageSize]() {
                             return arena_.AllocateAligned(rounded_up,
                                                           hugePageSize);
                           });
    }
    auto ApproximateMemoryUsage() const -> size_t {
      std::unique_lock<SpinMutex> lock(arenaMutex_, std::defer_lock);
      lock.lock();
      return arena_.ApproximateMemoryUsage() - ShardAllocatedAndUnused_();
    }

    auto MemoryAllocatedBytes() const -> size_t {
      return memoryAllocatedBytes_.load(std::memory_order_relaxed);
    }

    auto AllocatedAndUnused() const -> size_t {
      return arenaAllocatedAndUnused_.load(std::memory_order_relaxed) +
             ShardAllocatedAndUnused_();
    }

    auto IrregularBlockNum() const -> size_t {
      return irregularBlockNum_.load(std::memory_order_relaxed);
    }

    auto BlockSize() const -> size_t override { return arena_.BlockSize(); }

   private:
    struct Shard {
      char padding[40]; // NOLINT(modernize-avoid-c-arrays)
      mutable SpinMutex mutex;
      char *freeBegin{nullptr};
      std::atomic<size_t> allocatedAndUnused;

      Shard() : allocatedAndUnused(0) {}
    };

    static thread_local size_t tlsCpuid_;

    [[maybe_unused]] char padding0_[56];

    size_t shardBlockSize_;

    CoreLocalArray<Shard> shards_;

    Arena arena_;
    mutable SpinMutex arenaMutex_;
    std::atomic<size_t> arenaAllocatedAndUnused_{};
    std::atomic<size_t> memoryAllocatedBytes_{};
    std::atomic<size_t> irregularBlockNum_{};

    [[maybe_unused]] char padding1_[56];

    auto Repick_() -> Shard * {
      auto shardAndIndex = shards_.AccessElementAndIndex();
      // even if we are cpu 0, use a non-zero tls_cpuid so we can tell we
      // have repicked
      tlsCpuid_ = shardAndIndex.second | shards_.Size();
      return shardAndIndex.first;
    }

    auto ShardAllocatedAndUnused_() const -> size_t {
      size_t total = 0;
      for(size_t i = 0; i < shards_.Size(); ++i) {
        total += shards_.AccessAtCore(i)->allocatedAndUnused.load(
          std::memory_order_relaxed);
      }
      return total;
    }

    template <typename Func>
    auto
    AllocateImpl_(size_t bytes, bool forceArena, const Func &func) -> char * {
      size_t cpu;

      // Go directly to the arena if the allocation is too large, or if
      // we've never needed to Repick() and the arena mutex is available
      // with no waiting.  This keeps the fragmentation penalty of
      // concurrency zero unless it might actually confer an advantage.
      std::unique_lock<SpinMutex> arenaLock(arenaMutex_, std::defer_lock);
      if(bytes > shardBlockSize_ / 4 || forceArena ||
         ((cpu = tlsCpuid_) == // NOLINT(bugprone-assignment-in-if-condition)
            0 &&
          !shards_.AccessAtCore(0)->allocatedAndUnused.load(
            std::memory_order_relaxed) &&
          arenaLock.try_lock())) {
        if(!arenaLock.owns_lock()) {
          arenaLock.lock();
        }
        auto rv = func();
        Fixup_();
        return rv;
      }

      // pick a shard from which to allocate
      Shard *s = shards_.AccessAtCore(cpu & (shards_.Size() - 1));
      if(!s->mutex.try_lock()) {
        s = Repick_();
        s->mutex.lock();
      }
      std::unique_lock<SpinMutex> lock(s->mutex, std::adopt_lock);

      size_t avail = s->allocatedAndUnused.load(std::memory_order_relaxed);
      if(avail < bytes) {
        // reload
        std::lock_guard<SpinMutex> reloadLock(arenaMutex_);

        // If the arena's current block is within a factor of 2 of the right
        // size, we adjust our request to avoid arena waste.
        auto exact = arenaAllocatedAndUnused_.load(std::memory_order_relaxed);
        assert(exact == arena_.AllocatedAndUnused());

        if(exact >= bytes && arena_.IsInInlineBlock()) {
          // If we haven't exhausted arena's inline block yet, allocate from
          // arena directly. This ensures that we'll do the first few small
          // allocations without allocating any blocks. In particular this
          // prevents empty memtables from using disproportionately large amount
          // of memory: a memtable allocates on the order of 1 KB of memory when
          // created; we wouldn't want to allocate a full arena block (typically
          // a few megabytes) for that, especially if there are thousands of
          // empty memtables.
          auto rv = func();
          Fixup_();
          return rv;
        }

        avail = exact >= shardBlockSize_ / 2 && exact < shardBlockSize_ * 2
                  ? exact
                  : shardBlockSize_;
        s->freeBegin = arena_.AllocateAligned(avail, 0);
        Fixup_();
      }
      s->allocatedAndUnused.store(avail - bytes, std::memory_order_relaxed);

      char *rv;
      if((bytes % sizeof(void *)) == 0) {
        // aligned allocation from the beginning
        rv = s->freeBegin;
        s->freeBegin += bytes;
      } else {
        // unaligned from the end
        rv = s->freeBegin + avail - bytes;
      }
      return rv;
    }

    void Fixup_() {
      arenaAllocatedAndUnused_.store(arena_.AllocatedAndUnused(),
                                     std::memory_order_relaxed);
      memoryAllocatedBytes_.store(arena_.MemoryAllocatedBytes(),
                                  std::memory_order_relaxed);
      irregularBlockNum_.store(arena_.IrregularBlockNum(),
                               std::memory_order_relaxed);
    }

   public:
    ConcurrentArena(const ConcurrentArena &) = delete;
    auto operator=(const ConcurrentArena &) -> ConcurrentArena & = delete;
  };

  class ConcurrentDramSkiplist {
   private:
    struct Splice;

   public:
    struct Node;

   public:
    class Iterator {
     public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit Iterator(const ConcurrentDramSkiplist *list) { SetList(list); }

      // Change the underlying skiplist used for this iterator
      // This enables us not changing the iterator without deallocating
      // an old one and then allocating a new one
      inline void SetList(const ConcurrentDramSkiplist *list) {
        list_ = list;
        node_ = nullptr;
      }

      // Returns true iff the iterator is positioned at a valid node.
      [[nodiscard]] inline bool Valid() const { return node_ != nullptr; }

      // Returns the key at the current position.
      // REQUIRES: Valid()
      [[nodiscard]] inline const char *key() const;

      // Advances to the next position.
      // REQUIRES: Valid()
      inline void Next();

      // Advances to the previous position.
      // REQUIRES: Valid()
      inline void Prev();

      // Advance to the first entry with a key >= target
      void Seek(const char *target) {
        node_ = list_->FindGreaterOrEqual(target);
      }

      // Retreat to the last entry with a key <= target
      void SeekForPrev(const char *target) {
        Seek(target);
        if(!Valid()) {
          SeekToLast();
        }
        while(Valid() && list_->LessThan(target, key())) {
          Prev();
        }
      }

      // Advance to a random entry in the list.
      //      void RandomSeek();

      // Position at the first entry in list.
      // Final state of iterator is Valid() iff list is not empty.
      inline void SeekToFirst();

      // Position at the last entry in list.
      // Final state of iterator is Valid() iff list is not empty.
      inline void SeekToLast() {
        node_ = list_->FindLast();
        if(node_ == list_->head_) {
          node_ = nullptr;
        }
      }

     private:
      const ConcurrentDramSkiplist *list_{};
      Node *node_{};
      // Intentionally copyable
    };

   public:
    static const uint16_t kMaxPossibleHeight = 32;
    inline ConcurrentDramSkiplist(InternalKeyComparator *internalKeyComparator,
                                  Allocator *allocator, int32_t maxHeight = 12,
                                  int32_t branchingFactor = 4);

    inline auto Insert(const char *key) -> bool;             
    inline auto InsertConcurrently(const char *key) -> bool; 

    template <bool UseCAS>
    auto
    Insert(const char *key, Splice *splice, bool allowPartialSpliceFix) -> bool;

    [[nodiscard]] inline auto RandomHeight() const -> int {
      auto rnd = Random::GetTlsInstance();

      // Increase height with probability 1 in kBranching
      int height = 1;
      while(height < kMaxHeight_ && height < kMaxPossibleHeight &&
            rnd->Next() < kScaledInverseBranching_) {
        height++;
      }
      assert(height > 0);
      assert(height <= kMaxHeight_);
      assert(height <= kMaxPossibleHeight);
      return height;
    }

   private:
    // Return the latest node with a key < key.
    // Return head_ if there is no such node.
    // Fills prev[level] with pointer to previous node at "level" for every
    // level in [0..max_height_-1], if prev is non-null.
    inline auto
    FindLessThan_(const char *key, Node **prev = nullptr) const -> Node * {
      return FindLessThan_(key, prev, head_,
                           maxHeight_.load(std::memory_order_relaxed), 0);
    }

    // Return the latest node with a key < key on bottom_level. Start searching
    // from root node on the level below top_level.
    // Fills prev[level] with pointer to previous node at "level" for every
    // level in [bottom_level..top_level-1], if prev is non-null.
    inline auto FindLessThan_(const char *key, Node **prev, Node *root,
                              int topLevel, int bottomLevel) const -> Node *;
    inline auto AllocateNode_(size_t key_size, int height) -> Node *;
    inline auto AllocateSplice_() -> Splice *;
    inline auto KeyIsAfterNode_(const char *key, Node *n) const -> bool;
    inline auto KeyIsAfterNode_(const Slice &key, Node *n) const -> bool;
    inline void RecomputeSpliceLevels_(const Slice &key, Splice *splice,
                                       int recomputeLevel);
    template <bool prefetch_before>
    void FindSpliceForLevel_(const Slice &key, Node *before, Node *after,
                             int level, Node **outPrev, Node **outNext);
    inline auto FindGreaterOrEqual(const char *key) const -> Node *;
    auto LessThan(const char *a, const char *b) const -> bool {
      return (internalKeyComparator_->Compare(GetLengthPrefixedSlice(a),
                                              GetLengthPrefixedSlice(b)) < 0);
    }
    [[nodiscard]] inline auto FindLast() const -> Node *;

   private:
    InternalKeyComparator *internalKeyComparator_;
    const uint16_t kMaxHeight_;
    const uint16_t kBranching_;
    const uint32_t kScaledInverseBranching_;

    Allocator *const allocator_;
    Node *const head_;
    std::atomic<int> maxHeight_;
    Splice *seqSplice_;
  };

  struct ConcurrentDramSkiplist::Node {
   public:
    void StashHeight(const int height) {
      memcpy(static_cast<void *>(&next_[0]), &height, sizeof(int));
    }
    [[nodiscard]] auto UnstashHeight() const -> int {
      int rv;
      memcpy(&rv, &next_[0], sizeof(int));
      return rv;
    }
    [[nodiscard]] auto Key() const -> const char * {
      return reinterpret_cast<const char *>(&next_[1]);
    }
    auto Next(int n) -> Node * {
      assert(n >= 0);
      // Use an 'acquire load' so that we observe a fully initialized
      // version of the returned Node.
      return ((&next_[0] - n)->load(std::memory_order_acquire));
    }
    void SetNext(int n, Node *x) {
      assert(n >= 0);
      // Use a 'release store' so that anybody who reads through this
      // pointer observes a fully initialized version of the inserted node.
      (&next_[0] - n)->store(x, std::memory_order_release);
    }
    auto CASNext(int n, Node *expected, Node *x) -> bool {
      assert(n >= 0);
      return (&next_[0] - n)->compare_exchange_strong(expected, x);
    }
    auto NoBarrier_Next(int n) -> Node * {
      assert(n >= 0);
      return (&next_[0] - n)->load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node *x) {
      assert(n >= 0);
      (&next_[0] - n)->store(x, std::memory_order_relaxed);
    }

   private:
    std::atomic<Node *> next_[1]; // NOLINT(modernize-avoid-c-arrays)
  };

  struct ConcurrentDramSkiplist::Splice {
    int height;
    Node **prev;
    Node **next;
  };
  ConcurrentDramSkiplist::ConcurrentDramSkiplist(
    InternalKeyComparator *internalKeyComparator, Allocator *allocator,
    int32_t maxHeight, // NOLINT(bugprone-easily-swappable-parameters)
    int32_t branchingFactor)
      : internalKeyComparator_(internalKeyComparator),
        kMaxHeight_(static_cast<uint16_t>(maxHeight)),
        kBranching_(static_cast<uint16_t>(branchingFactor)),
        kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
        allocator_(allocator), head_(AllocateNode_(0, maxHeight)),
        maxHeight_(1), seqSplice_(AllocateSplice_()) {
    for(int i = 0; i < kMaxHeight_; ++i) {
      head_->SetNext(i, nullptr);
    }
  }
  auto ConcurrentDramSkiplist::AllocateNode_(
    size_t key_size, // NOLINT(bugprone-easily-swappable-parameters)
    int height) -> ConcurrentDramSkiplist::Node * {

    auto prefix = sizeof(std::atomic<Node *>) * (height - 1);

    // prefix is space for the height - 1 pointers that we store before
    // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
    // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
    // next_[0].  key_size is the bytes for the key, which comes just after
    // the Node.
    char *raw =
      allocator_->AllocateAligned(prefix + sizeof(Node) + key_size, 0);
    Node *x = reinterpret_cast<Node *>(raw + prefix);

    // Once we've linked the node into the skip list we don't actually need
    // to know its height, because we can implicitly use the fact that we
    // traversed into a node at level h to known that h is a valid level
    // for that node.  We need to convey the height to the Insert step,
    // however, so that it can perform the proper links.  Since we're not
    // using the pointers at the moment, StashHeight temporarily borrow
    // storage from next_[0] for that purpose.
    x->StashHeight(height);
    return x;
  }
  auto ConcurrentDramSkiplist::AllocateSplice_()
    -> ConcurrentDramSkiplist::Splice * {
    // size of prev_ and next_
    size_t array_size =
      sizeof(Node *) * (kMaxHeight_ + 1); // NOLINT(bugprone-sizeof-expression)
    char *raw = allocator_->AllocateAligned(sizeof(Splice) + array_size * 2, 0);
    auto *splice = reinterpret_cast<Splice *>(raw);
    splice->height = 0;
    splice->prev = reinterpret_cast<Node **>(raw + sizeof(Splice));
    splice->next = reinterpret_cast<Node **>(raw + sizeof(Splice) + array_size);
    return splice;
  }
  auto ConcurrentDramSkiplist::InsertConcurrently(const char *key) -> bool {
    Node *prev[kMaxPossibleHeight]; // NOLINT(modernize-avoid-c-arrays)
    Node *next[kMaxPossibleHeight]; // NOLINT(modernize-avoid-c-arrays)
    Splice splice{};
    splice.prev = prev;
    splice.next = next;
    return Insert<true>(key, &splice, false);
  }
  auto ConcurrentDramSkiplist::Insert(const char *key) -> bool {
    return Insert<false>(key, seqSplice_, false);
  }
  template <bool UseCAS>
  auto ConcurrentDramSkiplist::Insert(const char *key,
                                      ConcurrentDramSkiplist::Splice *splice,
                                      bool allowPartialSpliceFix) -> bool {
    Node *x = reinterpret_cast<Node *>(const_cast<char *>(key)) - 1;
    Slice keyDecoded = GetLengthPrefixedSlice(key);
    int height = x->UnstashHeight();
    assert(height >= 1 && height <= kMaxHeight_);

    int maxHeight = maxHeight_.load(std::memory_order_relaxed);
    while(height > maxHeight) {
      if(maxHeight_.compare_exchange_weak(maxHeight, height)) {
        // successfully updated it
        maxHeight = height;
        break;
      }
      // else retry, possibly exiting the loop because somebody else
      // increased it
    }
    assert(maxHeight <= kMaxPossibleHeight);

    int recomputeHeight = 0;
    if(splice->height < maxHeight) {
      // Either splice has never been used or max_height has grown since
      // last use.  We could potentially fix it in the latter case, but
      // that is tricky.
      splice->prev[maxHeight] = head_;
      splice->next[maxHeight] = nullptr;
      splice->height = maxHeight;
      recomputeHeight = maxHeight;
    } else {
      // Splice is a valid proper-height splice that brackets some
      // key, but does it bracket this one?  We need to validate it and
      // recompute a portion of the splice (levels 0..recompute_height-1)
      // that is a superset of all levels that don't bracket the new key.
      // Several choices are reasonable, because we have to balance the work
      // saved against the extra comparisons required to validate the Splice.
      //
      // One strategy is just to recompute all of orig_splice_height if the
      // bottom level isn't bracketing.  This pessimistically assumes that
      // we will either get a perfect Splice hit (increasing sequential
      // inserts) or have no locality.
      //
      // Another strategy is to walk up the Splice's levels until we find
      // a level that brackets the key.  This strategy lets the Splice
      // hint help for other cases: it turns insertion from O(log N) into
      // O(log D), where D is the number of nodes in between the key that
      // produced the Splice and the current insert (insertion is aided
      // whether the new key is before or after the splice).  If you have
      // a way of using a prefix of the key to map directly to the closest
      // Splice out of O(sqrt(N)) Splices and we make it so that splices
      // can also be used as hints during read, then we end up with Oshman's
      // and Shavit's SkipTrie, which has O(log log N) lookup and insertion
      // (compare to O(log N) for skip list).
      //
      // We control the pessimistic strategy with allow_partial_splice_fix.
      // A good strategy is probably to be pessimistic for seq_splice_,
      // optimistic if the caller actually went to the work of providing
      // a Splice.
      while(recomputeHeight < maxHeight) {
        if(splice->prev[recomputeHeight]->Next(recomputeHeight) !=
           splice->next[recomputeHeight]) {
          // splice isn't tight at this level, there must have been some
          // inserts to this location that didn't update the splice.  We
          // might only be a little stale, but if the splice is very stale
          // it would be O(N) to fix it.  We haven't used up any of our
          // budget of comparisons, so always move up even if we are
          // pessimistic about
          // our chances of success.
          ++recomputeHeight;
        } else if(splice->prev[recomputeHeight] != head_ &&
                  !KeyIsAfterNode_(keyDecoded, splice->prev[recomputeHeight])) {
          // key is from before splice
          if(allowPartialSpliceFix) {
            // skip all levels with the same node without more comparisons
            Node *bad = splice->prev[recomputeHeight];
            while(splice->prev[recomputeHeight] == bad) {
              ++recomputeHeight;
            }
          } else {
            // we're pessimistic, recompute everything
            recomputeHeight = maxHeight;
          }
        } else if(KeyIsAfterNode_(keyDecoded, splice->next[recomputeHeight])) {
          // key is from after splice
          if(allowPartialSpliceFix) {
            Node *bad = splice->next[recomputeHeight];
            while(splice->next[recomputeHeight] == bad) {
              ++recomputeHeight;
            }
          } else {
            recomputeHeight = maxHeight;
          }
        } else {
          // this level brackets the key, we won!
          break;
        }
      }
    }
    assert(recomputeHeight <= maxHeight);
    if(recomputeHeight > 0)
      RecomputeSpliceLevels_(keyDecoded, splice, recomputeHeight);

    bool spliceIsValid = true;
    if(UseCAS) {
      for(int i = 0; i < height; ++i) {
        while(true) {
          // Checking for duplicate keys on the level 0 is sufficient
          if(UNLIKELY(i == 0 && splice->next[i] != nullptr &&
                      internalKeyComparator_->Compare(
                        GetLengthPrefixedSlice(x->Key()),
                        GetLengthPrefixedSlice(splice->next[i]->Key())) >= 0)) {
            return false;
          }
          if(UNLIKELY(i == 0 && splice->prev[i] != head_ &&
                      internalKeyComparator_->Compare(
                        GetLengthPrefixedSlice(splice->prev[i]->Key()),
                        GetLengthPrefixedSlice(x->Key())) >= 0)) {
            // duplicate key
            return false;
          }
          x->NoBarrier_SetNext(i, splice->next[i]);
          if(splice->prev[i]->CASNext(i, splice->next[i], x)) {
            // success
            break;
          }
          // CAS failed, we need to recompute prev and next. It is unlikely
          // to be helpful to try to use a different level as we redo the
          // search, because it should be unlikely that lots of nodes have
          // been inserted between prev[i] and next[i]. No point in using
          // next[i] as the after hint, because we know it is stale.
          FindSpliceForLevel_<false>(keyDecoded, splice->prev[i], nullptr, i,
                                     &splice->prev[i], &splice->next[i]);

          // Since we've narrowed the bracket for level i, we might have
          // violated the Splice constraint between i and i-1.  Make sure
          // we recompute the whole thing next time.
          if(i > 0) {
            spliceIsValid = false;
          }
        }
      }
    } else {
      for(int i = 0; i < height; ++i) {
        if(i >= recomputeHeight &&
           splice->prev[i]->Next(i) != splice->next[i]) {
          FindSpliceForLevel_<false>(keyDecoded, splice->prev[i], nullptr, i,
                                     &splice->prev[i], &splice->next[i]);
        }
        // Checking for duplicate keys on the level 0 is sufficient
        if(UNLIKELY(i == 0 && splice->next[i] != nullptr &&
                    internalKeyComparator_->Compare(
                      GetLengthPrefixedSlice(x->Key()),
                      GetLengthPrefixedSlice(splice->next[i]->Key())) >= 0)) {
          // duplicate key
          return false;
        }
        if(UNLIKELY(i == 0 && splice->prev[i] != head_ &&
                    internalKeyComparator_->Compare(
                      GetLengthPrefixedSlice(splice->prev[i]->Key()),
                      GetLengthPrefixedSlice(x->Key())) >= 0)) {
          // duplicate key
          return false;
        }
        x->NoBarrier_SetNext(i, splice->next[i]);
        splice->prev[i]->SetNext(i, x);
      }
    }
    if(spliceIsValid) {
      for(int i = 0; i < height; ++i) {
        splice->prev[i] = x;
      }
      for(int i = 0; i < splice->height; ++i) {}
    } else {
      splice->height = 0;
    }
    return true;
  }

  auto
  ConcurrentDramSkiplist::KeyIsAfterNode_(const char *key,
                                          ConcurrentDramSkiplist::Node *n) const
    -> bool {
    Slice k1 = GetLengthPrefixedSlice(key);
    Slice k2 = GetLengthPrefixedSlice(n->Key());
    return (n) && (internalKeyComparator_->Compare(k2, k1) < 0);
  }

  auto ConcurrentDramSkiplist::KeyIsAfterNode_(const Slice &key, Node *n) const
    -> bool {
    Slice k2 = GetLengthPrefixedSlice(n->Key());
    return (n) && (internalKeyComparator_->Compare(k2, key) < 0);
  }
  void ConcurrentDramSkiplist::RecomputeSpliceLevels_(
    const Slice &key, ConcurrentDramSkiplist::Splice *splice,
    int recomputeLevel) {
    for(int i = recomputeLevel - 1; i >= 0; --i)
      FindSpliceForLevel_<true>(key, splice->prev[i + 1], splice->next[i + 1],
                                i, &splice->prev[i], &splice->next[i]);
  }
  template <bool prefetch_before>
  void ConcurrentDramSkiplist::FindSpliceForLevel_(
    const Slice &key,
    ConcurrentDramSkiplist::Node // NOLINT(bugprone-easily-swappable-parameters)
      *before,
    ConcurrentDramSkiplist::Node *after, int level,
    ConcurrentDramSkiplist::Node * // NOLINT(bugprone-easily-swappable-parameters)
      *outPrev,
    ConcurrentDramSkiplist::Node **outNext) {
    while(true) {
      Node *next = before->Next(level);
      if(next != nullptr)
        PREFETCH(next->Next(level), 0, 1);
      if(prefetch_before) {
        if(next != nullptr && level > 0)
          PREFETCH(next->Next(level - 1), 0, 1);
      }
      assert(before == head_ || next == nullptr ||
             KeyIsAfterNode_(next->Key(), before));
      assert(before == head_ || KeyIsAfterNode_(key, before));
      if(next == after || !KeyIsAfterNode_(key, next)) {
        // found it
        *outPrev = before;
        *outNext = next;
        return;
      }
      before = next;
    }
  }
  auto
  ConcurrentDramSkiplist::FindLessThan_(const char *key,
                                        ConcurrentDramSkiplist::Node **prev,
                                        ConcurrentDramSkiplist::Node *root,
                                        int topLevel, int bottomLevel) const
    -> ConcurrentDramSkiplist::Node * {
    assert(topLevel > bottomLevel);
    int level = topLevel - 1;
    Node *x = root;
    // KeyIsAfter(key, last_not_after) is definitely false
    Node *lastNotAfter = nullptr;
    const Slice keyDecoded = GetLengthPrefixedSlice(key);
    while(true) {
      assert(x != nullptr);
      Node *next = x->Next(level);
      if(next != nullptr) {
        PREFETCH(next->Next(level), 0, 1);
      }
      assert(x == head_ || next == nullptr || KeyIsAfterNode_(next->Key(), x));
      assert(x == head_ || KeyIsAfterNode_(keyDecoded, x));
      if(next != lastNotAfter && KeyIsAfterNode_(keyDecoded, next)) {
        // Keep searching in this list
        assert(next != nullptr);
        x = next;
      } else {
        if(prev != nullptr) {
          prev[level] = x;
        }
        if(level == bottomLevel) {
          return x;
        } else {
          // Switch to next list, reuse KeyIsAfterNode() result
          lastNotAfter = next;
          level--;
        }
      }
    }
  }
  auto ConcurrentDramSkiplist::FindGreaterOrEqual(const char *key) const
    -> ConcurrentDramSkiplist::Node * {
    // Note: It looks like we could reduce duplication by implementing
    // this function as FindLessThan(key)->Next(0), but we wouldn't be able
    // to exit early on equality and the result wouldn't even be correct.
    // A concurrent insert might occur after FindLessThan(key) but before
    // we get a chance to call Next(0).
    Node *x = head_;
    int level = maxHeight_.load(std::memory_order_relaxed) - 1;
    Node *lastBigger = nullptr;
    const Slice keyDecoded = GetLengthPrefixedSlice(key);
    while(true) {
      Node *next = x->Next(level);
      if(next != nullptr) {
        PREFETCH(next->Next(level), 0, 1);
      }
      // Make sure the lists are sorted
      assert(x == head_ || next == nullptr || KeyIsAfterNode_(next->Key(), x));
      // Make sure we haven't overshot during our search
      assert(x == head_ || KeyIsAfterNode_(keyDecoded, x));
      int cmp = (next == nullptr || next == lastBigger)
                  ? 1
                  : internalKeyComparator_->Compare(
                      GetLengthPrefixedSlice(next->Key()), keyDecoded);
      if(cmp == 0 || (cmp > 0 && level == 0)) {
        return next;
      } else if(cmp < 0) {
        // Keep searching in this list
        x = next;
      } else {
        // Switch to next list, reuse compare_() result
        lastBigger = next;
        level--;
      }
    }
  }
  auto
  ConcurrentDramSkiplist::FindLast() const -> ConcurrentDramSkiplist::Node * {
    Node *x = head_;
    int level = maxHeight_.load(std::memory_order_relaxed) - 1;
    while(true) {
      Node *next = x->Next(level);
      if(next == nullptr) {
        if(level == 0) {
          return x;
        } else {
          // Switch to next list
          level--;
        }
      } else {
        x = next;
      }
    }
  }

  auto leveldb::pmCache::ConcurrentDramSkiplist::Iterator::key() const -> const
    char * {
    assert(Valid());
    return node_->Key();
  }
  void ConcurrentDramSkiplist::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
  }
  void ConcurrentDramSkiplist::Iterator::Prev() {
    // Instead of using explicit "prev" links, we just search for the
    // last node that falls before key.
    assert(Valid());
    node_ = list_->FindLessThan_(node_->Key());
    if(node_ == list_->head_) {
      node_ = nullptr;
    }
  }
  void ConcurrentDramSkiplist::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
  }
} // namespace leveldb::pmCache
#endif // LEVELDB_PMCACHE_DBWITHPMCACHE_HELP_DRAMSKIPLIST_CONCURRENTDRAMSKIPLIST_H_
