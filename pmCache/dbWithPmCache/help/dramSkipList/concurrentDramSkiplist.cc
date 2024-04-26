//

//

#include "concurrentDramSkiplist.h"
namespace {
  // If the shard block size is too large, in the worst case, every core
  // allocates a block without populate it. If the shared block size is
  // 1MB, 64 cores will quickly allocate 64MB, and may quickly trigger a
  // flush. Cap the size instead.
  const size_t kMaxShardBlockSize = size_t{128 * 1024};
} // namespace

leveldb::pmCache::ConcurrentArena::ConcurrentArena(size_t blockSize,
                                                   size_t hugePageSize)
    : shardBlockSize_(std::min(kMaxShardBlockSize, blockSize / 8)), shards_(),
      arena_(blockSize, hugePageSize) {
  Fixup_();
}
