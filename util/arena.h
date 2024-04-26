// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

  class Arena {
   public:
    Arena();

    Arena(const Arena &) = delete;

    Arena &operator=(const Arena &) = delete;

    ~Arena();

    // Return a pointer to a newly allocated memory block of "bytes" bytes.
    char *Allocate(size_t bytes);

    // Allocate memory with the normal alignment guarantees provided by malloc.
    char *AllocateAligned(size_t bytes);

    // Returns an estimate of the total memory usage of data allocated
    // by the arena.
    [[nodiscard]] size_t MemoryUsage() const {
      return memoryUsage_.load(std::memory_order_relaxed);
    }

   private:
    char *AllocateFallback_(size_t bytes);

    char *AllocateNewBlock_(size_t blockBytes);

    // Allocation state
    char *allocPtr_;
    size_t allocBytesRemaining_;

    // Array of new[] allocated memory blocks
    std::vector<char *> blocks_;

    // Total memory usage of the arena.
    //
    // TODO(costan): This member is accessed via atomics, but the others are
    //               accessed without any locking. Is this OK?
    std::atomic<size_t> memoryUsage_;
  };

  inline char *Arena::Allocate(size_t bytes) {
    // The semantics of what to return are a bit messy if we allow
    // 0-byte allocations, so we disallow them here (we don't need
    // them for our internal use).
    assert(bytes > 0);
    if(bytes <= allocBytesRemaining_) {
      char *result = allocPtr_;
      allocPtr_ += bytes;
      allocBytesRemaining_ -= bytes;
      return result;
    }
    return AllocateFallback_(bytes);
  }

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_ARENA_H_
