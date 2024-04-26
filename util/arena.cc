// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

  static const int kBlockSize = 4096;

  Arena::Arena()
      : allocPtr_(nullptr), allocBytesRemaining_(0), memoryUsage_(0) {}

  //
  Arena::~Arena() {
    for(auto &block : blocks_) {
      delete[] block;
    }
  }

  char *Arena::AllocateFallback_(size_t bytes) {
    if(bytes > kBlockSize / 4) {
      // Object is more than a quarter of our block size.  Allocate it
      // separately to avoid wasting too much space in leftover bytes.
      char *result = AllocateNewBlock_(bytes);
      return result;
    }

    // We waste the remaining space in the current block.
    allocPtr_ = AllocateNewBlock_(kBlockSize);
    allocBytesRemaining_ = kBlockSize;

    char *result = allocPtr_;
    allocPtr_ += bytes;
    allocBytesRemaining_ -= bytes;
    return result;
  }

  char *Arena::AllocateAligned(size_t bytes) {
    const int align = 8;
    static_assert((align & (align - 1)) == 0,
                  "Pointer size should be a power of 2");
    size_t currentMod = reinterpret_cast<uintptr_t>(allocPtr_) & (align - 1);
    size_t slop = (currentMod == 0 ? 0 : align - currentMod);
    size_t needed = bytes + slop;
    char *result;
    if(needed <= allocBytesRemaining_) {
      result = allocPtr_ + slop;
      allocPtr_ += needed;
      allocBytesRemaining_ -= needed;
    } else {
      // AllocateFallback always returned aligned memory
      result = AllocateFallback_(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
    return result;
  }

  char *Arena::AllocateNewBlock_(size_t blockBytes) {
    char *result = new char[blockBytes];
    blocks_.push_back(result);
    memoryUsage_.fetch_add(blockBytes + sizeof(char *),
                           std::memory_order_relaxed);
    return result;
  }
} // namespace leveldb
