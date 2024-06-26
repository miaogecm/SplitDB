// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

  // See doc/table_format.md for an explanation of the filter block format.

  // Generate new filter every 2KB of data
  static const size_t kFilterBaseLg = 11;
  static const size_t kFilterBase = 1 << kFilterBaseLg;

  FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy *policy)
      : policy_(policy) {}

  void FilterBlockBuilder::StartBlock(uint64_t blockOffset) {
    uint64_t filterIndex = (blockOffset / kFilterBase);
    assert(filterIndex >= filter_offsets_.size());
    while(filterIndex > filter_offsets_.size())
      GenerateFilter_();
  }

  void FilterBlockBuilder::AddKey(const Slice &key) {
    Slice k = key;
    start_.push_back(keys_.size());
    keys_.append(k.data(), k.size());
  }

  Slice FilterBlockBuilder::Finish() {
    if(!start_.empty()) {
      GenerateFilter_();
    }

    // Append array of per-filter offsets
    const uint32_t array_offset = result_.size();
    for(unsigned int filter_offset : filter_offsets_)
      PutFixed32(&result_, filter_offset);

    PutFixed32(&result_, array_offset);
    result_.push_back(kFilterBaseLg); // Save encoding parameter in result
    return Slice(result_);
  }

  void FilterBlockBuilder::GenerateFilter_() {
    const size_t numKeys = start_.size();
    if(numKeys == 0) {
      // Fast path if there are no keys for this filter
      filter_offsets_.push_back(result_.size());
      return;
    }

    // Make list of keys from flattened key structure
    start_.push_back(keys_.size()); // Simplify length computation
    tmp_keys_.resize(numKeys);
    for(size_t i = 0; i < numKeys; i++) {
      const char *base = keys_.data() + start_[i];
      size_t length = start_[i + 1] - start_[i];
      tmp_keys_[i] = Slice(base, length);
    }

    // Generate filter for current set of keys and append to result_.
    filter_offsets_.push_back(result_.size());
    policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(numKeys), &result_);

    tmp_keys_.clear();
    keys_.clear();
    start_.clear();
  }

  FilterBlockReader::FilterBlockReader(const FilterPolicy *policy,
                                       const Slice &contents)
      : policy_(policy), data_(nullptr), offset_(nullptr), num_(0),
        base_lg_(0) {
    size_t n = contents.size();
    if(n < 5)
      return; // 1 byte for base_lg_ and 4 for start of offset array
    base_lg_ = contents[n - 1];
    uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
    if(last_word > n - 5)
      return;
    data_ = contents.data();
    offset_ = data_ + last_word;
    num_ = (n - 5 - last_word) / 4;
  }

  auto FilterBlockReader::KeyMayMatch(uint64_t blockOffset, const Slice &key)
    -> bool {
    uint64_t index = blockOffset >> base_lg_;
    if(index < num_) {
      uint32_t start = DecodeFixed32(offset_ + index * 4);
      uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
      if(start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
        Slice filter = Slice(data_ + start, limit - start);
        return policy_->KeyMayMatch(key, filter);
      } else if(start == limit) {
        // Empty filters do not match any keys
        return false;
      }
    }
    return true; // Errors are treated as potential matches
  }

} // namespace leveldb
