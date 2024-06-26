// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/logging.h"
#include "util/testutil.h"
#include "gtest/gtest.h"

namespace leveldb {

  // For testing: emit an array with one hash value per key
  class TestHashFilter : public FilterPolicy {
   public:
    const char *Name() const override { return "TestHashFilter"; }

    void
    CreateFilter(const Slice *keys, int n, std::string *dst) const override {
      for(int i = 0; i < n; i++) {
        uint32_t h = Hash(keys[i].data(), keys[i].size(), 1);
        PutFixed32(dst, h);
      }
    }

    bool KeyMayMatch(const Slice &key, const Slice &filter) const override {
      uint32_t h = Hash(key.data(), key.size(), 1);
      for(size_t i = 0; i + 4 <= filter.size(); i += 4) {
        if(h == DecodeFixed32(filter.data() + i)) {
          return true;
        }
      }
      return false;
    }
  };

  class FilterBlockTest : public testing::Test {
   public:
    TestHashFilter policy_;
  };

  TEST_F(FilterBlockTest, EmptyBuilder) {
    FilterBlockBuilder builder(&policy_);
    Slice block = builder.Finish();
    ASSERT_EQ("\\x00\\x00\\x00\\x00\\x0b", EscapeString(block));
    FilterBlockReader reader(&policy_, block);
    ASSERT_TRUE(reader.KeyMayMatch(0, Slice{"foo"}));
    ASSERT_TRUE(reader.KeyMayMatch(100000, Slice{"foo"}));
  }

  TEST_F(FilterBlockTest, SingleChunk) {
    FilterBlockBuilder builder(&policy_);
    builder.StartBlock(100);
    builder.AddKey(Slice{"foo"});
    builder.AddKey(Slice{"bar"});
    builder.AddKey(Slice{"box"});
    builder.StartBlock(200);
    builder.AddKey(Slice{"box"});
    builder.StartBlock(300);
    builder.AddKey(Slice{"hello"});
    Slice block = builder.Finish();
    FilterBlockReader reader(&policy_, block);
    ASSERT_TRUE(reader.KeyMayMatch(100, Slice{"foo"}));
    ASSERT_TRUE(reader.KeyMayMatch(100, Slice{"bar"}));
    ASSERT_TRUE(reader.KeyMayMatch(100, Slice{"box"}));
    ASSERT_TRUE(reader.KeyMayMatch(100, Slice{"hello"}));
    ASSERT_TRUE(reader.KeyMayMatch(100, Slice{"foo"}));
    ASSERT_TRUE(!reader.KeyMayMatch(100, Slice{"missing"}));
    ASSERT_TRUE(!reader.KeyMayMatch(100, Slice{"other"}));
  }

  TEST_F(FilterBlockTest, MultiChunk) {
    FilterBlockBuilder builder(&policy_);

    // First filter
    builder.StartBlock(0);
    builder.AddKey(Slice{"foo"});
    builder.StartBlock(2000);
    builder.AddKey(Slice{"bar"});

    // Second filter
    builder.StartBlock(3100);
    builder.AddKey(Slice{"box"});

    // Third filter is empty

    // Last filter
    builder.StartBlock(9000);
    builder.AddKey(Slice{"box"});
    builder.AddKey(Slice{"hello"});

    Slice block = builder.Finish();
    FilterBlockReader reader(&policy_, block);

    // Check first filter
    ASSERT_TRUE(reader.KeyMayMatch(0, Slice{"foo"}));
    ASSERT_TRUE(reader.KeyMayMatch(2000, Slice{"bar"}));
    ASSERT_TRUE(!reader.KeyMayMatch(0, Slice{"box"}));
    ASSERT_TRUE(!reader.KeyMayMatch(0, Slice{"hello"}));

    // Check second filter
    ASSERT_TRUE(reader.KeyMayMatch(3100, Slice{"box"}));
    ASSERT_TRUE(!reader.KeyMayMatch(3100, Slice{"foo"}));
    ASSERT_TRUE(!reader.KeyMayMatch(3100, Slice{"bar"}));
    ASSERT_TRUE(!reader.KeyMayMatch(3100, Slice{"hello"}));

    // Check third filter (empty)
    ASSERT_TRUE(!reader.KeyMayMatch(4100, Slice{"foo"}));
    ASSERT_TRUE(!reader.KeyMayMatch(4100, Slice{"bar"}));
    ASSERT_TRUE(!reader.KeyMayMatch(4100, Slice{"box"}));
    ASSERT_TRUE(!reader.KeyMayMatch(4100, Slice{"hello"}));

    // Check last filter
    ASSERT_TRUE(reader.KeyMayMatch(9000, Slice{"box"}));
    ASSERT_TRUE(reader.KeyMayMatch(9000, Slice{"hello"}));
    ASSERT_TRUE(!reader.KeyMayMatch(9000, Slice{"foo"}));
    ASSERT_TRUE(!reader.KeyMayMatch(9000, Slice{"bar"}));
  }

} // namespace leveldb
