// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "gtest/gtest.h"

namespace leveldb {

  static void TestEncodeDecode(const VersionEdit &edit) {
    std::string encoded, encoded2;
    edit.EncodeTo(&encoded);
    VersionEdit parsed;
    Status s = parsed.DecodeFrom(Slice{encoded});
    ASSERT_TRUE(s.ok()) << s.ToString();
    parsed.EncodeTo(&encoded2);
    ASSERT_EQ(encoded, encoded2);
  }

  TEST(VersionEditTest, EncodeDecode) {
    static const uint64_t kBig = 1ull << 50;

    VersionEdit edit;
    for(int i = 0; i < 4; i++) {
      TestEncodeDecode(edit);
      edit.AddFile(3, kBig + 300 + i, kBig + 400 + i,
                   InternalKey(Slice{"foo"}, kBig + 500 + i, kTypeValue),
                   InternalKey(Slice{"zoo"}, kBig + 600 + i, kTypeDeletion));
      edit.RemoveFile(4, kBig + 700 + i);
      edit.SetCompactPointer(
        i, InternalKey(Slice{"x"}, kBig + 900 + i, kTypeValue));
    }

    edit.SetComparatorName(Slice{"foo"});
    edit.SetLogNumber(kBig + 100);
    edit.SetNextFile(kBig + 200);
    edit.SetLastSequence(kBig + 1000);
    TestEncodeDecode(edit);
  }

} // namespace leveldb
