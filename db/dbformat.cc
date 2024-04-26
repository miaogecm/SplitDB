// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

namespace leveldb {

  static auto PackSequenceAndType(uint64_t seq, ValueType t) -> uint64_t {
    assert(seq <= kMaxSequenceNumber);
    assert(t <= kValueTypeForSeek);
    return (seq << 8) | t;
  }

  void AppendInternalKey(std::string *result, const ParsedInternalKey &key) {
    result->append(key.userKey.data(), key.userKey.size());
    PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
  }

  auto ParsedInternalKey::DebugString() const -> std::string {
    std::ostringstream ss;
    ss << '\'' << EscapeString(Slice{userKey.ToString()}) << "' @ " << sequence
       << " : " << static_cast<int>(type);
    return ss.str();
  }

  auto InternalKey::DebugString() const -> std::string {
    ParsedInternalKey parsed;
    if(ParseInternalKey(Slice{rep}, &parsed)) {
      return parsed.DebugString();
    }
    std::ostringstream ss;
    ss << "(bad)" << EscapeString(Slice{rep});
    return ss.str();
  }

  auto InternalKeyComparator::Name() const -> const char * {
    return "leveldb.InternalKeyComparator";
  }

  auto InternalKeyComparator::Compare(const Slice &aKey,
                                      const Slice &bKey) const -> int {
    // Order by:
    //    increasing user key (according to user-supplied comparator)
    //    decreasing sequence number
    //    decreasing type (though sequence# should be enough to disambiguate)
    int r = ExtractUserKey(aKey).compare(ExtractUserKey(bKey));
    if(r == 0) {
      const uint64_t aNum = DecodeFixed64(aKey.data() + aKey.size() - 8);
      const uint64_t bNum = DecodeFixed64(bKey.data() + bKey.size() - 8);
      if(aNum > bNum)
        r = -1;
      else if(aNum < bNum)
        r = +1;
    }
    return r;
  }

  void InternalKeyComparator::FindShortestSeparator(std::string *start,
                                                    const Slice &limit) const {
    // Attempt to shorten the user portion of the key
    Slice userStart = ExtractUserKey(Slice{*start});
    Slice userLimit = ExtractUserKey(limit);
    std::string tmp(userStart.data(), userStart.size());
    userComparator_->FindShortestSeparator(&tmp, userLimit);
    if(tmp.size() < userStart.size() &&
       userComparator_->Compare(userStart, Slice{tmp}) < 0) {
      // User key has become shorter physically, but larger logically.
      // Tack on the earliest possible number to the shortened user key.
      PutFixed64(&tmp,
                 PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
      assert(this->Compare(Slice{*start}, Slice{tmp}) < 0);
      assert(this->Compare(Slice{tmp}, Slice{limit}) < 0);
      start->swap(tmp);
    }
  }

  void InternalKeyComparator::FindShortSuccessor(std::string *key) const {
    Slice userKey = ExtractUserKey(Slice{*key});
    std::string tmp(userKey.data(), userKey.size());
    userComparator_->FindShortSuccessor(&tmp);
    if(tmp.size() < userKey.size() &&
       userComparator_->Compare(userKey, Slice{tmp}) < 0) {
      // User key has become shorter physically, but larger logically.
      // Tack on the earliest possible number to the shortened user key.
      PutFixed64(&tmp,
                 PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
      assert(this->Compare(Slice{*key}, Slice{tmp}) < 0);
      key->swap(tmp);
    }
  }

  auto InternalFilterPolicy::Name() const -> const char * {
    return user_policy_->Name();
  }

  void InternalFilterPolicy::CreateFilter(const Slice *keys, int n,
                                          std::string *dst) const {
    // We rely on the fact that the code in table.cc does not mind us
    // adjusting keys[].
    auto *mkey = const_cast<Slice *>(keys);
    for(int i = 0; i < n; i++) {
      mkey[i] = ExtractUserKey(keys[i]);
      // TODO(sanjay): Suppress dups?
    }
    user_policy_->CreateFilter(keys, n, dst);
  }

  auto InternalFilterPolicy::KeyMayMatch(const Slice &key, const Slice &f) const
    -> bool {
    return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
  }

  LookupKey::LookupKey(const Slice &userKey, SequenceNumber s) {
    size_t userKeySize = userKey.size();
    size_t needed = userKeySize + 13; // A conservative estimate
    char *dst = needed <= sizeof(space_) ? space_ : new char[needed];
    start_ = dst;
    dst = EncodeVarint32(dst, userKeySize + 8);
    kstart_ = dst;
    std::memcpy(dst, userKey.data(), userKeySize);
    dst += userKeySize;
    EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
    dst += 8;
    end_ = dst;
  }

} // namespace leveldb
