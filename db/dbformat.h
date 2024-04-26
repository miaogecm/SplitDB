// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

  // Grouping of constants.  We may want to make some of these
  // parameters set via options.
  namespace config {

    static const std::vector<uint8_t> kMaxIndexHeights = // NOLINT(cert-err58-cpp)
      { // NOLINT(cert-err58-cpp)
        //   1, 2, 3, 4, 5, 6, 7, 8, 9, 10,11,12
        /**/ 3, 4, 5, 6, 7, 8, 9, 9, 9, 9, 9, 9};

    static const int kNumLevels = 7;

    // Level-0 compaction is started when we hit this many files.
    static const int kL0_CompactionTrigger = 8;

    // Soft limit on number of level-0 files.  We slow down writes at this point.
    static const int kL0_SlowdownWritesTrigger = 16;

    // Maximum number of level-0 files.  We stop writes at this point.
    static const int kL0_StopWritesTrigger = 30;

    // Maximum level to which a new compacted memtable is pushed if it
    // does not create overlap.  We try to push to level 2 to avoid the
    // relatively expensive level 0=>1 compactions and to avoid some
    // expensive manifest file operations.  We do not push all the way to
    // the largest level since that can generate a lot of wasted disk
    // space if the same key space is being repeatedly overwritten.
    static const int kMaxMemCompactLevel = 2;

    // Approximate gap in bytes between samples of data read during iteration.
    static const int kReadBytesPeriod = 1048576;

  } // namespace config

  class InternalKey;

  // Value types encoded as the last component of internal keys.
  // DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
  // data structures.
  enum ValueType { kTypeDeletion = 0x0, kTypeValue = 0x1 };
  // kValueTypeForSeek defines the ValueType that should be passed when
  // constructing a ParsedInternalKey object for seeking to a particular
  // sequence number (since we sort sequence numbers in decreasing order
  // and the value type is embedded as the low 8 bits in the sequence
  // number in internal keys, we need to use the highest-numbered
  // ValueType, not the lowest).
  static const ValueType kValueTypeForSeek = kTypeValue;

  typedef uint64_t SequenceNumber;

  // We leave eight bits empty at the bottom so a type and sequence#
  // can be packed together into 64-bits.
  static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);

  struct ParsedInternalKey {
    Slice userKey{};
    SequenceNumber sequence{};
    ValueType type{};

    ParsedInternalKey() =
      default; // Intentionally left uninitialized (for speed)
    ParsedInternalKey(const Slice &u, const SequenceNumber &seq, ValueType t)
        : userKey(u), sequence(seq), type(t) {}

    [[nodiscard]] std::string DebugString() const;
  };

  // Return the length of the encoding of "key".
  inline size_t InternalKeyEncodingLength(const ParsedInternalKey &key) {
    return key.userKey.size() + 8;
  }

  // Append the serialization of "key" to *result.
  void AppendInternalKey(std::string *result, const ParsedInternalKey &key);

  // Attempt to parse an internal key from "internal_key".  On success,
  // stores the parsed data in "*result", and returns true.
  //
  // On error, returns false, leaves "*result" in an undefined state.
  auto
  ParseInternalKey(const Slice &internalKey, ParsedInternalKey *result) -> bool;

  // Returns the user key portion of an internal key.
  inline auto ExtractUserKey(const Slice &internalKey) -> Slice {
    assert(internalKey.size() >= 8);
    return {internalKey.data(), internalKey.size() - 8};
  }

  // A comparator for internal keys that uses a specified comparator for
  // the user key portion and breaks ties by decreasing sequence number.
  class InternalKeyComparator : public Comparator {
   private:
    const Comparator *userComparator_;

   public:
    explicit InternalKeyComparator(const Comparator *c) : userComparator_(c) {}

    [[nodiscard]] auto Name() const -> const char * override;

    [[nodiscard]] int
    Compare(const Slice &aKey, const Slice &bKey) const override;

    void FindShortestSeparator(std::string *start,
                               const Slice &limit) const override;

    void FindShortSuccessor(std::string *key) const override;

    [[nodiscard]] const Comparator *user_comparator() const {
      return userComparator_;
    }

    [[nodiscard]] int Compare(const InternalKey &a, const InternalKey &b) const;
  };

  // Filter policy wrapper that converts from internal keys to user keys
  class InternalFilterPolicy : public FilterPolicy {
   private:
    const FilterPolicy *const user_policy_;

   public:
    explicit InternalFilterPolicy(const FilterPolicy *p) : user_policy_(p) {}

    const char *Name() const override;

    void
    CreateFilter(const Slice *keys, int n, std::string *dst) const override;

    bool KeyMayMatch(const Slice &key, const Slice &filter) const override;
  };

  // Modules in this directory should keep internal keys wrapped inside
  // the following class instead of plain strings so that we do not
  // incorrectly use string comparisons instead of an InternalKeyComparator.
  class InternalKey {

   public:
    InternalKey() = default; // Leave rep_ as empty to indicate it is invalid
    InternalKey(const Slice &userKey, SequenceNumber s, ValueType t) {
      AppendInternalKey(&rep, ParsedInternalKey(userKey, s, t));
    }
    //    InternalKey(InternalKey &&other) noexcept { rep_ =
    //    std::move(other.rep_); }

    auto DecodeFrom(const Slice &s) -> bool {
      rep.assign(s.data(), s.size());
      return !rep.empty();
    }

    [[nodiscard]] auto Encode() const -> Slice {
      assert(!rep.empty());
      return Slice{rep};
    }

    [[nodiscard]] auto UserKey() const -> Slice {
      return ExtractUserKey(Slice{rep});
    }

    void SetFrom(const ParsedInternalKey &p) {
      rep.clear();
      AppendInternalKey(&rep, p);
    }

    void Clear() { rep.clear(); }

    [[nodiscard]] std::string DebugString() const;
    std::string rep;
  };

  inline int InternalKeyComparator::Compare(const InternalKey &a,
                                            const InternalKey &b) const {
    return Compare(a.Encode(), b.Encode());
  }

  inline auto ParseInternalKey(const Slice &internalKey,
                               ParsedInternalKey *result) -> bool {
    const size_t n = internalKey.size();
    if(n < 8) {
      return false;
    }
    uint64_t num = DecodeFixed64(internalKey.data() + n - 8);
    uint8_t c = num & 0xff;
    result->sequence = num >> 8;
    result->type = static_cast<ValueType>(c);
    result->userKey = {internalKey.data(), n - 8};
    return (c <= static_cast<uint8_t>(kTypeValue));
  }

  // A helper class useful for DBImpl::Get()
  class LookupKey {
   public:
    // Initialize *this for looking up user_key at a snapshot with
    // the specified sequence number.
    LookupKey(const Slice &userKey, SequenceNumber sequence);

    LookupKey(const LookupKey &) = delete;

    LookupKey &operator=(const LookupKey &) = delete;

    ~LookupKey();

    // Return a key suitable for lookup in a MemTable.
    [[nodiscard]] Slice MemtableKey() const {
      return {start_, (size_t)(end_ - start_)};
    }

    // Return an internal key (suitable for passing to an internal iterator)
    [[nodiscard]] Slice InternalKey() const {
      return {kstart_, size_t(end_ - kstart_)};
    }

    // Return the user key
    [[nodiscard]] Slice UserKey() const {
      return {kstart_, size_t(end_ - kstart_ - 8)};
    }

   private:
    // We construct a char array of the form:
    //    klength  varint32               <-- start_
    //    userkey  char[klength]          <-- kstart_
    //    tag      uint64
    //                                    <-- end_
    // The array is a suitable MemTable key.
    // The suffix starting with "userkey" can be used as an InternalKey.
    const char *start_;
    const char *kstart_;
    const char *end_;
    char space_[200]{}; // Avoid allocation for short keys
  };

  inline LookupKey::~LookupKey() {
    if(start_ != space_)
      delete[] start_;
  }

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DBFORMAT_H_
