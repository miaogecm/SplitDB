// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h"
#include "pmCache/dbWithPmCache/pm_file_in_one_level_opt/pm_file_in_one_level_opt.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <map>
#include <queue>
#include <set>
#include <vector>

namespace leveldb {
  namespace pmCache {
    class PmCachePoolRoot;
  }
  namespace log {
    class Writer;
  }

  class Compaction;

  class Iterator;

  class MemTable;

  class TableBuilder;

  class TableCache;

  class Version;

  class VersionSet;

  class WritableFile;

  // Return the smallest index i such that files[i]->largest >= key.
  // Return files.size() if there is no such file.
  // REQUIRES: "files" contains a sorted list of non-overlapping files.
  int FindFile(const InternalKeyComparator &icmp,
               const std::vector<FileMetaData *> &files, const Slice &key);

  // Returns true iff some file in "files" overlaps the user key range
  // [*smallest,*largest].
  // smallest==nullptr represents a key smaller than all keys in the DB.
  // largest==nullptr represents a key largest than all keys in the DB.
  // REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
  //           in sorted order.
  bool SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                             bool disjointSortedFiles,
                             const std::vector<FileMetaData *> &files,
                             const Slice *smallestUserKey,
                             const Slice *largestUserKey);

  class Version {
   public:
    Version(const Version &) = delete;
    enum StorageMedia {
      K_STORAGE_MEDIA_NVM = 0,
      K_STORAGE_MEDIA_DISK = 1,
      K_UNKNOWN = 2
    };
    struct GetStats {
      FileMetaData *seekFile{nullptr}, *hotKeyFile{nullptr};
      int seekFileLevel{-1}, hotKeyFileLevel{-1}, hotNessOfKeyFromBuffer{0};
    };

    // Append to *iters a sequence of iterators that will
    // yield the contents of this Version when merged together.
    // REQUIRES: This version has been saved (see VersionSet::SaveTo)
    void AddIterators(const ReadOptions &, std::vector<Iterator *> *iterators);

    // Lookup the value for key.  If found, store it in *val and
    // return OK.  Else return a non-OK status.  Fills *stats.
    // REQUIRES: lock is not held
    auto Get(const ReadOptions &, const LookupKey &key, std::string *val,
             GetStats *stats, bool readNewestVersion,
             std::atomic<uint64_t> *getHitCntPtr
             //        ,
             //               pmCache::Buffer::HotKeyValueWrap &hotKeyValueWrap,
             //        std::array<pmCache::Buffer, config::kNumLevels> *buffers
             ) -> Status;

    // Adds "stats" into the current state.  Returns true if a new
    // compaction may need to be triggered, false otherwise.
    // REQUIRES: lock is held
    auto UpdateStats(const GetStats &stats, bool shouldPromote) -> bool;

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod
    // bytes.  Returns true if a new compaction may need to be triggered.
    // REQUIRES: lock is held
    auto RecordReadSample(Slice internalKey) -> bool;

    // Reference count management (so Versions do not disappear out from
    // under live iterators)
    void Ref();

    void Unref();

    void GetOverlappingInputs(
      int level,
      const InternalKey *begin, // nullptr means before all keys
      const InternalKey *end,   // nullptr means after all keys
      std::vector<FileMetaData *> *inputs);

    // Returns true iff some file in the specified level overlaps
    // some part of [*smallest_user_key,*largest_user_key].
    // smallest_user_key==nullptr represents a key smaller than all the DB's
    // keys. largest_user_key==nullptr represents a key larger than all the
    // DB's keys.
    auto OverlapInLevel(int level, const Slice *smallestUserKey,
                        const Slice *largestUserKey) -> bool;

    // Return the level at which we should place a new memtable compaction
    // result that covers the range [smallest_user_key,largest_user_key].
    auto PickLevelForMemTableOutput(const Slice &smallestUserKey,
                                    const Slice &largestUserKey) -> int;

    [[nodiscard]] auto NumFiles(int level) const -> int {
      return (int)files[level].size();
    }

    auto operator=(const Version &) -> Version & = delete;

    // Return a human-readable string that describes this version's contents.
    [[nodiscard]] std::string DebugString() const;

    uint64_t GetLevel0FileSize() { return level0FileSize_; }

    leveldb::pmCache::GlobalTimestamp *globalTimestamp = nullptr;
    leveldb::pmCache::GlobalTimestamp::TimestampToken t{};

    // List of files per level
    std::array<std::vector<FileMetaData *>, config::kNumLevels> files{};
    //    std::vector<FileMetaData *> files[config::kNumLevels];

    std::mutex tmpFilesMutex;
    std::queue<std::vector<FileMetaData *> *> tmpFiles;
    Version *next; // Next version in linked list
    Version *prev; // Previous version in linked list

   private:
    friend class Compaction;

    friend class VersionSet;

    class LevelFileNumIterator;

    explicit Version(VersionSet *versionSet)
        : next(this), prev(this), versionSet_(versionSet) {}

    ~Version();

    [[nodiscard]] auto NewConcatenatingIterator(const ReadOptions &,
                                                int level) const -> Iterator *;

    // Call func(arg, level, f) for every file that overlaps user_key in
    // order from newest to oldest.  If an invocation of func returns
    // false, makes no more calls.
    //
    // REQUIRES: user portion of internal_key == user_key.
    void ForEachOverlapping_(Slice userKey, Slice internalKey, void *arg,
                             bool (*func)(void *, int, FileMetaData *));

    void TryToGetFromEachOverlappingTable_(Slice userKey, Slice internalKey,
                                           void *arg, bool readNewestVersion,
                                           std::atomic<uint64_t> *);

   private:
    VersionSet *versionSet_; // VersionSet to which this Version belongs
    int refs_{0};            // Number of live refs to this version

    // Next file to compact based on seek stats.
    FileMetaData *fileToCompact_{nullptr};
    int fileToCompactLevel_{-1};

    FileMetaData *fileToPromote_{nullptr};
    int fileToPromoteLevel_{-1};

   public:
    // Level that should be compacted next and its compaction score.
    // Score < 1 means compaction is not strictly needed.  These fields
    // are initialized by Finalize().
    double compactionScore{-1};
    int compactionLevel{-1};

   private:
    uint64_t level0FileSize_ = 0;
  };

  class VersionSet {
   private:
    friend class DBImpl;

   public:
    VersionSet(std::string dbname, const Options *options,
               TableCache *tableCache, const InternalKeyComparator *);

    VersionSet(const VersionSet &) = delete;

    VersionSet &operator=(const VersionSet &) = delete;

    ~VersionSet();

    // Apply *edit to the current version to form a new descriptor that
    // is both saved to persistent state and installed as the new
    // current version.  Will release *mu while actually writing to the file.
    // REQUIRES: *mu is held on entry.
    // REQUIRES: no other thread concurrently calls LogAndApply()
    Status LogAndApply(VersionEdit *edit, port::Mutex *mu,
                       pmCache::GlobalTimestamp::TimestampToken *newVersionTime,
                       pmCache::GlobalTimestamp *timePool, bool sync = true)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

    // Recover the last saved descriptor from persistent storage.
    Status Recover(bool *saveManifest, pmCache::PmCachePoolRoot * = nullptr);

    // Return the current version.
    Version *current() const { return current_; }

    // Return the current manifest file number
    uint64_t ManifestFileNumber() const { return manifestFileNumber_; }

    // Allocate and return a new file number
    uint64_t NewFileNumber() { return nextFileNumber_++; }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    void ReuseFileNumber(uint64_t file_number) {
      if(nextFileNumber_ == file_number + 1)
        nextFileNumber_ = file_number;
    }

    // Return the number of Table files at the specified level.
    int NumLevelFiles(int level) const;

    // Return the combined file size of all files at the specified level.
    int64_t NumLevelBytes(int level) const;

    // Return the last sequence number.
    [[nodiscard]] uint64_t LastSequence() const { 
      //      return lastSequence_;
      return lastSequenceAtomic_.load(std::memory_order_acquire);
    }

    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s) {
      //      assert(s >= lastSequence_);
      //      lastSequence_ = s;

      lastSequenceAtomic_.store(s, std::memory_order_release);
    }

    // Mark the specified file number as used.
    void MarkFileNumberUsed(uint64_t number);

    // Return the current log file number.
    uint64_t LogNumber() const { return logNumber_; }

    // Return the log file number for the log file that is currently
    // being compacted, or zero if there is no such log file.
    uint64_t PrevLogNumber() const { return prevLogNumber_; }

    // Pick level and inputs for a new compaction.
    // Returns nullptr if there is no compaction to be done.
    // Otherwise, returns a pointer to a heap-allocated object that
    // describes the compaction.  Caller should delete the result.
    auto PickCompactionOrPromotion(bool enoughRoomForNvm) -> Compaction *;

    Compaction *PickCompactionByLru(FileMetaData *fileToCompact, int level);

    // Return a compaction object for compacting the range [begin,end] in
    // the specified level.  Returns nullptr if there is nothing in that
    // level that overlaps the specified range.  Caller should delete
    // the result.
    Compaction *
    CompactRange(int level, const InternalKey *begin, const InternalKey *end);

    // Return the maximum overlapping data (in bytes) at next level for any
    // file at a level >= 1.
    int64_t MaxNextLevelOverlappingBytes();

    // Create an iterator that reads over the compaction inputs for "*c".
    // The caller should delete the iterator when no longer needed.
    Iterator *MakeInputIterator(Compaction *c);

    // Returns true iff some level needs a compaction.
    [[nodiscard]] auto NeedsCompactionOrPromotion(bool enoughNvm) -> bool;

    // Add all files listed in any live version to *live.
    // May also mutate some internal state.
    void AddLiveFiles(std::set<uint64_t> *live);

    // Return the approximate offset in the database of the data for
    // "key" as of version "v".
    uint64_t ApproximateOffsetOf(Version *v, const InternalKey &key);

    // Return a human-readable short (single-line) summary of the number
    // of files per level.  Uses *scratch as backing store.
    struct LevelSummaryStorage {
      char buffer[100];
    };

    [[maybe_unused]] [[maybe_unused]] auto
    LevelSummary(LevelSummaryStorage *scratch) const -> const char *;

    ////////////////////////////////////////new1
    //    pmCache::PmFileInOneLevelOptDramEntrance
    //      *pmFileInOneLevelOptDramEntrance_[20]{};

    std::array<pmCache::skiplistWithFanOut::PmSkiplistDramEntrance *,
               config::kNumLevels>
      pmSkiplistDramEntrance{};
    ////////////////////////////////////////new2

    TableCache *const tableCache;

   private:
    class Builder;

    friend class Compaction;

    friend class Version;

    bool ReuseManifest(const std::string &describeName,
                       const std::string &describeBase);

    static void Finalize_(Version *v);

    void GetRange(const std::vector<FileMetaData *> &inputs,
                  InternalKey *smallest, InternalKey *largest);

    void GetRange2(const std::vector<FileMetaData *> &inputs1,
                   const std::vector<FileMetaData *> &inputs2,
                   InternalKey *smallest, InternalKey *largest);

    void SetupOtherInputs(Compaction *c);

    // Save current contents to *log
    Status WriteSnapshot(log::Writer *log);

    void AppendVersion(Version *v);

    Env *const env_;
    const std::string dbname_;
    const Options *const options_;
    const InternalKeyComparator icmp_;
    uint64_t nextFileNumber_;
    uint64_t manifestFileNumber_;
    std::atomic<uint64_t> lastSequenceAtomic_;
    uint64_t logNumber_;
    uint64_t prevLogNumber_; // 0 or backing store for memtable being compacted

    // Opened lazily
    WritableFile *descriptorFile_;
    log::Writer *descriptorLog_;

   public:
    Version dummyVersions; // Head of circular doubly-linked list of versions.

    inline auto LevelIsInfluencedByCompaction(int level) -> bool {
      return levelInfluencedByCompaction_.find(level) !=
             levelInfluencedByCompaction_.end();
    }

    inline auto LevelBelowIsInfluencedByCompaction(int level) {
      auto ret = false;
      for(int i = 0; i <= level; ++i) {
        ret |= LevelIsInfluencedByCompaction(i);
        if(ret)
          return ret;
      }
      return ret;
    }

    inline void MarkLevelInfluencedByCompaction(int level) {
      levelInfluencedByCompaction_.insert(level);
    }

    inline void UnMarkLevelInfluencedByCompaction(int level) {
      levelInfluencedByCompaction_.erase(level);
    }

   private:
    Version *current_; // == dummy_versions_.prev_

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    std::array<std::string, config::kNumLevels> compactPointer_{};

    std::set<int> levelInfluencedByCompaction_;
  };

  // A Compaction encapsulates information about a compaction.
  class Compaction {
   public:
    ~Compaction();

    // Return the level that is being compacted.  Inputs from "level"
    // and "level+1" will be merged to produce a set of "level+1" files.
    [[nodiscard]] auto level() const -> int { return level_; }

    // Return the object that holds the edits to the descriptor done
    // by this compaction.
    auto edit() -> VersionEdit * { return &edit_; }

    // "which" must be either 0 or 1
    [[nodiscard]] auto NumInputFiles(int which) const -> int {
      return (int)inputs[which].size();
    }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    [[nodiscard]] auto input(int which, int i) const -> FileMetaData * {
      return inputs[which][i];
    }

    // Maximum size of files to build during this compaction.
    [[nodiscard]] auto MaxOutputFileSize() const -> uint64_t {
      return maxOutputFileSize_;
    }

    // Is this a trivial compaction that can be implemented by just
    // moving a single input file to the next level (no merging or splitting)
    [[nodiscard]] auto IsTrivialMove() const -> bool;

    // Add all inputs to this compaction as delete operations to *edit.
    void AddInputDeletions(VersionEdit *edit);

    // Returns true if the information we have available guarantees that
    // the compaction is producing data in "level+1" for which no data exists
    // in levels greater than "level+1".
    auto IsBaseLevelForKey(const Slice &userKey) -> bool;

    // Returns true iff we should stop building the current output
    // before processing "internal_key".
    auto ShouldStopBefore(const Slice &internalKey) -> bool;

    // Release the input version for the compaction, once the compaction
    // is successful.
    void ReleaseInputs();

    auto DebugString() {
      std::stringstream ss;
      ss << "\nlevel: " << level_ << std::endl;
      for(int i = 0; i < 2; ++i) {
        ss << "inputs[" << i + level_ << "]: ";
        for(auto &f : inputs[i]) {
          ss << f->number << " ";
        }
        ss << std::endl;
      }
      return ss.str();
    }

    // Each compaction reads inputs from "level_" and "level_+1"
    std::array<std::vector<FileMetaData *>, 2> inputs;

   private:
    friend class Version;

    friend class VersionSet;

    Compaction(const Options *options, int level);

   public:
    Version *inputVersion;
    // State used to check for number of overlapping grandparent files
    // (parent == level_ + 1, grandparent == level_ + 2)
    std::vector<FileMetaData *> grandparents;

   private:
    int level_;
    uint64_t maxOutputFileSize_;
    VersionEdit edit_;

    // The two sets of inputs
    std::array<std::vector<FileMetaData *>, 4> tmpInputs_;

    size_t grandparentIndex_; // Index in grandparent_starts_
    bool seenKey_;            // Some output key has been seen
    int64_t overlappedBytes_; // Bytes of overlap between current output
    // and grandparent files

    // State for implementing IsBaseLevelForKey

    // level_ptrs_ holds indices into input_version_->levels_: our state
    // is that we are positioned at one of the file ranges for each
    // higher level than the ones involved in this compaction (i.e. for
    // all L >= level_ + 2).
    std::array<size_t, config::kNumLevels> levelPtrs_{0};
  };

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_SET_H_
