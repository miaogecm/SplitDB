// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <utility>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/table_cache.h"
#include "gsl/include/gsl/gsl"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "pmCache/dbWithPmCache/pm_cache_pool_impl.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"

namespace leveldb {

  static auto TargetFileSize(const Options *options) -> size_t {
    return options->max_file_size;
  }

  // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level->level+1 compaction.
  static auto MaxGrandParentOverlapBytes(const Options *options) -> int64_t {
    return 6 * (int64_t)TargetFileSize(options);
  }

  // Maximum number of bytes in all compacted files.  We avoid expanding
  // the lower level file set of a compaction if it would make the
  // total compaction cover more than this many bytes.
  static auto
  ExpandedCompactionByteSizeLimit(const Options *options) -> int64_t {
    return 25 * (int64_t)TargetFileSize(options);
  }

  [[maybe_unused]] static auto
  MaxBytesForLevel(const Options *, int level) -> double {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.

    // Result for both level-0 and level-1
    auto result = (double)1058823; // L1 10M
    while(level-- > 1)
      result *= 4.0;
    return result;
  }

  [[maybe_unused]] static auto
  MaxBytesForLevelNvm(const Options *, int level) -> double {
    double maxLevelForNvm = (double)LSM_NVM_BASE_LEVEL_SIZE;
    while(level-- > 1)
      maxLevelForNvm *= LSM_NVM_LEVEL_SIZE_MULTIPLE;
    return maxLevelForNvm;
  }

  static auto
  MaxFileSizeForLevel(const Options *options, int level) -> uint64_t {
    return TargetFileSize(options);
  }

  static auto
  TotalFileSize(const std::vector<FileMetaData *> &files) -> int64_t {
    int64_t sum = 0;
    for(auto file : files)
      sum += (int64_t)file->fileSize;
    return sum;
  }

  static auto TotalFileSizeConsiderNvm(const std::vector<FileMetaData *> &files)
    -> std::pair<uint64_t, uint64_t> {
    int64_t sumSsd = 0;
    int64_t sumNvm = 0;
    for(auto file : files) {
      if(file->whereIsFile == FileMetaData::K_NVM)
        sumNvm += (long long)file->fileSize;
      else if(file->whereIsFile == FileMetaData::K_SSD)
        sumSsd += (long long)file->fileSize;
      else {
        sumNvm += (long long)file->fileSize;
        sumSsd += (long long)file->fileSize;
      }
    }
    return {sumSsd, sumNvm};
  }

  Version::~Version() {
    assert(refs_ == 0);

    // Remove from linked list
    prev->next = next;
    next->prev = prev;

    // Drop references to files
    for(auto &file : files) {
      for(auto f : file) {
        assert(f->refs > 0);
        f->refs--;
        if(f->refs <= 0)
          delete f;
      }
    }
    if(t.whichNode != nullptr) {
      //        pmCache::printTimestampReference("d9",t_.thisTime);
      globalTimestamp->DereferenceTimestamp(t);
    }
    while(!tmpFiles.empty()) {
      delete tmpFiles.front();
      tmpFiles.pop();
    }
  }

  auto
  FindFile(const InternalKeyComparator &icmp,
           const std::vector<FileMetaData *> &files, const Slice &key) -> int {
    uint32_t left = 0;
    uint32_t right = files.size();
    while(left < right) {
      uint32_t mid = (left + right) / 2;
      const FileMetaData *f = files[mid];
      if(icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
        // Key at "mid.largest" is < "target".  Therefore all
        // files at or before "mid" are uninteresting.
        left = mid + 1;
      } else {
        // Key at "mid.largest" is >= "target".  Therefore all files
        // after "mid" are uninteresting.
        right = mid;
      }
    }
    return (int)right;
  }

  static auto AfterFile(const Comparator *ucmp, const Slice *userKey,
                        const FileMetaData *f) -> bool {
    // null user_key occurs before all keys and is therefore never after *f
    return (userKey != nullptr &&
            ucmp->Compare(*userKey, f->largest.UserKey()) > 0);
  }

  static auto BeforeFile(const Comparator *ucmp, const Slice *userKey,
                         const FileMetaData *f) -> bool {
    // null user_key occurs after all keys and is therefore never before *f
    return (userKey != nullptr &&
            ucmp->Compare(*userKey, f->smallest.UserKey()) < 0);
  }

  auto SomeFileOverlapsRange(const InternalKeyComparator &icmp,
                             bool disjointSortedFiles,
                             const std::vector<FileMetaData *> &files,
                             const Slice *smallestUserKey,
                             const Slice *largestUserKey) -> bool {
    const Comparator *ucmp = icmp.user_comparator();
    if(!disjointSortedFiles) {
      // Need to check against all files
      return std::any_of(files.begin(), files.end(), [&](auto f) {
        if(AfterFile(ucmp, smallestUserKey, f) ||
           BeforeFile(ucmp, largestUserKey, f))
          return false;
        else
          return true;
      });

      //      for(auto f : files) {
      //        if(AfterFile(ucmp, smallestUserKey, f) ||
      //           BeforeFile(ucmp, largestUserKey, f)) {
      //
      //          // No overlap
      //        } else {
      //          return true; // Overlap
      //        }
      //      }
      //      return false;
    }

    // Binary search over file list
    uint32_t index = 0;
    if(smallestUserKey != nullptr) {
      // Find the earliest possible internal key for smallest_user_key
      InternalKey smallKey(*smallestUserKey, kMaxSequenceNumber,
                           kValueTypeForSeek);
      index = FindFile(icmp, files, smallKey.Encode());
    }

    if(index >= files.size()) {
      // beginning of range is after all files, so no overlap.
      return false;
    }

    return !BeforeFile(ucmp, largestUserKey, files[index]);
  }

  // An internal iterator.  For a given version/level pair, yields
  // information about the files in the level.  For a given entry, key()
  // is the largest key that occurs in the file, and value() is an
  // 16-byte value containing the file number and file size, both
  // encoded using EncodeFixed64.
  class Version::LevelFileNumIterator : public Iterator {
   public:
    LevelFileNumIterator(InternalKeyComparator icmp,
                         const std::vector<FileMetaData *> *fList)
        : icmp_(std::move(icmp)), fList_(fList),
          index_(fList->size()) { // Marks as invalid
#ifndef NDEBUG
      std::cout << "";
#endif
    }

    auto WhichFile() -> FileMetaData * override { return (*fList_)[index_]; }

    auto Valid() const -> bool override { return index_ < fList_->size(); }

    void Seek(const Slice &target) override {
      index_ = FindFile(icmp_, *fList_, target);
    }

    void SeekToFirst() override { index_ = 0; }

    void SeekToLast() override {
      index_ = fList_->empty() ? 0 : fList_->size() - 1;
    }

    void Next() override {
      assert(Valid());
      index_++;
    }

    void Prev() override {
      assert(Valid());
      if(index_ == 0) {
        index_ = fList_->size(); // Marks as invalid
      } else {
        index_--;
      }
    }

    auto key() const -> Slice override {
      assert(Valid());
      return (*fList_)[index_]->largest.Encode();
    }

    auto value() const -> Slice override {
      assert(Valid());
      EncodeFixed64(valueBuf_, (*fList_)[index_]->number);
      EncodeFixed64(valueBuf_ + 8, (*fList_)[index_]->fileSize);
      return {valueBuf_, sizeof(valueBuf_)};
    }

    auto status() const -> Status override { return Status::OK(); }

   private:
    const InternalKeyComparator icmp_;
    const std::vector<FileMetaData *> *const fList_;
    uint32_t index_;

    // Backing store for value().  Holds the file number and size.
    mutable char valueBuf_[16]{}; // NOLINT(modernize-avoid-c-arrays)
  };

  static auto LookFor(const std::vector<FileMetaData *> &files,
                      const unsigned long long int num) -> int {
    assert(num > 0);
    int i = 0;
    int s = (int)files.size();
    while(i < s) {
      if(files[i]->number == num)
        return i;
      i++;
    }
    return -1;
  }

  static auto GetPmSsTableIterator(void *arg, const ReadOptions &options,
                                   const Slice &fileValue) -> Iterator * {
    auto *files = (std::vector<FileMetaData *> *)arg;
    auto k = LookFor(*files, (int)DecodeFixed64(fileValue.data()));
    return k == -1 ? NewErrorIterator(
                       Status::Corruption(Slice{"Cannot find file in NVM"}))
                   : (*files)[k]->nvmSingleNode->NewIterator();
  }

  static auto GetFileIterator(void *arg, const ReadOptions &options,
                              const Slice &fileValue) -> Iterator * {
    auto *cache = reinterpret_cast<TableCache *>(arg);
    return fileValue.size() != 16
             ? NewErrorIterator(Status::Corruption(
                 Slice{"FileReader invoked with unexpected value"}))
             : cache->NewIterator(options, DecodeFixed64(fileValue.data()),
                                  DecodeFixed64(fileValue.data() + 8));
  }

  [[maybe_unused]] auto
  Version::NewConcatenatingIterator(const ReadOptions &options,
                                    int level) const -> Iterator * {
    return NewTwoLevelIterator(
      new LevelFileNumIterator(versionSet_->icmp_, &files[level]),
      &GetFileIterator, versionSet_->tableCache, options);
  }

  void Version::AddIterators(const ReadOptions &options,
                             std::vector<Iterator *> *iterators) {
    // Merge all level zero files together since they may overlap
    for(auto &i : files[0])
      iterators->push_back(i->nvmSingleNode->NewIterator());

    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    for(int level = 1; level < config::kNumLevels; level++) {
      auto sstableFromSsd = new std::vector<FileMetaData *>;
      auto sstableFromNvm = new std::vector<FileMetaData *>;
      {
        std::unique_lock<std::mutex> lock(tmpFilesMutex);
        tmpFiles.push(sstableFromSsd);
        tmpFiles.push(sstableFromNvm);
      }
      for(auto file : files[level])
        if(file->whereIsFile == FileMetaData::K_SSD)
          sstableFromSsd->push_back(file);
        else
          sstableFromNvm->push_back(file);
      if(!sstableFromSsd->empty())
        iterators->push_back(NewTwoLevelIterator(
          new LevelFileNumIterator(versionSet_->icmp_, sstableFromSsd),
          &GetFileIterator, versionSet_->tableCache, options));
      if(!sstableFromNvm->empty())
        iterators->push_back(NewTwoLevelIterator(
          new LevelFileNumIterator(versionSet_->icmp_, sstableFromNvm),
          &GetPmSsTableIterator, sstableFromNvm, options));
    }
  }

  // Callback from TableCache::Get()
  namespace {
    enum SaverState {
      K_NOT_FOUND,
      K_FOUND,
      K_DELETED,
      K_CORRUPT,
    };
    class Saver {
     public:
      Saver(SaverState state, const Comparator *ucmp, Slice userKey,
            std::string *value)
          : state(state), ucmp(ucmp), userKey(std::move(userKey)),
            value(value) {}

      SaverState state;
      const Comparator *ucmp;
      Slice userKey;
      std::string *value;
    };
  } // namespace
  static void
  SaveValue(void *arg,
            const Slice &ikey, // NOLINT(bugprone-easily-swappable-parameters)
            const Slice &v) {
    auto *s = reinterpret_cast<Saver *>(arg);
    ParsedInternalKey parsedKey;
    if(!ParseInternalKey(ikey, &parsedKey))
      s->state = K_CORRUPT;
    else {
      if(s->ucmp->Compare(parsedKey.userKey, s->userKey) == 0) {
        s->state = (parsedKey.type == kTypeValue) ? K_FOUND : K_DELETED;
        if(s->state == K_FOUND)
          s->value->assign(v.data(), v.size());
      }
    }
  }

  static auto NewestFirst(FileMetaData *a, FileMetaData *b) -> bool {
    return a->number > b->number;
  }

  void Version::ForEachOverlapping_(
    Slice userKey, // NOLINT(bugprone-easily-swappable-parameters)
    Slice internalKey, void *arg, bool (*func)(void *, int, FileMetaData *)) {
    const Comparator *ucmp = versionSet_->icmp_.user_comparator();

    // Search level-0 in order from newest to oldest.
    std::vector<FileMetaData *> tmp;
    tmp.reserve(files[0].size());
    for(auto pFileMetaData : files[0]) {
      if(ucmp->Compare(userKey, pFileMetaData->smallest.UserKey()) >= 0 &&
         ucmp->Compare(userKey, pFileMetaData->largest.UserKey()) <= 0)
        tmp.push_back(pFileMetaData);
    }
    if(!tmp.empty()) {
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      for(auto &pFileMetaData : tmp)
        if(!(*func)(arg, 0, pFileMetaData))
          return;
    }

    // Search other levels.
    for(int level = 1; level < config::kNumLevels; level++) {
      size_t numFiles = files[level].size();
      if(numFiles == 0)
        continue;
      // Binary search to find the earliest index whose largest key >=
      // internal_key.
      uint32_t index = FindFile(versionSet_->icmp_, files[level], internalKey);
      if(index < numFiles) {
        FileMetaData *f = files[level][index];
        if(ucmp->Compare(userKey, f->smallest.UserKey()) < 0) {
          // All of "f" is past any data for user_key
        } else if(!(*func)(arg, level, f))
          return;
      }
    }
  }

  namespace {
    class ReadLockGuard {
     public:
      ReadLockGuard(std::shared_mutex &mutex, bool tryLock) : mutex(mutex) {
        if(tryLock)
          lockOk = mutex.try_lock_shared();
        else {
          mutex.lock_shared();
          lockOk = true;
        }
      }
      ~ReadLockGuard() {
        if(lockOk)
          mutex.unlock_shared();
      }
      std::shared_mutex &mutex;
      bool lockOk = false;
    };
  } // namespace

  class InnerGetState {
   public:
    InnerGetState(Version::GetStats *stats, const ReadOptions *options,
                  Slice ikey, VersionSet *versionSet, SaverState state,
                  const Comparator *ucmp, const Slice &userKey,
                  std::string *value)
        : saver(state, ucmp, userKey, value), stats(stats), options(options),
          ikey(std::move(ikey)), versionSet(versionSet) {}
    Saver saver;
    Version::GetStats *stats;
    const ReadOptions *options;
    Slice ikey;
    FileMetaData *lastFileRead{nullptr};
    int lastFileReadLevel{-1};
    VersionSet *versionSet;
    Status s;
    bool found{false};

    static auto MatchMayNotKnowFile(
      void *stateArg, int level, FileMetaData *fileMetaData,
      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *pFile) -> bool {
      auto *state = reinterpret_cast<InnerGetState *>(stateArg);

      if(fileMetaData) {
        if(state->stats->seekFile == nullptr &&
           state->lastFileRead != nullptr) {
          // We have had more than one seek for this read.  Charge the 1st file.
          state->stats->seekFile = state->lastFileRead;
          state->stats->seekFileLevel = state->lastFileReadLevel;
        }

        state->lastFileRead = fileMetaData;
        state->lastFileReadLevel = level;
      }

      if(fileMetaData && fileMetaData->whereIsFile == FileMetaData::K_SSD) {
        state->s = state->versionSet->tableCache->Get(
          *state->options, fileMetaData->number, fileMetaData->fileSize,
          state->ikey, &state->saver, SaveValue);
      } else {
        if(fileMetaData)
          pFile = fileMetaData->nvmSingleNode;
        leveldb::pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode::GetKind
          getKind;
        pFile->Get(state->ikey, state->saver.value, &getKind);
        switch(getKind) {
        case pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode::K_DELETED:
          state->saver.state = K_DELETED;
          state->s = Status::OK();
          break;
        case pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode::K_FOUND:
          state->saver.state = K_FOUND;
          state->s = Status::OK();
          break;
        case pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode::K_NOT_FOUND:
          state->saver.state = K_NOT_FOUND;
          state->s = Status::OK();
          break;
        case pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode::K_CORRUPTED:
          state->saver.state = K_CORRUPT;
          state->s =
            Status::Corruption(Slice("corrupted when searching NvTable"));
          break;
        }
      }
      if(!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch(state->saver.state) {
      case K_NOT_FOUND: return true; // Keep searching in other files
      case K_FOUND: state->found = true; return false;
      case K_DELETED: return false;
      case K_CORRUPT:
        state->s =
          Status::Corruption(Slice{"corrupted key for "}, state->saver.userKey);
        state->found = true;
        return false;
      }
      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  void Version::TryToGetFromEachOverlappingTable_(
    Slice userKey, // NOLINT(bugprone-easily-swappable-parameters)
    Slice internalKey, void *arg, bool readNewestVersion,
    std::atomic<uint64_t> *getHitCntPtr) {

    const Comparator *ucmp = versionSet_->icmp_.user_comparator();
    // Search level-0 in order from newest to oldest.
    std::vector<FileMetaData *> tmp;
    tmp.reserve(files[0].size());
    for(auto pFileMetaData : files[0])
      if(ucmp->Compare(userKey, pFileMetaData->smallest.UserKey()) >= 0 &&
         ucmp->Compare(userKey, pFileMetaData->largest.UserKey()) <= 0)
        tmp.push_back(pFileMetaData);

    //    auto t1 = std::chrono::high_resolution_clock::now();

    if(!tmp.empty()) {
      std::sort(tmp.begin(), tmp.end(), NewestFirst);
      for(auto &pFileMetaData : tmp)
        if(!leveldb::InnerGetState::MatchMayNotKnowFile(arg, 0, pFileMetaData,
                                                        nullptr)) {
          getHitCntPtr[0]++;
          //          std::stringstream ss;
          //          ss << userKey.ToString() << " N" << 0 << "\n";
          //          std::cout << ss.str() << std::flush;
          //          auto t2 = std::chrono::high_resolution_clock::now();
          //          std::cout << "nt " +
          //                         std::to_string(
          //                           std::chrono::duration_cast<std::chrono::nanoseconds>(
          //                             t2 - t1)
          //                             .count()) +
          //                         "\n"
          //                    << std::flush;

          return;
        }
    }

    //    auto t2 = std::chrono::high_resolution_clock::now();
    //    std::cout << "nt " +
    //                   std::to_string(
    //                     std::chrono::duration_cast<std::chrono::nanoseconds>(t2
    //                     -
    //                                                                          t1)
    //                       .count()) +
    //                   "\n"
    //              << std::flush;

    pmCache::skiplistWithFanOut::PmSkiplistUpperNode *lastLevelNode = nullptr;

    for(int currentLevel = 1; currentLevel < config::kNumLevels;
        ++currentLevel) {

      auto skiplistEntrance = versionSet_->pmSkiplistDramEntrance[currentLevel];

      pmCache::skiplistWithFanOut::PmSkiplistUpperNode *thisLevelStartNode =
                                                         nullptr,
                                                       *targetUpperNode =
                                                         nullptr,
                                                       *hintNodeForNextLevel =
                                                         nullptr;

      pmCache::skiplistWithFanOut::PmSkiplistNvmSingleNode *targetNvmSingleNode =
        nullptr;

      pmCache::skiplistWithFanOut::PmSkiplistUpperNode *hintNodeFromLastLevel =
        nullptr;

      bool findOk = currentLevel > NVM_MAX_LEVEL;

      while(!findOk) {

        targetNvmSingleNode = nullptr;

        auto currentTime = globalTimestamp->GetCurrentTimeFromDram();

        if(lastLevelNode) {

          pmCache::skiplistWithFanOut::PmSkiplistUpperNode *newHint = nullptr;

          ReadLockGuard readLockGuard{lastLevelNode->nextLevelHintMutex, true};
          if(readLockGuard.lockOk) {
            hintNodeFromLastLevel = lastLevelNode->GetNextLevelHint();

            auto closerNode =
              hintNodeFromLastLevel &&
                  hintNodeFromLastLevel->disappearNode.load(
                    std::memory_order_acquire)
                ? &skiplistEntrance->dummyHead
                : skiplistEntrance->LowerBoundSearchUpperNode(
                    hintNodeFromLastLevel, lastLevelNode->GetKey().GetKey(),
                    newHint, t.thisTime);

            if(hintNodeFromLastLevel &&
               hintNodeFromLastLevel->disappearNode.load(
                 std::memory_order_acquire))
              closerNode = &skiplistEntrance->dummyHead;

            if(hintNodeFromLastLevel == nullptr ||
               !hintNodeFromLastLevel->disappearNode.load(
                 std::memory_order_acquire)) {

              if(hintNodeFromLastLevel == nullptr ||
                 newHint != hintNodeFromLastLevel) {

                lastLevelNode->nextLevelHintMutex.unlock_shared();
                readLockGuard.lockOk = false;

                bool updateHintNodeOk =
                  lastLevelNode->SetNextLevelHint(newHint);
                if(!updateHintNodeOk) {
                  lastLevelNode = nullptr;
                }
              }

              thisLevelStartNode = closerNode;
            } else {

              lastLevelNode->nextLevelHintMutex.unlock_shared();
              readLockGuard.lockOk = false;
              lastLevelNode->SetNextLevelHint(nullptr);

              thisLevelStartNode = &skiplistEntrance->dummyHead;
            }
          } else
            thisLevelStartNode = &skiplistEntrance->dummyHead;
        } else
          thisLevelStartNode = &skiplistEntrance->dummyHead;

        targetUpperNode = skiplistEntrance->LowerBoundSearchUpperNode(
          thisLevelStartNode, internalKey, hintNodeForNextLevel, t.thisTime);

        if(hintNodeForNextLevel == &skiplistEntrance->dummyHead)
          hintNodeForNextLevel = nullptr;

        if(targetUpperNode == &skiplistEntrance->dummyTail) {
          targetUpperNode = nullptr;
          findOk = true;
          break;
        }

        std::shared_ptr<pmCache::skiplistWithFanOut::PmSkiplistFanOutGroupNodes>
          fanOutNodes;
        if(targetUpperNode) {
          fanOutNodes = targetUpperNode->GetFanOutNodes();
          if(fanOutNodes->lastSplitTime >= currentTime) {
            findOk = false;
            continue;
          } else {
            auto fanOutArray = fanOutNodes->fanOutNodes.get();
            auto fanOutNodesCnt = fanOutNodes->fanOutNodesCount;
            auto fanOutNvmSingleNode = std::lower_bound(
              fanOutArray, fanOutArray + fanOutNodesCnt,
              pmCache::skiplistWithFanOut::PmSkiplistDramEntrance::
                CompareNodeForLevel1P{internalKey, t.thisTime},
              [skiplistEntrance](
                const pmCache::skiplistWithFanOut::PmSkiplistFanOutSingleNode &a,
                const pmCache::skiplistWithFanOut::PmSkiplistDramEntrance::
                  CompareNodeForLevel1P &b) {
                return skiplistEntrance->Compare(a, b) < 0;
              });

            if(fanOutArray != nullptr) {
              if(fanOutNvmSingleNode == fanOutArray + fanOutNodesCnt)
                fanOutNvmSingleNode = fanOutArray + fanOutNodesCnt - 1;
              targetNvmSingleNode = fanOutNvmSingleNode->nvmNode;
            }

            if(targetNvmSingleNode || fanOutArray == nullptr) {
              bool error = false;
              while(true) {
                if(fanOutArray == nullptr ||
                   (fanOutNvmSingleNode->GetBornSq() > t.thisTime ||
                    fanOutNvmSingleNode->GetDeadSq() < t.thisTime) ||
                   (skiplistEntrance->Compare(
                      *fanOutNvmSingleNode,
                      pmCache::skiplistWithFanOut::PmSkiplistDramEntrance::
                        CompareNodeForLevel1P{internalKey, t.thisTime}) < 0)) {
                  fanOutNvmSingleNode++;
                  if(fanOutArray == nullptr ||
                     fanOutNvmSingleNode == fanOutArray + fanOutNodesCnt) {
                    targetUpperNode = targetUpperNode->GetNext(0);
                    if(targetUpperNode == &skiplistEntrance->dummyTail) {
                      targetNvmSingleNode = nullptr;
                      break;
                    } else {
                      fanOutNodes = targetUpperNode->GetFanOutNodes();
                      if(fanOutNodes->lastSplitTime >= currentTime) {
                        error = true;
                        break;
                      }
                      fanOutArray = fanOutNodes->fanOutNodes.get();
                      fanOutNodesCnt = fanOutNodes->fanOutNodesCount;
                      fanOutNvmSingleNode = fanOutArray;
                    }
                  }
                } else {
                  targetNvmSingleNode = fanOutNvmSingleNode->nvmNode;
                  break;
                }
              }
              if(error) {
                findOk = false;
                continue;
              }
            }

            findOk = true;
            break;
          }
        }
      }

      //            auto t4 = std::chrono::high_resolution_clock::now();
      //      std::cout << "ft " +
      //                     std::to_string(
      //                       std::chrono::duration_cast<std::chrono::nanoseconds>(t4
      //                       -
      //                                                                            t3)
      //                         .count()) +
      //                     "\n"
      //                << std::flush;

      lastLevelNode = hintNodeForNextLevel;

      if(currentLevel <= NVM_MAX_LEVEL) {
        if(targetNvmSingleNode &&
           !leveldb::InnerGetState::MatchMayNotKnowFile(
             arg, currentLevel, nullptr, targetNvmSingleNode)) {

          getHitCntPtr[currentLevel]++;

          //                    std::stringstream ss;
          //          ss << userKey.ToString() << " N" << currentLevel << "\n";
          //          std::cout << ss.str() << std::flush;
          //          auto t5 = std::chrono::high_resolution_clock::now();
          //          std::cout << "nt " +
          //                         std::to_string(
          //                           std::chrono::duration_cast<std::chrono::nanoseconds>(
          //                             t5 - t4)
          //                             .count()) +
          //                         "\n"
          //                    << std::flush;

          return;
        } else {
        }
      } else if(currentLevel > NVM_MAX_LEVEL) {
        auto state = reinterpret_cast<InnerGetState *>(arg);

        size_t numFiles = files[currentLevel].size();
        if(numFiles == 0)
          continue;
        auto index =
          FindFile(versionSet_->icmp_, files[currentLevel], internalKey);

        if(index < (int)numFiles) {

          FileMetaData *f = files[currentLevel][index];
          if(ucmp->Compare(userKey, f->smallest.UserKey()) >= 0) {

            auto bufferForCurrentFile = f->bufferForEachFile.get();

            if(readNewestVersion && bufferForCurrentFile) {
              std::shared_lock<std::shared_mutex> sharedLock(
                bufferForCurrentFile->mutex, std::try_to_lock);
              if(sharedLock.owns_lock()) {
                std::string &toSaveValueString = *state->saver.value;

                uint16_t hotNess = 0;
                toSaveValueString =
                  bufferForCurrentFile->Search(userKey, hotNess);
                if(!toSaveValueString.empty()) {

                  state->stats->hotKeyFile = f;
                  state->stats->hotKeyFileLevel = currentLevel;
                  state->stats->hotNessOfKeyFromBuffer = hotNess + 1;

                  state->saver.state = K_FOUND;
                  state->s = Status::OK();
                  state->found = true;

                  getHitCntPtr[currentLevel]++;

                  return;
                } else {
                }
              }
            }

            if(!leveldb::InnerGetState::MatchMayNotKnowFile(arg, currentLevel,
                                                            f, nullptr)) {

              getHitCntPtr[currentLevel]++;

              if(readNewestVersion && state->saver.state == K_FOUND) {

                assert(currentLevel <= NVM_MAX_LEVEL ||
                       f->countMinSketch.get());
                if(f->countMinSketch->Add(userKey) &&
                   (f->bufferForEachFile->keyValueS.size() <
                      f->fileSize / 2000L ||
                    f->fileSize < 1024L * 36)) {

                  state->stats->hotKeyFile = f;
                  state->stats->hotKeyFileLevel = currentLevel;

                  if(!f->compacting.load(std::memory_order_acquire)) {
                    auto buffer = f->bufferForEachFile.get();
                    assert(buffer);
                    {
                      std::unique_lock<std::shared_mutex> uniqueLock(
                        buffer->mutex);
                      if(!f->compacting.load(std::memory_order_acquire)) {
                        buffer->keyValueS.emplace(internalKey.ToString(),
                                                  internalKey.size(),
                                                  *state->saver.value);
                        assert(f->bufferForEachFile->GetSize());
                      }
                    }
                  }
                }
              }

              return;
            } else {
            }
          }
        }
      }
    }
  }

  auto Version::Get(const ReadOptions &options, const LookupKey &k,
                    std::string *value, GetStats *stats, bool readNewestVersion,
                    std::atomic<uint64_t> *getHitCntPtr) -> Status {

    InnerGetState innerGetState(
      stats, &options, k.InternalKey(), versionSet_, K_NOT_FOUND,
      versionSet_->icmp_.user_comparator(), k.UserKey(), value);

    TryToGetFromEachOverlappingTable_(innerGetState.saver.userKey,
                                      innerGetState.ikey, &innerGetState,
                                      readNewestVersion, getHitCntPtr);

    return innerGetState.found ? innerGetState.s : Status::NotFound(Slice());
  }

  auto Version::UpdateStats(const GetStats &stats, bool shouldPromote) -> bool {

    FileMetaData *f = stats.seekFile;

    if(shouldPromote && fileToPromote_ == nullptr &&
       !versionSet_->LevelIsInfluencedByCompaction(stats.hotKeyFileLevel - 1) &&
       !versionSet_->LevelIsInfluencedByCompaction(stats.hotKeyFileLevel - 2)) {
      fileToPromote_ = stats.hotKeyFile;
      fileToPromoteLevel_ = stats.hotKeyFileLevel;
      return true;
    }
    if(f != nullptr && (--f->allowedSeeks) <= 0 && fileToCompact_ == nullptr &&
       !versionSet_->LevelIsInfluencedByCompaction(stats.seekFileLevel) &&
       !versionSet_->LevelIsInfluencedByCompaction(stats.seekFileLevel + 1)) {
      fileToCompact_ = f;
      fileToCompactLevel_ = stats.seekFileLevel;
      return true;
    }
    return false;
  }

  auto Version::RecordReadSample(Slice internalKey) -> bool {
    return false;

    ParsedInternalKey ikey;
    if(!ParseInternalKey(internalKey, &ikey)) {
      return false;
    }

    struct State {
      GetStats stats; // Holds first matching file
      int matches{};

      static auto Match(void *arg, int level, FileMetaData *f) -> bool {
        auto *state = reinterpret_cast<State *>(arg);
        state->matches++;
        if(state->matches == 1) {
          // Remember first match.
          state->stats.seekFile = f;
          state->stats.seekFileLevel = level;
        }
        // We can stop iterating once we have a second match.
        return state->matches < 2;
      }
    };

    State state{};
    state.matches = 0;
    ForEachOverlapping_(ikey.userKey, internalKey, &state, &State::Match);

    // Must have at least two matches since we want to merge across
    // files. But what if we have a single file that contains many
    // overwrites and deletions?  Should we have another mechanism for
    // finding such files?
    if(state.matches >= 2) {
      // 1MB cost is about 1 seek (see comment in Builder::Apply).
      return UpdateStats(state.stats, false);
    }
    return false;
  }

  void Version::Ref() { ++refs_; }

  void Version::Unref() {
    assert(this != &versionSet_->dummyVersions);
    assert(refs_ >= 1);
    --refs_;
    if(refs_ == 0) {
      delete this;
    }
  }

  auto Version::OverlapInLevel(int level, const Slice *smallestUserKey,
                               const Slice *largestUserKey) -> bool {
    return SomeFileOverlapsRange(versionSet_->icmp_, (level > 0), files[level],
                                 smallestUserKey, largestUserKey);
  }

  auto Version::PickLevelForMemTableOutput(const Slice &smallestUserKey,
                                           const Slice &largestUserKey) -> int {
    int level = 0;
    if(!OverlapInLevel(0, &smallestUserKey, &largestUserKey)) {
      // Push to next level if there is no overlap in next level,
      // and the #bytes overlapping in the level after that are limited.
      InternalKey start(smallestUserKey, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey limit(largestUserKey, 0, static_cast<ValueType>(0));
      std::vector<FileMetaData *> overlaps;
      while(level < config::kMaxMemCompactLevel) {
        if(OverlapInLevel(level + 1, &smallestUserKey, &largestUserKey)) {
          break;
        }
        if(level + 2 < config::kNumLevels) {
          // Check that file does not overlap too many grandparent bytes.
          GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
          const int64_t sum = TotalFileSize(overlaps);
          if(sum > MaxGrandParentOverlapBytes(versionSet_->options_)) {
            break;
          }
        }
        level++;
      }
    }
    return level;
  }

  // Store in "*inputs" all files in "level" that overlap [begin,end]
  void Version::GetOverlappingInputs(int level, const InternalKey *begin,
                                     const InternalKey *end,
                                     std::vector<FileMetaData *> *inputs) {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    inputs->clear();
    Slice userBegin, userEnd;
    if(begin != nullptr)
      userBegin = begin->UserKey();
    if(end != nullptr)
      userEnd = end->UserKey();
    const Comparator *userCmp = versionSet_->icmp_.user_comparator();
    for(size_t i = 0; i < files[level].size();) {
      FileMetaData *f = files[level][i++];
      const Slice fileStart = f->smallest.UserKey();
      const Slice fileLimit = f->largest.UserKey();
      if(begin != nullptr && userCmp->Compare(fileLimit, userBegin) <
                               0) { // NOLINT(bugprone-branch-clone)
        // "f" is completely before specified range; skip it
      } else if(end != nullptr && userCmp->Compare(fileStart, userEnd) > 0) {
        // "f" is completely after specified range; skip it
      } else {
        inputs->push_back(f);
        if(level == 0) {
          // Level-0 files may overlap each other.  So check if the newly
          // added file has expanded the range.  If so, restart search.
          if(begin != nullptr && userCmp->Compare(fileStart, userBegin) < 0) {
            userBegin = fileStart;
            inputs->clear();
            i = 0;
          } else if(end != nullptr &&
                    userCmp->Compare(fileLimit, userEnd) > 0) {
            userEnd = fileLimit;
            inputs->clear();
            i = 0;
          }
        }
      }
    }
  }

  auto Version::DebugString() const -> std::string {
    std::string r;
    for(int level = 0; level < config::kNumLevels; level++) {
      // E.g.,
      //   --- level 1 ---
      //   17:123['a' .. 'd']
      //   20:43['e' .. 'g']
      r.append("--- level ");
      AppendNumberTo(&r, level);
      r.append(" ---\n");
      const std::vector<FileMetaData *> &filesS = files[level];
      for(auto file : filesS) {
        r.push_back(' ');
        AppendNumberTo(&r, file->number);
        r.push_back(':');
        AppendNumberTo(&r, file->fileSize);
        r.append("[");
        r.append(file->smallest.DebugString());
        r.append(" .. ");
        r.append(file->largest.DebugString());
        r.append("]\n");
      }
    }
    return r;
  }

  // A helper class, so we can efficiently apply a whole sequence
  // of edits to a particular state without creating intermediate
  // Versions that contain full copies of the intermediate state.
  class VersionSet::Builder {
   private:
    // Helper to sort by v->files_[file_number].smallest
    struct BySmallestKey {
      const InternalKeyComparator *internalComparator;

      auto operator()(FileMetaData *f1, FileMetaData *f2) const -> bool {
        int r = internalComparator->Compare(f1->smallest, f2->smallest);
        if(r != 0) {
          return (r < 0);
        } else {
          // Break ties by file number
          return (f1->number < f2->number);
        }
      }
    };

    using FileSet = std::set<FileMetaData *, BySmallestKey>;
    struct LevelState {
      std::set<uint64_t> deletedFiles;
      FileSet *addedFiles{};
    };

    VersionSet *versionSet_;
    Version *base_;
    std::array<LevelState, config::kNumLevels> levels_;

   public:
    // Initialize a builder with the files from *base and other info from
    // *versionSet
    Builder(VersionSet *versionSet, Version *base)
        : versionSet_(versionSet), base_(base) {
      base_->Ref();
      BySmallestKey cmp{};
      cmp.internalComparator = &versionSet_->icmp_;
      for(auto &level : levels_) {
        level.addedFiles = new FileSet(cmp);
      }
    }

    ~Builder() {
      for(auto &level : levels_) {
        const FileSet *added = level.addedFiles;
        std::vector<FileMetaData *> toUnref;
        toUnref.reserve(added->size());
        for(auto it : *added)
          toUnref.push_back(it);
        delete added;
        for(auto f : toUnref) {
          f->refs--;
          if(f->refs <= 0)
            delete f;
        }
      }
      base_->Unref();
    }

    // Apply all the edits in *edit to the current state.
    void Apply(const VersionEdit *edit) {
      // Update compaction pointers
      for(const auto &compactPointer : edit->compact_pointers_)
        versionSet_->compactPointer_[compactPointer.first] =
          compactPointer.second.Encode().ToString();

      // Delete files
      for(const auto &deletedFileSetKvp : edit->deleted_files_) {
        const int level = deletedFileSetKvp.first;
        const uint64_t number = deletedFileSetKvp.second;
        levels_[level].deletedFiles.insert(number);
      }

      // Add new files
      for(auto &newFile : edit->new_files_) {
        const int level = newFile.first;
        auto *f = new FileMetaData(
          std::move(const_cast<FileMetaData &>(newFile.second)));
        f->refs = 1;

        // We arrange to automatically compact this file after
        // a certain number of seeks.  Let's assume:
        //   (1) One seek costs 10ms
        //   (2) Writing or reading 1MB costs 10ms (100MB/s)
        //   (3) A compaction of 1MB does 25MB of IO:
        //         1MB read from this level
        //         10-12MB read from next level (boundaries may be misaligned)
        //         10-12MB written to next level
        // This implies that 25 seeks cost the same as the compaction
        // of 1MB of data.  I.e., one seek costs approximately the
        // same as the compaction of 40KB of data.  We are a little
        // conservative and allow approximately one seek for every 16KB
        // of data before triggering a compaction.
        f->allowedSeeks = static_cast<int>((f->fileSize / 16384U));
        if(f->allowedSeeks < 100)
          f->allowedSeeks = 100;

        levels_[level].deletedFiles.erase(f->number);
        levels_[level].addedFiles->insert(f);
      }
    }

    // Save the current state in *v.
    void SaveTo(Version *v) {
      BySmallestKey cmp{};
      cmp.internalComparator = &versionSet_->icmp_;
      for(int level = 0; level < config::kNumLevels; level++) {
        // Merge the set of added files with the set of pre-existing files.
        // Drop any deleted files.  Store the result in *v.
        const std::vector<FileMetaData *> &baseFiles = base_->files[level];
        auto baseIter = baseFiles.begin();
        auto baseEnd = baseFiles.end();
        const FileSet *addedFiles = levels_[level].addedFiles;
        v->files[level].reserve(baseFiles.size() + addedFiles->size());
        for(const auto &addedFile : *addedFiles) {
          // Add all smaller files listed in base_
          for(auto bpos = std::upper_bound(baseIter, baseEnd, addedFile, cmp);
              baseIter != bpos; ++baseIter) {
            MaybeAddFile(v, level, *baseIter);
          }

          MaybeAddFile(v, level, addedFile);
        }

        // Add remaining base files
        for(; baseIter != baseEnd; ++baseIter) {
          MaybeAddFile(v, level, *baseIter);
        }

#ifndef NDEBUG
        // Make sure there is no overlap in levels > 0
        if(level > 0) {
          for(uint32_t i = 1; i < v->files[level].size(); i++) {
            const InternalKey &prevEnd = v->files[level][i - 1]->largest;
            const InternalKey &thisBegin = v->files[level][i]->smallest;
            if(versionSet_->icmp_.Compare(prevEnd, thisBegin) >= 0) {
              std::fprintf(
                stderr, "overlapping ranges in same level %s vs. %s\n",
                prevEnd.DebugString().c_str(), thisBegin.DebugString().c_str());
              std::abort();
            }
          }
        }
#endif
      }
      for(auto *f : v->files[0])
        v->level0FileSize_ += f->fileSize;
    }

    void MaybeAddFile(Version *v, int level, FileMetaData *f) {
      if(levels_[level].deletedFiles.count(f->number) > 0) {
        // File is deleted: do nothing
      } else {
        std::vector<FileMetaData *> *files = &v->files[level];
        if(level > 0 && !files->empty()) {
          // Must not overlap
          assert(versionSet_->icmp_.Compare(
                   (*files)[files->size() - 1]->largest, f->smallest) < 0);
        }
        f->refs++;
        files->push_back(f);
      }
    }
  };

  VersionSet::VersionSet(std::string dbname, const Options *options,
                         TableCache *tableCache,
                         const InternalKeyComparator *cmp)
      : tableCache(tableCache), env_(options->env), dbname_(std::move(dbname)),
        options_(options), icmp_(*cmp), nextFileNumber_(2),
        manifestFileNumber_(0), // Filled by Recover()
        lastSequenceAtomic_(0), logNumber_(0), prevLogNumber_(0),
        descriptorFile_(nullptr), descriptorLog_(nullptr), dummyVersions(this),
        current_(nullptr) {
    AppendVersion(new Version(this));
  }

  VersionSet::~VersionSet() {
    current_->Unref();
    assert(dummyVersions.next == &dummyVersions); // List must be empty
    delete descriptorLog_;
    delete descriptorFile_;
  }

  void VersionSet::AppendVersion(Version *v) {
    // Make "v" current
    assert(v->refs_ == 0);
    assert(v != current_);
    if(current_ != nullptr) {
      current_->Unref();
    }
    current_ = v;
#ifndef NDEBUG
    if(!v->t.thisTime) {
      std::cout << "";
    }
#endif
    v->Ref();

    v->prev = dummyVersions.prev;
    v->next = &dummyVersions;
    v->prev->next = v;
    v->next->prev = v;
  }

  auto VersionSet::LogAndApply(
    VersionEdit *edit, port::Mutex *mu,
    pmCache::GlobalTimestamp::TimestampToken *newVersionTime,
    pmCache::GlobalTimestamp *timePool, bool sync) -> Status {

    if(edit->has_log_number_) {
      assert(edit->log_number_ >= logNumber_);
      assert(edit->log_number_ < nextFileNumber_);
    } else
      edit->SetLogNumber(logNumber_);

    if(!edit->has_prev_log_number_)
      edit->SetPrevLogNumber(prevLogNumber_);

    edit->SetNextFile(nextFileNumber_);
    edit->SetLastSequence(LastSequence());

    auto *v = new Version(this);
    {
      Builder builder(this, current_);
      builder.Apply(edit);
      builder.SaveTo(v);
    }
    VersionSet::Finalize_(v);
    if(timePool != nullptr) {
      if(newVersionTime)
        v->t = *newVersionTime;
      v->globalTimestamp = timePool;
    } else {
      v->t = current_->t;
      v->globalTimestamp = current_->globalTimestamp;
      current_->t = pmCache::GlobalTimestamp::TimestampToken();
      current_->globalTimestamp = nullptr;
    }
    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string newManifestFile;
    Status s;
    if(descriptorLog_ == nullptr) {
      // No reason to unlock *mu here since we only hit this path in the
      // first call to LogAndApply (when opening the database).
      assert(descriptorFile_ == nullptr);
      newManifestFile = DescriptorFileName(dbname_, manifestFileNumber_);
      s = env_->NewWritableFile(newManifestFile, &descriptorFile_);
      if(s.ok()) {
        descriptorLog_ = new log::Writer(descriptorFile_);
        s = WriteSnapshot(descriptorLog_);
      }
    }

    // Unlock during expensive MANIFEST log write
    {
      //      mu->Unlock();

      // Write new record to MANIFEST log
      if(s.ok()) {
        std::string record;
        edit->EncodeTo(&record);
        s = descriptorLog_->AddRecord(Slice{record});
        if(s.ok() && sync)
          s = descriptorFile_->Sync();
        //        if(!s.ok())
        //          Log(options_->info_log, "MANIFEST write: %s\n",
        //          s.ToString().c_str());
      }

      // If we just created a new descriptor file, install it by writing a
      // new CURRENT file that points to it.
      if(s.ok() && !newManifestFile.empty())
        s = SetCurrentFile(env_, dbname_, manifestFileNumber_);

      //      mu->Lock();
    }

    // Install the new version
    if(s.ok()) {
      assert(v->globalTimestamp);
      AppendVersion(v);
      logNumber_ = edit->log_number_;
      prevLogNumber_ = edit->prev_log_number_;
    } else {
      delete v;
      if(!newManifestFile.empty()) {
        delete descriptorLog_;
        delete descriptorFile_;
        descriptorLog_ = nullptr;
        descriptorFile_ = nullptr;
        env_->RemoveFile(newManifestFile);
      }
    }

    return s;
  }

  auto VersionSet::Recover(bool *saveManifest,
                           pmCache::PmCachePoolRoot *root) -> Status {
    struct LogReporter : public log::Reader::Reporter {
      Status *status{};

      void Corruption(size_t bytes, const Status &s) override {
        if(this->status->ok())
          *this->status = s;
      }
    };

    // Read "CURRENT" file, which contains a pointer to the current manifest
    // file
    std::string current;
    Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
    if(!s.ok()) {
      return s;
    }
    if(current.empty() || current[current.size() - 1] != '\n') {
      return Status::Corruption(
        Slice{"CURRENT file does not end with newline"});
    }
    current.resize(current.size() - 1);

    std::string describeFileName = dbname_ + "/" + current;
    SequentialFile *file;
    s = env_->NewSequentialFile(describeFileName, &file);
    if(!s.ok()) {
      if(s.IsNotFound()) {
        return Status::Corruption(
          Slice{"CURRENT points to a non-existent file"}, Slice{s.ToString()});
      }
      return s;
    }

    bool haveLogNumber = false;
    bool havePrevLogNumber = false;
    bool haveNextFile = false;
    bool haveLastSequence = false;
    uint64_t nextFile = 0;
    uint64_t lastSequence = 0;
    uint64_t logNumber = 0;
    uint64_t prevLogNumber = 0;
    Builder builder(this, current_);
    int readRecords = 0;

    {
      LogReporter reporter;
      reporter.status = &s;

      log::Reader reader(file, &reporter, true /*checksum*/,
                         0 /*initial_offset*/);
      Slice record;
      std::string scratch;
      while(reader.ReadRecord(&record, &scratch) && s.ok()) {
        ++readRecords;
        VersionEdit edit;
        s = edit.DecodeFrom(record);
        if(s.ok()) {
          if(edit.has_comparator_ &&
             edit.comparator_ != icmp_.user_comparator()->Name()) {
            s = Status::InvalidArgument(
              Slice{edit.comparator_ + " does not match existing comparator "},
              Slice{icmp_.user_comparator()->Name()});
          }
        }

        if(s.ok()) {
          builder.Apply(&edit);
        }

        if(edit.has_log_number_) {
          logNumber = edit.log_number_;
          haveLogNumber = true;
        }

        if(edit.has_prev_log_number_) {
          prevLogNumber = edit.prev_log_number_;
          havePrevLogNumber = true;
        }

        if(edit.has_next_file_number_) {
          nextFile = edit.next_file_number_;
          haveNextFile = true;
        }

        if(edit.has_last_sequence_) {
          lastSequence = edit.last_sequence_;
          haveLastSequence = true;
        }
      }
    }
    delete file;
    file = nullptr;

    if(s.ok()) {
      if(!haveNextFile) {
        s = Status::Corruption(Slice{"no meta-nextfile entry in descriptor"});
      } else if(!haveLogNumber) {
        s = Status::Corruption(Slice{"no meta-lognumber entry in descriptor"});
      } else if(!haveLastSequence) {
        s = Status::Corruption(
          Slice{"no last-sequence-number entry in descriptor"});
      }

      if(!havePrevLogNumber) {
        prevLogNumber = 0;
      }

      MarkFileNumberUsed(prevLogNumber);
      MarkFileNumberUsed(logNumber);
    }

    if(s.ok()) {
      auto *v = new Version(this);
      builder.SaveTo(v);

      if(root) {
        pmCache::GlobalTimestamp::TimestampToken t{};
        v->globalTimestamp = pmSkiplistDramEntrance[0]->timestamp;
        v->globalTimestamp->ReferenceTimestamp(t);
        v->t = t;
        for(int i = 0; i < config::kNumLevels; ++i) {
          auto nvmSkiplistDramEntrance = pmSkiplistDramEntrance[i];
          auto fanOutOfDummyHead =
            nvmSkiplistDramEntrance->dummyHead.GetFanOutNodes();
          auto fanOutOfDummyTail =
            nvmSkiplistDramEntrance->dummyTail.GetFanOutNodes();
          auto dummyHead = fanOutOfDummyHead->fanOutNodes[0].nvmNode;
          auto dummyTail = fanOutOfDummyTail->fanOutNodes[0].nvmNode;
          auto currentNode = dummyHead->nextNode.GetVPtr();
          while(currentNode != dummyTail) {
            auto index = LookFor(v->files[i], currentNode->fileNum);
            assert(index >= 0);
            v->files[i][index]->whereIsFile = FileMetaData::K_NVM;
            v->files[i][index]->nvmSingleNode = currentNode;
            assert(currentNode->fileNum == v->files[i][index]->number);
            currentNode = currentNode->nextNode.GetVPtr();
          }
        }
      }
      for(auto &fileInOneLevel : v->files) {
        for(auto &fileInSsd : fileInOneLevel) {
          if(fileInSsd->whereIsFile == FileMetaData::K_SSD) {
            auto fileName = TableFileName(dbname_, fileInSsd->number);
            std::ifstream inFile(fileName.c_str(), std::ios::binary);
            inFile.seekg(0, std::ios::end);
            int fileSize = (int)inFile.tellg();
            fileInSsd->fileSize = fileSize;
            fileInSsd->bufferForEachFile =
              std::make_shared<pmCache::BufferForEachFile>();
            fileInSsd->countMinSketch.reset(new pmCache::CountMinSketch());
          }
        }
      }

      // Install recovered version
      VersionSet::Finalize_(v);

      AppendVersion(v);
      manifestFileNumber_ = nextFile;
      nextFileNumber_ = nextFile + 1;
      lastSequenceAtomic_.store(lastSequence, std::memory_order_seq_cst);
      logNumber_ = logNumber;
      prevLogNumber_ = prevLogNumber;

      // See if we can reuse the existing MANIFEST file.
      if(ReuseManifest(describeFileName, current)) {
        // No need to save new manifest
      } else
        *saveManifest = true;
    } else {
      std::string error = s.ToString();
      Log(options_->info_log,
          "Error recovering version set with %d records: %s", readRecords,
          error.c_str());
    }

    return s;
  }

  auto VersionSet::ReuseManifest(const std::string &describeName,
                                 const std::string &describeBase) -> bool {
    if(!options_->reuseLogs) {
      return false;
    }
    FileType manifestType;
    uint64_t manifestNumber;
    uint64_t manifestSize;
    if(!ParseFileName(describeBase, &manifestNumber, &manifestType) ||
       manifestType != kDescriptorFile ||
       !env_->GetFileSize(describeName, &manifestSize).ok() ||
       // Make new compacted MANIFEST if old one is too big
       manifestSize >= TargetFileSize(options_)) {
      return false;
    }

    assert(descriptorFile_ == nullptr);
    assert(descriptorLog_ == nullptr);
    Status r = env_->NewAppendableFile(describeName, &descriptorFile_);
    if(!r.ok()) {
      Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
      assert(descriptorFile_ == nullptr);
      return false;
    }

    Log(options_->info_log, "Reusing MANIFEST %s\n", describeName.c_str());
    descriptorLog_ = new log::Writer(descriptorFile_, manifestSize);
    manifestFileNumber_ = manifestNumber;
    return true;
  }

  void VersionSet::MarkFileNumberUsed(uint64_t number) {
    if(nextFileNumber_ <= number) {
      nextFileNumber_ = number + 1;
    }
  }

  static std::array<double, 16> maxBytesForLevel = {
    LSM_NVM_BASE_LEVEL_SIZE,
    LSM_NVM_BASE_LEVEL_SIZE,
    LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
      *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
      *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
      *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
        *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                    *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                    *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                    *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                      *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                    *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                      *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE,
    1.0 * LSM_NVM_BASE_LEVEL_SIZE *LSM_NVM_LEVEL_SIZE_MULTIPLE
            *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
              *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                  *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                    *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                      *LSM_NVM_LEVEL_SIZE_MULTIPLE *LSM_NVM_LEVEL_SIZE_MULTIPLE
                        *LSM_NVM_LEVEL_SIZE_MULTIPLE};

  [[maybe_unused]] static double baseSize = maxBytesForLevel[NVM_MAX_LEVEL];

  void VersionSet::Finalize_(Version *v) {
    // Precomputed best level for next compaction

    int bestLevel = -1;
    double bestNvmScore = -1;
    double bestSsdScore = -1;
    Version::StorageMedia whereIsFile = Version::StorageMedia::K_UNKNOWN;
    double lastLevelNvmSize =
      (double)TotalFileSizeConsiderNvm(v->files[NVM_MAX_LEVEL]).second;
    double nvmScore = -1;
    double ssdScore = -1;
    for(int level = 0; level < config::kNumLevels - 1; level++) {

      if(v->versionSet_->LevelIsInfluencedByCompaction(level) ||
         v->versionSet_->LevelIsInfluencedByCompaction(level + 1)) {
        continue;
      }

      if(level == 0) {
        nvmScore = (double)v->files[level].size() /
                   static_cast<double>(config::kL0_CompactionTrigger);
      } else {
        std::pair<uint64_t, uint64_t> fileSizeS =
          TotalFileSizeConsiderNvm(v->files[level]);
        uint64_t levelSsdBytes = fileSizeS.first;
        uint64_t levelNvmBytes = fileSizeS.second;

        if(level <= NVM_MAX_LEVEL) {
          auto suitableSize = lastLevelNvmSize;

          suitableSize = maxBytesForLevel[level];

          nvmScore = (double)levelNvmBytes / suitableSize;
          ssdScore = 0;
        }

        else if(level > NVM_MAX_LEVEL) {
          double suitableSize = maxBytesForLevel[level];

          nvmScore = 0;
          ssdScore = (double)levelSsdBytes / suitableSize;
        }
      }

      if(nvmScore == 0 && ssdScore == 0 && level > NVM_MAX_LEVEL)
        break;

      if(nvmScore > bestNvmScore) {
        bestNvmScore = nvmScore;
        if(bestNvmScore >= 1 && bestNvmScore > bestSsdScore) {
          bestLevel = level;
          whereIsFile = Version::StorageMedia::K_STORAGE_MEDIA_NVM;
        }
      }
      if(ssdScore > bestSsdScore) {
        bestSsdScore = ssdScore;
        if(bestSsdScore >= 1 && bestSsdScore > bestNvmScore) {
          bestLevel = level;
          whereIsFile = Version::StorageMedia::K_STORAGE_MEDIA_DISK;
        }
      }
    }

    v->compactionLevel = bestLevel;
    switch(whereIsFile) {
    case Version::StorageMedia::K_STORAGE_MEDIA_NVM:
      v->compactionScore = bestNvmScore;
      break;
    case Version::StorageMedia::K_STORAGE_MEDIA_DISK:
      v->compactionScore = bestSsdScore;
      break;
    default: v->compactionScore = std::max(bestNvmScore, bestSsdScore); break;
    }
  }

  auto VersionSet::WriteSnapshot(log::Writer *log) -> Status {
    // TODO: Break up into multiple records to reduce memory usage on recovery?

    // Save metadata
    VersionEdit edit;
    edit.SetComparatorName(Slice{icmp_.user_comparator()->Name()});

    // Save compaction pointers
    for(int level = 0; level < config::kNumLevels; level++) {
      if(!compactPointer_[level].empty()) {
        InternalKey key;
        key.DecodeFrom(Slice{compactPointer_[level]});
        edit.SetCompactPointer(level, std::move(key));
      }
    }

    // Save files
    for(int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData *> &files = current_->files[level];
      for(auto f : files)
        edit.AddFile(level, f->number, f->fileSize, f->smallest, f->largest);
    }

    std::string record;
    edit.EncodeTo(&record);
    return log->AddRecord(Slice{record});
  }

  auto VersionSet::NumLevelFiles(int level) const -> int {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return (int)current_->files[level].size();
  }

  [[maybe_unused]] auto
  VersionSet::LevelSummary(LevelSummaryStorage *scratch) const -> const char * {
    // Update code if kNumLevels changes
    //    static_assert(config::kNumLevels == 7, "");
    std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files[0].size()), int(current_->files[1].size()),
      int(current_->files[2].size()), int(current_->files[3].size()),
      int(current_->files[4].size()), int(current_->files[5].size()),
      int(current_->files[6].size()));
    return scratch->buffer;
  }

  auto VersionSet::ApproximateOffsetOf(Version *v,
                                       const InternalKey &ikey) -> uint64_t {
    uint64_t result = 0;
    for(int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData *> &files = v->files[level];
      for(auto file : files) {
        if(icmp_.Compare(file->largest, ikey) <= 0) {
          // Entire file is before "ikey", so just add the file size
          result += file->fileSize;
        } else if(icmp_.Compare(file->smallest, ikey) > 0) {
          // Entire file is after "ikey", so ignore
          if(level > 0) {
            // Files other than level 0 are sorted by meta->smallest, so
            // no further files in this level will contain data for
            // "ikey".
            break;
          }
        } else {
          // "ikey" falls in the range for this table.  Add the
          // approximate offset of "ikey" within the table.
          Table *tablePtr;
          Iterator *iter = tableCache->NewIterator(ReadOptions(), file->number,
                                                   file->fileSize, &tablePtr);
          if(tablePtr != nullptr)
            result += tablePtr->ApproximateOffsetOf(ikey.Encode());
          delete iter;
        }
      }
    }
    return result;
  }

  void VersionSet::AddLiveFiles(std::set<uint64_t> *live) {
    for(Version *v = dummyVersions.next; v != &dummyVersions; v = v->next) {
      for(int i = NVM_MAX_LEVEL + 1; i < config::kNumLevels; i++) {
        auto &files = v->files[i];
        for(auto &file : files)
          live->insert(file->number);
      }
      //      for(auto &files : v->files)
      //        for(auto &file : files)
      //          live->insert(file->number);
    }
  }

  auto VersionSet::NumLevelBytes(int level) const -> int64_t {
    assert(level >= 0);
    assert(level < config::kNumLevels);
    return TotalFileSize(current_->files[level]);
  }

  auto VersionSet::MaxNextLevelOverlappingBytes() -> int64_t {
    int64_t result = 0;
    std::vector<FileMetaData *> overlaps;
    for(int level = 1; level < config::kNumLevels - 1; level++) {
      for(size_t i = 0; i < current_->files[level].size(); i++) {
        const FileMetaData *f = current_->files[level][i];
        current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                       &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if(sum > result) {
          result = sum;
        }
      }
    }
    return result;
  }

  // Stores the minimal range that covers all entries in inputs in
  // *smallest, *largest.
  // REQUIRES: inputs is not empty
  void VersionSet::GetRange(const std::vector<FileMetaData *> &inputs,
                            InternalKey *smallest, InternalKey *largest) {
    assert(!inputs.empty());
    smallest->Clear();
    largest->Clear();
    for(size_t i = 0; i < inputs.size(); i++) {
      FileMetaData *f = inputs[i];
      if(i == 0) {
        *smallest = f->smallest;
        *largest = f->largest;
      } else {
        if(icmp_.Compare(f->smallest, *smallest) < 0) {
          *smallest = f->smallest;
        }
        if(icmp_.Compare(f->largest, *largest) > 0) {
          *largest = f->largest;
        }
      }
    }
  }

  // Stores the minimal range that covers all entries in inputs1 and inputs2
  // in *smallest, *largest.
  // REQUIRES: inputs is not empty
  void VersionSet::GetRange2(const std::vector<FileMetaData *> &inputs1,
                             const std::vector<FileMetaData *> &inputs2,
                             InternalKey *smallest, InternalKey *largest) {
    std::vector<FileMetaData *> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    GetRange(all, smallest, largest);
  }

  auto VersionSet::MakeInputIterator(Compaction *c) -> Iterator * {
    ReadOptions options;
    options.verify_checksums = options_->paranoid_checks;
    options.fill_cache = false;

    const int space = c->level() == 0 ? (int)c->inputs[0].size() + 2 : 4;
    std::vector<Iterator *> list(space);
    int num = 0;
    std::vector<bool> isInNvm;
    for(int which = 0; which < 2; which++)
      if(!c->inputs[which].empty()) {

        if(c->level() + which == 0) {
          const std::vector<FileMetaData *> &files = c->inputs[which];
          for(auto file : files)
            if(GSL_UNLIKELY(
                 (file->whereIsFile == FileMetaData::WhereIsFile::K_SSD))) {
              list[num++] =
                tableCache->NewIterator(options, file->number, file->fileSize);
              isInNvm.push_back(false);
            } else {
              assert(file->whereIsFile == FileMetaData::WhereIsFile::K_NVM);
              list[num++] = file->nvmSingleNode->NewIterator();
              isInNvm.push_back(true);
            }
        } else {
          // Create concatenating iterator for the files from this level
          std::vector<FileMetaData *> *inputFromSsd =
            &c->tmpInputs_[which * 2 + 0];
          std::vector<FileMetaData *> *inputFromNvm =
            &c->tmpInputs_[which * 2 + 1];
          for(auto file : c->inputs[which])
            (file->whereIsFile == FileMetaData::K_NVM ? inputFromNvm
                                                      : inputFromSsd)
              ->push_back(file);
          if(!inputFromSsd->empty()) {
            list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, inputFromSsd),
              &GetFileIterator, tableCache, options);
            isInNvm.push_back(false);
          }
          if(!inputFromNvm->empty()) {
            list[num++] = NewTwoLevelIterator(
              new Version::LevelFileNumIterator(icmp_, inputFromNvm),
              &GetPmSsTableIterator, inputFromNvm, options);
            isInNvm.push_back(true);
          }
        }
      }
    assert(num <= space);
    assert(num == isInNvm.size());
    Iterator *result = NewMergingIterator(&icmp_, list.data(), num, isInNvm);
    return result;
  }

  auto
  VersionSet::PickCompactionOrPromotion(bool enoughRoomForNvm) -> Compaction * {

    Compaction *c;
    int level;

    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.
    const bool sizeCompaction = ((double)current_->compactionScore >= 1.0);

    bool seekCompaction = (current_->fileToCompact_ != nullptr);
    if(seekCompaction) {
      seekCompaction &=
        (!LevelIsInfluencedByCompaction(current_->fileToCompactLevel_) &&
         !LevelIsInfluencedByCompaction(current_->fileToCompactLevel_ + 1));
      if(!seekCompaction)
        current_->fileToCompact_ = nullptr;
    }

    bool promotion = (current_->fileToPromote_ != nullptr);
    if(promotion) {
      promotion &=
        !LevelIsInfluencedByCompaction(current_->fileToPromoteLevel_ - 1) &&
        !LevelIsInfluencedByCompaction(current_->fileToPromoteLevel_ - 2);
      if(!promotion)
        current_->fileToPromote_ = nullptr;
    }

    if(sizeCompaction) {

      level = current_->compactionLevel;

      assert(level >= 0);
      assert(level + 1 < config::kNumLevels);

      c = new Compaction(options_, level);

      auto &filesInLevelToCompact = current_->files[level];
      Slice compactPointer{compactPointer_[level]};

      for(auto *f : filesInLevelToCompact) {

        if(compactPointer_[level].empty() ||
           icmp_.Compare(f->largest.Encode(), compactPointer) > 0) {

          c->inputs[0].push_back(f);
          break;
        }
      }

      if(c->inputs[0].empty()) {
        for(auto *f : filesInLevelToCompact) {
          c->inputs[0].push_back(f);
          break;
        }
      }

    } else if(promotion) {

      level = current_->fileToPromoteLevel_ - 2;
      assert(level >= 0);
      auto lowerLevel = level + 1;
      assert(lowerLevel >= NVM_MAX_LEVEL);
      auto fileToPromoteSmallestInternalKey =
        current_->fileToPromote_->smallest.Encode();
      auto fileToPromoteLargestInternalKey =
        current_->fileToPromote_->largest.Encode();
      c = new Compaction(options_, level);

      auto &lowerLevelFiles = current_->files[lowerLevel];
      auto from = std::lower_bound(
        lowerLevelFiles.begin(), lowerLevelFiles.end(),
        fileToPromoteSmallestInternalKey,
        [this](FileMetaData *a, const Slice &internalKey) {
          return icmp_.Compare(a->smallest.Encode(), internalKey) < 0;
        });
      if(from != lowerLevelFiles.begin())
        from--;
      auto to = std::lower_bound(
        from, lowerLevelFiles.end(), fileToPromoteLargestInternalKey,
        [this](FileMetaData *a, const Slice &internalKey) {
          return icmp_.Compare(a->largest.Encode(), internalKey) < 0;
        });

      auto it = from;
      for(; it != to; it++)
        c->inputs[1].push_back(*it);
      if(it == to && to != lowerLevelFiles.end())
        c->inputs[1].push_back(*it);

    } else if(seekCompaction) {

      level = current_->fileToCompactLevel_;
      c = new Compaction(options_, level);
      c->inputs[0].push_back(current_->fileToCompact_);
    } else
      return nullptr;

    c->inputVersion = current_;
    c->inputVersion->Ref();

    if(c->inputs[0].empty()) {
      if(!c->inputs[1].empty()) {
        SetupOtherInputs(c);

        if(c->grandparents.empty()) {
          c->grandparents.push_back(current_->fileToPromote_);
        } else {
          auto fileToPromote = current_->fileToPromote_;
          auto fileToPromoteMax = fileToPromote->largest.Encode();
          auto fileToPromoteMin = fileToPromote->smallest.Encode();
          if(BytewiseComparator()->Compare(
               {fileToPromoteMax.data(), fileToPromoteMax.size() - 8},
               c->grandparents[0]->smallest.UserKey()) < 0) {

            std::vector<FileMetaData *> tmp;
            auto levelToPromoteFiles =
              current_->files[current_->fileToPromoteLevel_];
            auto from = std::lower_bound(
              levelToPromoteFiles.begin(), levelToPromoteFiles.end(),
              fileToPromote->smallest.Encode(),
              [this](FileMetaData *a, const Slice &internalKey) {
                return icmp_.Compare(a->smallest.Encode(), internalKey) < 0;
              });
            auto to = std::lower_bound(
              from, levelToPromoteFiles.end(),
              (*c->grandparents.begin())->smallest.Encode(),
              [this](FileMetaData *a, const Slice &internalKey) {
                return icmp_.Compare(a->smallest.Encode(), internalKey) < 0;
              });
            while(from != to)
              tmp.push_back(*from++);
            for(auto f : c->grandparents)
              tmp.push_back(f);
            c->grandparents = std::move(tmp);
          } else if(BytewiseComparator()->Compare(
                      {fileToPromoteMin.data(), fileToPromoteMin.size() - 8},
                      c->grandparents.back()->largest.UserKey()) > 0) {
            auto levelToPromoteFiles =
              current_->files[current_->fileToPromoteLevel_];
            auto from = std::lower_bound(
              levelToPromoteFiles.begin(), levelToPromoteFiles.end(),
              (*c->grandparents.rbegin())->smallest.Encode(),
              [this](FileMetaData *a, const Slice &internalKey) {
                return icmp_.Compare(a->smallest.Encode(), internalKey) < 0;
              });
            from++;
            auto to = std::lower_bound(
              from, levelToPromoteFiles.end(), fileToPromote->smallest.Encode(),
              [this](FileMetaData *a, const Slice &internalKey) {
                return icmp_.Compare(a->smallest.Encode(), internalKey) < 0;
              });
            while(from != to)
              c->grandparents.push_back(*from++);
            c->grandparents.push_back(fileToPromote);
          }
        }
      } else {
        current_->fileToPromote_ = nullptr;
        current_->fileToPromoteLevel_ = -1;
        delete c;
        return nullptr;
      }
    } else {

      // Files in level 0 may overlap each other, so pick up all overlapping ones
      if(level == 0) {
        InternalKey smallest, largest;
        GetRange(c->inputs[0], &smallest, &largest);
        // Note that the next call will discard the file we placed in
        // c->inputs_[0] earlier and replace it with an overlapping set
        // which will include the picked file.
        current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs[0]);
        assert(!c->inputs[0].empty());
      }

      SetupOtherInputs(c);
    }

    return c;
  }

  auto VersionSet::PickCompactionByLru(FileMetaData *fileToCompact,
                                       int level) -> Compaction * {
    Compaction *c;

    c = new Compaction(options_, level);
    c->inputs[0].push_back(fileToCompact);

    c->inputVersion = current_;
    c->inputVersion->Ref();

    // Files in level 0 may overlap each other, so pick up all overlapping ones
    if(level == 0) {
      InternalKey smallest, largest;
      GetRange(c->inputs[0], &smallest, &largest);
      // Note that the next call will discard the file we placed in
      // c->inputs_[0] earlier and replace it with an overlapping set
      // which will include the picked file.
      current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs[0]);
      assert(!c->inputs[0].empty());
    }

    SetupOtherInputs(c);

    return c;
  }

  // Finds the largest key in a vector of files. Returns true if files is not
  // empty.
  auto FindLargestKey(const InternalKeyComparator &icmp,
                      const std::vector<FileMetaData *> &files,
                      InternalKey *largestKey) -> bool {
    if(files.empty()) {
      return false;
    }
    *largestKey = files[0]->largest;
    for(size_t i = 1; i < files.size(); ++i) {
      FileMetaData *f = files[i];
      if(icmp.Compare(f->largest, *largestKey) > 0) {
        *largestKey = f->largest;
      }
    }
    return true;
  }

  // Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
  // user_key(l2) = user_key(u1)
  auto
  FindSmallestBoundaryFile(const InternalKeyComparator &icmp,
                           const std::vector<FileMetaData *> &levelFiles,
                           const InternalKey &largestKey) -> FileMetaData * {
    const Comparator *userCmp = icmp.user_comparator();
    FileMetaData *smallestBoundaryFile = nullptr;
    for(auto f : levelFiles) {
      if(icmp.Compare(f->smallest, largestKey) > 0 &&
         userCmp->Compare(f->smallest.UserKey(), largestKey.UserKey()) == 0) {
        if(smallestBoundaryFile == nullptr ||
           icmp.Compare(f->smallest, smallestBoundaryFile->smallest) < 0) {
          smallestBoundaryFile = f;
        }
      }
    }
    return smallestBoundaryFile;
  }

  // Extracts the largest file b1 from |compaction_files| and then searches
  // for a b2 in |level_files| for which user_key(u1) = user_key(l2). If it
  // finds such a file b2 (known as a boundary file) it adds it to
  // |compaction_files| and then searches again using this new upper bound.
  //
  // If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
  // user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
  // subsequent get operation will yield an incorrect result because it will
  // return the record from b2 in level i rather than from b1 because it
  // searches level by level for records matching the supplied user key.
  //
  // parameters:
  //   in     level_files:      List of files to search for boundary files.
  //   in/out compaction_files: List of files to extend by adding boundary
  //   files.
  void AddBoundaryInputs(const InternalKeyComparator &icmp,
                         const std::vector<FileMetaData *> &levelFiles,
                         std::vector<FileMetaData *> *compactionFiles) {
    InternalKey largestKey;

    // Quick return if compaction_files is empty.
    if(!FindLargestKey(icmp, *compactionFiles, &largestKey)) {
      return;
    }

    bool continueSearching = true;
    while(continueSearching) {
      FileMetaData *smallestBoundaryFile =
        FindSmallestBoundaryFile(icmp, levelFiles, largestKey);

      // If a boundary file was found advance largest_key, otherwise we're
      // done.
      if(smallestBoundaryFile != nullptr) {
        compactionFiles->push_back(smallestBoundaryFile);
        largestKey = smallestBoundaryFile->largest;
      } else {
        continueSearching = false;
      }
    }
  }

  void VersionSet::SetupOtherInputs(Compaction *c) {
    const int level = c->level();
    InternalKey smallest, largest;
    bool isPromotion = c->inputs[0].empty();

    if(!isPromotion) {

      AddBoundaryInputs(icmp_, current_->files[level], &c->inputs[0]);

      GetRange(c->inputs[0], &smallest, &largest);

      current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                     &c->inputs[1]);
    }

    AddBoundaryInputs(icmp_, current_->files[level + 1], &c->inputs[1]);

    // Get entire range covered by compaction

    InternalKey allStart, allLimit;
    GetRange2(c->inputs[0], c->inputs[1], &allStart, &allLimit);

    // See if we can grow the number of inputs in "level" without
    // changing the number of "level+1" files we pick up.
    if(!isPromotion && !c->inputs[1].empty()) {
      std::vector<FileMetaData *> expanded0;
      current_->GetOverlappingInputs(level, &allStart, &allLimit, &expanded0);
      AddBoundaryInputs(icmp_, current_->files[level], &expanded0);
      //      const int64_t inputs0Size = TotalFileSize(c->inputs[0]);
      const int64_t inputs1Size = TotalFileSize(c->inputs[1]);
      const int64_t expanded0Size = TotalFileSize(expanded0);
      if(expanded0.size() > c->inputs[0].size() &&
         inputs1Size + expanded0Size <
           ExpandedCompactionByteSizeLimit(options_)) {
        InternalKey newStart, newLimit;
        GetRange(expanded0, &newStart, &newLimit);
        std::vector<FileMetaData *> expanded1;
        current_->GetOverlappingInputs(level + 1, &newStart, &newLimit,
                                       &expanded1);
        AddBoundaryInputs(icmp_, current_->files[level + 1], &expanded1);
      
        if(expanded1.size() == c->inputs[1].size()) {
          smallest = newStart;
          largest = newLimit;
          c->inputs[0] = expanded0;
          c->inputs[1] = expanded1;
          GetRange2(c->inputs[0], c->inputs[1], &allStart, &allLimit);
        }
      }
    }

    // Compute the set of grandparent files that overlap this compaction
    // (parent == level+1; grandparent == level+2)
    //
    if(level + 2 < config::kNumLevels)
      current_->GetOverlappingInputs(level + 2, &allStart, &allLimit,
                                     &c->grandparents);

    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    //

    if(isPromotion) {
      if(!c->grandparents.empty() &&
         (icmp_.Compare((*c->grandparents.rbegin())->largest, allLimit) > 0))
        allLimit = (*c->grandparents.rbegin())->largest;
      compactPointer_[level + 1] = allLimit.Encode().ToString();
      c->edit_.SetCompactPointer(level + 1, std::move(allLimit));
    } else {
      compactPointer_[level] = largest.Encode().ToString();
      c->edit_.SetCompactPointer(level, std::move(largest));
    }
  }

  auto VersionSet::CompactRange(int level, const InternalKey *begin,
                                const InternalKey *end) -> Compaction * {
    std::vector<FileMetaData *> inputs;
    current_->GetOverlappingInputs(level, begin, end, &inputs);
    if(inputs.empty()) {
      return nullptr;
    }

    // Avoid compacting too much in one shot in case the range is large.
    // But we cannot do this for level-0 since level-0 files can overlap
    // and we must not pick one file and drop another older file if the
    // two files overlap.
    if(level > 0) {
      const uint64_t limit = MaxFileSizeForLevel(options_, level);
      uint64_t total = 0;
      for(size_t i = 0; i < inputs.size(); i++) {
        uint64_t s = inputs[i]->fileSize;
        total += s;
        if(total >= limit) {
          inputs.resize(i + 1);
          break;
        }
      }
    }

    auto *c = new Compaction(options_, level);
    c->inputVersion = current_;
    c->inputVersion->Ref();
    c->inputs[0] = inputs;
    SetupOtherInputs(c);
    return c;
  }

  Compaction::Compaction(const Options *options, int level)
      : inputVersion(nullptr), level_(level),
        maxOutputFileSize_(MaxFileSizeForLevel(options, level)),
        grandparentIndex_(0), seenKey_(false), overlappedBytes_(0) {
    //    for(unsigned long &levelPtr : levelPtrs_)
    //      levelPtr = 0;
  }

  Compaction::~Compaction() {
    if(inputVersion != nullptr) {
      inputVersion->Unref();
    }
  }

  auto Compaction::IsTrivialMove() const -> bool {
    const VersionSet *versionSet = inputVersion->versionSet_;
    // Avoid a move if there is lots of overlapping grandparent data.
    // Otherwise, the move could create a parent file that will require
    // a very expensive merge later on.

    return level() != NVM_MAX_LEVEL && NumInputFiles(0) == 1 &&
           NumInputFiles(1) == 0 &&
           TotalFileSize(grandparents) <=
             MaxGrandParentOverlapBytes(versionSet->options_);
  }

  void Compaction::AddInputDeletions(VersionEdit *edit) {
    for(int which = 0; which < 2; which++)
      for(size_t i = 0; i < inputs[which].size(); i++)
        edit->RemoveFile(level_ + which, inputs[which][i]->number);
  }

  auto Compaction::IsBaseLevelForKey(const Slice &userKey) -> bool {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator *userCmp =
      inputVersion->versionSet_->icmp_.user_comparator();
    for(int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
      const std::vector<FileMetaData *> &files = inputVersion->files[lvl];
      while(levelPtrs_[lvl] < files.size()) {
        FileMetaData *f = files[levelPtrs_[lvl]];
        if(userCmp->Compare(userKey, f->largest.UserKey()) <= 0) {
          // We've advanced far enough
          if(userCmp->Compare(userKey, f->smallest.UserKey()) >= 0) {
            // Key falls in this file's range, so definitely not base
            // level
            return false;
          }
          break;
        }
        levelPtrs_[lvl]++;
      }
    }
    return true;
  }

  auto Compaction::ShouldStopBefore(const Slice &internalKey) -> bool {
    const VersionSet *versionSet = inputVersion->versionSet_;
    // Scan to find the earliest grandparent file that contains key.
    //
    const InternalKeyComparator *icmp = &versionSet->icmp_;
    while(grandparentIndex_ < grandparents.size() &&
          icmp->Compare(internalKey,
                        grandparents[grandparentIndex_]->largest.Encode()) >
            0) {
      if(seenKey_)
        overlappedBytes_ += (int64_t)grandparents[grandparentIndex_]->fileSize;
      grandparentIndex_++;
    }
    seenKey_ = true;

    if(overlappedBytes_ > MaxGrandParentOverlapBytes(versionSet->options_)) {
      // Too much overlap for current output; start new output
      overlappedBytes_ = 0;
      return true;
    } else
      return false;
  }

  void Compaction::ReleaseInputs() {
    if(inputVersion != nullptr) {
      inputVersion->Unref();
      inputVersion = nullptr;
    }
  }

  auto VersionSet::NeedsCompactionOrPromotion(bool enoughNvm) -> bool {
    Version *v = current_;
    std::vector<bool> levelInfluencedByCompaction(config::kNumLevels, false);
    if(!enoughNvm) {
      levelInfluencedByCompaction.resize(NVM_MAX_LEVEL + 1);
      for(int level = 0; level <= NVM_MAX_LEVEL; level++) {
        levelInfluencedByCompaction[level] =
          LevelIsInfluencedByCompaction(level);
        if(!levelInfluencedByCompaction[level])
          MarkLevelInfluencedByCompaction(level);
      }
    }

    VersionSet::Finalize_(current_);

    if(!enoughNvm)
      for(int level = 0; level <= NVM_MAX_LEVEL; level++)
        if(!levelInfluencedByCompaction[level])
          UnMarkLevelInfluencedByCompaction(level);

    auto ret = (v->compactionScore >= 1) || (v->fileToCompact_ != nullptr) ||
               (v->fileToPromote_ != nullptr);
    if(!ret)
      return ret;

    return ret &= (!LevelIsInfluencedByCompaction(v->compactionLevel) ||
                   !LevelIsInfluencedByCompaction(v->fileToCompactLevel_) ||
                   !LevelIsInfluencedByCompaction(v->fileToPromoteLevel_));
  }

} // namespace leveldb
