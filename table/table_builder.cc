// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

  struct TableBuilder::Rep {
    Rep(const Options &opt, WritableFile *f)
        : options(opt), index_block_options(opt), file(f), offset(0),
          data_block(&options), index_block(&index_block_options),
          num_entries(0), closed(false),
          filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
          pending_index_entry(false) {
      index_block_options.block_restart_interval = 1;
    }

    Options options;
    Options index_block_options;
    WritableFile *file;
    uint64_t offset;
    Status status;
    BlockBuilder data_block;
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    bool closed; // Either Finish() or Abandon() has been called.
    FilterBlockBuilder *filter_block;

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    //
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    bool pending_index_entry;
    BlockHandle pending_handle; // Handle to add to index block

    std::string compressed_output;
  };

  TableBuilder::TableBuilder(const Options &options, WritableFile *file)
      : rep_(new Rep(options, file)) {
    if(rep_->filter_block != nullptr) {
      rep_->filter_block->StartBlock(0);
    }
  }

  TableBuilder::~TableBuilder() {
    assert(rep_->closed); // Catch errors where caller forgot to call Finish()
    delete rep_->filter_block;
    delete rep_;
  }

  Status TableBuilder::ChangeOptions(const Options &options) {
    // Note: if more fields are added to Options, update
    // this function to catch changes that should not be allowed to
    // change in the middle of building a Table.
    if(options.comparator != rep_->options.comparator) {
      return Status::InvalidArgument(
        Slice{"changing comparator while building table"});
    }

    // Note that any live BlockBuilders point to rep_->options and therefore
    // will automatically pick up the updated options.
    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval = 1;
    return Status::OK();
  }

  void TableBuilder::Add(const Slice &key, const Slice &value) {
    Rep *r = rep_;
    assert(!r->closed);
    if(!ok())
      return;


    if(r->pending_index_entry) {

      assert(r->data_block.empty());
      r->options.comparator->FindShortestSeparator(
        &r->last_key,
        key); 
        
      std::string handleEncoding;
      r->pending_handle.EncodeTo(&handleEncoding);
      r->index_block.Add(
        Slice{r->last_key},
        Slice(handleEncoding)); 
      r->pending_index_entry =
        false; 
    }

    if(r->filter_block != nullptr)
      r->filter_block->AddKey(key);
    assert(r->last_key.empty() ||
           BytewiseComparator()->Compare(
             {r->last_key.data(), r->last_key.size() - 8},
             {key.data(), key.size() - 8}) <= 0);
    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    const size_t estimatedBlockSize = r->data_block.CurrentSizeEstimate();

    if(estimatedBlockSize >= r->options.block_size)
      Flush();
  }

  void TableBuilder::Flush() {
    Rep *r = rep_;
    assert(!r->closed);
    if(!ok())
      return;
    if(r->data_block.empty())
      return;
    assert(!r->pending_index_entry);
    WriteBlock(&r->data_block, &r->pending_handle);
    if(ok()) {
      r->pending_index_entry = true;
      r->status = r->file->Flush();
    }
    if(r->filter_block != nullptr)
      r->filter_block->StartBlock(r->offset);
  }

  void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    assert(ok());
    Rep *r = rep_;
    Slice raw = block->Finish();

    Slice blockContents;
    CompressionType type = r->options.compression;
    // TODO(postrelease): Support more compression options: zlib?
    switch(type) {
    case kNoCompression: blockContents = raw; break;

    case kSnappyCompression: {
      std::string *compressed = &r->compressed_output;
      if(port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
         compressed->size() < raw.size() - (raw.size() / 8u))
        blockContents = Slice{*compressed};
      else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        blockContents = raw;
        type = kNoCompression;
      }
      break;
    }
    }
    WriteRawBlock(blockContents, type, handle);
    r->compressed_output.clear();
    block->Reset();
  }

  void TableBuilder::WriteRawBlock(const Slice &blockContents,
                                   CompressionType type, BlockHandle *handle) {
    Rep *r = rep_;
    handle->SetOffset(r->offset);
    handle->SetSize(blockContents.size());
    r->status = r->file->Append(blockContents);
    if(r->status.ok()) {
      char trailer[kBlockTrailerSize];
      trailer[0] = type;
      uint32_t crc = crc32c::Value(blockContents.data(), blockContents.size());
      crc = crc32c::Extend(crc, trailer, 1); // Extend crc to cover block type
      EncodeFixed32(trailer + 1, crc32c::Mask(crc));
      r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
      if(r->status.ok())
        r->offset += blockContents.size() + kBlockTrailerSize;
    }
  }

  Status TableBuilder::status() const { return rep_->status; }

  Status TableBuilder::Finish() {
    Rep *r = rep_;
    Flush();
    assert(!r->closed);
    r->closed = true;

    BlockHandle filterBlockHandle, metaindexBlockHandle, indexBlockHandle;

    // Write filter block
    if(ok() && r->filter_block != nullptr)
      WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                    &filterBlockHandle);

    // Write meta-index block
    if(ok()) {
      BlockBuilder metaIndexBlock(&r->options);
      if(r->filter_block != nullptr) {
        // Add mapping from "filter.Name" to location of filter data
        std::string key = "filter.";
        key.append(r->options.filter_policy->Name());
        std::string handleEncoding;
        filterBlockHandle.EncodeTo(&handleEncoding);
        metaIndexBlock.Add(Slice{key}, Slice{handleEncoding});
      }

      // TODO(postrelease): Add stats and other meta blocks
      WriteBlock(&metaIndexBlock, &metaindexBlockHandle);
    }

    // Write index block
    if(ok()) {
      if(r->pending_index_entry) {
        r->options.comparator->FindShortSuccessor(&r->last_key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(Slice{r->last_key}, Slice(handle_encoding));
        r->pending_index_entry = false;
      }
      WriteBlock(&r->index_block, &indexBlockHandle);
    }

    // Write footer
    if(ok()) {
      Footer footer;
      footer.set_metaindex_handle(metaindexBlockHandle);
      footer.set_index_handle(indexBlockHandle);
      std::string footerEncoding;
      footer.EncodeTo(&footerEncoding);
      r->status = r->file->Append(Slice{footerEncoding});
      if(r->status.ok()) {
        r->offset += footerEncoding.size();
      }
    }
    return r->status;
  }

  void TableBuilder::Abandon() {
    Rep *r = rep_;
    assert(!r->closed);
    r->closed = true;
  }

  uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

  uint64_t TableBuilder::FileSize() const { return rep_->offset; }

} // namespace leveldb
