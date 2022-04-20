//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>

#include <stdint.h>
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/block_based/data_block_hash_index.h"
#include "rocksdb/leco.h"


namespace ROCKSDB_NAMESPACE {

class BlockBuilder_leco {
 public:
  BlockBuilder_leco(const BlockBuilder_leco&) = delete;
  void operator=(const BlockBuilder_leco&) = delete;


  explicit BlockBuilder_leco(bool padding_enabled = false,
                              int block_size = 100,
                              int totalNum = 100,
                              int key_num_per_block = 1);

  // Reset the contents as if the BlockBuilder_leco was just constructed.
  void Reset();

  // Swap the contents in BlockBuilder_leco with buffer, then reset the BlockBuilder_leco.
  void SwapAndReset(std::string& buffer);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
  void Add(const Slice& key,  Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  uint32_t EncodeValue(std::string& buffer,std::vector<uint64_t> & data);
  template <typename T>
  void EncodingOneDataSegment(std::vector<std::string>& data_vec_tmp, int start_ind, int block_length, int max_padding_length, char padding_char, std::vector<std::string>& descriptor_of_each_block, std::vector<uint32_t>& offset_of_each_block, uint32_t& start_byte, std::string* common_prefix, int common_prefix_length, uint8_t encoding_type, int interval);

  template <typename S>
  uint32_t EncodeVal(std::string& buffer, std::vector<S>& data_vec, bool with_first_key);
  
  Slice Finish();

  // Return true if no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

  void get_uncompressed_size(uint64_t & with_pad, uint64_t & without_pad){
      with_pad = totalsize_with_padding;
      without_pad = totalsize_without_padding;
  }

 private:
  inline void AddWithLastKeyImpl(const Slice& key,
                                   Slice& value);



  std::string buffer_;              // Destination buffer
  std::vector<std::string> key_vec;
  std::vector<uint64_t> offset;
  std::vector<uint64_t> size;
  bool padding_enable;
  int block_size_;
  int N;
  int key_num_per_block_;
  // Leco_string do the encoding & writing bitwise work
  uint8_t max_padding_length;
  uint64_t totalsize_without_padding;
  uint64_t totalsize_with_padding;
  bool finished_;  // Has Finish() been called?
  
  
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif
};

}  // namespace ROCKSDB_NAMESPACE
