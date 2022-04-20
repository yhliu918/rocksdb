//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder_leco generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_based/block_builder_leco.h"

#include <assert.h>

#include <algorithm>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/block_based/data_block_footer.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

BlockBuilder_leco::BlockBuilder_leco(bool padding_enabled /*= false*/,
                                     int block_size, int totalNum,
                                     int key_num_per_block)
    : padding_enable(padding_enabled),
      block_size_(block_size),
      N(totalNum),
      key_num_per_block_(key_num_per_block),
      totalsize_without_padding(0),
      totalsize_with_padding(0),
      finished_(false) {}

void BlockBuilder_leco::Reset() {
  buffer_.clear();
  finished_ = false;
#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif
}

void BlockBuilder_leco::SwapAndReset(std::string& buffer) {
  std::swap(buffer_, buffer);
  Reset();
}

uint32_t BlockBuilder_leco::EncodeValue(std::string& buffer,
                                        std::vector<uint64_t>& data) {
  int block_num = N / block_size_;
  while (block_size_ * block_num < N) {
    block_num++;
  }
  Leco_integral codec;
  int start_byte = 0;
  PutFixed32(&buffer, start_byte);
  std::vector<std::string> string_key_vec;
  for (int i = 0; i < block_num; i++) {
    int block_length = block_size_;
    if (i == block_num - 1) {
      block_length = N - (block_num - 1) * block_size_;
    }

    uint8_t* descriptor =
        (uint8_t*)malloc(block_length * sizeof(uint64_t) * 10);
    uint8_t* res = descriptor;
    res = codec.encodeArray8(data.data() + (i * block_size_), block_length,
                             descriptor, i);
    // std::cout<<"encode data[1] "<<data[1]<<std::endl;

    int descriptor_length = res - descriptor;
    descriptor = (uint8_t*)realloc(descriptor, descriptor_length);

    std::string descriptor_str(reinterpret_cast<const char*>(descriptor),
                               descriptor_length);
    string_key_vec.emplace_back(descriptor_str);
    free(descriptor);

    start_byte += descriptor_length;
    if (i == block_num - 1) {
      break;
    }
    PutFixed32(&buffer, start_byte);
  }
  for (int i = 0; i < block_num; i++) {
    buffer.append(string_key_vec[i]);
  }

  return buffer.size();
}

// return size of the compressed block
// For binary search sake, we may need to add first key.
// If we are encoding key, our default parameter is [with_first_key = true,
// padding_enable = true (if fix length, then false)] If we are encoding value,
// our default parameter is [with_first_key = false, padding_enable = false]
// into the buffer
// TOTAL COMPONENT:
// 0. max_padding_length (if padding_enable) [uint8_t]
// 1. first key (if with_first_key) [(max_padding_length+1) * block_num]
// P.S.: we need to store the first key length in the first byte
// 2.(deprecated) each block string length after padding (if with_first_key &
// padding_enable) [uint8_t * block_num] 3.(deprecated) origin string length (if
// with_first_key & padding_enable) [uint8_t * N]
// 4. start_byte [int * block_num]
// 5. compressed block
/*
template <typename T, typename S>
uint32_t BlockBuilder_leco::EncodeVal(std::string& buffer,
                                      std::vector<S>& data_vec,
                                      bool with_first_key) {
  int block_num = N / block_size_;
  while (block_size_ * block_num < N)
  {
      block_num++;
  }
  Leco_string<T> codec;
  char padding_char = 2;
  std::vector<S> data_vec_tmp(data_vec);

  codec.setBlockSize(block_size_, block_num);
  if (padding_enable) {
    codec.Padding_string(data_vec_tmp, N, padding_char);
    codec.get_uncompressed_size(totalsize_with_padding,
                                totalsize_without_padding);
    max_padding_length = codec.get_max_padding_length();
  }
  buffer.append(reinterpret_cast<const char*>(&max_padding_length),
                sizeof(max_padding_length));
  //std::cout<<buffer.size()<<std::endl;

  if (with_first_key) {  // when encoding value (offset, size), we usually do
                         // not turn on this option
    int interval = block_size_ / key_num_per_block_;
    // we only store key_num_per_block_ in each block
    // and left alone the last block (if it's not full)
    int start = 0;
    int counter = 0;

    while (start < N) {
      counter++;
      std::string tmp_str = data_vec[start];
      if (padding_enable) {
        codec.Padd_one_string(tmp_str, padding_char);
      }
      codec.push_back_firstkey(tmp_str);
      uint8_t first_key_length = data_vec[start].size();

      buffer.append(reinterpret_cast<const char*>(&first_key_length),
                    sizeof(first_key_length));
      buffer.append(tmp_str);  // fix length string array
      //std::cout<<buffer.size()<<std::endl;
      start += interval;
    }
  }

  int start_byte = 0;
  PutFixed32(&buffer, start_byte);
  std::vector<std::string> string_key_vec;
  for (int i = 0; i < block_num; i++) {
    int block_length = block_size_;
    if (i == block_num - 1) {
      block_length = N - (block_num - 1) * block_size_;
    }

    uint8_t* descriptor =
        (uint8_t*)malloc(block_length * sizeof(uint64_t) * 1000);
    uint8_t* res = descriptor;

    res = codec.encodeArray8_string(data_vec_tmp, i * block_size_, block_length,
                                    descriptor, i);
    int descriptor_length = res - descriptor;
    descriptor = (uint8_t*)realloc(descriptor, descriptor_length);
    std::string descriptor_str(reinterpret_cast<const char*>(descriptor),
                               descriptor_length);
    string_key_vec.emplace_back(descriptor_str);

    start_byte += descriptor_length;
    if (i == block_num - 1) {
      break;
    }
    // std::cout<< i+1<<" "<< start_byte<<std::endl;
    PutFixed32(&buffer, start_byte);
  }
  // above is block_num start_bytes
  for (int i = 0; i < block_num; i++) {
    buffer.append(string_key_vec[i]);
  }
  return buffer.size();
}*/

template <typename T>
void BlockBuilder_leco::EncodingOneDataSegment(std::vector<std::string>& data_vec_tmp, int start_ind, int block_length, int padding_length, char padding_char, std::vector<std::string>& descriptor_of_each_block, std::vector<uint32_t>& offset_of_each_block, uint32_t& start_byte,std::string* common_prefix, int common_prefix_length, uint8_t encoding_type, int interval) {
  Leco_string<T> codec;
  if (padding_enable) {
    codec.Padding_string(data_vec_tmp, start_ind, block_length,
                         padding_char, padding_length);
    codec.get_uncompressed_size(totalsize_with_padding,
                                totalsize_without_padding);
    totalsize_with_padding += common_prefix_length*block_length;
    totalsize_without_padding += common_prefix_length*block_length;
  }
  uint8_t* descriptor =
      (uint8_t*)malloc(block_length * sizeof(uint64_t) * 1000);
  uint8_t* res = descriptor;

  res = codec.encodeArray8_string(data_vec_tmp, start_ind, block_length,
                                  descriptor, common_prefix, common_prefix_length, encoding_type, interval);
  int descriptor_length = res - descriptor;
  descriptor = (uint8_t*)realloc(descriptor, descriptor_length);
  std::string descriptor_str(reinterpret_cast<const char*>(descriptor),
                             descriptor_length);
  descriptor_of_each_block.emplace_back(descriptor_str);
  free(descriptor);
  offset_of_each_block.emplace_back(start_byte);
  start_byte += descriptor_length;
}

template <typename S>
uint32_t BlockBuilder_leco::EncodeVal(std::string& buffer,
                                      std::vector<S>& data_vec,
                                      bool with_first_key) {
  int block_num = N / block_size_;
  while (block_size_ * block_num < N) {
    block_num++;
  }

  char padding_char = 2;
  std::vector<S> data_vec_tmp(data_vec);

  int interval = block_size_ / key_num_per_block_;
  if (with_first_key) {  // when encoding value (offset, size), we usually do
                         // not turn on this option
    uint8_t total_padding_length = 0;
    for (int i = 0; i < N; i++) {
      total_padding_length =
          std::max(total_padding_length, (uint8_t)data_vec_tmp[i].size());
    }
    buffer.append(reinterpret_cast<const char*>(&total_padding_length),
                  sizeof(total_padding_length));
    
    // we only store key_num_per_block_ in each block
    // and left alone the last block (if it's not full)
    int start = 0;
    int counter = 0;

    while (start < N) {
      counter++;
      std::string tmp_str = data_vec[start];
      if (padding_enable) {
        Padd_one_string(tmp_str, padding_char, total_padding_length);
      }
      uint8_t first_key_length = data_vec[start].size();

      buffer.append(reinterpret_cast<const char*>(&first_key_length),
                    sizeof(first_key_length));
      buffer.append(tmp_str);  // fix length string array
      // std::cout<<buffer.size()<<std::endl;
      start += interval;
    }
    // std::cout<<"counter "<<counter<<" max_padding_length "<<(unsigned)total_padding_length<<std::endl;
  }

  std::vector<uint32_t> offset_of_each_block;
  std::vector<std::string> descriptor_of_each_block;

  uint32_t start_byte = 0;

  for (int i = 0; i < block_num; i++) {
    int block_length = block_size_;
    if (i == block_num - 1) {
      block_length = N - (block_num - 1) * block_size_;
    }
    int common_prefix_length = 0;
    std::string common_prefix = "";
    int padding_length = extract_common_prefix(
        data_vec_tmp, i * block_size_, block_length, common_prefix_length, common_prefix);
    // std::cout<<"using common prefix: "<<common_prefix<<" prefix length "<<common_prefix_length<<" padding length: "<<padding_length<<std::endl;
    if (padding_length < 7) {  // the rest can be represented by a uint64_t
      EncodingOneDataSegment<uint64_t>(data_vec_tmp, i * block_size_, block_length,
                             padding_length, padding_char,
                             descriptor_of_each_block, offset_of_each_block,
                             start_byte, &common_prefix,common_prefix_length, 0, interval);

    } else if (padding_length <
               15) {  // the rest can be represented by a uint128_t
      EncodingOneDataSegment<uint128_t>(data_vec_tmp, i * block_size_, block_length,
                             padding_length, padding_char,
                             descriptor_of_each_block, offset_of_each_block,
                             start_byte, &common_prefix,common_prefix_length, 1, interval);

    } else if (padding_length <
               31) {  // the rest can be represented by a uint256_t
      EncodingOneDataSegment<uint256_t>(data_vec_tmp, i * block_size_, block_length,
                             padding_length, padding_char,
                             descriptor_of_each_block, offset_of_each_block,
                             start_byte, &common_prefix,common_prefix_length,2, interval);
    } else {
      throw("max_padding_length is too large, fall back to mpz");
      // EncodingOneDataSegment<mpz_t>(data_vec_tmp, i * block_size_, block_length,
      //                        max_padding_length, padding_char,
      //                        descriptor_of_each_block, offset_of_each_block,
      //                        start_byte, common_prefix,common_prefix_length,3);
    }

    // std::cout<<"block "<<i<<" length "<<start_byte<<std::endl;
  }

  // std::cout<<buffer.size()<<std::endl;
  for(int i=0;i<block_num;i++){
    PutFixed32(&buffer, offset_of_each_block[i]);
  }
  for (int i = 0; i < block_num; i++) {
    buffer.append(descriptor_of_each_block[i]);
  }
  return buffer.size();
}

Slice BlockBuilder_leco::Finish() {
  uint32_t key_size = 0;
  uint32_t offset_size = 0;
  uint32_t size_size = 0;
  std::string key_buf;
  std::string offset_buf;
  std::string size_buf;
  N = key_vec.size();
  // TODO(yhliu918): we need to decide the key type based on its scale

  key_size = EncodeVal<std::string>(key_buf, key_vec, true);
  offset_size = EncodeValue(offset_buf, offset) + key_size;
  size_size = EncodeValue(size_buf, size) + offset_size;

  std::cout << "key size: " << key_size << std::endl;
  std::cout << "offset size: " << offset_size - key_size << std::endl;
  std::cout << "size size: " << size_size - offset_size << std::endl;

  PutFixed32(&buffer_, N);
  PutFixed32(&buffer_, key_size);
  PutFixed32(&buffer_, offset_size);
  PutFixed32(&buffer_, size_size);
  buffer_.append(key_buf);
  buffer_.append(offset_buf);
  buffer_.append(size_buf);

  finished_ = true;
  return Slice(buffer_);
}

// add kv pair, append them to some global array

void BlockBuilder_leco::Add(const Slice& key, Slice& value) {
  // Ensure no unsafe mixing of Add and AddWithLastKey
  assert(!add_with_last_key_called_);

  AddWithLastKeyImpl(key, value);
}

inline void BlockBuilder_leco::AddWithLastKeyImpl(const Slice& key,
                                                  Slice& value) {
  std::string s = key.ToString();
  // std::cout<<s<<std::endl;
  key_vec.emplace_back(s);
  uint64_t value_0, value_1;
  GetVarint64(&value, &value_0);
  GetVarint64(&value, &value_1);

  offset.emplace_back(value_0);
  size.emplace_back(value_1);
}

}  // namespace ROCKSDB_NAMESPACE
