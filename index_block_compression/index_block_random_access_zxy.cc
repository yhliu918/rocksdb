#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

void GenerateSeparators(std::vector<std::string> *separators,
                        std::vector<std::string> &keys) {
  int len = keys.size();
  std::vector<std::string> first_keys;
  std::vector<int> s_length;
  for (auto it = keys.begin(); it != keys.end();) {
    std::string prev = *it;
    // std::cout<< "record "<< *it<<std::endl;
    first_keys.emplace_back(*it++);
    BytewiseComparator()->FindShortestSeparator(&prev, *it);
    separators->emplace_back(prev);
    s_length.emplace_back(prev.size());
  }

  int maxi = s_length[0];
  int mini = s_length[0];
  int sum = 0;
  for (int i = 0; i < (int)s_length.size(); i++) {
    if (s_length[i] < mini) {
      mini = s_length[i];
    }
    if (s_length[i] > maxi) {
      maxi = s_length[i];
    }
    sum += s_length[i];
  }
  std::cout << "length of the block " << separators->size() << std::endl;
  std::cout << "max length of sep " << maxi << std::endl;
  std::cout << "min length of sep " << mini << std::endl;
  std::cout << "avg length of sep " << (double)sum / (double)s_length.size()
            << std::endl;
}

void randomaccess(std::string key_file, std::string offset_file,
                  std::string size_file, int restart_interval,
                  bool useValueDeltaEncoding = true,
                  bool includeFirstKey = false) {
  Options options = Options();

  std::vector<std::string> separators;
  std::vector<std::string> keys;
  std::vector<BlockHandle> block_handles;

  std::ifstream keyFile(key_file, std::ios::in);
  if (!keyFile) {
    std::cout << "error opening source file." << std::endl;
    return;
  }
  while (1) {
    std::string next;
    keyFile >> next;
    if (keyFile.eof()) {
      break;
    }
    keys.emplace_back(next);
  }
  keyFile.close();

  std::ifstream offsetFile(offset_file, std::ios::in);
  std::ifstream sizeFile(size_file, std::ios::in);
  if (!offsetFile || !sizeFile) {
    std::cout << "error opening source file." << std::endl;
    return;
  }
  while (1) {
    uint64_t off;
    uint64_t size;
    offsetFile >> off;
    sizeFile >> size;
    if (offsetFile.eof()) {
      break;
    }
    BlockHandle bh(off, size);
    block_handles.emplace_back(bh);
  }
  offsetFile.close();
  sizeFile.close();

  const bool kUseDeltaEncoding = true;
  BlockBuilder builder(restart_interval, kUseDeltaEncoding,
                       useValueDeltaEncoding);
  int num_records = block_handles.size();
  int key_length = keys[0].size();

  GenerateSeparators(&separators, keys);
  BlockHandle last_encoded_handle;
  int total_size = 0;
  for (int i = 0; i < num_records; i++) {
    IndexValue entry(block_handles[i], keys[i]);
    std::string encoded_entry;
    std::string delta_encoded_entry;
    entry.EncodeTo(&encoded_entry, includeFirstKey, nullptr);
    if (useValueDeltaEncoding && i > 0) {
      entry.EncodeTo(&delta_encoded_entry, includeFirstKey,
                     &last_encoded_handle);
    }
    last_encoded_handle = entry.handle;
    const Slice delta_encoded_entry_slice(delta_encoded_entry);
    // builder.Add(separators[i], encoded_entry, &delta_encoded_entry_slice);
    total_size += keys[i].size();
    builder.Add(keys[i], encoded_entry, &delta_encoded_entry_slice);
  }
  //   if (includeFirstKey)
  //     // varint32 used to save first_key_size, so it actually takes only 1
  //     byte
  //     // instead of 4.
  //     std::cout << "uncompressed size: "
  //               << (key_length + 16 + key_length + 4) * num_records <<
  //               std::endl;

  //   else
  //     std::cout << "uncompressed size: " << (key_length + 3) * num_records
  //               << std::endl;

  // read serialized contents of the block
  Slice rawblock = builder.Finish();

  std::cout << "compressed size: " << rawblock.size() << " uncompressed size "
            << num_records * 16 + total_size << " compression rate "
            << (double)rawblock.size() * 100 / (num_records * 16 + total_size)
            << std::endl;

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  const bool kTotalOrderSeek = true;
  const bool kIncludesSeq = false;
  const bool kValueIsFull = !useValueDeltaEncoding;
  IndexBlockIter *kNullIter = nullptr;
  Statistics *kNullStats = nullptr;
  // read contents of block sequentially
  double seq_start = clock();
  InternalIteratorBase<IndexValue> *iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey, kIncludesSeq, kValueIsFull);
  iter->SeekToFirst();

  for (int index = 0; index < num_records; ++index) {
    // ASSERT_TRUE(iter->Valid());
    Slice k = iter->key();
    IndexValue v = iter->value();
    k.ToString();
    v.handle.offset();
    // std::cout<< "index "<< index<<" key "<< k.ToString()<<" value
    // "<<v.handle.offset()<<" , "<<v.handle.size()<<std::endl;
    //  EXPECT_EQ(separators[index], k.ToString());
    //  EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    //  EXPECT_EQ(block_handles[index].size(), v.handle.size());
    //  EXPECT_EQ(includeFirstKey ? keys[index] : "",
    //            v.first_internal_key.ToString());
    iter->Next();
  }
  delete iter;
  double seq_end = clock();
  std::cout << "seq access "
            << (seq_end - seq_start) * 1000000000 /
                   (num_records * CLOCKS_PER_SEC)
            << std::endl;

  // read block contents randomly
  iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey, kIncludesSeq, kValueIsFull);
  Random rnd(301);
  double start = clock();
  for (size_t i = 0; i < separators.size(); ++i) {
    separators[i].append("00000000");
    keys[i].append("00000000");
  }
  for (int i = 0; i < num_records; i++) {
    // find a random key in the lookaside array
    // iter->SeekToFirst();
    int index = rnd.Uniform(num_records);
    Slice k(keys[index]);
    // Slice k(separators[index]);

    // std::cout<<i<< " separator "<< k.ToString()<<std::endl;
    //  search in block for this key
    iter->Seek(k);
    IndexValue v = iter->value();
    // std::cout<<"i "<<i<< " index "<< index<<" key "<< k.ToString()<<" value
    // "<<v.handle.offset()<<" , "<<v.handle.size()<<std::endl;

    v.handle.offset();
    // EXPECT_EQ(separators[index], iter->key().ToString());
    // EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    // EXPECT_EQ(block_handles[index].size(), v.handle.size());
    // EXPECT_EQ(includeFirstKey ? keys[index] : "",
    //           v.first_internal_key.ToString());
  }
  delete iter;
  double end = clock();
  std::cout << "random access "
            << (end - start) * 1000000000 / (num_records * CLOCKS_PER_SEC)
            << " ns per record" << std::endl;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, const char *argv[]) {
  std::string key_path = std::string(argv[1]);
  std::string offset_path = std::string(argv[2]);
  std::string size_path = std::string(argv[3]);
  int interval = atoi(argv[4]);
  rocksdb::randomaccess(key_path, offset_path, size_path, interval, true,
                        false);
  // rocksdb::randomaccess( "/home/lyh/rocksdb/dump_data/key_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/offset_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/size_2000000.txt",32,true, true);
  return 0;
}