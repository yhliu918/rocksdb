#include <gperftools/profiler.h>
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
    builder.Add(separators[i], encoded_entry, &delta_encoded_entry_slice);
  }

  Slice rawblock = builder.Finish();
  std::cout << "compressed size: " << rawblock.size() << " uncompressed size "
            << num_records * (key_length + 16) << " compression rate "
            << (double)rawblock.size() * 100 / (num_records * (key_length + 16))
            << std::endl;

  // create block reader
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  const bool kTotalOrderSeek = true;
  const bool kIncludesSeq = true;
  const bool kValueIsFull = !useValueDeltaEncoding;
  IndexBlockIter *kNullIter = nullptr;
  Statistics *kNullStats = nullptr;
  IndexBlockIter *iter;

  // read contents of block sequentially
  // iter = reader.NewIndexIterator(
  //     options.comparator, kDisableGlobalSequenceNumber, kNullIter,
  //     kNullStats, kTotalOrderSeek, includeFirstKey, kIncludesSeq,
  //     kValueIsFull);
  // double seq_start = clock();
  // ProfilerStart("../../seq_access_16.prof");
  // iter->SeekToFirst();

  // for (int index = 0; index < num_records; ++index) {
  //   // ASSERT_TRUE(iter->Valid());
  //   Slice k = iter->key();
  //   IndexValue v = iter->value();
  //   k.ToString();
  //   v.handle.offset();
  //   // std::cout<< "index "<< index<<" key "<< k.ToString()<<" value
  //   // "<<v.handle.offset()<<" , "<<v.handle.size()<<std::endl;
  //   //  EXPECT_EQ(separators[index], k.ToString());
  //   //  EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
  //   //  EXPECT_EQ(block_handles[index].size(), v.handle.size());
  //   //  EXPECT_EQ(includeFirstKey ? keys[index] : "",
  //   //            v.first_internal_key.ToString());
  //   iter->Next();
  // }
  // delete iter;
  // ProfilerStop();
  // double seq_end = clock();
  // std::cout << "seq access "
  //           << (seq_end - seq_start) * 1000000000 /
  //                  (num_records * CLOCKS_PER_SEC)
  //           << std::endl;

  // read block contents randomly
  iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey, kIncludesSeq, kValueIsFull);
  Random rnd(301);
  double start = clock();
  ProfilerStart("../../random_access_16_0209_comparator.prof");
  // for (int i = 0; i < 1000000; i++) {
  //   iter->icmp();
  // }
  for (int i = 0; i < num_records; i++) {
    // find a random key in the lookaside array
    // iter->SeekToFirst();
    int index = rnd.Uniform(num_records);
    Slice k(separators[index]);

    // std::cout<<i<< " separator "<< k.ToString()<<std::endl;
    //  search in block for this key
    iter->Seek(k);
    IndexValue v = iter->value();
    // std::cout<<"i "<<i<< " index "<< index<<" key "<< k.ToString()<<" value
    // "<<v.handle.offset()<<" , "<<v.handle.size()<<std::endl;

    v.handle.offset();
    // EXPECT_EQ(separators[index], iter->key().ToString());
    //EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    //EXPECT_EQ(block_handles[index].size(), v.handle.size());
    // EXPECT_EQ(includeFirstKey ? keys[index] : "",
    //           v.first_internal_key.ToString());
  }
  delete iter;
  ProfilerStop();
  double end = clock();
  std::cout << "random access "
            << (end - start) * 1000000000 / (num_records * CLOCKS_PER_SEC)
            << " ns per record" << std::endl;
}

}  // namespace ROCKSDB_NAMESPACE

int main() {
  rocksdb::randomaccess("/home/lyh/rocksdb/dump_data/wholestring_max_30_sep_key.txt",
                        "/home/lyh/rocksdb/dump_data/wholestring_max_30_sep_offset.txt",
                        "/home/lyh/rocksdb/dump_data/wholestring_max_30_sep_size.txt", 32,
                        true, false);
  return 0;
}