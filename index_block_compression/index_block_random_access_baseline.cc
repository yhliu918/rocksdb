#pragma GCC diagnostic ignored "-Wunused-but-set-variable"
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

void randomaccess(std::string key_file, std::string seek_key_path, std::string offset_file,
                  std::string size_file, int restart_interval,
                  bool useValueDeltaEncoding = true,
                  bool includeFirstKey = false) {
  Options options = Options();

  std::vector<std::string> separators;
  std::vector<std::string> keys;
  std::vector<BlockHandle> block_handles;
  std::vector<std::string> seek_keys;

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

  std::ifstream seekKeyFile(seek_key_path, std::ios::in);
  if (!seekKeyFile) {
    std::cout << "error opening source file." << std::endl;
    return;
  }
  while (1) {
    std::string next;
    seekKeyFile >> next;
    if (seekKeyFile.eof()) {
      break;
    }
    seek_keys.emplace_back(next);
  }
  seekKeyFile.close();


  const bool kUseDeltaEncoding = true;
  BlockBuilder builder(restart_interval, kUseDeltaEncoding,
                       useValueDeltaEncoding);
  int num_records = block_handles.size();
  int key_length = keys[0].size();

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

  // read block contents randomly
  InternalIteratorBase<IndexValue> *iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey, kIncludesSeq, kValueIsFull, false, nullptr, false, 0, false, 1);
  for (size_t i = 0; i < seek_keys.size(); ++i) {
    seek_keys[i].append("00000000");
  }
  for(int i=0;i<(int)keys.size();i++){
    keys[i].append("00000000");
  }

  int seek_num = seek_keys.size();
  Random rnd(301);
  double start = clock();
  int seek_count = seek_num*100;
  for (int i = 0; i < seek_count; i++) {
    // find a random key in the lookaside array
    // iter->SeekToFirst();

    int index = rnd.Uniform(seek_num);
    
    // int index = i;

    Slice k(seek_keys[index]);
    iter->Seek(k);
    IndexValue v = iter->value();


    v.handle.offset();
    // assert(v.handle.offset() == block_handles[index].offset());
    // assert(v.handle.size() == block_handles[index].size());


  }
  delete iter;
  double end = clock();
  std::cout << "random access "
            << (end - start) * 1000000000 / (seek_count * CLOCKS_PER_SEC)
            << " ns per record" << std::endl;

}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, const char *argv[]) {
  std::string key_path = std::string(argv[1]);
  std::string offset_path = std::string(argv[2]);
  std::string size_path = std::string(argv[3]);
  int interval = atoi(argv[4]);
  std::string seek_key_path = std::string(argv[5]);
  rocksdb::randomaccess(key_path,seek_key_path, offset_path, size_path, interval, true,
                        false);
  // rocksdb::randomaccess( "/home/lyh/rocksdb/dump_data/key_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/offset_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/size_2000000.txt",32,true, true);
  return 0;
}