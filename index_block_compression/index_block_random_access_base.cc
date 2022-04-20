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
                        std::vector<std::string> &keys,
                        std::vector<std::string> *first_keys,
                        std::vector<int> *block_index) {
  int len = keys.size();
  std::vector<int> s_length;
  int counter = 0;
  std::string last_key = "";
  int block_counter = 0;
  for (auto it = keys.begin(); it != keys.end();) {
    if(*it == last_key){
      *it++;
      continue;
    }
    if(counter % 4 == 0){
      std::string prev = *it;
      *it++;
      BytewiseComparator()->FindShortestSeparator(&prev, *it);
      separators->emplace_back(prev);
      s_length.emplace_back(prev.size());
      block_counter++;
    }
    first_keys->emplace_back(*it);
    block_index->emplace_back(block_counter);
    *it++;
    counter++;
    last_key = *it;
    
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
  std::vector<std::string> first_keys;
  std::vector<BlockHandle> block_handles;
  std::vector<int> block_index;

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
  std::ofstream first_keyfile ("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_index_no_repeat.txt");
  std::ofstream myfile ("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_index_key.txt");
  std::ofstream offfile ("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_index_off.txt");
  std::ofstream sizefile ("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_index_size.txt");
  std::ofstream block_index_file ("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_index_block_index.txt");


  GenerateSeparators(&separators, keys, &first_keys, &block_index);
  int offset = 0;
  for(auto item: separators){
    myfile << item << std::endl;
    offfile << offset << std::endl;
    int size = item.size();
    sizefile << size << std::endl;
    offset += (size+5);

  }
  myfile.close();
  offfile.close();
  sizefile.close();

  for(auto item : first_keys){
    first_keyfile << item << std::endl;
  }
  first_keyfile.close();

  for(auto item : block_index){
    block_index_file << item << std::endl;
  }
  block_index_file.close();

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

}

}  // namespace ROCKSDB_NAMESPACE

int main() {
  rocksdb::randomaccess("/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24.txt",
                        "/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_offset.txt",
                        "/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24_size.txt", 32,
                        true, false);
  return 0;
}