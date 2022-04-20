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
#include "table/block_based/block_builder_leco.h"
#include "table/format.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include <gperftools/profiler.h>


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

void randomaccess(std::string key_file,std::string seek_key_path, std::string offset_file,
                  std::string size_file, int block_size, int key_num_per_block, bool padding = false, bool includeFirstKey = false) {
  Options options = Options();

  std::vector<std::string> keys;
  std::vector<BlockHandle> block_handles;
// index block key loading
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

// test key loading
std::vector<std::string> seek_keys;
  std::ifstream keySFile(seek_key_path, std::ios::in);
  if (!keySFile) {
    std::cout << "error opening source file." << std::endl;
    return;
  }
  while (1) {
    std::string next;
    keySFile >> next;
    if (keySFile.eof()) {
      break;
    }
    seek_keys.emplace_back(next);
  }
  keySFile.close();

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



  
  int N = keys.size();
  int block_num = N/block_size;
  if (N%block_size != 0) {
    block_num++;
  }

  BlockBuilder_leco builder(padding, block_size, N, key_num_per_block);
  
  uint64_t keys_without_padding = 0;
  uint64_t keys_with_padding = 0;


  for (int i = 0; i < N; i++) {
    IndexValue entry(block_handles[i], keys[i]);
    std::string encoded_entry("");
    entry.EncodeTo(&encoded_entry, includeFirstKey, nullptr);
    Slice encoded_entry_slice(encoded_entry);
    builder.Add(keys[i], encoded_entry_slice);
  }

  // read serialized contents of the block
  Slice rawblock = builder.Finish();
  builder.get_uncompressed_size(keys_with_padding, keys_without_padding);

  // if need padding, we output both pad & unpad cr, otherwise only unpad cr
  std::cout << "compressed size: " << rawblock.size() << std::endl;
  if (padding) {
    int pad_uncompressed_size =
        keys_without_padding + sizeof(uint64_t) * 2 * N;
    std::cout << "padding uncompressed size " << pad_uncompressed_size
              << " padding compression rate "
              << (double)rawblock.size() * 100 / pad_uncompressed_size
              << std::endl;
  }
  int unpad_uncompressed_size =
      keys_with_padding + sizeof(uint64_t) * 2 * N;
  std::cout << "unpadding uncompressed size " << unpad_uncompressed_size
            << " unpadding compression rate "
            << (double)rawblock.size() * 100 / unpad_uncompressed_size
            << std::endl;

  // create block reader
  int seek_num = seek_keys.size();
  for(int i=0;i<seek_num;i++){
    // append a 8B internal key, because when using Seek()
    // we need to cut off 8B internal key
    seek_keys[i].append("00000000"); 
  }
  for(int i=0;i<(int)keys.size();i++){
    keys[i].append("00000000");
  }
  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents),0,nullptr, true);

  const bool kTotalOrderSeek = true;
  const bool kIncludesSeq = false;
  const bool kValueIsFull = true;
  IndexBlockIter *kNullIter = nullptr;
  Statistics *kNullStats = nullptr;

  // read block contents randomly
  bool using_leco_encode = true;
  InternalIteratorBase<IndexValue> * iter = reader.NewIndexIterator(
      options.comparator, kDisableGlobalSequenceNumber, kNullIter, kNullStats,
      kTotalOrderSeek, includeFirstKey, kIncludesSeq, kValueIsFull, false, nullptr, using_leco_encode, block_size, padding, key_num_per_block);
  Random rnd(301);
  double start = clock();

  int seek_count = seek_num * 100;
  for (int i = 0; i < seek_count; i++) {
    // find a random key in the lookaside array
    // iter->SeekToFirst();

    int index = rnd.Uniform(seek_num);
    // int index = i;
    
    // int index = 587;
    // std::string seek_key = "00000000275AF403";
    // seek_key = seek_key.append("00000000");
    // Slice k(seek_key);

    std::string key = seek_keys[index];
    Slice k(key);
    // std::cout<< "index "<< index<<" key "<< k.ToString()<<std::endl;
    // std::cout<<"offset "<<block_handles[index].offset()<<" size "<<block_handles[index].size()<<std::endl;

    //  search in block for this key
    // if(index == 5122){
    //   std::cout<<"hello"<<std::endl;
    // }
    iter->Seek(k);
    IndexValue v = iter->value(); // first is offset, second is searched index,
    // need to verify k>=seek_keys[index] && k<=seek_keys[index+1]
    v.handle.offset();

    // std::cout<< "index "<< index<<" key "<< seek_keys[index]<<std::endl;
    // std::cout<<"offset "<<v.handle.offset()<<" truth: "<<block_handles[index].offset()<<std::endl;
    // std::cout<<"size "<<v.handle.size()<<" truth: "<<block_handles[index].size()<<std::endl;
    
    // assert(v.handle.offset() == block_handles[std::min(index/4,N-1)].offset());
    // assert(v.handle.size() == block_handles[std::min(index/4,N-1)].size());
    // assert(v.handle.offset() == block_handles[index].offset());
    int searched_index = v.handle.size();
    if(searched_index==0 ){
      int cmphigher = memcmp(key.c_str(), keys[0].c_str(), key.size());
      if(cmphigher<=0){
        continue;
      }
      
    }
    // std::cout<<i<<" target key "<<key<<" lower bound "<<keys[searched_index-1]<<" upper bound "<<keys[searched_index]<<std::endl;
    int cmplower = memcmp(key.c_str(), keys[searched_index-1].c_str(), std::min(key.size(),keys[searched_index-1].size())-8);
    int cmphigher = memcmp(key.c_str(), keys[std::min(searched_index,N-1)].c_str(), std::min(key.size(),keys[std::min(searched_index,N-1)].size())-8);
    assert( cmplower >=0 );
    assert(cmphigher<=0);
    

    // assert(iter->key().ToString() == keys[index]);

    // assert(v.handle.offset() == block_handles[block_ind].offset());
    // assert(v.handle.size() == block_handles[block_ind].size() );
    // EXPECT_EQ(separators[index], iter->key().ToString());
    // EXPECT_EQ(block_handles[index].offset(), v.handle.offset());
    // EXPECT_EQ(block_handles[index].size(), v.handle.size());
    // EXPECT_EQ(includeFirstKey ? keys[index] : "",
    //           v.first_internal_key.ToString());

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
  int block_size = atoi(argv[4]);
  int key_num_per_block = atoi(argv[5]);
  std::string seek_key_path = std::string(argv[6]);
  rocksdb::randomaccess(key_path,seek_key_path, offset_path, size_path, block_size, key_num_per_block, true,
                        false);

  
  // rocksdb::randomaccess( "/home/lyh/rocksdb/dump_data/key_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/offset_2000000.txt",
  // "/home/lyh/rocksdb/dump_data/size_2000000.txt",32,true, true);
  return 0;
}