// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma GCC diagnostic ignored "-Wunused-result"
#include <endian.h>
#include <errno.h>
#include <time.h>

#include <atomic>
#include <cinttypes>
#include <climits>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "util/random.h"

//#include "rocksdb/utilities/leveldb_options.h"

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size, std::mt19937_64& e,
                    std::uniform_int_distribution<unsigned long long>& dist,
                    std::string key) {
  memset(value_buf, 0, size);
  int pos = size / 2;
  // while (pos < size) {
  //   uint64_t num = dist(e);
  //   char* num_bytes = reinterpret_cast<char*>(&num);
  //   memcpy(value_buf + pos, num_bytes, 8);
  //   pos += 8;
  // }
  memcpy(value_buf, key.c_str(), key.size());
}

void init(const std::string& key_path, const std::string& db_path,
          rocksdb::DB** db, rocksdb::Options* options,
          rocksdb::BlockBasedTableOptions* table_options, int use_direct_io,
          uint64_t key_count, uint64_t value_size, int filter_type,
          int compression_type, uint64_t insert_or_query, uint64_t using_leco,
          uint64_t query_times, uint64_t key_num_per_block) {
  std::mt19937_64 e(2017);
  std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);

  char value_buf[value_size];

  // if (filter_type == 1)
  //   table_options->filter_policy.reset(
  //       rocksdb::NewBloomFilterPolicy(14, false));
  //   else if (filter_type == 2)
  //     table_options->filter_policy.reset(
  //         rocksdb::NewSuRFPolicy(0, 0, true, 16, false));
  //   else if (filter_type == 3)
  //     table_options->filter_policy.reset(
  //         rocksdb::NewSuRFPolicy(1, 4, true, 16, false));
  //   else if (filter_type == 4)
  //     table_options->filter_policy.reset(
  //         rocksdb::NewSuRFPolicy(2, 4, true, 16, false));

  if (table_options->filter_policy == nullptr)
    std::cout << "Filter DISABLED\n";
  else
    std::cout << "Using " << table_options->filter_policy->Name() << "\n";

  if (compression_type == 0) {
    options->compression = rocksdb::CompressionType::kNoCompression;
    std::cout << "No Compression\n";
  } else if (compression_type == 1) {
    options->compression = rocksdb::CompressionType::kSnappyCompression;
    std::cout << "Snappy Compression\n";
  }

  // table_options->block_cache = rocksdb::NewLRUCache(10 * 1048576);
  // table_options->block_cache = rocksdb::NewLRUCache(1000 * 1048576);

  // table_options->pin_l0_filter_and_index_blocks_in_cache = true;
  // table_options->cache_index_and_filter_blocks = true;

  // begin index block tuning
  // table_options->index_block_restart_interval = 1;
  // table_options->enable_index_compression = false;
  // table_options->format_version = 5;
  // table_options->block_size = 16 * 1024;
  // disable shortest separator
  if (using_leco == 1) {
    table_options->padding_enable = true;
    table_options->leco_block_size = 64;
    table_options->total_length = key_count;
    table_options->key_num_per_block = key_num_per_block;
    table_options->index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchLeco;
  } else {
    table_options->index_block_restart_interval = 1;
    table_options->enable_index_compression = false;
    table_options->index_type =
        rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;
  }

  table_options->index_shortening =
      rocksdb::BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
  // kNoShortening
  // kShortenSeparators

  // table_options->index_type =
  //     rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;

  options->table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(*table_options));

  options->max_open_files = -1;  // pre-load indexes and filters

  // 2GB config
  // options->write_buffer_size = 2 * 1048576;
  // options->max_bytes_for_level_base = 10 * 1048576;
  // options->target_file_size_base = 2 * 1048576;

  // 100GB config
  // options->write_buffer_size = 64 * 1048576;
  // options->max_bytes_for_level_base = 256 * 1048576;
  options->target_file_size_base = 64 * 1048576;

  if (use_direct_io > 0) options->use_direct_reads = true;

  options->statistics = rocksdb::CreateDBStatistics();

  // options->create_if_missing = false;
  // options->error_if_exists = false;

  // options->prefix_extractor = nullptr;
  // options->disable_auto_compactions = false;

  if (insert_or_query == 1) {
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
      std::cout << "creating new DB\n";
      options->create_if_missing = true;
      status = rocksdb::DB::Open(*options, db_path, db);

      if (!status.ok()) {
        std::cout << status.ToString().c_str() << "\n";
        assert(false);
      }

      std::cout << "loading timestamp keys\n";
      std::ifstream keyFile(key_path);
      std::vector<std::string> keys;

      std::string key;
      for (uint64_t i = 0; i < key_count; i++) {
        keyFile >> key;
        keys.push_back(key);
      }

      std::cout << "inserting keys\n";
      for (uint64_t i = 0; i < key_count; i++) {
        key = keys[i];
        rocksdb::Slice s_key(key);
        // rocksdb::Slice s_key(key.c_str(), key.size());
        setValueBuffer(value_buf, value_size, e, dist, key);
        rocksdb::Slice s_value(value_buf, value_size);
        // std::cout<<value_buf<<"\n";
        status = (*db)->Put(rocksdb::WriteOptions(), s_key, s_value);
        if (!status.ok()) {
          std::cout << status.ToString().c_str() << "\n";
          assert(false);
        }
        if (i % (key_count / 100) == 0)
          std::cout << i << "/" << key_count << " ["
                    << ((i + 0.0) / (key_count + 0.0) * 100.) << "]\n";
      }

      rocksdb::Random rnd(301);
      std::string prev = "";
      uint64_t key_num = key_count;
      double start = clock();
      for (uint64_t i = 0; i < key_num; i++) {
        // int index = random() % key_count;
        int index = i;
        // int index = rnd.Uniform(key_count);
        key = keys[index];

        rocksdb::Slice s_key(key);
        std::string value;
        status = (*db)->Get(rocksdb::ReadOptions(), s_key, &value);
        // std::cout<<value.c_str()<<std::endl;
        // std::cout<< " truth: "<<key.c_str()<<std::endl;
        assert(strncmp(value.c_str(), key.c_str(), key.size()) == 0);
        if (i % (key_num / 100) == 0)
          std::cout << i << "/" << key_num << " ["
                    << ((i + 0.0) / (key_num + 0.0) * 100.) << "]\n";
      }
      double end = clock();
      std::cout << "time: " << (end - start) / CLOCKS_PER_SEC << "\n";

      // std::cout << "compacting\n";
      // rocksdb::CompactRangeOptions compact_range_options;
      //(*db)->CompactRange(compact_range_options, NULL, NULL);
    }
  } else {
    rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
    if (!status.ok()) {
      std::cout << status.ToString().c_str() << "\n";
      assert(false);
    }
      std::cout << "loading timestamp keys\n";
      std::ifstream keyFile(key_path);
      std::vector<std::string> keys;

      std::string key;
      for (uint64_t i = 0; i < key_count; i++) {
        keyFile >> key;
        keys.push_back(key);
      }

      rocksdb::Random rnd(301);
      std::string prev = "";
      uint64_t key_num = key_count * query_times;
      double start = clock();
      for (uint64_t i = 0; i < key_num; i++) {
        // int index = random() % key_count;
        int index = i;
        // int index = rnd.Uniform(key_count);
        key = keys[index];
        // key = "com.aol@jtalanca";

        rocksdb::Slice s_key(key);
        std::string value;
        status = (*db)->Get(rocksdb::ReadOptions(), s_key, &value);
        if(strncmp(value.c_str(), key.c_str(), key.size())!=0){
          std::cout<<i<<std::endl;
          std::cout<<key.c_str()<<std::endl;}
        // std::cout<<i<<" "<<value.c_str()<<std::endl;
        // std::cout<< " truth: "<<key.c_str()<<std::endl;
        // std::cout<<i<<std::endl;
        assert(strncmp(value.c_str(), key.c_str(), key.size()) == 0);
        if (i % (key_num / 100) == 0)
          std::cout << i << "/" << key_num << " ["
                    << ((i + 0.0) / (key_num + 0.0) * 100.) << "]\n";
      }
      double end = clock();
      std::cout << "time: " << (end - start) / CLOCKS_PER_SEC << "\n";

  }
}

void close(rocksdb::DB* db) { delete db; }

int main(int argc, const char* argv[]) {
  if (argc < 7) {
    std::cout << "Usage:\n";
    std::cout << "arg 1: path to datafiles\n";
    std::cout << "arg 2: filter type\n";
    std::cout << "\t0: no filter\n";
    std::cout << "\t1: Bloom filter\n";
    std::cout << "\t2: SuRF\n";
    std::cout << "\t3: SuRF Hash\n";
    std::cout << "\t4: SuRF Real\n";
    std::cout << "arg 3: compression?\n";
    std::cout << "\t0: no compression\n";
    std::cout << "\t1: Snappy\n";
    std::cout << "arg 4: use direct I/O?\n";
    std::cout << "\t0: no\n";
    std::cout << "\t1: yes\n";
    std::cout << "arg 5: query type\n";
    std::cout << "\t0: init\n";
    std::cout << "\t1: point query\n";
    std::cout << "\t2: open range query\n";
    std::cout << "\t3: closed range query\n";
    std::cout << "arg 6: range size\n";
    std::cout << "arg 7: warmup # of queries\n";
    return -1;
  }

  std::string db_path = std::string(argv[1]);
  int filter_type = 1;
  int compression_type = atoi(argv[2]);
  int use_direct_io = atoi(argv[3]);
  int query_type = 0;
  uint64_t range_size = 1;
  uint64_t insert_or_query =
      (uint64_t)atoi(argv[4]);  // 1 for insert, 0 for query only
  uint64_t using_leco = (uint64_t)atoi(argv[5]);
  uint64_t query_times = (uint64_t)atoi(argv[6]);
  uint64_t key_num_per_block = (uint64_t)atoi(argv[7]);
  // one typical parameter for leco will be xxx(db_path), 1, 0, 0, 1, 10, 16
  // one typical parameter for baseline will be xxx(db_path), 1, 0, 0, 0, 10, 0
  uint64_t scan_length = 1;

  // const std::string kKeyPath =
  // "/home/lyh/string_data/email_list/padding_a_prefix.txt";
  //"/home/zxy/rocksdb/index_block_compression/poisson_timestamps.csv";
  //  const std::string kKeyPath =
  //  "/home/lyh/string_data/email_list/wholestring_min_12_max_24.txt";
  const std::string kKeyPath =
      "/home/lyh/string_data/email_list/"
      "mail_server_host_reverse_min_10_max_26.txt";

  // const std::string kKeyPath =
  // "/home/lyh/rocksdb/dump_data/wholestring_min_12_max_24_no_repeat.txt";
  // const std::string kKeyPath =
  // "/home/lyh/Learn-to-Compress/scripts/poisson_timestamps_2000000.csv";
  const uint64_t kValueSize = 1000;
  const uint64_t kKeyRange = 10000000000000;
  const uint64_t kQueryCount = 50000;

  // 2GB config
  const uint64_t kKeyCount = 20000000;
  const uint64_t kWarmupSampleGap = 100;

  // 100GB config
  // const uint64_t kKeyCount = 100000000;
  // const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

  //=========================================================================

  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;

  init(kKeyPath, db_path, &db, &options, &table_options, use_direct_io,
       kKeyCount, kValueSize, filter_type, compression_type, insert_or_query, using_leco, query_times,key_num_per_block);
  system("cat /proc/$PPID/io");
  close(db);
  if (query_type == 0) return 0;

  //=========================================================================

  // testScan(kKeyPath, db, kKeyCount);

  // std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) <<
  // "\n"; std::cout << "Mem Aavilable diff: " << (mem_available_before -
  // mem_available_after) << "\n";

  close(db);

  return 0;
}