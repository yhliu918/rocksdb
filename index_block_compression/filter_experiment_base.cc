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
#include "util/random.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

//#include "rocksdb/utilities/leveldb_options.h"

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size, std::mt19937_64& e,
                    std::uniform_int_distribution<unsigned long long>& dist, std::string& key) {
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
          int compression_type) {
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
  table_options->index_block_restart_interval = 1;
  table_options->enable_index_compression = false;
  // table_options->format_version = 5;
  // table_options->block_size = 16 * 1024;
  // disable shortest separator

  table_options->index_shortening =
      rocksdb::BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators;
  // kNoShortening
  // kShortenSeparators


  // table_options->index_type =
  //     rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchLeco;

  // table_options->padding_enable = true;
  // table_options->leco_block_size = 64;
  // table_options->total_length = key_count;
  // table_options->key_num_per_block = 1;



  table_options->index_type =
      rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch;


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
      //key = htobe64(key);
      // reverse(key.begin(), key.end());

      rocksdb::Slice s_key(key);
      // rocksdb::Slice s_key(key.c_str(), key.size());
      setValueBuffer(value_buf, value_size, e, dist, key);
      rocksdb::Slice s_value(value_buf, value_size);

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
    uint64_t key_num = key_count  ;
    double start = clock();
    for (uint64_t i = 0; i < key_num; i++) {
      //int index = random() % key_count;
      int index = i;
      // int index = rnd.Uniform(key_count);
      key = keys[index];
      rocksdb::Slice s_key(key);
      std::string value;
      status = (*db)->Get(rocksdb::ReadOptions(), s_key, &value);
      // std::cout<<value.c_str()<<std::endl;
      // std::cout<<counter<< " truth: "<<key.c_str()<<std::endl;
      // assert(strncmp(value.c_str(), key.c_str(), key.size()) == 0);
      if(i % (key_num / 100) == 0)
        std::cout << i << "/" << key_num << " ["
                  << ((i + 0.0) / (key_num + 0.0) * 100.) << "]\n";
      
    }
    double end = clock();
    std::cout << "time: " << (end - start) / CLOCKS_PER_SEC << "\n";    



    // std::cout << "compacting\n";
    // rocksdb::CompactRangeOptions compact_range_options;
    //(*db)->CompactRange(compact_range_options, NULL, NULL);
  }
}

void close(rocksdb::DB* db) { delete db; }

void testScan(const std::string& key_path, rocksdb::DB* db,
              uint64_t key_count) {
  std::cout << "testScan: loading timestamp keys\n";
  std::ifstream keyFile(key_path);
  std::vector< std::string> keys;

  std::string key;
  for (uint64_t i = 0; i < key_count; i++) {
    keyFile >> key;
    keys.push_back(key);
  }

  struct timespec ts_start;
  struct timespec ts_end;
  uint64_t elapsed;

  clock_gettime(CLOCK_MONOTONIC, &ts_start);

  for (uint64_t i = 0; i < 1000; i++) {
    // key = htobe64(keys[i]);
    std::cerr << std::hex << key << std::endl;

    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    std::string s_value;
    uint64_t value;

    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

    if (status.ok()) {
      assert(s_value.size() >= sizeof(uint64_t));
      value = *reinterpret_cast<const uint64_t*>(s_value.data());
      (void)value;
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_end.tv_nsec) -
            static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_start.tv_nsec);

  std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.)
            << "\n";
  std::cout << "throughput: "
            << (static_cast<double>(key_count) /
                (static_cast<double>(elapsed) / 1000000000.))
            << "\n";
}

void warmup(const std::string key_path, uint64_t key_count, uint64_t sample_gap,
            rocksdb::DB* db) {
  std::ifstream keyFile(key_path);
  std::vector< std::string> keys;
   std::string key;
  for (uint64_t i = 0; i < key_count; i++) {
    keyFile >> key;
    if (i % sample_gap == 0) keys.push_back(key);
  }

  struct timespec ts_start;
  struct timespec ts_end;
  // uint64_t elapsed;

  // std::cout << "warming up\n";
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);

  for (uint64_t i = 0; i < keys.size(); i++) {
    key = keys[i];
    // key = htobe64(key);

    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    std::string s_value;
    uint64_t value;

    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

    if (status.ok()) {
      assert(s_value.size() >= sizeof(uint64_t));
      value = *reinterpret_cast<const uint64_t*>(s_value.data());
      (void)value;
    }
  }

  // clock_gettime(CLOCK_MONOTONIC, &ts_end);
  // elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
  // static_cast<uint64_t>(ts_end.tv_nsec) -
  // static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
  // static_cast<uint64_t>(ts_start.tv_nsec);

  // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.)
  // << "\n"; std::cout << "throughput: " << (static_cast<double>(keys.size()) /
  // (static_cast<double>(elapsed) / 1000000000.)) << "\n";
}

void benchPointQuery(rocksdb::DB* db, rocksdb::Options* options,
                     uint64_t key_range, uint64_t query_count) {
  // std::random_device rd;
  // std::mt19937_64 e(rd());
  std::mt19937_64 e(2017);
  std::uniform_int_distribution<unsigned long long> dist(0, key_range);

  std::vector<uint64_t> query_keys;

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t r = dist(e);
    query_keys.push_back(r);
  }

  struct timespec ts_start;
  struct timespec ts_end;
  uint64_t elapsed;

  printf("point query\n");
  clock_gettime(CLOCK_MONOTONIC, &ts_start);

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t key = query_keys[i];
    key = htobe64(key);

    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    std::string s_value;
    uint64_t value;

    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), s_key, &s_value);

    if (status.ok()) {
      assert(s_value.size() >= sizeof(uint64_t));
      value = *reinterpret_cast<const uint64_t*>(s_value.data());
      (void)value;
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_end.tv_nsec) -
            static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_start.tv_nsec);

  // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.)
  // << "\n";
  std::cout << "throughput: "
            << (static_cast<double>(query_count) /
                (static_cast<double>(elapsed) / 1000000000.))
            << "\n";

  std::string stats = options->statistics->ToString();
  size_t pos = stats.find("rocksdb.db.get.micros statistics Percentiles");
  size_t end_pos = stats.find("rocksdb.db.write.micros statistics Percentiles");
  std::string latencies = stats.substr(pos, (end_pos - pos));
  std::cout << latencies;
}

void benchOpenRangeQuery(rocksdb::DB* db, rocksdb::Options* options,
                         uint64_t key_range, uint64_t query_count,
                         uint64_t scan_length) {
  // std::random_device rd;
  // std::mt19937_64 e(rd());
  std::mt19937_64 e(2017);
  std::uniform_int_distribution<unsigned long long> dist(0, key_range);

  std::vector<uint64_t> query_keys;

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t r = dist(e);
    query_keys.push_back(r);
  }

  struct timespec ts_start;
  struct timespec ts_end;
  uint64_t elapsed;

  printf("open range query\n");
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

  clock_gettime(CLOCK_MONOTONIC, &ts_start);

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t key = query_keys[i];
    key = htobe64(key);
    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));

    std::string s_value;
    uint64_t value;

    uint64_t j = 0;
    for (it->Seek(s_key); it->Valid() && j < scan_length; it->Next(), j++) {
      uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
      assert(it->value().size() >= sizeof(uint64_t));
      value = *reinterpret_cast<const uint64_t*>(it->value().data());
      (void)value;
      break;
    }
  }

  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_end.tv_nsec) -
            static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_start.tv_nsec);

  // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.)
  // << "\n";
  std::cout << "throughput: "
            << (static_cast<double>(query_count) /
                (static_cast<double>(elapsed) / 1000000000.))
            << "\n";

  std::string stats = options->statistics->ToString();
  size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
  size_t end_pos = stats.find("rocksdb.db.write.stall statistics Percentiles");
  std::string latencies = stats.substr(pos, (end_pos - pos));
  std::cout << latencies;

  delete it;
}

void benchClosedRangeQuery(rocksdb::DB* db, rocksdb::Options* options,
                           uint64_t key_range, uint64_t query_count,
                           uint64_t range_size) {
  // std::random_device rd;
  // std::mt19937_64 e(rd());
  std::mt19937_64 e(2017);
  std::uniform_int_distribution<unsigned long long> dist(0, key_range);

  std::vector<uint64_t> query_keys;

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t r = dist(e);
    query_keys.push_back(r);
  }

  struct timespec ts_start;
  struct timespec ts_end;
  uint64_t elapsed;

  printf("closed range query\n");

  clock_gettime(CLOCK_MONOTONIC, &ts_start);

  for (uint64_t i = 0; i < query_count; i++) {
    uint64_t key = query_keys[i];
    uint64_t upper_key = key + range_size;
    key = htobe64(key);
    rocksdb::Slice s_key(reinterpret_cast<const char*>(&key), sizeof(key));
    upper_key = htobe64(upper_key);
    rocksdb::Slice s_upper_key(reinterpret_cast<const char*>(&upper_key),
                               sizeof(upper_key));

    std::string s_value;
    uint64_t value;

    rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
    read_options.iterate_upper_bound = &s_upper_key;
    rocksdb::Iterator* it = db->NewIterator(read_options);

    uint64_t j = 0;
    for (it->Seek(s_key); it->Valid(); it->Next(), j++) {
      uint64_t found_key = *reinterpret_cast<const uint64_t*>(it->key().data());
      assert(it->value().size() >= sizeof(uint64_t));
      value = *reinterpret_cast<const uint64_t*>(it->value().data());
      (void)value;
      break;
    }

    delete it;
  }

  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  elapsed = static_cast<uint64_t>(ts_end.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_end.tv_nsec) -
            static_cast<uint64_t>(ts_start.tv_sec) * 1000000000UL +
            static_cast<uint64_t>(ts_start.tv_nsec);

  // std::cout << "elapsed:    " << (static_cast<double>(elapsed) / 1000000000.)
  // << "\n";
  std::cout << "throughput: "
            << (static_cast<double>(query_count) /
                (static_cast<double>(elapsed) / 1000000000.))
            << "\n";

  std::string stats = options->statistics->ToString();
  size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
  size_t end_pos = stats.find("rocksdb.db.write.stall statistics Percentiles");
  std::string latencies = stats.substr(pos, (end_pos - pos));
  std::cout << latencies;
}

void testIndexBlock(rocksdb::DB* db, rocksdb::Options* options) {
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  assert(it->status().ok());
  int cnt = 0;
  printf("hi\n");
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    printf("key_size: %ld, v size: %ld\n", it->key().size(),
           it->value().size());
    std::cout << std::atoi(it->key().ToString().c_str()) << ": "
              << it->value().ToString() << std::endl;
    ++cnt;
    // if (cnt == 1000) break;
  }
  assert(it->status().ok());
  delete it;
  //   std::string stats = options->statistics->ToString();
  //   size_t pos = stats.find("rocksdb.db.seek.micros statistics Percentiles");
  //   size_t end_pos = stats.find("rocksdb.db.write.stall statistics
  //   Percentiles"); std::string latencies = stats.substr(pos, (end_pos -
  //   pos)); std::cout << latencies;
}

void printIO() {
  FILE* fp = fopen("/sys/block/sda/sda1/stat", "r");
  if (fp == NULL) {
    printf("Error: empty fp\n");
    printf("%s\n", strerror(errno));
    return;
  }
  char buf[4096];
  if (fgets(buf, sizeof(buf), fp) != NULL) printf("%s", buf);
  fclose(fp);
  printf("\n");
}

uint64_t getIOCount() {
  std::ifstream io_file(std::string("/sys/block/sda/sda1/stat"));
  uint64_t io_count = 0;
  io_file >> io_count;
  return io_count;
}

uint64_t getMemFree() {
  std::ifstream mem_file(std::string("/proc/meminfo"));
  std::string str;
  uint64_t free_mem = 0;
  for (int i = 0; i < 4; i++) mem_file >> str;
  mem_file >> free_mem;
  return free_mem;
}

uint64_t getMemAvailable() {
  std::ifstream mem_file(std::string("/proc/meminfo"));
  std::string str;
  uint64_t mem_available = 0;
  for (int i = 0; i < 7; i++) mem_file >> str;
  mem_file >> mem_available;
  return mem_available;
}

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
  int filter_type = atoi(argv[2]);
  int compression_type = atoi(argv[3]);
  int use_direct_io = atoi(argv[4]);
  int query_type = atoi(argv[5]);
  uint64_t range_size = (uint64_t)atoi(argv[6]);
  uint64_t warmup_query_count = (uint64_t)atoi(argv[7]);
  uint64_t scan_length = 1;

  //const std::string kKeyPath = "/home/lyh/string_data/email_list/padding_a_prefix.txt";
  //"/home/zxy/rocksdb/index_block_compression/poisson_timestamps.csv";
  const std::string kKeyPath = "/home/lyh/string_data/email_list/mail_server_host_reverse_min_10_max_26.txt";
  // const std::string kKeyPath = "/home/lyh/rocksdb/dump_data/wholestring_min_16_max_24.txt";
  // const std::string kKeyPath = "/home/lyh/rocksdb/dump_data/wholestring_min_12_max_24_no_repeat.txt";
  // const std::string kKeyPath = "/home/lyh/Learn-to-Compress/scripts/poisson_timestamps_20000.csv";
  const uint64_t kValueSize = 1000;
  const uint64_t kKeyRange = 10000000000000;
  const uint64_t kQueryCount = 50000;

  // 2GB config
  const uint64_t kKeyCount = 2000000;
  const uint64_t kWarmupSampleGap = 100;

  // 100GB config
  // const uint64_t kKeyCount = 100000000;
  // const uint64_t kWarmupSampleGap = kKeyCount / warmup_query_count;

  //=========================================================================

  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;

  init(kKeyPath, db_path, &db, &options, &table_options, use_direct_io,
       kKeyCount, kValueSize, filter_type, compression_type);
  system("cat /proc/$PPID/io");
  close(db);
  if (query_type == 0) return 0;

  //=========================================================================

  // testScan(kKeyPath, db, kKeyCount);

  uint64_t mem_free_before = getMemFree();
  uint64_t mem_available_before = getMemAvailable();
  // std::cout << options.statistics->ToString() << "\n";
  // printIO();
  warmup(kKeyPath, kKeyCount, kWarmupSampleGap, db);
  // warmup(kKeyPath, db, kKeyRange, kWarmupQueryCount);
  // std::cout << "read_count = " << (static_cast<double>(read_count -
  // current_read_count) / kWarmupQueryCount) << " per op\n\n";

  // std::cout << options.statistics->ToString() << "\n";
  // printIO();

  // uint64_t mem_free_after = getMemFree();
  // uint64_t mem_available_after = getMemAvailable();
  // std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) <<
  // "\n"; std::cout << "Mem Aavilable diff: " << (mem_available_before -
  // mem_available_after) << "\n";

  uint64_t io_before = getIOCount();
  // mem_free_before = getMemFree();
  // mem_available_before = getMemAvailable();

  if (query_type == 1)
    benchPointQuery(db, &options, kKeyRange, kQueryCount);
  else if (query_type == 2)
    benchOpenRangeQuery(db, &options, kKeyRange, kQueryCount, scan_length);
  else if (query_type == 3)
    benchClosedRangeQuery(db, &options, kKeyRange, kQueryCount, range_size);
  else if (query_type == 4)
    testScan(kKeyPath, db, kKeyCount);

  uint64_t io_after = getIOCount();
  // mem_free_after = getMemFree();
  // mem_available_after = getMemAvailable();
  // std::cout << options.statistics->ToString() << "\n";
  // std::string stats;
  // db->GetProperty(rocksdb::Slice("rocksdb.stats"), &stats);
  // std::cout << stats << "\n";
  // printIO();

  std::cout << "I/O count: " << (io_after - io_before) << "\n";
  // std::cout << "Mem Free diff: " << (mem_free_before - mem_free_after) <<
  // "\n"; std::cout << "Mem Aavilable diff: " << (mem_available_before -
  // mem_available_after) << "\n";

  close(db);

  return 0;
}