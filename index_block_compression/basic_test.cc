#include <iostream>
#include <string>

#include "rocksdb/db.h"
#include <stdio.h>

#include <algorithm>
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
using namespace ROCKSDB_NAMESPACE;

int main() {
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::Status status = rocksdb::DB::Open(
      options, "/home/lyh/rocksdb/index_block_compression/new_db_dir", &db);

  if (!status.ok()) {
    std::cout << "creating new DB\n";
    options.create_if_missing = true;
    status = rocksdb::DB::Open(
        options, "/home/lyh/rocksdb/index_block_compression/new_db_dir",
        &db);
    assert(status.ok());

    rocksdb::Slice value = "123";
    for (int i = 0; i < 10000; i++) {
      std::string str = "test" + std::to_string(i);
      rocksdb::Slice key = str;
      status = db->Put(rocksdb::WriteOptions(), key, value);
      if (!status.ok()) {
        std::cout << status.ToString().c_str() << "\n";
        assert(false);
      }
    }
  }

  std::string value;
  for (int i = 0; i < 10000; i++) {
    std::string str = "test" + std::to_string(i);
    rocksdb::Slice key = str;
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok()) {
      std::cout << "count: " << i << std::endl
                << status.ToString().c_str() << "\n";
      assert(false);
    }
  }

  rocksdb::Status s = db->Close();
  assert(s.ok());
  return 0;
}