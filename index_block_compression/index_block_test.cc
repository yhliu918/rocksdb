#include <iostream>

#include "rocksdb/db.h"

int main() {
  rocksdb::DB* db;
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::Status status = rocksdb::DB::Open(
      options, "/root/zxy/rocksdb/index_block_compression/db_dir", &db);
  assert(status.ok());
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
  assert(it->status().ok());
  int cnt = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
    ++cnt;
    if (cnt == 1000) break;
  }
  assert(it->status().ok());
  delete it;
  rocksdb::Status s = db->Close();
  assert(s.ok());
  return 0;
}
