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
  std::string seek_key_path="/home/lyh/string_data/email_list/mail_server_host_reverse_min_10_max_26.txt";
  std::ifstream inFile(seek_key_path, std::ios::in);
  std::string out_path="/home/lyh/rocksdb/dump_data/mail_server_host_reverse_min_10_max_26_datablock.txt";
  std::ofstream outFile(out_path, std::ios::out);

  std::vector<std::string> keys;
  if (!inFile) {
    std::cout << "error opening source file." << std::endl;
    return 0 ;
  }
  while (1) {
    std::string next;
    inFile >> next;
    if (inFile.eof()) {
      break;
    }
    keys.emplace_back(next);
  }
  inFile.close();
  for(auto key:keys){
    // rocksdb::Slice s_key(reinterpret_cast<const char*>(&key_), sizeof(key_));
    // std::string hex_out = s_key.ToString(true);
    if(key.compare("com.aol@alhugh")>=0 && key.compare("com.aol@anpe")<=0){
      outFile<<key<<std::endl;
    }
    
  }
  outFile.close();
  
  return 0;
}