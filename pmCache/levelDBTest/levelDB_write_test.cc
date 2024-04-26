#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include <experimental/filesystem>
#include <fstream>
#include <string>

std::string traceFile = "/media/disk/code/SplitDB/evaluation/benchmarks/"
                        "facebook/workloads/zippy1/load";

std::string traceFile1 =
  "/media/disk/code/SplitDB/evaluation/benchmarks/facebook/workloads/zippy1/op";

auto main() -> int {
  std::stringstream valueStream;
  for(int i = 0; i < 1000; ++i) {
    valueStream << "0";
  }
  auto value = valueStream.str();
  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing = true;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);

  leveldb::Status status =
    leveldb::DB::Open(options, "/media/disk/TestDbSplitDB_JL_tmp", &db);

  assert(status.ok());

  std::ifstream trace(traceFile);
  std::string line;
  std::string key;

  auto timeStart = std::chrono::high_resolution_clock::now();

  auto cnt = 0;
  while(std::getline(trace, line)) {
    std::istringstream iss(line);
    iss >> key;

    //    status = db->Get(leveldb::ReadOptions(), leveldb::Slice{key}, &value);
    //
    status = db->Put(leveldb::WriteOptions(), leveldb::Slice{key},
                     leveldb::Slice{value});

    cnt++;
    //    if(cnt == 1000)
    //      break;
    if(cnt % 1000 == 0)
      std::cerr << cnt << std::endl;
    assert(status.ok());
  }

  auto timeEnd = std::chrono::high_resolution_clock::now();
  auto timeDiff =
    std::chrono::duration_cast<std::chrono::milliseconds>(timeEnd - timeStart)
      .count();
  std::cerr << "Time taken: " << timeDiff << " ms" << std::endl;

  delete db;
  return 0;
}