#!/bin/bash
apt install -y libpmemobj-cpp-dev pmdk-tools

rm -rf /root/code/splitdb/leveldb-x-new/cmake-build-debug
mkdir -p /root/code/splitdb/leveldb-x-new/cmake-build-debug

/usr/bin/cmake --build ./cmake-build-debug --target clean

export CC=/usr/local/bin/clang
export CPP=/usr/local/bin/clang
export CXX=/usr/local/bin/clang

sudo rm -r ./cmake-build-debug/
sudo mkdir ./cmake-build-debug/
sudo rm ./cmake-build-debug/CMakeCache.txt
sudo rm -r ./cmake-build-debug/CMakeFiles
sudo rm -r ./cmake-build-debug/third_party
sudo rm -r ./cmake-build-debug/YCSB
cd cmake-build-debug || exit

# Please modify PMEM_CACHE_PATH to the address where the NVM device is mounted.

sudo cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo \
  -D PMEM_CACHE_SIZE=13*1024*1024*1024L \
  -D LSM_NVM_TOTAL_LEVEL_SIZE=10L*1024*1024*1024 \
  -D PMEM_CACHE_PATH="\"/mnt/pmemNuma1/SplitDB/cachePool\"" \
  -D LSM_NVM_LEVEL_SIZE_MULTIPLE=5.0 \
  -D LSM_NVM_BASE_LEVEL_SIZE=58*1024*1024L \
  -D DRAM_MEM_TABLE_SIZE=3L*1024*1024L \
  -D ZIPFIAN_CONST=0.99 \
  \
  -D PM_CACHE_MAX_INDEX_HEIGHT=10 \
  -D PM_CACHE_RECOMMEND_KEY_LEN=24 \
  -D TEST_POOL_NAME="\"/mnt/pmem/testPool\"" \
  -D PMEM_SSTABLE_RECOMMEND_KEY_LEN=24+8 \
  -D PMCACHE_GC_THREAD_NUM=4 \
  -D PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA=3*1024*1024L \
  -D NVM_MAX_LEVEL=4 \
  -D PM_SKIPLIST_MAX_FAN_OUT=4 \
  -D L1_CACHE_BYTES=64 \
  -D MAX_KEY_PRE_BUFFER=0.1 \
  -D STATIC_RAND=1 \
  -D SHOW_SPEED=1 \
  ..

# Modify config::kMaxIndexHeights in dbformat.h to adjust the height of the index in each layer of the skip list

sudo cmake --build . --clean-first --target leveldb -- -j 16
sudo cmake --build . --target YCSBcpp -- -j 16
sudo cmake --build . --target ListCompactionTest -- -j 16
