#!/usr/bin/fish

sudo numactl --cpunodebind=1 --membind=1 \
  /media/disk/code/SplitDB/cmake-build-debug/YCSBcpp \
  -run -db leveldb -P /media/disk/code/SplitDB/YCSB/workloads/workloada \
  -P /media/disk/code/SplitDB/YCSB/leveldb/leveldb.properties \
  -s -p threadcount=1 >/mnt/vssd1/stallData/SplitDB_stall_1_20.txt 2>&1