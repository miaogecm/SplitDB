ulimit -n 1000000
echo "run"
# levelDB disk0-pmem_x
sudo rsync -av --delete /media/disk/back/TestDbLevelDB_80G/ /media/disk/TestDbLevelDB_80G/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/LevelDB/cmake-build-release/YCSBcpp -run -db leveldb -P /media/disk/code/LevelDB/YCSB/workloads/workloadc -P /media/disk/code/LevelDB/YCSB/leveldb/leveldb.properties -s -p threadcount=1 > levelDB_C_1G_10G_cache.txt 2>&1
sudo rsync -av --delete /media/disk/back/TestDbLevelDB_80G/ /media/disk/TestDbLevelDB_80G/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/LevelDB/cmake-build-release/YCSBcpp -run -db leveldb -P /media/disk/code/LevelDB/YCSB/workloads/workloadc -P /media/disk/code/LevelDB/YCSB/leveldb/leveldb_no_block_cache.properties -s -p threadcount=1 > levelDB_C_1G_8M_cache.txt 2>&1


# RocksDB disk0-pmem_x
sudo rsync -av --delete /media/disk/back/TestDbRocksDB_80G/ /media/disk/TestDbRocksDB_80G/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/RocksDB/YCSB -run -db rocksdb -P /media/disk/code/RocksDB/YCSB-cpp/workloads/workloadc -P /media/disk/code/RocksDB/YCSB-cpp/rocksdb/rocksdb.properties -s -p threadcount=1 > rocksDB_C_1G_10G_cache.txt 2>&1
sudo rsync -av --delete /media/disk/back/TestDbRocksDB_80G/ /media/disk/TestDbRocksDB_80G/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/RocksDB/YCSB -run -db rocksdb -P /media/disk/code/RocksDB/YCSB-cpp/workloads/workloadc -P /media/disk/code/RocksDB/YCSB-cpp/rocksdb/rocksdb_no_block_cache.properties -s -p threadcount=1 > rocksDB_C_1G_8M_cache.txt 2>&1

# SplitDB disk0-pmem0
sudo rsync -av --delete /media/disk2/back/TestDbSplitDB_80G/ /media/disk/TestDbSplitDB_80G_tmp/
sudo rsync -av --delete /media/disk2/PMEM/SplitDB/ /mnt/pmem/SplitDB/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/SplitDB/cmake-build-debug/YCSBcpp -run -db leveldb -P /media/disk/code/SplitDB/YCSB/workloads/workloadc -P /media/disk/code/SplitDB/YCSB/leveldb/leveldb.properties -s -p threadcount=1 > SplitDB_C_1G.txt 2>&1

# NoveLSM disk1-pmem1
sudo rsync -av --delete /media/disk/back/TestDbNoveLSM_80G_test5Gx2/ /media/disk2/TestDbNoveLSM_80G_test5Gx2/
sudo rsync -av --delete /media/disk2/PMEM/NoveLSM_test5Gx2/ /mnt/pmemNuma1/NoveLSM/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=1 --membind=1 /media/disk/code/NoveLSM/YCSB1 -run -db leveldb -P /media/disk/code/NoveLSM/YCSB/workloads/workloadc -P /media/disk/code/NoveLSM/YCSB/leveldb/leveldb_5Gx2.properties -s -p threadcount=1 > NoveLSM_C_1G.txt 2>&1

# MatrixKv disk1-pmem1
sudo rsync -av --delete /media/disk2/back/TestDbMatrixKv_80G_same_property/ /media/disk2/TestDbMatrixKv_80G_same_property/
sudo rsync -av --delete /media/disk2/PMEM/MatrixKV_same_property/ /mnt/pmemNuma1/MatrixKv/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh
sudo numactl --cpunodebind=1 --membind=1 /media/disk/code/MatrixKV/YCSB -run -db rocksdb -P /media/disk/code/MatrixKV/YCSB-cpp/workloads/workloadc -P /media/disk/code/MatrixKV/YCSB-cpp/rocksdb/rocksdb_same_property.properties -s -p threadcount=1 > MatrixKv_C_1G.txt 2>&1



sudo rsync -av --delete /media/disk2/back/TestDbSplitDB_80G/ /media/disk/TestDbSplitDB_80G_tmp/
sudo rsync -av --delete /media/disk2/PMEM/SplitDB/ /mnt/pmem/SplitDB/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh

sudo rsync -av --delete /media/disk/back/TestDbNoveLSM_80G_test5Gx2/ /media/disk2/TestDbNoveLSM_80G_test5Gx2/
sudo rsync -av --delete /media/disk2/PMEM/NoveLSM_test5Gx2/ /mnt/pmemNuma1/NoveLSM/
sudo bash /media/disk/code/SplitDB/clean_page_cache.sh


sudo numactl --cpunodebind=0 --membind=0 /media/disk/code/SplitDB/cmake-build-debug/YCSBcpp_special -run -db leveldb -P /media/disk/code/SplitDB/YCSB/workloads/workloada -P /media/disk/code/SplitDB/YCSB/leveldb/leveldb.properties -s -p threadcount=1 > SplitDB_SA_40G.txt 2>&1 &

sudo numactl --cpunodebind=1 --membind=1 /media/disk/code/NoveLSM/YCSB1_special -run -db leveldb -P /media/disk/code/NoveLSM/YCSB/workloads/workloada -P /media/disk/code/NoveLSM/YCSB/leveldb/leveldb_5Gx2.properties -s -p threadcount=1 > NoveLSM_SA_40G.txt 2>&1 &