sudo rsync -av --delete /media/disk/TestDbNoveLSM_80G/ /media/disk/back/TestDbNoveLSM_80G/
sudo rsync -av --delete /mnt/pmem/NoveLSM/ /media/disk2/PMEM/NoveLSM/
sudo rsync -av --delete /media/disk/TestDbRocksDB_80G/ /media/disk/back/TestDbRocksDB_80G/
sudo rsync -av --delete /media/disk2/TestDbMatrixKv_80G/ /media/disk2/back/TestDbMatrixKv_80G/
sudo rsync -av --delete /mnt/pmem/MatrixKv/ /media/disk2/PMEM/MatrixKv/
sudo rsync -av --delete /media/disk/TestDbLevelDB_80G/ /media/disk/back/TestDbLevelDB_80G/