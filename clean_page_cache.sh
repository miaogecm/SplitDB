#!/bin/bash
sudo sync
sudo sync
sleep 1s
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
sleep 1s
sudo sync
sudo sync
echo "clean page cache done"