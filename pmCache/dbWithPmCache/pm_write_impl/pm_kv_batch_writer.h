////
//// Created by jxz on 22-7-26.
////
//
//#ifndef LEVELDB_PM_KV_BATCH_WRITER_H
//#define LEVELDB_PM_KV_BATCH_WRITER_H
//
//#include "leveldb/status.h"
//#include "pm_wal_log.h"
//#include "pm_write_batch.h"
//#include "port/port.h"
//
//namespace leveldb::pmCache {
//  class KVBatchWriter {
//   public:
//    explicit KVBatchWriter(port::Mutex *mutex);
//
//    Status status;
//    WriteBatchForPmWal *batch;
//    bool sync;
//    bool done;
//    port::CondVar condVar;
//  };
//} // namespace leveldb::pmCache
//
//#endif // LEVELDB_PM_KV_BATCH_WRITER_H
