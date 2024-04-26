//
//  client.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "countdown_latch.h"
#include "db.h"
#include "unistd.h"
#include "utils.h"
#include <string>
#include <sys/syscall.h>

namespace ycsbc {

  inline auto ClientThread(ycsbc::DB *db, ycsbc::CoreWorkload *wl,
                           const int numOps, bool isLoading, bool init_db,
                           bool cleanupDb, CountDownLatch *latch) -> int {
    // printf("YCSB work thread:%ld\n", syscall(SYS_gettid));

    //    cpu_set_t cpuSet;
    //    CPU_ZERO(&cpuSet);
    //    CPU_SET(2, &cpuSet);
    //    sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);

    if(init_db)
      db->Init();

    int oks = 0;
    for(int i = 0; i < numOps; ++i) {
      if(isLoading)
        oks += wl->DoInsert(*db);
      else
        oks += wl->DoTransaction(*db);
    }

    //    getchar();
    //    getchar();

    if(cleanupDb)
      db->Cleanup();

    latch->CountDown();
    return oks;
  }

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
