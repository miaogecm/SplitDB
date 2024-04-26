//
//  measurements.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "measurements.h"

#include <atomic>
#include <iostream>
#include <limits>
#include <numeric>
#include <sstream>

namespace ycsbc {

  Measurements::Measurements() : count_{}, latency_sum_{}, latency_max_{} {
    std::fill(std::begin(latency_min_), std::end(latency_min_),
              std::numeric_limits<uint64_t>::max());
  }

  void Measurements::Report(Operation op, uint64_t latency) {
    count_[op].fetch_add(1, std::memory_order_relaxed);
    latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
    uint64_t prevMin = latency_min_[op].load(std::memory_order_relaxed);
    while(prevMin > latency && !latency_min_[op].compare_exchange_weak(
                                 prevMin, latency, std::memory_order_relaxed))
      ;
    uint64_t prevMax = latency_max_[op].load(std::memory_order_relaxed);
    while(prevMax < latency && !latency_max_[op].compare_exchange_weak(
                                 prevMax, latency, std::memory_order_relaxed))
      ;
  }

  std::string Measurements::GetStatusMsg() {
    std::ostringstream msgStream;
    msgStream.precision(2);
    uint64_t totalCnt = 0;
    msgStream << std::fixed << " operations;";
    for(int i = 0; i < MAXOPTYPE; i++) {
      Operation op = static_cast<Operation>(i);
      uint64_t cnt = GetCount(op);
      if(cnt == 0)
        continue;
      msgStream << " [" << kOperationString[op] << ":"
                << " Count=" << cnt << " Max="
                << latency_max_[op].load(std::memory_order_relaxed) / 1000.0
                << " Min="
                << latency_min_[op].load(std::memory_order_relaxed) / 1000.0
                << " Avg="
                << ((cnt > 0) ? static_cast<double>(latency_sum_[op].load(
                                  std::memory_order_relaxed)) /
                                  cnt
                              : 0) /
                     1000.0
                << "]";
      totalCnt += cnt;
    }
    return std::to_string(totalCnt) + msgStream.str();
  }

  void Measurements::Reset() {
    std::fill(std::begin(count_), std::end(count_), 0);
    std::fill(std::begin(latency_sum_), std::end(latency_sum_), 0);
    std::fill(std::begin(latency_min_), std::end(latency_min_),
              std::numeric_limits<uint64_t>::max());
    std::fill(std::begin(latency_max_), std::end(latency_max_), 0);
  }

} // namespace ycsbc
