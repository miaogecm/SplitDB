//
// Created by jxz on 22-7-26.
//

#ifndef LEVELDB_HEIGHT_GENERATOR_H
#define LEVELDB_HEIGHT_GENERATOR_H

#include <cstdint>
#include <cstdlib>

namespace leveldb::pmCache {
  class HeightGenerator {
   public:
    static inline uint8_t GenerateHeight(uint8_t maxHeight) {
      int height = 1;
      while(height < maxHeight && rand() % 2 == 0) // NOLINT(cert-msc50-cpp)
        height++;
      return height;
    }
  };
} // namespace leveldb::pmCache
#endif // LEVELDB_HEIGHT_GENERATOR_H
