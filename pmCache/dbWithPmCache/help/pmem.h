//
// Created by jxz on 22-7-11.
//

#ifndef LEVELDB_PMEM1_H
#define LEVELDB_PMEM1_H

#include "libpmemobj++/allocator.hpp"
#include "libpmemobj++/container/concurrent_hash_map.hpp"
#include "libpmemobj++/container/string.hpp"
// #include "libpmemobj++/experimental/atomic_persistent_aware_ptr.hpp"
#include "libpmemobj++/container/concurrent_hash_map.hpp"
#include "libpmemobj++/experimental/inline_string.hpp"
#include "libpmemobj++/experimental/v.hpp"
#include "libpmemobj++/make_persistent.hpp"
#include "libpmemobj++/make_persistent_atomic.hpp"
#include "libpmemobj++/mutex.hpp"
#include "libpmemobj++/p.hpp"
#include "libpmemobj++/persistent_ptr.hpp"
#include "libpmemobj++/pool.hpp"
#include "libpmemobj++/shared_mutex.hpp"
#include "libpmemobj++/transaction.hpp"
#include "libpmemobj++/utils.hpp"
#include <cstdlib>
#include <immintrin.h>
#include <libpmemobj/iterator.h>
#include <sstream>
#include <string>

namespace leveldb::pmCache {
  using pmem::obj::p;
  using p_uint32_t = p<uint32_t>;
  using p_uint64_t = p<uint64_t>;
  using p_uint8_t = p<uint8_t>;
  using p_uint16_t = p<uint16_t>;
  using pmem::obj::delete_persistent;
  //  using pmem::obj::flat_transaction;
  using pmem::obj::persistent_ptr;
  using pmem::obj::pool;
  using pmem::obj::pool_by_vptr;
  using pmem::obj::transaction;
  using p_bool_t = p<bool>;

  static void memcpy_nt(void *dst, void *src, size_t len) {
    int i;
    long long t1, t2, t3, t4;
    unsigned char *from, *to;
    size_t remain = len & (L1_CACHE_BYTES - 1);

    from = (unsigned char *)src;
    to = (unsigned char *)dst;
    i = len / L1_CACHE_BYTES;

    for(; i > 0; i--) {
      __asm__ __volatile__("  mov (%4), %0\n"
                           "  mov 8(%4), %1\n"
                           "  mov 16(%4), %2\n"
                           "  mov 24(%4), %3\n"
                           "  movnti %0, (%5)\n"
                           "  movnti %1, 8(%5)\n"
                           "  movnti %2, 16(%5)\n"
                           "  movnti %3, 24(%5)\n"
                           "  mov 32(%4), %0\n"
                           "  mov 40(%4), %1\n"
                           "  mov 48(%4), %2\n"
                           "  mov 56(%4), %3\n"
                           "  movnti %0, 32(%5)\n"
                           "  movnti %1, 40(%5)\n"
                           "  movnti %2, 48(%5)\n"
                           "  movnti %3, 56(%5)\n"
                           : "=&r"(t1), "=&r"(t2), "=&r"(t3), "=&r"(t4)
                           : "r"(from), "r"(to)
                           : "memory");

      from += L1_CACHE_BYTES;
      to += L1_CACHE_BYTES;
    }

    /*
     * Now do the tail of the block:
     */
    if(remain) {
      memcpy(to, from, remain);
      pmemobj_flush(_pobj_cached_pool.pop, to, remain);
      //      bonsai_flush(to, remain, 0);
    }
  }

  template <class T> auto GetVPtr(uint64_t off) -> T * {
    assert(_pobj_cached_pool.pop);
    return off != 0 ? (T *)((char *)(_pobj_cached_pool.pop) + off) : nullptr;
  }

  inline auto GetOffset(const void *vPtr) -> uint64_t {
    assert(_pobj_cached_pool.pop);
    return vPtr ? (uint64_t)(vPtr) - (uint64_t)(_pobj_cached_pool.pop) : 0;
  }

  inline auto
  GetOffset(const pmem::obj::persistent_ptr_base &persistentPtr) -> uint64_t {
    return persistentPtr.raw().off;
  }

  void inline Clflush64(void *ptr) {
    if(ptr != nullptr) {
      _mm_clflush(ptr);
      if((uint64_t)ptr % 64) {
        _mm_clflush(((char *)ptr) + 64);
      }
    }
  }

  void inline SFence() { _mm_sfence(); }

  void inline LFence() { _mm_lfence(); }

  void inline MFence() { _mm_mfence(); }

#ifndef NDEBUG

  auto inline GetNumOfPmPieces() -> uint64_t {
    uint64_t c = 0;
    PMEMoid rawObj;
    auto pop = _pobj_cached_pool.pop;
    POBJ_FOREACH(pop, rawObj) { c++; }
    return c;
  }

#else
  auto inline GetNumOfPmPieces() -> uint64_t { return 0; }
#endif

#ifndef NDEBUG
  inline void
  PrintPmAllocate(const std::string &s, void *locate, uint64_t size) {
    //    std::stringstream ss;
    //    ss << s << " " << (uint64_t)locate << " " << size << "\n";
    //    std::cout << ss.str() << std::flush;
  }
#else
#define PrintPmAllocate(a, b, c)                                               \
  ;                                                                            \
  ; // do nothing
#endif

} // namespace leveldb::pmCache

#endif // LEVELDB_PMEM1_H
