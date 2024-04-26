# Copyright 2017 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.9)
# Keep the version below in sync with the one in db.h
project(leveldb VERSION 1.23.0 LANGUAGES C CXX)

link_directories(include/nondebug)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER_LAUNCHER ccache)
# C standard can be overridden when this is used as a sub-project.
if (NOT CMAKE_C_STANDARD)
    # This project can use C11, but will gracefully decay down to C89.
    set(CMAKE_C_STANDARD 11)
    set(CMAKE_C_STANDARD_REQUIRED OFF)
    set(CMAKE_C_EXTENSIONS OFF)
endif (NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if (NOT CMAKE_CXX_STANDARD)
    # This project requires C++11.
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif (NOT CMAKE_CXX_STANDARD)

if (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_WINDOWS)
    # TODO(cmumford): Make UNICODE configurable for Windows.
    add_definitions(-D_UNICODE -DUNICODE)
else (WIN32)
    set(LEVELDB_PLATFORM_NAME LEVELDB_PLATFORM_POSIX)
endif (WIN32)

option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" ON)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" ON)
option(LEVELDB_INSTALL "Install LevelDB's header and library" ON)

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)
#set(HAVE_TCMALLOC 0)

include(CheckCXXSymbolExists)
# Using check_cxx_symbol_exists() instead of check_c_symbol_exists() because
# we're including the header from C++, and feature detection should use the same
# compiler language that the project will use later. Principles aside, some
# versions of do not expose fdatasync() in <unistd.h> in standard C mode
# (-std=c11), but do expose the function in standard C++ mode (-std=c++11).
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Disable C++ exceptions.
    string(REGEX REPLACE "/EH[a-z]+" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHs-c-")
    add_definitions(-D_HAS_EXCEPTIONS=0)

    # Disable RTTI.
    string(REGEX REPLACE "/GR" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /GR-")
else (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # Enable strict prototype warnings for C code in clang and gcc.
    if (NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
    endif (NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

    # Disable C++ exceptions.
    string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    #    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

    # Disable RTTI.
    string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    #    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")
endif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fconcepts")
# Test whether -Wthread-safety is available. See
# https://clang.llvm.org/docs/ThreadSafetyAnalysis.html
include(CheckCXXCompilerFlag)
check_cxx_compiler_flag(-Wthread-safety HAVE_CLANG_THREAD_SAFETY)

# Used by googletest.
check_cxx_compiler_flag(-Wno-missing-field-initializers
        LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

include(CheckCXXSourceCompiles)

# Test whether C++17 __has_include is available.
check_cxx_source_compiles("
#if defined(__has_include) &&  __has_include(<string>)
#include <string>
#endif
int main() { std::string str; return 0; }
" HAVE_CXX17_HAS_INCLUDE)

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

configure_file(
        "port/port_config.h.in"
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
)

include_directories(
        "${PROJECT_BINARY_DIR}/include"
        "."
        "pmCache"
)

if (BUILD_SHARED_LIBS)
    # Only export LEVELDB_EXPORT symbols from the shared library.
    add_compile_options(-fvisibility=hidden)
endif (BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(leveldb
        ""
        pmCache/dbWithPmCache/dram_lru/dram_lru.h
        pmCache/dbWithPmCache/dram_lru/dram_lru.cc
        pmCache/dbWithPmCache/help/thread_pool/thread_pool.h
        pmCache/dbWithPmCache/help/thread_pool/thread_pool.cc
        pmCache/dbWithPmCache/help/pm_timestamp/pm_time_stamp.cc
        pmCache/dbWithPmCache/help/pm_timestamp/pm_time_stamp.h
        pmCache/dbWithPmCache/pmem_sstable/pm_sstable_old.cc
        pmCache/dbWithPmCache/pmem_sstable/pm_sstable_old.h
        pmCache/dbWithPmCache/pm_write_impl/pm_wal_log.cc
        pmCache/dbWithPmCache/pm_write_impl/pm_wal_log.h
        pmCache/dbWithPmCache/pm_write_impl/pm_write_batch.cc
        pmCache/dbWithPmCache/pm_write_impl/pm_write_batch.h
        pmCache/dbWithPmCache/pm_write_impl/pm_kv_batch_writer.cc
        pmCache/dbWithPmCache/pm_write_impl/pm_kv_batch_writer.h
        pmCache/dbWithPmCache/help/height_generator.h
        pmCache/dbWithPmCache/pm_cache_pool_impl.cc
        pmCache/dbWithPmCache/pm_cache_pool_impl.h
        pmCache/dbWithPmCache/pm_version_set/pm_version_set.cc
        pmCache/dbWithPmCache/pm_version_set/pm_version_set.h
        pmCache/dbWithPmCache/pm_version_edit/pm_version_edit.cc
        pmCache/dbWithPmCache/pm_version_edit/pm_version_edit.h
        pmCache/dbWithPmCache/pm_file_meta_data/file_meta_data_with_pmCache.cc
        pmCache/dbWithPmCache/pm_file_meta_data/file_meta_data_with_pmCache.h
        pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.cc
        pmCache/dbWithPmCache/pmem_sstable/pm_SSTable.h
        pmCache/dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.h
        pmCache/dbWithPmCache/pm_file_in_one_level/pm_file_in_one_level.cc
        pmCache/dbWithPmCache/pm_file_in_one_level_opt/pm_file_in_one_level_opt.cc
        pmCache/dbWithPmCache/pm_file_in_one_level_opt/pm_file_in_one_level_opt.h
        pmCache/dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.cc
        pmCache/dbWithPmCache/help/pm_timestamp_opt/NVTimestamp.h
        pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.cc
        pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out.h
        pmCache/dbWithPmCache/help/dramSkipList/concurrentDramSkiplist.cc
        pmCache/dbWithPmCache/help/dramSkipList/concurrentDramSkiplist.h
        pmCache/dbWithPmCache/help/dramSkipList/concurrentMemtable.cc
        pmCache/dbWithPmCache/help/dramSkipList/concurrentMemtable.h
        pmCache/dbWithPmCache/promotion/nv_cache_promotion.cc
        pmCache/dbWithPmCache/promotion/nv_cache_promotion.h
        pmCache/dbWithPmCache/count_min_sketch/count_min_sketch.h
        pmCache/dbWithPmCache/count_min_sketch/count_min_sketch.cc
        )
target_sources(leveldb
        PRIVATE
        "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
        "db/builder.cc"
        "db/builder.h"
        "db/c.cc"
        "db/db_impl.cc"
        "db/db_impl.h"
        "db/db_iter.cc"
        "db/db_iter.h"
        "db/dbformat.cc"
        "db/dbformat.h"
        "db/dumpfile.cc"
        "db/filename.cc"
        "db/filename.h"
        "db/log_format.h"
        "db/log_reader.cc"
        "db/log_reader.h"
        "db/log_writer.cc"
        "db/log_writer.h"
        "db/memtable.cc"
        "db/memtable.h"
        "db/repair.cc"
        "db/skiplist.h"
        "db/snapshot.h"
        "db/table_cache.cc"
        "db/table_cache.h"
        "db/version_edit.cc"
        "db/version_edit.h"
        "db/version_set.cc"
        "db/version_set.h"
        "db/write_batch_internal.h"
        "db/write_batch.cc"
        "port/port_stdcxx.h"
        "port/port.h"
        "port/thread_annotations.h"
        "table/block_builder.cc"
        "table/block_builder.h"
        "table/block.cc"
        "table/block.h"
        "table/filter_block.cc"
        "table/filter_block.h"
        "table/format.cc"
        "table/format.h"
        "table/iterator_wrapper.h"
        "table/iterator.cc"
        "table/merger.cc"
        "table/merger.h"
        "table/table_builder.cc"
        "table/table.cc"
        "table/two_level_iterator.cc"
        "table/two_level_iterator.h"
        "util/arena.cc"
        "util/arena.h"
        "util/bloom.cc"
        "util/cache.cc"
        "util/coding.cc"
        "util/coding.h"
        "util/comparator.cc"
        "util/crc32c.cc"
        "util/crc32c.h"
        "util/env.cc"
        "util/filter_policy.cc"
        "util/hash.cc"
        "util/hash.h"
        "util/logging.cc"
        "util/logging.h"
        "util/mutexlock.h"
        "util/no_destructor.h"
        "util/options.cc"
        "util/random.h"
        "util/status.cc"

        # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
        $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
        "${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
        )

if (WIN32)
    target_sources(leveldb
            PRIVATE
            "util/env_windows.cc"
            "util/windows_logger.h"
            )
else (WIN32)
    target_sources(leveldb
            PRIVATE
            "util/env_posix.cc"
            "util/posix_logger.h"
            )
endif (WIN32)

# MemEnv is not part of the interface and could be pulled to a separate library.
target_sources(leveldb
        PRIVATE
        "helpers/memenv/memenv.cc"
        "helpers/memenv/memenv.h"
        )

target_include_directories(leveldb
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )

set_target_properties(leveldb
        PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(leveldb
        PRIVATE
        # Used by include/export.h when building shared libraries.
        LEVELDB_COMPILE_LIBRARY
        # Used by port/port.h.
        ${LEVELDB_PLATFORM_NAME}=1
        )
if (NOT HAVE_CXX17_HAS_INCLUDE)
    target_compile_definitions(leveldb
            PRIVATE
            LEVELDB_HAS_PORT_CONFIG_H=1
            )
endif (NOT HAVE_CXX17_HAS_INCLUDE)

if (BUILD_SHARED_LIBS)
    target_compile_definitions(leveldb
            PUBLIC
            # Used by include/export.h.
            LEVELDB_SHARED_LIBRARY
            )
endif (BUILD_SHARED_LIBS)

if (HAVE_CLANG_THREAD_SAFETY)
    target_compile_options(leveldb
            PUBLIC
            -Wthread-safety -Wall)
endif (HAVE_CLANG_THREAD_SAFETY)

target_compile_options(leveldb
        PUBLIC -Wall)

if (HAVE_CRC32C)
    target_link_libraries(leveldb crc32c)
    message('have\ crc32')
endif (HAVE_CRC32C)
if (HAVE_SNAPPY)
    target_link_libraries(leveldb snappy)
endif (HAVE_SNAPPY)
if (HAVE_TCMALLOC)
    target_link_libraries(leveldb tcmalloc)
    message('have\ tcmalloc')
endif (HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads)

add_executable(leveldbutil
        "db/leveldbutil.cc"
        )
target_link_libraries(leveldbutil leveldb)

if (LEVELDB_BUILD_TESTS)
    enable_testing()

    # Prevent overriding the parent project's compiler/linker settings on Windows.
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(install_gtest OFF)
    set(install_gmock OFF)
    set(build_gmock ON)

    # This project is tested using GoogleTest.
    add_subdirectory("third_party/googletest")

    # GoogleTest triggers a missing field initializers warning.
    if (LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)
        set_property(TARGET gtest
                APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
        set_property(TARGET gmock
                APPEND PROPERTY COMPILE_OPTIONS -Wno-missing-field-initializers)
    endif (LEVELDB_HAVE_NO_MISSING_FIELD_INITIALIZERS)

    add_executable(leveldb_tests "")
    target_sources(leveldb_tests
            PRIVATE
            # "db/fault_injection_test.cc"
            # "issues/issue178_test.cc"
            # "issues/issue200_test.cc"
            # "issues/issue320_test.cc"
            "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
            # "util/env_test.cc"
            "util/status_test.cc"
            "util/no_destructor_test.cc"
            "util/testutil.cc"
            "util/testutil.h"
            )
    if (NOT BUILD_SHARED_LIBS)
        target_sources(leveldb_tests
                PRIVATE
                "db/autocompact_test.cc"
                "db/corruption_test.cc"
                "db/db_test.cc"
                "db/dbformat_test.cc"
                "db/filename_test.cc"
                "db/log_test.cc"
                "db/recovery_test.cc"
                "db/skiplist_test.cc"
                "db/version_edit_test.cc"
                "db/version_set_test.cc"
                "db/write_batch_test.cc"
                "helpers/memenv/memenv_test.cc"
                "table/filter_block_test.cc"
                "table/table_test.cc"
                "util/arena_test.cc"
                "util/bloom_test.cc"
                "util/cache_test.cc"
                "util/coding_test.cc"
                "util/crc32c_test.cc"
                "util/hash_test.cc"
                "util/logging_test.cc"
                )
    endif (NOT BUILD_SHARED_LIBS)
    target_link_libraries(leveldb_tests leveldb gmock gtest gtest_main)
    target_compile_definitions(leveldb_tests
            PRIVATE
            ${LEVELDB_PLATFORM_NAME}=1
            )
    if (NOT HAVE_CXX17_HAS_INCLUDE)
        target_compile_definitions(leveldb_tests
                PRIVATE
                LEVELDB_HAS_PORT_CONFIG_H=1
                )
    endif (NOT HAVE_CXX17_HAS_INCLUDE)

    add_test(NAME "leveldb_tests" COMMAND "leveldb_tests")

    function(leveldb_test test_file)
        get_filename_component(test_target_name "${test_file}" NAME_WE)

        add_executable("${test_target_name}" "")
        target_sources("${test_target_name}"
                PRIVATE
                "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
                "util/testutil.cc"
                "util/testutil.h"

                "${test_file}"
                )
        target_link_libraries("${test_target_name}" leveldb gmock gtest pmem pmemobj gsl gslcblas m)
        target_compile_definitions("${test_target_name}"
                PRIVATE
                ${LEVELDB_PLATFORM_NAME}=1
                )
        if (NOT HAVE_CXX17_HAS_INCLUDE)
            target_compile_definitions("${test_target_name}"
                    PRIVATE
                    LEVELDB_HAS_PORT_CONFIG_H=1
                    )
        endif (NOT HAVE_CXX17_HAS_INCLUDE)

        add_test(NAME "${test_target_name}" COMMAND "${test_target_name}")
    endfunction(leveldb_test)

    leveldb_test("db/c_test.c")

    if (NOT BUILD_SHARED_LIBS)
        # TODO(costan): This test also uses
        #               "util/env_{posix|windows}_test_helper.h"
        if (WIN32)
            leveldb_test("util/env_windows_test.cc")
        else (WIN32)
            leveldb_test("util/env_posix_test.cc")
        endif (WIN32)
    endif (NOT BUILD_SHARED_LIBS)
endif (LEVELDB_BUILD_TESTS)

if (LEVELDB_BUILD_BENCHMARKS)
    # This project uses Google benchmark for benchmarking.
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "" FORCE)
    add_subdirectory("third_party/benchmark")

    function(leveldb_benchmark bench_file)
        get_filename_component(bench_target_name "${bench_file}" NAME_WE)

        add_executable("${bench_target_name}" "")
        target_sources("${bench_target_name}"
                PRIVATE
                "${PROJECT_BINARY_DIR}/${LEVELDB_PORT_CONFIG_DIR}/port_config.h"
                "util/histogram.cc"
                "util/histogram.h"
                "util/testutil.cc"
                "util/testutil.h"

                "${bench_file}"
                )
        target_link_libraries("${bench_target_name}" leveldb gmock gtest benchmark stdc++fs)
        target_compile_definitions("${bench_target_name}"
                PRIVATE
                ${LEVELDB_PLATFORM_NAME}=1
                )
        if (NOT HAVE_CXX17_HAS_INCLUDE)
            target_compile_definitions("${bench_target_name}"
                    PRIVATE
                    LEVELDB_HAS_PORT_CONFIG_H=1
                    )
        endif (NOT HAVE_CXX17_HAS_INCLUDE)
    endfunction(leveldb_benchmark)

    if (NOT BUILD_SHARED_LIBS)
        leveldb_benchmark("benchmarks/db_bench.cc")
    endif (NOT BUILD_SHARED_LIBS)

    check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
    if (HAVE_SQLITE3)
        leveldb_benchmark("benchmarks/db_bench_sqlite3.cc")
        target_link_libraries(db_bench_sqlite3 sqlite3)
    endif (HAVE_SQLITE3)

    # check_library_exists is insufficient here because the library names have
    # different manglings when compiled with clang or gcc, at least when installed
    # with Homebrew on Mac.
    set(OLD_CMAKE_REQURED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
    list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
    check_cxx_source_compiles("
#include <kcpolydb.h>

int main() {
  kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
  delete db;
  return 0;
}
  " HAVE_KYOTOCABINET)
    set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQURED_LIBRARIES})
    if (HAVE_KYOTOCABINET)
        leveldb_benchmark("benchmarks/db_bench_tree_db.cc")
        target_link_libraries(db_bench_tree_db kyotocabinet)
    endif (HAVE_KYOTOCABINET)
endif (LEVELDB_BUILD_BENCHMARKS)

if (LEVELDB_INSTALL)
    install(TARGETS leveldb
            EXPORT leveldbTargets
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            )
    install(
            FILES
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/c.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/cache.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/comparator.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/db.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/dumpfile.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/env.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/export.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/filter_policy.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/iterator.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/options.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/slice.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/status.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/table_builder.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/table.h"
            "${LEVELDB_PUBLIC_INCLUDE_DIR}/write_batch.h"
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/leveldb"
    )

    include(CMakePackageConfigHelpers)
    configure_package_config_file(
            "cmake/${PROJECT_NAME}Config.cmake.in"
            "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
            INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
    )
    write_basic_package_version_file(
            "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
            COMPATIBILITY SameMajorVersion
    )
    install(
            EXPORT leveldbTargets
            NAMESPACE leveldb::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
    )
    install(
            FILES
            "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
            "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
    )
endif (LEVELDB_INSTALL)

if (NOT PM_CACHE_MAX_INDEX_HEIGHT)
    SET(PM_CACHE_MAX_INDEX_HEIGHT 8)
endif ()
if (NOT PM_CACHE_RECOMMEND_KEY_LEN)
    SET(PM_CACHE_RECOMMEND_KEY_LEN 24)
endif ()
if (NOT MAX_TRY_TIME)
    SET(MAX_TRY_TIME 100)
endif ()
if (NOT TEST_POOL_NAME)
    SET(TEST_POOL_NAME "\"/home/jxz/pmem0/testPool\"")
endif ()
if (NOT PMEM_CACHE_PATH)
    SET(PMEM_CACHE_PATH "\"/home/jxz/pmem0/cache\"")
endif ()
if (NOT PMEM_CACHE_SIZE)
    SET(PMEM_CACHE_SIZE 1.1*1024*1024*1024)
endif ()
if (NOT PM_SSTABLE_MAX_INDEX_HEIGHT)
    SET(PM_SSTABLE_MAX_INDEX_HEIGHT 8)
endif ()
if (NOT PMEM_SSTABLE_RECOMMEND_KEY_LEN)
    SET(PMEM_SSTABLE_RECOMMEND_KEY_LEN 24+8)
endif ()
if (NOT PMCACHE_GC_THREAD_NUM)
    SET(PMCACHE_GC_THREAD_NUM 2)
endif ()
if (NOT LSM_LEVEL_SIZE_MULTIPLE)
    SET(LSM_LEVEL_SIZE_MULTIPLE 4.0)
endif ()
if (NOT LSM_BASE_LEVEL_SIZE)
    SET(LSM_BASE_LEVEL_SIZE 10*1024*1024)
endif ()
if (NOT LSM_NVM_BASE_LEVEL_SIZE)
    SET(LSM_NVM_BASE_LEVEL_SIZE 5*1024*1024)
endif ()
if (NOT LSM_NVM_LEVEL_SIZE_MULTIPLE)
    SET(LSM_NVM_LEVEL_SIZE_MULTIPLE 4)
endif ()
if (NOT MAX_NUM_FILE_COMPACT_TO_NVM_ONCE)
    SET(MAX_NUM_FILE_COMPACT_TO_NVM_ONCE 10)
endif ()
if (NOT PM_LRU_SHARD_COUNT)
    SET(PM_LRU_SHARD_COUNT 1)
endif ()
if (NOT LSM_NVM_TOTAL_LEVEL_SIZE)
    SET(LSM_NVM_TOTAL_LEVEL_SIZE 31457280)
endif ()
if (NOT PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA)
    SET(PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA 2*1024*1024)
endif ()
if (NOT NVM_MAX_LEVEL)
    SET(NVM_MAX_LEVEL 4)
endif ()
if (NOT DRAM_MEM_TABLE_SIZE)
    SET(DRAM_MEM_TABLE_SIZE 4*1024*1024)
endif ()
if (NOT PM_SKIPLIST_MAX_FAN_OUT)
    SET(PM_SKIPLIST_MAX_FAN_OUT 4)
endif ()
if (NOT L1_CACHE_BYTES)
    SET(L1_CACHE_BYTES 64)
endif ()
if (NOT MAX_KEY_PRE_BUFFER)
    SET(MAX_KEY_PRE_BUFFER 0.01)
endif ()
if (NOT ZIPFIAN_CONST)
    SET(ZIPFIAN_CONST 0.99)
endif ()
if (NOT STATIC_RAND)
    SET(STATIC_RAND 0)
endif ()
if (NOT SHOW_SPEED)
    SET(SHOW_SPEED 0)
endif ()
if (NOT MAX_HOTNESS_OF_KEY_FROM_BUFFER)
    SET(MAX_HOTNESS_OF_KEY_FROM_BUFFER 10)
endif ()
target_link_libraries(leveldb stdc++fs)
target_compile_definitions(leveldb
        PUBLIC
        PM_CACHE_MAX_INDEX_HEIGHT=${PM_CACHE_MAX_INDEX_HEIGHT}
        PM_CACHE_RECOMMEND_KEY_LEN=${PM_CACHE_RECOMMEND_KEY_LEN}
        MAX_TRY_TIME=${MAX_TRY_TIME}
        TEST_POOL_NAME=${TEST_POOL_NAME}
        PMEM_CACHE_PATH=${PMEM_CACHE_PATH}
        PMEM_CACHE_SIZE=${PMEM_CACHE_SIZE}
        PM_SSTABLE_MAX_INDEX_HEIGHT=${PM_SSTABLE_MAX_INDEX_HEIGHT}
        PMEM_SSTABLE_RECOMMEND_KEY_LEN=${PMEM_SSTABLE_RECOMMEND_KEY_LEN}
        PMCACHE_GC_THREAD_NUM=${PMCACHE_GC_THREAD_NUM}
        LSM_LEVEL_SIZE_MULTIPLE=${LSM_LEVEL_SIZE_MULTIPLE}
        LSM_BASE_LEVEL_SIZE=${LSM_BASE_LEVEL_SIZE}
        LSM_NVM_BASE_LEVEL_SIZE=${LSM_NVM_BASE_LEVEL_SIZE}
        LSM_NVM_LEVEL_SIZE_MULTIPLE=${LSM_NVM_LEVEL_SIZE_MULTIPLE}
        MAX_NUM_FILE_COMPACT_TO_NVM_ONCE=${MAX_NUM_FILE_COMPACT_TO_NVM_ONCE}
        PM_LRU_SHARD_COUNT=${PM_LRU_SHARD_COUNT}
        LSM_NVM_TOTAL_LEVEL_SIZE=${LSM_NVM_TOTAL_LEVEL_SIZE}
        PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA=${PM_SSTABLE_MAX_SIZE_INCLUDE_METADATA}
        NVM_MAX_LEVEL=${NVM_MAX_LEVEL}
        DRAM_MEM_TABLE_SIZE=${DRAM_MEM_TABLE_SIZE}
        PM_SKIPLIST_MAX_FAN_OUT=${PM_SKIPLIST_MAX_FAN_OUT}
        L1_CACHE_BYTES=${L1_CACHE_BYTES}
        MAX_KEY_PRE_BUFFER=${MAX_KEY_PRE_BUFFER}
        ZIPFIAN_CONST=${ZIPFIAN_CONST}
        STATIC_RAND=${STATIC_RAND}
        SHOW_SPEED=${SHOW_SPEED}
        MAX_HOTNESS_OF_KEY_FROM_BUFFER=${MAX_HOTNESS_OF_KEY_FROM_BUFFER}
        )

target_link_libraries(leveldb pmem pmemobj pthread)

#add_executable(myTest "")
#target_sources(myTest
#        PRIVATE
#        "myTest/MyTest.cc"
#        )
#target_link_libraries(myTest leveldb)

#add_executable(LevelDBTest "")
#target_sources(LevelDBTest
#        PRIVATE
#        "myTest/LevelDBTest.cc"
#        )
#target_link_libraries(LevelDBTest leveldb)


#leveldb_test(pmCache/levelDBTest/thread_pool_test.cc)
#leveldb_test(pmCache/levelDBTest/pmem_skiplist_test.cc)
leveldb_test(pmCache/levelDBTest/levelDB_write_test.cc)
leveldb_test(pmCache/levelDBTest/zipfianTest.cc)
leveldb_test(pmCache/levelDBTest/data_statistics_test.cc)

#leveldb_test(pmCache/levelDBTest/ListCompactionTest_Prepare0.cc)
#leveldb_test(pmCache/levelDBTest/ListCompactionTest_Prepare1.cc)

#leveldb_test(pmCache/levelDBTest/pm_write_batch_test.cc)
#leveldb_test(pmCache/levelDBTest/pm_new_sstable_test.cpp)
#leveldb_test(pmCache/levelDBTest/pmdk_tx_test.cpp)
#leveldb_test(pmCache/levelDBTest/pm_sstable_with_skiplist_wrap_test.cc)

#leveldb_test(pmCache/dbWithPmCache/pm_file_in_one_level_fan_out/pm_file_in_one_level_fan_out_test.cc)


add_executable(YCSBcpp "")
target_sources(YCSBcpp
        PRIVATE
        "./YCSB/leveldb/leveldb_db.cc"
        "./YCSB/core/acknowledged_counter_generator.cc"
        "./YCSB/core/acknowledged_counter_generator.h"
        "./YCSB/core/basic_db.cc"
        "./YCSB/core/basic_db.h"
        "./YCSB/core/client.h"
        "./YCSB/core/const_generator.h"
        "./YCSB/core/core_workload.cc"
        "./YCSB/core/core_workload.h"
        "./YCSB/core/countdown_latch.h"
        "./YCSB/core/counter_generator.h"
        "./YCSB/core/db.h"
        "./YCSB/core/db_factory.cc"
        "./YCSB/core/db_factory.h"
        "./YCSB/core/db_wrapper.h"
        "./YCSB/core/discrete_generator.h"
        "./YCSB/core/generator.h"
        "./YCSB/core/measurements.cc"
        "./YCSB/core/measurements.h"
        "./YCSB/core/properties.h"
        "./YCSB/core/random_byte_generator.h"
        "./YCSB/core/scrambled_zipfian_generator.h"
        "./YCSB/core/skewed_latest_generator.h"
        "./YCSB/core/timer.h"
        "./YCSB/core/uniform_generator.h"
        "./YCSB/core/utils.h"
        "./YCSB/core/ycsbc.cc"
        "./YCSB/core/zipfian_generator.h"
        )

target_link_libraries(YCSBcpp pthread leveldb atomic)
target_include_directories(YCSBcpp PUBLIC ./YCSB/leveldb ./YCSB/core)
target_compile_options(YCSBcpp PUBLIC -std=c++17 -Wall -pthread -I./ -I../YCSB -I../include)

add_executable(ListCompactionTest "./pmCache/levelDBTest/ListCompactionTest_Run.cc")
target_link_libraries(ListCompactionTest pthread leveldb atomic pmem pmemobj)
target_compile_options(ListCompactionTest PUBLIC -std=c++17 -Wall -pthread -I./ -I../YCSB -I../include)