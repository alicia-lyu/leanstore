add_library(PremergedJoinStatsLogger STATIC shared/merge-join/premerged_join_stats_logger.cpp)
target_link_libraries(PremergedJoinStatsLogger PUBLIC leanstore)

# loggers also provide shared libs and directories
add_library(Logger STATIC shared/logger/logger.cpp)
target_link_libraries(Logger PUBLIC PremergedJoinStatsLogger)
target_include_directories(Logger PUBLIC ${SHARED_INCLUDE_DIRECTORY})

add_library(LeanStoreLogger STATIC shared/logger/leanstore_logger.cpp)
target_link_libraries(LeanStoreLogger PUBLIC Logger)

# rocksdb_typeinfo also provides third-party libs for rocksdb
add_library(rocksdb_typeinfo STATIC shared/rocksdb.cpp)
target_link_libraries(rocksdb_typeinfo PUBLIC rocksdb dl z libbz2.a lz4 snappy zstd uring)

add_library(RocksDB STATIC shared/RocksDB.cpp)
target_link_libraries(RocksDB PUBLIC leanstore rocksdb_typeinfo)

add_library(RocksDBLogger STATIC shared/logger/rocksdb_logger.cpp)
target_link_libraries(RocksDBLogger PUBLIC Logger RocksDB)

add_executable(geo_btree geo/executable_leanstore.cpp)
target_link_libraries(geo_btree PUBLIC LeanStoreLogger RocksDB Threads::Threads )

add_executable(geo_lsm geo/executable_rocksdb.cpp)
target_link_libraries(geo_lsm PUBLIC RocksDBLogger Threads::Threads)

# Add the executable for the workload test
add_executable(job_workload_test job/workload.cpp)

# Link libraries to the test executable
target_link_libraries(job_workload_test PUBLIC
    Logger              # For the base Logger class
    GTest::gtest        # Google Test framework (assumes this target is defined by your root CMake)
    gflags              # Use target name from lib/gflags.cmake
    Threads::Threads    # For multi-threading support
)

# Discover tests in the executable
include(GoogleTest)
gtest_discover_tests(job_workload_test)