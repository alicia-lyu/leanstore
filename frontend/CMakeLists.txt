add_library(PremergedJoinStatsLogger STATIC shared/merge-join/premerged_join_stats_logger.cpp)
target_link_libraries(PremergedJoinStatsLogger PUBLIC leanstore)

add_library(HashJoin STATIC shared/merge-join/hash_join.cpp)
target_link_libraries(HashJoin PUBLIC leanstore)

# loggers also provide shared libs and directories
add_library(Logger STATIC shared/logger/logger.cpp)
target_link_libraries(Logger PUBLIC PremergedJoinStatsLogger HashJoin)
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