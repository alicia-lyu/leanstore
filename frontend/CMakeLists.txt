# loggers also provide shared libs

add_library(Logger STATIC tpc-h/logger.cpp)
target_link_libraries(Logger PUBLIC leanstore Threads::Threads)
target_include_directories(Logger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(LeanStoreLogger STATIC tpc-h/leanstore_logger.cpp)
target_link_libraries(LeanStoreLogger PUBLIC Logger)
target_include_directories(LeanStoreLogger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# rocksdb_typeinfo also provides third-party libs for rocksdb
add_library(rocksdb_typeinfo STATIC shared/rocksdb.cpp)
target_link_libraries(rocksdb_typeinfo PUBLIC rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
target_include_directories(rocksdb_typeinfo PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(RocksDB STATIC shared/RocksDB.cpp)
target_link_libraries(RocksDB PUBLIC leanstore Threads::Threads rocksdb_typeinfo)
target_include_directories(RocksDB PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(RocksDBLogger STATIC tpc-h/rocksdb_logger.cpp)
target_link_libraries(RocksDBLogger PUBLIC Logger RocksDB)
target_include_directories(RocksDBLogger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(geo_load tpc-h/geo_join/load.cpp)
target_link_libraries(geo_load PUBLIC Logger)
target_include_directories(geo_load PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(geo_join tpc-h/geo_join/executable.cpp)
target_link_libraries(geo_join PUBLIC geo_load LeanStoreLogger RocksDB)
target_include_directories(geo_join PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(geo_lsm tpc-h/geo_join/executable_rocksdb.cpp)
target_link_libraries(geo_lsm PUBLIC geo_load RocksDBLogger)
target_include_directories(geo_lsm PRIVATE ${SHARED_INCLUDE_DIRECTORY})