add_library(Logger STATIC tpc-h/logger.cpp)
target_link_libraries(Logger leanstore Threads::Threads)
target_include_directories(Logger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(LeanStoreLogger STATIC tpc-h/leanstore_logger.cpp)
target_link_libraries(LeanStoreLogger Logger leanstore Threads::Threads)
target_include_directories(LeanStoreLogger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(RocksDBLogger STATIC tpc-h/rocksdb_logger.cpp)
target_link_libraries(RocksDBLogger Logger leanstore Threads::Threads rocksdb)
target_include_directories(RocksDBLogger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(RocksDB STATIC shared/RocksDB.cpp)
target_link_libraries(RocksDB rocksdb zstd uring leanstore)
target_include_directories(RocksDB PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_library(rocksdb_executable STATIC shared/rocksdb.cpp)
target_link_libraries(rocksdb_executable RocksDB)
target_include_directories(rocksdb_executable PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_join tpc-h/basic_join/executable.cpp)
target_link_libraries(basic_join LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_join PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_group tpc-h/basic_group/executable.cpp)
target_link_libraries(basic_group LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_group PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_group_variant tpc-h/basic_group/executable_variant.cpp)
target_link_libraries(basic_group_variant LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_group_variant PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_join_group tpc-h/basic_join_group/executable.cpp)
target_link_libraries(basic_join_group LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_join_group PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(geo_join tpc-h/geo_join/executable.cpp)
target_link_libraries(geo_join rocksdb_executable LeanStoreLogger leanstore Threads::Threads)
target_include_directories(geo_join PRIVATE ${SHARED_INCLUDE_DIRECTORY})