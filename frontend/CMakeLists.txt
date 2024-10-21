if(NOT DEFINED INCLUDED_COLUMNS)
    set(INCLUDED_COLUMNS 1)
endif()

add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)

# add_executable(bulk_loading mix/bulk_loading.cpp)
# target_link_libraries(bulk_loading leanstore Threads::Threads)
# target_include_directories(bulk_loading PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(ycsb ycsb/ycsb.cpp)
# target_link_libraries(ycsb leanstore Threads::Threads)
# target_include_directories(ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(deterministic ycsb/deterministic.cpp)
# target_link_libraries(deterministic leanstore Threads::Threads)
# target_include_directories(deterministic PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads)
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(graveyard micro-benchmarks/graveyard.cpp)
# target_link_libraries(graveyard leanstore Threads::Threads)
# target_include_directories(graveyard PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(queue micro-benchmarks/queue.cpp)
# target_link_libraries(queue leanstore Threads::Threads)
# target_include_directories(queue PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(minimal_example minimal-example/main.cpp)
# target_link_libraries(minimal_example leanstore Threads::Threads)
# target_include_directories(minimal_example PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(scaled_example scaled-example/main.cpp)
# target_link_libraries(scaled_example leanstore Threads::Threads)
# target_include_directories(scaled_example PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(rocksdb_tpcc tpc-c/rocksdb_tpcc.cpp shared/rocksdb.cpp)
# target_link_libraries(rocksdb_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
# target_include_directories(rocksdb_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(rocksdb_tpcc PUBLIC ROCKSDB_ADAPTER)

add_executable(rocksdb_join_tpcc join-exec/rocksdb_join_tpcc.cpp shared/rocksdb.cpp)
target_link_libraries(rocksdb_join_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
target_include_directories(rocksdb_join_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(rocksdb_join_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

add_executable(rocksdb_merged_tpcc join-exec/rocksdb_merged_tpcc.cpp shared/rocksdb.cpp)
target_link_libraries(rocksdb_merged_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
target_include_directories(rocksdb_merged_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(rocksdb_merged_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

add_executable(rocksdb_base_tpcc join-exec/rocksdb_base_tpcc.cpp shared/rocksdb.cpp)
target_link_libraries(rocksdb_base_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
target_include_directories(rocksdb_base_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(rocksdb_base_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(rocksdb_ycsb ycsb/rocksdb_ycsb.cpp shared/rocksdb.cpp)
# target_link_libraries(rocksdb_ycsb leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
# target_include_directories(rocksdb_ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(rocksdb_ycsb PUBLIC ROCKSDB_ADAPTER)
# # sudo apt-get install libsnappy-dev  zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev

# add_executable(wiredtiger_tpcc tpc-c/wiredtiger_tpcc.cpp)
# target_link_libraries(wiredtiger_tpcc leanstore wiredtiger Threads::Threads dl z libbz2.a lz4 snappy)
# target_include_directories(wiredtiger_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(wiredtiger_ycsb ycsb/wiredtiger_ycsb.cpp)
# target_link_libraries(wiredtiger_ycsb leanstore wiredtiger Threads::Threads dl z libbz2.a lz4 snappy)
# target_include_directories(wiredtiger_ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# # prefer https://source.wiredtiger.com/10.0.0/build-posix.html over sudo apt-get install wiredtiger libwiredtiger-dev


# add_executable(lmdb_tpcc tpc-c/lmdb_tpcc.cpp)
# target_link_libraries(lmdb_tpcc leanstore lmdb Threads::Threads)
# target_include_directories(lmdb_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(lmdb_ycsb ycsb/lmdb_ycsb.cpp)
# target_link_libraries(lmdb_ycsb leanstore lmdb Threads::Threads)
# target_include_directories(lmdb_ycsb PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# # sudo apt-get install liblmdb-dev
add_executable(join_tpcc join-exec/join_tpcc.cpp)
target_link_libraries(join_tpcc leanstore Threads::Threads)
target_include_directories(join_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(join_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

add_executable(merged_tpcc join-exec/merged_tpcc.cpp)
target_link_libraries(merged_tpcc leanstore Threads::Threads)
target_include_directories(merged_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(merged_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

add_executable(base_tpcc join-exec/base_tpcc.cpp)
target_link_libraries(base_tpcc leanstore Threads::Threads)
target_include_directories(base_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
target_compile_definitions(base_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})