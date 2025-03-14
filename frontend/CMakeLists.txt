# if(NOT DEFINED INCLUDED_COLUMNS)
#     set(INCLUDED_COLUMNS 1)
# endif()

# add_executable(frontend frontend.cpp)
# add_dependencies(frontend leanstore)
# target_link_libraries(frontend leanstore)

# add_executable(tpcc tpc-c/tpcc.cpp)
# target_link_libraries(tpcc leanstore Threads::Threads)
# target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# add_executable(rocksdb_join_tpcc join-exec/rocksdb_join_tpcc.cpp shared/rocksdb.cpp)
# target_link_libraries(rocksdb_join_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
# target_include_directories(rocksdb_join_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(rocksdb_join_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(rocksdb_merged_tpcc join-exec/rocksdb_merged_tpcc.cpp shared/rocksdb.cpp)
# target_link_libraries(rocksdb_merged_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
# target_include_directories(rocksdb_merged_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(rocksdb_merged_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(rocksdb_base_tpcc join-exec/rocksdb_base_tpcc.cpp shared/rocksdb.cpp)
# target_link_libraries(rocksdb_base_tpcc leanstore rocksdb Threads::Threads dl z libbz2.a lz4 snappy zstd uring)
# target_include_directories(rocksdb_base_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(rocksdb_base_tpcc PUBLIC ROCKSDB_ADAPTER INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(join_tpcc join-exec/join_tpcc.cpp)
# target_link_libraries(join_tpcc rocksdb leanstore Threads::Threads)
# target_include_directories(join_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(join_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(merged_tpcc join-exec/merged_tpcc.cpp)
# target_link_libraries(merged_tpcc rocksdb leanstore Threads::Threads)
# target_include_directories(merged_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(merged_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

# add_executable(base_tpcc join-exec/base_tpcc.cpp)
# target_link_libraries(base_tpcc rocksdb leanstore Threads::Threads)
# target_include_directories(base_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
# target_compile_definitions(base_tpcc PUBLIC INCLUDE_COLUMNS=${INCLUDED_COLUMNS})

add_executable(basic_join tpc-h/basic_join.cpp)
target_link_libraries(basic_join leanstore Threads::Threads)
target_include_directories(basic_join PRIVATE ${SHARED_INCLUDE_DIRECTORY})