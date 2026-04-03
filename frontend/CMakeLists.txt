if (APPLE)
    # =========================================================================
    # macOS (ROCKSDB_ONLY) build: only geo_lsm, no leanstore backend library
    # =========================================================================

    # Backend headers are still needed for compilation (Config.hpp, KVInterface.hpp, etc.)
    # but we don't link the backend library — standalone .cpp files provide the symbols.
    set(BACKEND_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/backend)

    # --- Standalone libraries replacing leanstore backend symbols ---
    add_library(config_standalone STATIC shared/config_standalone.cpp)
    target_link_libraries(config_standalone PUBLIC gflags)
    target_include_directories(config_standalone PUBLIC ${BACKEND_INCLUDE_DIR} ${SHARED_INCLUDE_DIRECTORY})

    add_library(jumpmu_standalone STATIC shared/jumpmu_standalone.cpp)
    target_include_directories(jumpmu_standalone PUBLIC ${BACKEND_INCLUDE_DIR} ${SHARED_INCLUDE_DIRECTORY})

    add_library(random_standalone STATIC shared/random_standalone.cpp)
    target_include_directories(random_standalone PUBLIC ${BACKEND_INCLUDE_DIR} ${SHARED_INCLUDE_DIRECTORY})

    add_library(profiling_stubs STATIC shared/profiling_stubs.cpp)
    target_compile_definitions(profiling_stubs PUBLIC ROCKSDB_ONLY)
    target_include_directories(profiling_stubs PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}  # for shared/profiling_stubs/ headers
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(profiling_stubs PUBLIC config_standalone)

    # --- RocksDB adapter (includes method implementations from RocksDB.cpp) ---
    add_library(RocksDB STATIC shared/RocksDB.cpp)
    target_compile_definitions(RocksDB PUBLIC ROCKSDB_ONLY)
    target_include_directories(RocksDB PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}  # for shared/ headers
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(RocksDB PUBLIC rocksdb config_standalone jumpmu_standalone)

    # --- PremergedJoinStatsLogger (no leanstore dependency) ---
    add_library(PremergedJoinStatsLogger STATIC shared/merge-join/premerged_join_stats_logger.cpp)
    target_compile_definitions(PremergedJoinStatsLogger PUBLIC ROCKSDB_ONLY)
    target_include_directories(PremergedJoinStatsLogger PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(PremergedJoinStatsLogger PUBLIC config_standalone)

    # --- Logger ---
    add_library(Logger STATIC shared/logger/logger.cpp)
    target_compile_definitions(Logger PUBLIC ROCKSDB_ONLY)
    target_include_directories(Logger PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(Logger PUBLIC PremergedJoinStatsLogger profiling_stubs tabluate)

    # --- RocksDBLogger ---
    add_library(RocksDBLogger STATIC shared/logger/rocksdb_logger.cpp)
    target_compile_definitions(RocksDBLogger PUBLIC ROCKSDB_ONLY)
    target_include_directories(RocksDBLogger PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(RocksDBLogger PUBLIC Logger RocksDB)

    # --- geo_lsm executable ---
    add_executable(geo_lsm geo/executable_rocksdb.cpp)
    target_compile_definitions(geo_lsm PRIVATE ROCKSDB_ONLY)
    target_include_directories(geo_lsm PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${BACKEND_INCLUDE_DIR}
        ${SHARED_INCLUDE_DIRECTORY}
    )
    target_link_libraries(geo_lsm PUBLIC
        RocksDBLogger
        jumpmu_standalone
        random_standalone
        Threads::Threads
    )

else()
    # =========================================================================
    # Linux build: full leanstore + RocksDB (original behavior)
    # =========================================================================

    add_library(PremergedJoinStatsLogger STATIC shared/merge-join/premerged_join_stats_logger.cpp)
    target_link_libraries(PremergedJoinStatsLogger PUBLIC leanstore)

    # loggers also provide shared libs and directories
    add_library(Logger STATIC shared/logger/logger.cpp)
    target_link_libraries(Logger PUBLIC PremergedJoinStatsLogger)
    target_include_directories(Logger PUBLIC ${SHARED_INCLUDE_DIRECTORY})

    add_library(LeanStoreLogger STATIC shared/logger/leanstore_logger.cpp)
    target_link_libraries(LeanStoreLogger PUBLIC Logger)

    # rocksdb_typeinfo also provides third-party libs for rocksdb
    add_library(rocksdb_typeinfo STATIC shared/rocksdb_typeinfo.cpp)
    target_link_libraries(rocksdb_typeinfo PUBLIC rocksdb dl z libbz2.a lz4 snappy zstd uring)

    add_library(RocksDB STATIC shared/RocksDB.cpp)
    target_link_libraries(RocksDB PUBLIC leanstore rocksdb_typeinfo)

    add_library(RocksDBLogger STATIC shared/logger/rocksdb_logger.cpp)
    target_link_libraries(RocksDBLogger PUBLIC Logger RocksDB)

    add_executable(geo_btree geo/executable_leanstore.cpp)
    target_link_libraries(geo_btree PUBLIC LeanStoreLogger RocksDB Threads::Threads )

    add_executable(geo_lsm geo/executable_rocksdb.cpp)
    target_link_libraries(geo_lsm PUBLIC RocksDBLogger Threads::Threads)

endif()
