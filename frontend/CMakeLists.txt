add_library(LeanStoreLogger OBJECT tpc-h/leanstore_logger.cpp tpc-h/leanstore_logger.hpp)
target_link_libraries(LeanStoreLogger leanstore Threads::Threads)
target_include_directories(LeanStoreLogger PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_join tpc-h/basic_join/executable.cpp)
target_link_libraries(basic_join LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_join PRIVATE ${SHARED_INCLUDE_DIRECTORY})

add_executable(basic_group tpc-h/basic_group/executable.cpp)
target_link_libraries(basic_group LeanStoreLogger leanstore Threads::Threads)
target_include_directories(basic_group PRIVATE ${SHARED_INCLUDE_DIRECTORY})