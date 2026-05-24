# DBToaster refresh_sales baseline — copy-adapted from geodb-dbtoaster.
# Builds one target `refresh_sales` from main.cpp, which #includes the
# DBToaster-generated header refresh_sales.hpp (produced by `make refresh_sales.hpp`).
cmake_minimum_required(VERSION 3.10)
project(RefreshSalesDBT CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_COMPILER "/usr/bin/g++")

# DBToaster 2.3 prebuilt C++ runtime (installed by Phase 0 into /opt/dbtoaster).
# The generated header pulls in libdbtoaster headers; the dbt runtime is built
# with the old libstdc++ ABI, so match it.
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
set(DBT_LIB_DIR /opt/dbtoaster/lib/dbt_c++)

add_executable(refresh_sales main.cpp)
target_link_options(refresh_sales PRIVATE "-no-pie")
target_include_directories(refresh_sales PRIVATE ${DBT_LIB_DIR})
target_link_directories(refresh_sales PRIVATE ${DBT_LIB_DIR})
target_link_libraries(refresh_sales PRIVATE dbtoaster)

set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_BUILD_TYPE Release)
