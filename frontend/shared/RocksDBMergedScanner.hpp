#pragma once

#include "RocksDBMergedAdapter.hpp"
#include "MergedScanner.hpp"

template <typename JK, typename JR, typename... Records>
struct RocksDBMergedScanner : public MergedScanner<JK, JR, Records...>
{

};