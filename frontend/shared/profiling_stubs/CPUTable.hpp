// Stub CPUTable header for macOS (ROCKSDB_ONLY) builds.
// Mirrors the interface from backend/leanstore/profiling/tables/CPUTable.hpp
// without pulling in BufferManager.hpp (which requires <libaio.h>).
#pragma once
#include "leanstore/profiling/tables/ProfilingTable.hpp"

namespace leanstore
{
namespace profiling
{
class CPUTable : public ProfilingTable
{
  public:
   std::unordered_map<std::string, double> workers_agg_events, pp_agg_events, ww_agg_events;
   std::unordered_map<std::string, std::unordered_map<std::string, double>> workers_events;
   virtual std::string getName();
   virtual void open();
   virtual void next();
};
}  // namespace profiling
}  // namespace leanstore
