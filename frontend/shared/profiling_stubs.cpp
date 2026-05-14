// Stub implementations of leanstore profiling tables for macOS (ROCKSDB_ONLY) builds.
// On Linux these come from backend/leanstore/profiling/tables/*.cpp via libleanstore.a.
// The stubs provide no-op behavior since CPU perf counters are Linux-specific.

#include "profiling_stubs/CPUTable.hpp"
#include "profiling_stubs/ConfigsTable.hpp"
#include "leanstore/Config.hpp"

namespace leanstore
{
namespace profiling
{

// --- CPUTable stubs ---
std::string CPUTable::getName() { return "cpu"; }
void CPUTable::open() {}
void CPUTable::next() { clear(); }

// --- ConfigsTable stubs ---
std::string ConfigsTable::getName() { return "configs"; }
void ConfigsTable::add(string name, string value)
{
   columns.emplace(name, [value](Column& col) { col << value; });
}
void ConfigsTable::open()
{
   columns.emplace("c_tag", [](Column& col) { col << FLAGS_tag; });
   columns.emplace("c_worker_threads", [](Column& col) { col << FLAGS_worker_threads; });
   columns.emplace("c_dram_gib", [](Column& col) { col << FLAGS_dram_gib; });
   columns.emplace("c_isolation_level", [](Column& col) { col << FLAGS_isolation_level; });
   for (auto& c : columns) {
      c.second.generator(c.second);
   }
}
u64 ConfigsTable::hash()
{
   std::stringstream config_concatenation;
   for (const auto& c : columns) {
      if (!c.second.values.empty())
         config_concatenation << c.second.values[0];
   }
   return std::hash<std::string>{}(config_concatenation.str());
}
void ConfigsTable::next() { return; }

}  // namespace profiling
}  // namespace leanstore
