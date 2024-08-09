#pragma once
#include "ProfilingTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace profiling
{
using namespace storage;
class DTTable : public ProfilingTable
{
  private:
   string dt_name;
   DTID dt_id;
   BufferManager& bm;

  public:
   DTTable(BufferManager& bm);
   // -------------------------------------------------------------------------------------
   virtual std::string getName();
   virtual void open();
   virtual void next();
   // Sum across data types during a certain time period (after last next() call)
   u64 getSum(std::string column_name);
};
}  // namespace profiling
}  // namespace leanstore
