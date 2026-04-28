// Template method bodies for Q12Workload<Backend> load / size / ctor.
// All bodies are TODO stubs. Each comment cites the CLAUDE.md section
// or infrastructure header the implementer should consult.

#pragma once

#include <gflags/gflags.h>

DECLARE_int32(storage_structure);

namespace tpch::q12
{

template <typename Backend>
Q12Workload<Backend>::Q12Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : tpch(tpch),
      ol(orders, lineitem, merged_ol),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
   // TODO(skeleton): Wire gflags (shipmode1, shipmode2, receiptdate_lo/hi)
   // into `params` instead of always using defaults. See:
   //   - frontend/tpch/q12/CLAUDE.md §Substitution Parameters
   //   - frontend/tpch/q12/views.hpp Params::defaults()
}

template <typename Backend>
void Q12Workload<Backend>::load()
{
   // Base tables are loaded by the caller via tpch.load() before this method.
   // Secondary structures are owned by the pipeline; dispatch here is a
   // one-liner per the Pipeline Convention (frontend/tpch/CLAUDE.md).
   switch (FLAGS_storage_structure) {
      case 1: case 4: break;                         // base tables only
      case 2: ol.populate_view(pipeline_view); break;
      case 3: ol.populate_merged(); break;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

template <typename Backend>
double Q12Workload<Backend>::get_size() const
{
   switch (FLAGS_storage_structure) {
      case 1: case 4:
         // TODO(skeleton): return tpch.get_size() or sum relevant base
         // table adapters once TPCHWorkload exposes a size() method.
         return 0.0;
      case 2: return ol.get_view_size(pipeline_view);
      case 3: return ol.get_merged_size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q12
