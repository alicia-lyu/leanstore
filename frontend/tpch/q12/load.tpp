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
   // TODO(skeleton): Call tpch.load() for base tables (all structures), then
   // dispatch on FLAGS_storage_structure to build secondary structures:
   //   2 -> ol.populate_pipeline_view([&](const joined_ol_t& j) { pipeline_view.insert(...) })
   //   3 -> ol.populate_merged_ol()
   //   4 -> base tables only (no secondary structure)
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Stage 1: Loading.
}

template <typename Backend>
double Q12Workload<Backend>::get_size() const
{
   // TODO(skeleton): Dispatch on FLAGS_storage_structure:
   //   1, 4 -> 0.0 (base tables only, no secondary structure)
   //   2    -> pipeline_view.size()
   //   3    -> ol.get_merged_size()
   // See: frontend/tpch/q12/CLAUDE.md §Storage Structure Options.
   return 0;
}

}  // namespace tpch::q12
