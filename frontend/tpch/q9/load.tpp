// Template method bodies for Q9Workload<Backend> load / size / ctor.
// All bodies are TODO stubs. Each comment cites the CLAUDE.md section
// or infrastructure header the implementer should consult.

#pragma once

#include <gflags/gflags.h>

DECLARE_int32(storage_structure);

namespace tpch::q9
{

template <typename Backend>
Q9Workload<Backend>::Q9Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<nation_t>& nation,
    typename Backend::template Adapter<supplier_t>& supplier,
    typename Backend::template Adapter<part_t>& part,
    typename Backend::template Adapter<partsupp_t>& partsupp,
    typename Backend::template Adapter<q9_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : tpch(tpch),
      ol(orders, lineitem, merged_ol),
      nation(nation),
      supplier(supplier),
      part(part),
      partsupp(partsupp),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
   // TODO(skeleton): Wire gflags (name_pattern / color substring)
   // into `params` instead of always using defaults. See:
   //   - frontend/tpch/q9/CLAUDE.md §Substitution Parameters
   //   - frontend/tpch/q9/views.hpp Params::defaults()
}

template <typename Backend>
void Q9Workload<Backend>::load()
{
   // TODO(skeleton): Call tpch.load() for base tables (all structures), then
   // dispatch on FLAGS_storage_structure to build secondary structures:
   //   2 -> ol.populate_pipeline_view([&](const joined_ol_t& j) { pipeline_view.insert(...) })
   //   3 -> ol.populate_merged_ol()
   //   1, 4 -> base tables only (no secondary structure)
   // Note: NATION, SUPPLIER, PART, PARTSUPP are loaded by tpch.load() as
   // base tables; they are not inserted into any merged index.
   // See: frontend/tpch/q12/CLAUDE.md §Stages x Options, Stage 1: Loading
   //      (Q9 follows the same OL load pattern; dimension tables are base-only).
}

template <typename Backend>
double Q9Workload<Backend>::get_size() const
{
   // TODO(skeleton): Dispatch on FLAGS_storage_structure:
   //   1, 4 -> 0.0 (base tables only, no secondary structure)
   //   2    -> pipeline_view.size()
   //   3    -> ol.get_merged_size()
   // See: frontend/tpch/q9/CLAUDE.md §Merged Index.
   return 0;
}

}  // namespace tpch::q9
