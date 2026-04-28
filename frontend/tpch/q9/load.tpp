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
   // Base tables are loaded by the caller via tpch.load() before this method.
   // NATION, SUPPLIER, PART, PARTSUPP are base-only; only the OL pipeline
   // has secondary structures. Dispatch per the Pipeline Convention
   // (frontend/tpch/CLAUDE.md).
   switch (FLAGS_storage_structure) {
      case 1: case 4: break;                         // base tables only
      case 2: ol.populate_view(pipeline_view); break;
      case 3: ol.populate_merged(); break;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

template <typename Backend>
double Q9Workload<Backend>::get_size() const
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

}  // namespace tpch::q9
