// Template method bodies for Q10Workload<Backend>: ctor, load, get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

#include "../q10_family/view_loaders.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q10
{

template <typename Backend>
Q10Workload<Backend>::Q10Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_t>&   lineitem,
    typename Backend::template Adapter<nation_t>&     nation,
    typename Backend::template Adapter<q10_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      nation(nation),
      col(customer, orders, lineitem, merged_col, split_orders, split_lineitem),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

// Populate every secondary unconditionally so a single DB image can serve
// all four --storage_structure query-time variants (PLAYBOOK §3.6 PITFALL
// — `load()` populating only one secondary).
//
// S2 view loader uses Pattern B (Decision E1): populate_q10_view reuses
// the S3 Q10GroupWalkVisitor in ViewLoad mode with parameterised filters
// dropped and a forwarding sink into pipeline_view. Same walker code
// drives both — view-vs-walker drift is structurally impossible.
template <typename Backend>
void Q10Workload<Backend>::load()
{
   tpch.load();
   col.populate_split();    // S1: custkey-sorted split indexes
   col.populate_merged();   // S3: COL merged index
   // S2: must run after populate_merged — the loader consumes col MI.
   populate_q10_view<Backend>(col.merged_adapter(), pipeline_view,
                               nation, stats);
   // S4: base tables only — nothing to populate.
}

template <typename Backend>
double Q10Workload<Backend>::get_size() const
{
   // Include base + active secondary so all four structures are comparable.
   // NATION is in the base set (used by every storage structure for the
   // per-customer INL lookup at emit, Decision D6).
   double base = customer.size() + orders.size() + lineitem.size()
               + nation.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + col.get_split_size();
      case 2: return base + pipeline_view.size();
      case 3: return base + col.get_merged_size();
      case 4: return base;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q10
