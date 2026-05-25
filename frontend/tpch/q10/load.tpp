// Template method bodies for Q10Workload<Backend>: ctor, load, get_size.
// Operator-translation reference: see ../OPERATORS.md §3 op 4.

#pragma once

#include <gflags/gflags.h>
#include <stdexcept>

#include "../q10_family/view_loaders.hpp"

DECLARE_int32(storage_structure);
DECLARE_string(q10_view_variant);

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
    typename Backend::template Adapter<q10_pipeline_view_preagg_t>& pipeline_view_preagg,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem,
    typename Backend::template MergedAdapter<customer_coli_t, orders_acol_t>& acol)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      nation(nation),
      col(customer, orders, lineitem, merged_col, split_orders, split_lineitem),
      pipeline_view(pipeline_view),
      pipeline_view_preagg(pipeline_view_preagg),
      acol(acol),
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
   // S2 variant A (per-lineitem) + variant B (per-order pre-agg). Both run
   // after populate_merged — both loaders consume the col MI. One image
   // carries both so --q10_view_variant selects the A/B arm at query time.
   populate_q10_view<Backend>(col.merged_adapter(), pipeline_view,
                               nation, stats);
   populate_q10_view_preagg<Backend>(col.merged_adapter(), pipeline_view_preagg,
                                      nation, stats);
   // S4: base tables only — nothing to populate.
   // S5: aCOL MI (customer_coli_t + orders_acol_t). Built from the COL MI
   // (same per-order returned-revenue accumulation as the preagg view) plus a
   // customer base scan for the full customer records.
   populate_q10_acol<Backend>(col.merged_adapter(), customer, acol,
                              nation, stats);
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
      // S2 reports the size of the active view variant (A/B).
      case 2: return base + (FLAGS_q10_view_variant == "preagg"
                                 ? pipeline_view_preagg.size()
                                 : pipeline_view.size());
      case 3: return base + col.get_merged_size();
      case 4: return base;
      case 5: return base + acol.size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q10
