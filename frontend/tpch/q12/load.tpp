// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading may use a manual two-pointer merge;
//                              query-time joins must use BinaryMergeJoin /
//                              PremergedJoin / HashJoin (shared JoinState).
//   §4 (per-query view rationale, Q12 bullet) — unfiltered, predicate hoisted
//
// Template method bodies for Q12Workload<Backend> load / size / ctor,
// plus the free function populate_q12_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// View loading: materialise the ORDERS x LINEITEM join into a flat adapter.
//
// Manual two-pointer merge over the two base scanners. Both tables are sorted
// by orderkey (orders by {o_orderkey}, lineitem by {l_orderkey, l_linenumber}).
// Each iteration emits one joined_ol_t row per lineitem (1:N join).
//
// We bypass BinaryMergeJoin here because its hierarchical-key wildcard
// semantics (linenumber==0 matches any linenumber) cause the join-state
// machine to emit only ~1 row per order group instead of N. A manual loop
// is simpler and produces the correct cardinality.

template <typename Backend>
static void populate_q12_view(
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<q12_pipeline_view_t>& view)
{
   auto orders_scanner = orders.getScanner();
   auto lineitem_scanner = lineitem.getScanner();

   auto cur_order = orders_scanner->next();

   while (auto cur_line = lineitem_scanner->next()) {
      // Advance orders to the matching orderkey. Since both streams are
      // sorted by orderkey, we never need to backtrack.
      while (cur_order && cur_order->first.o_orderkey < cur_line->first.l_orderkey) {
         cur_order = orders_scanner->next();
      }
      if (!cur_order || cur_order->first.o_orderkey != cur_line->first.l_orderkey) {
         continue;  // orphan lineitem — should not happen in well-formed data
      }

      joined_ol_t row(cur_order->second, cur_line->second);
      joined_ol_t::Key key(cur_order->first, cur_line->first);
      view.insert(key, row);
   }
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q12Workload<Backend>::Q12Workload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<q12_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol)
    : tpch(tpch),
      orders(orders),
      lineitem(lineitem),
      ol(orders, lineitem, merged_ol),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q12Workload<Backend>::load()
{
   tpch.load();
   populate_q12_view<Backend>(orders, lineitem, pipeline_view);
   ol.populate_merged();
}

template <typename Backend>
double Q12Workload<Backend>::get_size() const
{
   switch (FLAGS_storage_structure) {
      case 1: case 4: return 0.0;
      case 2: return pipeline_view.size();
      case 3: return ol.get_merged_size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q12
