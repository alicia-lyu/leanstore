#pragma once

// Shared ORDERS x LINEITEM substrate used by Q12, Q3, and Q9.
//
// The shared scope is intentionally narrow: this class owns
// `MI[0] = MergedIndex(orders_t, lineitem_t)` interleaved by orderkey, and
// exposes a load + size pair for it. It also holds non-owning references to
// the two base-table adapters (orders, lineitem) so the dual-write replay
// can scan them.
//
// What this class does NOT own:
//   - Any view (per-query row type — view loading and sizing live in
//     `q{N}/load.tpp` once the per-query refactor lands).
//   - Any operator driver (BinaryMergeJoin / HashJoin / PremergedJoin) —
//     those execute query-specific post-join logic and are instantiated
//     in `q{N}/query.tpp`.

#include "backend.hpp"
#include "views_ol.hpp"
#include "tpch_tables.hpp"

namespace tpch
{

template <typename Backend>
class OrdersLineitemPipeline
{
   // References are non-owning; lifetime is managed by the per-query workload.
   typename Backend::template Adapter<orders_t>&                        orders;
   typename Backend::template Adapter<lineitem_t>&                      lineitem;
   typename Backend::template MergedAdapter<orders_t, lineitem_t>&      merged_ol;

  public:
   OrdersLineitemPipeline(
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template MergedAdapter<orders_t, lineitem_t>& merged_ol);

   // Dual-write replay: scans base tables and inserts each orders_t /
   // lineitem_t record into merged_ol as well.
   // See: OPERATORS.md §4 (query-agnostic dual-write).
   void populate_merged();

   // Returns the estimated size of merged_ol in MiB.
   double get_merged_size() const;

   // Returns a scanner over MI[0], typed for the ol_sort_key_t / joined_ol_t
   // join pair. Used by query_by_merged in q{N}/query.tpp.
   auto merged_scanner()
   {
      return merged_ol.template getScanner<ol_sort_key_t, joined_ol_t>();
   }
};

}  // namespace tpch

#include "ol_pipeline.tpp"  // IWYU pragma: keep
