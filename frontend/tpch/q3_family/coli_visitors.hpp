#pragma once

// Q3FamilyVisitor — CRTP base for group-walk visitors over the COL and COLI
// merged indexes.
//
// Implements the C×O×L core shared by Q3 and Q3I:
//   on_customer  — gates by c_mktsegment; SkipGroup on miss
//   on_order     — gates by o_orderdate; flushes the previous order register
//                  when a new order boundary is crossed; SkipOrder on date
//                  miss; SkipGroup if the first-order admit check fails.
//   on_lineitem  — accumulates revenue via LineitemRevenueAccumulator;
//                  always returns Continue
//   on_group_end — flushes the last open order for the group; resets state
//
// Walker skip protocol: every on_<record> hook returns tpch::WalkAction.
// See ../tpch_family/walk_action.hpp for the contract.  No wants_skip_*()
// poll hooks (retired 2026-05-09; PLAYBOOK anti-pattern #19).
//
// CRTP customisation points (three total; all have base defaults):
//
//   bool per_order_admit_check(const OrdersType&)
//       Called at the first on_order for a custkey group, BEFORE the
//       orderdate filter. Return false to reject the entire remaining OL
//       sub-tree (the base on_order then returns SkipGroup to the walker).
//       Base returns true. Q3I overrides to gate on cust_open_due > threshold.
//
//   void extra_emit_fields(AggRow&)
//       Called just before a row is pushed into `out`, after all base fields
//       have been filled.  Base is a no-op.  Q3I fills cust_open_due.
//
//   void on_record_visited_hook()
//       Called from on_record_visited() after bumping the base counter, to
//       let derived classes bump additional counters.  Base is a no-op.
//
// Template parameters:
//   Derived      — the concrete visitor class (CRTP pattern)
//   Params       — per-query Params struct; must carry `mktsegment`,
//                  `orderdate`, `shipdate` fields
//   LineitemType — the lineitem record type dispatched by the walker
//                  (lineitem_coli_t for COLI, lineitem_col_t for COL)
//   AggRow       — output row type; must be default-constructible and carry
//                  the four base fields from q3_agg_row_base_t
//   Stats        — stats struct type; may be nullptr at runtime
//
// Usage:
//   struct MyVisitor : Q3FamilyVisitor<MyVisitor, Params, lineitem_col_t,
//                                      q3_agg_row_t, MyStats> { ... };

#include <string_view>
#include <vector>

#include "../tpch_family/views_coli.hpp"
#include "../tpch_family/walk_action.hpp"
#include "accumulators.hpp"

namespace tpch::q3_family
{

template <typename Derived, typename Params, typename LineitemType,
          typename AggRow, typename Stats>
struct Q3FamilyVisitor {
   const Params&         params;
   std::vector<AggRow>&  out;
   Stats*                stats = nullptr;  // optional — bumped by walker hooks

   // -------------------------------------------------------------------------
   // Per-group accumulators (factored — S1 reuses these via scanner-wrappers).

   LineitemRevenueAccumulator<Params> rev;

   // -------------------------------------------------------------------------
   // Per-custkey gates.

   bool mktsegment_ok = false;  // set by on_customer; gate for the entire group
   bool order_admitted = false;  // set by per_order_admit_check on first order

   // -------------------------------------------------------------------------
   // Current open order register: reset per order, flushed at the next order
   // boundary or at on_group_end.

   bool      have_open_order  = false;
   Integer   cur_orderkey     = 0;
   Timestamp cur_orderdate    = 0;
   Integer   cur_shippriority = 0;

   // -------------------------------------------------------------------------
   // Emit the current order and reset per-order state.
   // Called at each on_order boundary and at on_group_end.
   void flush_order()
   {
      if (!have_open_order) return;
      have_open_order = false;
      // Only emit when at least one lineitem passed the shipdate filter.
      // Orders whose lineitems all fail the shipdate predicate have no
      // matching rows in the SQL result.
      if (rev.revenue > Numeric(0)) {
         AggRow row;
         row.o_orderkey     = cur_orderkey;
         row.revenue        = rev.revenue;
         row.o_orderdate    = cur_orderdate;
         row.o_shippriority = cur_shippriority;
         // CRTP hook: derived fills in query-specific extra columns.
         static_cast<Derived*>(this)->extra_emit_fields(row);
         out.push_back(row);
      }
      rev.reset();
   }

   // -------------------------------------------------------------------------
   // Walker hooks — both coli_group_walk and col_group_walk dispatch to these.

   // on_record_visited: bumps the mi_records_visited counter (when stats != nullptr).
   void on_record_visited()
   {
      if (stats) stats->mi_records_visited++;
      static_cast<Derived*>(this)->on_record_visited_hook();
   }

   void on_group_skipped(Integer /*ck*/)
   {
      if (stats) stats->mi_groups_skipped++;
   }

   // Gate: c_mktsegment must match params.mktsegment.
   // Returns SkipGroup on miss; on_group_end is still called.
   ::tpch::WalkAction on_customer(Integer /*ck*/, const customer_coli_t& c)
   {
      if (stats) stats->customers_scanned++;
      auto sm  = std::string_view(c.c_mktsegment.data, c.c_mktsegment.length);
      auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
      mktsegment_ok = (sm == psm);
      if (!mktsegment_ok) return ::tpch::WalkAction::SkipGroup;
      if (stats) stats->customers_passing_filter++;
      return ::tpch::WalkAction::Continue;
   }

   // First on_order call: invoke CRTP per_order_admit_check (derived can gate on
   // e.g. a per-custkey sub-aggregate computed before any orders arrived) —
   // returns SkipGroup if rejected.  Subsequent calls: flush the previous
   // order register, open a new one only when the datefilter passes; SkipOrder
   // when the date filter fails.
   ::tpch::WalkAction on_order(const orders_coli_t::Key& k, const orders_coli_t& o)
   {
      if (stats) stats->orders_scanned++;
      if (!order_admitted) {
         if (!static_cast<Derived*>(this)->per_order_admit_check(o)) {
            return ::tpch::WalkAction::SkipGroup;
         }
         order_admitted = true;
      } else {
         flush_order();  // close previous order before opening a new one
      }
      if (o.o_orderdate >= params.orderdate) {
         // Date filter failed: skip this order's lineitems.  have_open_order
         // stays false so flush_order() is a no-op when the next order opens.
         have_open_order = false;
         return ::tpch::WalkAction::SkipOrder;
      }
      if (stats) stats->orders_passing_filter++;
      have_open_order  = true;
      cur_orderkey     = k.orderkey;
      cur_orderdate    = o.o_orderdate;
      cur_shippriority = o.o_shippriority;
      return ::tpch::WalkAction::Continue;
   }

   // Accumulate revenue; no-op when the group was not admitted or the order
   // was filtered by the date predicate.  Returns Continue unconditionally.
   ::tpch::WalkAction on_lineitem(const typename LineitemType::Key&, const LineitemType& l)
   {
      if (stats) stats->lineitems_scanned++;
      if (!order_admitted || !have_open_order) return ::tpch::WalkAction::Continue;
      bool passed = rev.consume(l, params);
      if (passed && stats) {
         stats->lineitems_passing_filter++;
         stats->join_callbacks++;
      }
      return ::tpch::WalkAction::Continue;
   }

   // Emit last open order for this group, then reset per-group state.
   void on_group_end(Integer /*ck*/)
   {
      if (order_admitted) flush_order();
      mktsegment_ok  = false;
      order_admitted = false;
   }

   // -------------------------------------------------------------------------
   // CRTP hook defaults.  Derived classes override only what they need.

   // Called at the first on_order for a custkey group.  Return false to reject
   // the entire OL sub-tree (skip_group_pending will be set by on_order).
   // Base: always admit.
   bool per_order_admit_check(const orders_coli_t& /*o*/) { return true; }

   // Called just before pushing an output row.  Base: no-op.
   void extra_emit_fields(AggRow& /*row*/) {}

   // Called from on_record_visited() after bumping mi_records_visited.  Base: no-op.
   void on_record_visited_hook() {}
};

}  // namespace tpch::q3_family
