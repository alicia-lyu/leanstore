#pragma once

// Q10GroupWalkVisitor — bespoke COL group-walk visitor for Q10.
//
// Phase 4a (Plan §F1–F5). Two compile-time modes:
//   Query    — apply orderdate window + l_returnflag='R'; per-customer
//              accumulator finalised at on_group_end; NATION INL on PK
//              (D6); offer the assembled q10_agg_row_t to a Sink that
//              wraps TopNSink<q10_agg_row_t, &q10_agg_row_t::cmp>.
//   ViewLoad — drop both parameterised filters; emit one
//              q10_pipeline_view_t per lineitem (FD-attached customer
//              cols + o_orderdate) into a Sink that forwards to the
//              pipeline_view adapter. on_group_end is a no-op.
//
// Hook contract: see tpch_family/col_pipeline.hpp + walk_action.hpp.
// Byte-lex order within a custkey group: customer → (orders → lineitems*)+.
//
// NOT a Q3FamilyVisitor subclass (Decision D1; user-memory rule
// `[[feedback_col_walk_is_shared_util]]`). The real shared util is
// col_group_walk itself; Q10's hook semantics (no per-customer gate,
// per-customer accumulator, per-customer NATION INL) don't fit the
// Q3-family per-order-with-mktsegment specialisation.

#include <stdexcept>
#include <unordered_map>

#include "../tpch_family/views_col.hpp"
#include "../tpch_family/walk_action.hpp"
#include "../tpch_tables.hpp"
#include "../q10/views.hpp"
#include "../q10/workload.hpp"  // Params, Q10Stats, predicates

namespace tpch::q10
{

enum class Q10FilterMode { Query, ViewLoad };

// Q10GroupWalkVisitor<NationAdapter, Sink, Mode>
//
//   Query mode    — NationAdapter is the live nation_t adapter; Sink
//                   exposes void emit(q10_agg_row_t); the visitor
//                   resolves n_name on group_end via lookup1 (cached
//                   in nationkey_cache, ~25 entries) then offers the
//                   assembled row to sink.emit.
//   ViewLoad mode — NationAdapter is unused (zero-cost — every site
//                   sits behind `if constexpr`); Sink exposes
//                   void emit_view(const q10_pipeline_view_t::Key&,
//                                  const q10_pipeline_view_t&).
template <typename NationAdapter, typename Sink, Q10FilterMode Mode>
struct Q10GroupWalkVisitor {
   const Params&  params;
   NationAdapter& nation;
   Sink&          sink;
   // ~25 distinct nationkeys at SF=1; near-100% hit after the first
   // pass over the customer set.
   std::unordered_map<Integer, Varchar<25>>& nationkey_cache;
   Q10Stats*      stats = nullptr;

   // ---- per-group state (Query-mode only — ViewLoad never reads them
   // beyond the FD snapshot in on_customer/on_lineitem). Reset at
   // on_group_end.

   // Set in on_customer; consumed by on_lineitem (ViewLoad emits the
   // FD cols) and by on_group_end (Query builds the agg row).
   Integer      cur_custkey    = 0;
   Integer      cur_c_nationkey = 0;
   Varchar<25>  cur_c_name{};
   Varchar<40>  cur_c_address{};
   Varchar<15>  cur_c_phone{};
   Numeric      cur_c_acctbal  = 0;
   Varchar<117> cur_c_comment{};
   bool         have_customer  = false;

   // Query-mode-only accumulator.
   Numeric      cur_revenue    = 0;
   Timestamp    cached_orderdate = 0;

   // ------------------------------------------------------------------
   // Walker callbacks for cardinality stats (optional hooks).

   void on_record_visited()
   {
      if (stats) stats->mi_records_visited++;
   }

   void on_group_skipped(Integer /*ck*/)
   {
      if (stats) stats->mi_groups_skipped++;
   }

   // ------------------------------------------------------------------
   // on_customer — snapshot FD cols. No gate: Q10 has no per-customer
   // predicate. Both modes need the FD cols downstream.
   ::tpch::WalkAction on_customer(Integer ck, const customer_coli_t& c)
   {
      if (stats) stats->customers_scanned++;
      cur_custkey     = ck;
      cur_c_nationkey = c.c_nationkey;
      cur_c_name      = c.c_name;
      cur_c_address   = c.c_address;
      cur_c_phone     = c.c_phone;
      cur_c_acctbal   = c.c_acctbal;
      cur_c_comment   = c.c_comment;
      have_customer   = true;
      if constexpr (Mode == Q10FilterMode::Query) {
         cur_revenue = 0;
      }
      return ::tpch::WalkAction::Continue;
   }

   // ------------------------------------------------------------------
   // on_order
   //   Query    — orderdate ∈ [date_lo, date_lo+90); SkipOrder on miss.
   //   ViewLoad — admit all; cache orderdate to ride on each view row.
   ::tpch::WalkAction on_order(const orders_coli_t::Key& /*k*/,
                               const orders_coli_t&      o)
   {
      if (stats) stats->orders_scanned++;
      if constexpr (Mode == Q10FilterMode::Query) {
         // Spec §2.4.10: o_orderdate ∈ [date_lo, date_lo + 3 months).
         // Mirrors q10_predicate_orders body; inlined here because the
         // free predicate takes orders_t and on_order receives the
         // tagged orders_coli_t.
         if (o.o_orderdate < params.date_lo
             || o.o_orderdate >= params.date_lo + 90) {
            return ::tpch::WalkAction::SkipOrder;
         }
         if (stats) stats->orders_passing_date++;
      }
      cached_orderdate = o.o_orderdate;
      return ::tpch::WalkAction::Continue;
   }

   // ------------------------------------------------------------------
   // on_lineitem
   //   Query    — l_returnflag='R' filter; accumulate revenue into
   //              cur_revenue.
   //   ViewLoad — emit one q10_pipeline_view_t per lineitem (FD cols
   //              + cached_orderdate from on_order).
   ::tpch::WalkAction on_lineitem(const lineitem_col_t::Key& k,
                                  const lineitem_col_t&      l)
   {
      if (stats) stats->lineitems_scanned++;
      if (!have_customer) {
         throw std::logic_error(
             "Q10GroupWalkVisitor::on_lineitem: customer snapshot "
             "missing (walker contract: customer must precede lineitem "
             "in byte-lex order within a custkey group)");
      }
      if constexpr (Mode == Q10FilterMode::Query) {
         if (l.l_returnflag.data[0] != 'R') {
            return ::tpch::WalkAction::Continue;
         }
         if (stats) stats->lineitems_passing_returnflag++;
         // revenue = l_extendedprice * (1 - l_discount).
         cur_revenue += l.l_extendedprice * (Numeric{1} - l.l_discount);
      } else {
         q10_pipeline_view_t::Key vk{cur_custkey, k.orderkey, k.linenumber};
         q10_pipeline_view_t      vr{
             /*l_extendedprice*/ l.l_extendedprice,
             /*l_discount     */ l.l_discount,
             /*l_returnflag   */ l.l_returnflag,
             /*o_orderdate    */ cached_orderdate,
             /*c_name         */ cur_c_name,
             /*c_address      */ cur_c_address,
             /*c_nationkey    */ cur_c_nationkey,
             /*c_phone        */ cur_c_phone,
             /*c_acctbal      */ cur_c_acctbal,
             /*c_comment      */ cur_c_comment,
         };
         sink.emit_view(vk, vr);
         if (stats) stats->view_rows_scanned++;
      }
      return ::tpch::WalkAction::Continue;
   }

   // ------------------------------------------------------------------
   // on_group_end
   //   Query    — if revenue accumulated, resolve NATION INL (D6),
   //              build q10_agg_row_t, offer to sink.
   //   ViewLoad — no-op.
   void on_group_end(Integer /*ck*/)
   {
      if constexpr (Mode == Q10FilterMode::Query) {
         if (have_customer && cur_revenue != Numeric{0}) {
            if (stats) stats->aggregator_rows_out++;
            // NATION INL on PK (D6). Cache hit is near-100% after the
            // first ~25 customers (TPC-H ships 25 nations).
            auto it = nationkey_cache.find(cur_c_nationkey);
            Varchar<25> n_name{};
            if (it != nationkey_cache.end()) {
               n_name = it->second;
            } else {
               nation.lookup1(typename nation_t::Key{cur_c_nationkey},
                              [&](const nation_t& n) { n_name = n.n_name; });
               nationkey_cache.emplace(cur_c_nationkey, n_name);
               if (stats) stats->nation_inl_lookups++;
            }
            q10_agg_row_t row{
                /*c_custkey */ cur_custkey,
                /*revenue   */ cur_revenue,
                /*c_name    */ cur_c_name,
                /*c_acctbal */ cur_c_acctbal,
                /*n_name    */ n_name,
                /*c_address */ cur_c_address,
                /*c_phone   */ cur_c_phone,
                /*c_comment */ cur_c_comment,
            };
            sink.emit(std::move(row));
         }
      }
      // Reset per-group state.
      have_customer    = false;
      cur_custkey      = 0;
      cur_c_nationkey  = 0;
      cur_revenue      = 0;
      cached_orderdate = 0;
   }
};

}  // namespace tpch::q10
