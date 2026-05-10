// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §3 op 4 (load vs query)            — keep query-time joins on shared JoinState
//   §3 op 6 (SortedAggregate)          — Q3's LineitemRevenueAccumulator wrapper
//   §5 (Q3 worked example)             — inline sketch for query_by_merged
//   §6 (comparison-integrity rules)    — read before changing join strategy
//   §7 (HashJoin downstream)           — CUSTOMER join uses HashJoin, not MergeJoin
//
// Template method bodies for Q3Workload<Backend> query methods,
// predicate implementations, Params::defaults(), and set_params_for_iter().
// query_by_* bodies are Phase-0.5 stubs — they return empty vectors.

#pragma once

#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "../tpch_family/col_pipeline.hpp"
#include "../operators.hpp"
#include "../q3_family/accumulators.hpp"
#include "../q3_family/agg_row.hpp"
#include "../q3_family/coli_visitors.hpp"
#include "../q3_family/lineitem_revenue_aggregator.hpp"
#include "../q3_family/params.hpp"
#include "../q3_family/predicates.hpp"
#include "../../shared/merge-join/binary_merge_join.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Params

inline Params Params::defaults()
{
   return Params{
       Varchar<10>("BUILDING"),
       DATE_1995_03_15,  // orderdate upper bound
       DATE_1995_03_15,  // shipdate lower bound
   };
}

template <typename Backend>
void Q3Workload<Backend>::set_params_for_iter(long iter)
{
   const auto& e =
       q3_family::PARAM_TABLE[iter % q3_family::PARAM_TABLE_SIZE];
   params.mktsegment = Varchar<10>(e.segment);
   params.orderdate  = e.date;
   params.shipdate   = e.date;
}

// ---------------------------------------------------------------------------
// Predicate implementations — delegate to q3_family shared filters.

// c_mktsegment == params.mktsegment
inline bool q3_predicate_customer(const customerh_t& c, const Params& p)
{
   return q3_family::q3_predicate_customer(c, p);
}

// o_orderdate < params.orderdate
inline bool q3_predicate_orders(const orders_t& o, const Params& p)
{
   return q3_family::q3_predicate_orders(o, p);
}

// l_shipdate > params.shipdate
inline bool q3_predicate_lineitem(const lineitem_t& l, const Params& p)
{
   return q3_family::q3_predicate_lineitem(l, p);
}

// Applied to a pipeline view row: checks mktsegment + orderdate + shipdate.
// All three are parameterised and were NOT baked at load time (predicate
// hoisting), so all three must be applied live here.
inline bool q3_predicate_view(const q3_pipeline_view_t& v, const Params& p)
{
   return v.c_mktsegment == p.mktsegment
       && v.o_orderdate  <  p.orderdate
       && v.l_shipdate   >  p.shipdate;
}

// ---------------------------------------------------------------------------
// q3_pipeline_view_t printer (Phase 0.5 — informational only)

inline void q3_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << "\t" << l_discount << "\t"
      << l_shipdate      << "\t" << c_mktsegment << "\t"
      << o_orderdate     << "\t" << o_shippriority << "\n";
}

// ---------------------------------------------------------------------------
// COLGroupWalkVisitor — Q3's subclass of Q3FamilyVisitor.
//
// Q3 has no invoice extension, so all three CRTP customisation hooks
// (per_order_admit_check, extra_emit_fields, on_record_visited_hook) use
// the base class no-op defaults.  The subclass exists only to satisfy the
// CRTP `static_cast<Derived*>(this)->...` plumbing inside the base.
//
// q3_pipeline_view_t lineitem variant for COL is `lineitem_col_t`
// (no invoicekey segment, mirrors lineitem_coli_t with that field dropped).

template <typename Sink>
struct COLGroupWalkVisitor
    : q3_family::Q3FamilyVisitor<COLGroupWalkVisitor<Sink>, Params,
                                  lineitem_col_t, q3_agg_row_t, Stats, Sink> {
   COLGroupWalkVisitor(const Params& p, Sink& s, Stats* st = nullptr)
       : q3_family::Q3FamilyVisitor<COLGroupWalkVisitor<Sink>, Params,
                                     lineitem_col_t, q3_agg_row_t, Stats, Sink>{p, s, st}
   {}
   // No overrides — Q3 uses all base defaults:
   //   • per_order_admit_check  → always admit (no threshold)
   //   • extra_emit_fields      → no-op (no cust_open_due column)
   //   • on_record_visited_hook → no-op (no invoice counters)
};

// ---------------------------------------------------------------------------
// query_by_* — Phase 4 §7.1: query_by_merged real body; S1/S2/S4 still stubs.
//
// The harness asserts all four digests agree; stubs trivially agree on the
// other three paths until §7.2/§7.3/§7.5 land.

template <typename Backend>
long Q3Workload<Backend>::query_by_base(std::vector<q3_agg_row_t>& out)
{
   // S1: 2-BMJ chain over custkey-sorted COL split indexes (Track 1 — no
   // invoice; Q3I has 3 BMJs because of the cust_open_due aggregator).
   //
   //   BMJ #1: customerh_t ⋈ orders_coli_t   on custkey
   //   BMJ #2: q3_jr1_t    ⋈ lineitem_agg_t  on (custkey, orderkey)
   //
   // Filters:
   //   • mktsegment fused into fetch_cust (filter pushdown).
   //   • orderdate fused into fetch_ord.
   //   • shipdate  fused inside LineitemRevenueAggregator at consume time.
   //
   // OPERATORS.md §6.1: the per-record arithmetic (LineitemRevenueAccumulator)
   // is the same as S3 — only the physical scan substrate differs.
   out.clear();

   // Streaming top-K sink (bounded memory: K = 10 entries) — same role as
   // Q5's NNameRevenueAggregator: post-pipeline aggregator the per-emit
   // path pushes into, replacing the previous "buffer everything in a
   // vector then partial_sort" pattern.
   auto cmp = [](const q3_agg_row_t& a, const q3_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3_agg_row_t, decltype(cmp)> sink(10, cmp);

   q3_family::LineitemRevenueAggregator<Backend, lineitem_col_t, Params>
       agg_lin(col.split_lineitem(), params);

   auto cust_scan = customer.getScanner();
   auto ord_scan  = col.split_orders().getScanner();

   auto fetch_cust = [&]() -> std::optional<std::pair<customerh_t::Key, customerh_t>> {
      while (auto kv = cust_scan->next()) {
         if (stats) stats->customers_scanned++;
         if (!q3_predicate_customer(kv->second, params)) continue;
         if (stats) stats->customers_passing_filter++;
         return kv;
      }
      return std::nullopt;
   };

   auto fetch_ord = [&]() -> std::optional<std::pair<orders_coli_t::Key, orders_coli_t>> {
      while (auto kv = ord_scan->next()) {
         if (stats) stats->orders_scanned++;
         if (kv->second.o_orderdate < params.orderdate) {
            if (stats) stats->orders_passing_filter++;
            return kv;
         }
      }
      return std::nullopt;
   };

   // BMJ #1: customer ⋈ orders on custkey.
   BinaryMergeJoin<q3_cust_jk_t::Key, q3_jr1_t, customerh_t, orders_coli_t>
       bmj1(fetch_cust, fetch_ord);

   auto fetch_bmj1 = [&]() { return bmj1.next(); };
   auto fetch_lin_agg = [&]() { return agg_lin.next(); };

   // BMJ #2: jr1 ⋈ lineitem_agg on (custkey, orderkey).
   BinaryMergeJoin<q3_family::lineitem_agg_t::Key, q3_jr2_t,
                   q3_jr1_t, q3_family::lineitem_agg_t>
       bmj2(fetch_bmj1, fetch_lin_agg);

   while (auto kv = bmj2.next()) {
      if (stats) {
         stats->join_callbacks++;
         stats->aggregator_rows_out++;
         stats->topN_candidates++;
      }
      const q3_jr2_t& jr2 = kv->second;
      const q3_jr1_t& jr1 = jr2.jr1();
      const orders_coli_t& o = jr1.order();
      const q3_family::lineitem_agg_t& lagg = jr2.linagg();
      Integer orderkey = kv->first.jk.orderkey;
      sink.offer({orderkey, lagg.revenue, o.o_orderdate, o.o_shippriority});
   }

   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3Workload<Backend>::query_by_view(std::vector<q3_agg_row_t>& out)
{
   // S2: sequential scan over q3_pipeline_view_t (per-lineitem rows).
   //
   // Scan order is (custkey, orderkey, linenumber).  Mktsegment + orderdate
   // filters are evaluated once per orderkey transition (constant within an
   // order); l_shipdate is applied per-lineitem inside the accumulator.
   // At each orderkey boundary we emit the agg row if revenue > 0.
   //
   // OPERATORS.md §4 / §6: all parameterised filters applied live at query
   // time; none baked into the view at load time.
   out.clear();

   auto cmp = [](const q3_agg_row_t& a, const q3_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3_agg_row_t, decltype(cmp)> sink(10, cmp);

   Integer   cur_orderkey     = -1;
   Timestamp cur_orderdate    = 0;
   Integer   cur_shippriority = 0;
   bool      cur_order_ok     = false;  // passes mktsegment + orderdate
   q3_family::LineitemRevenueAccumulator<Params> rev;

   auto flush_order = [&]() {
      if (!cur_order_ok || cur_orderkey < 0) return;
      if (rev.revenue <= Numeric(0)) { rev.reset(); return; }
      if (stats) {
         stats->join_callbacks++;
         stats->aggregator_rows_out++;
         stats->topN_candidates++;
      }
      sink.offer({cur_orderkey, rev.revenue, cur_orderdate, cur_shippriority});
      rev.reset();
   };

   auto vs = pipeline_view.getScanner();
   while (auto kv = vs->next()) {
      const q3_pipeline_view_t::Key& k   = kv->first;
      const q3_pipeline_view_t&      row = kv->second;
      if (stats) stats->lineitems_scanned++;

      if (k.orderkey != cur_orderkey) {
         flush_order();
         cur_orderkey     = k.orderkey;
         cur_orderdate    = row.o_orderdate;
         cur_shippriority = row.o_shippriority;
         cur_order_ok     = false;

         // Mktsegment filter.
         auto sm  = std::string_view(row.c_mktsegment.data, row.c_mktsegment.length);
         auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
         if (sm != psm) continue;
         if (stats) stats->customers_passing_filter++;

         // Orderdate filter.
         if (row.o_orderdate >= params.orderdate) continue;
         if (stats) stats->orders_passing_filter++;
         cur_order_ok = true;
      }

      if (!cur_order_ok) continue;

      // Reuse the LineitemRevenueAccumulator (OPERATORS.md §6.1: identical
      // per-record arithmetic across S1/S2/S3).  Bridge through a lineitem_t
      // proxy carrying only the three fields the accumulator reads.
      lineitem_t proxy;
      proxy.l_shipdate      = row.l_shipdate;
      proxy.l_extendedprice = row.l_extendedprice;
      proxy.l_discount      = row.l_discount;
      if (rev.consume(proxy, params)) {
         if (stats) stats->lineitems_passing_filter++;
      }
   }
   flush_order();

   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3Workload<Backend>::query_by_merged(std::vector<q3_agg_row_t>& out)
{
   // S3: COLGroupWalk over the 3-table COL MI using COLGroupWalkVisitor.
   //
   // Byte-lex order within a custkey group:
   //   customer → (orders → lineitems*)+
   //
   // The visitor:
   //   • on_customer  — gates by c_mktsegment; skips group on miss.
   //   • on_order     — gates by o_orderdate; flushes prior order; sets
   //                    skip_order_pending on miss to skip its lineitems.
   //   • on_lineitem  — accumulates revenue (l_extendedprice * (1 - l_discount))
   //                    via LineitemRevenueAccumulator, gated by l_shipdate.
   //   • on_group_end — flushes the last open order.
   //
   // After the walk, the sink drains a sorted top-K into `out`
   // (OPERATORS.md §3 op 8–9), with o_orderdate ASC, o_orderkey ASC as
   // deterministic tiebreakers (per q3_agg_row_base_t::cmp).
   out.clear();
   auto cmp = [](const q3_agg_row_t& a, const q3_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3_agg_row_t, decltype(cmp)> sink(10, cmp);
   COLGroupWalkVisitor<decltype(sink)> v(params, sink, stats);
   col_group_walk<Backend>(col.merged_adapter(), v);
   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3Workload<Backend>::query_by_hash(std::vector<q3_agg_row_t>& out)
{
   // S4: HashJoin chain baseline (no merged index, no custkey ordering).
   //
   //   1. Build qualifying-customer set (mktsegment filter pushed down).
   //   2. Build orders map (orderdate filter + custkey membership pushed
   //      down before the hashmap insert).
   //   3. Probe lineitems (shipdate filter pushed down); accumulate revenue
   //      into the OrderSlot directly.
   //   4. Emit non-zero slots, then apply_topN.
   //
   // S4 is the *only* path allowed hash-aggregates inside the pipeline
   // (PLAYBOOK §7.5 pitfall — the hashmap tax S4 pays for not having a
   // merged index).
   out.clear();

   auto cmp = [](const q3_agg_row_t& a, const q3_agg_row_t& b) {
      return q3_family::q3_agg_row_base_t::cmp(a, b);
   };
   TopNSink<q3_agg_row_t, decltype(cmp)> sink(10, cmp);

   std::unordered_set<Integer> cust_set;
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->customers_scanned++;
         if (!q3_predicate_customer(kv->second, params)) continue;
         if (stats) stats->customers_passing_filter++;
         cust_set.insert(kv->first.c_custkey);
      }
   }

   struct OrderSlot {
      Timestamp orderdate;
      Integer   shippriority;
      Numeric   revenue = 0;
   };
   std::unordered_map<Integer, OrderSlot> orders_map;
   {
      auto sc = orders.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         if (!q3_predicate_orders(kv->second, params)) continue;
         if (cust_set.find(kv->second.o_custkey) == cust_set.end()) continue;
         if (stats) stats->orders_passing_filter++;
         orders_map.emplace(kv->first.o_orderkey,
                            OrderSlot{kv->second.o_orderdate,
                                      kv->second.o_shippriority,
                                      Numeric(0)});
      }
   }

   {
      auto sc = lineitem.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->lineitems_scanned++;
         if (!q3_predicate_lineitem(kv->second, params)) continue;
         auto it = orders_map.find(kv->first.l_orderkey);
         if (it == orders_map.end()) continue;
         if (stats) stats->lineitems_passing_filter++;
         // Reuse LineitemRevenueAccumulator for the per-record arithmetic
         // (OPERATORS.md §6.1 comparison-integrity).  Reset so each
         // accumulate() folds exactly one row's revenue into the slot.
         q3_family::LineitemRevenueAccumulator<Params> acc;
         acc.consume(kv->second, params);
         it->second.revenue += acc.revenue;
      }
   }

   for (auto& [ok, slot] : orders_map) {
      if (slot.revenue > Numeric(0)) {
         if (stats) {
            stats->join_callbacks++;
            stats->aggregator_rows_out++;
            stats->topN_candidates++;
         }
         sink.offer({ok, slot.revenue, slot.orderdate, slot.shippriority});
      }
   }

   sink.drain_sorted(out);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q3
