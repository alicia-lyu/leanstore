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

#include <algorithm>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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

struct COLGroupWalkVisitor
    : q3_family::Q3FamilyVisitor<COLGroupWalkVisitor, Params,
                                  lineitem_col_t, q3_agg_row_t, Stats> {
   COLGroupWalkVisitor(const Params& p, std::vector<q3_agg_row_t>& o,
                       Stats* s = nullptr)
       : q3_family::Q3FamilyVisitor<COLGroupWalkVisitor, Params,
                                     lineitem_col_t, q3_agg_row_t, Stats>{p, o, s}
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
   //
   // Per-custkey physical seek-skip (apples-to-apples vs S3's COLGroupWalk
   // SkipGroup, tpch_family/col_pipeline.tpp §SkipGroup; and S2's
   // q3_pipeline_view_t custkey-seek in query_by_view above):
   //   • fetch_cust publishes the latest accepted custkey via
   //     min_ord_custkey (a side-channel).  On a mktsegment miss it does
   //     NOT publish — the rejected custkey stays implicit, and is skipped
   //     by the next fetch_ord call.
   //   • fetch_ord, before returning an order, physically seeks its
   //     scanner to orders_coli_t::Key{min_ord_custkey, 0} when the
   //     prefetched order's o_custkey is below it.  This is the direct
   //     equivalent of S3's SkipGroup seek on the merged-adapter scanner.
   //   • The lineitem aggregator seek (agg_lin.seek_to_custkey) is
   //     deliberately NOT wired here — see the NOTE next to fetch_lin_agg
   //     below for the BMJ#2 timing reason.  This means S1 closes the
   //     orders-side I/O gap to S3 but leaves the lineitem-side gap as
   //     deferred work (mirror of the abandoned Q3I §G6).
   // Stats: stats->s1_groups_skipped is bumped once per actual physical
   // seek on the orders scanner.
   out.clear();

   q3_family::LineitemRevenueAggregator<Backend, lineitem_col_t, Params>
       agg_lin(col.split_lineitem(), params);

   auto cust_scan = customer.getScanner();
   auto ord_scan  = col.split_orders().getScanner();

   // Pre-scan customers to build the set of accepted custkeys for
   // fetch_ord seek-skip.  This is a small upfront pass (linear in
   // |customer|) and lets fetch_ord answer "is this custkey valid? if
   // not, what's the next valid one?" in O(log n) without needing to
   // chase a moving target from inside fetch_cust.
   //
   // Why a pre-scan rather than co-evolving with fetch_cust:
   //   The natural "track the latest accepted custkey published by
   //   fetch_cust" pattern breaks correctness because BMJ#1 pulls
   //   fetch_cust for left-refill *before* its current jk_to_join is
   //   fully drained on the right (orders) side.  Any side-channel
   //   updated inside fetch_cust runs ahead of fetch_ord's actual
   //   processing front, so the seek target is the *next* group rather
   //   than the current — which loses the current group's orders.
   //   (Same race-condition pathology that abandoned Q3I §G6.)
   //
   //   Pre-scanning sidesteps that entirely: the accepted-custkey set is
   //   immutable for the query; fetch_ord answers from it directly and
   //   never has to coordinate with BMJ-state.
   //
   // Cost: |customer| extra scanner pulls per query.  At SF=1 that's 150
   // records — negligible against the 1500 orders / 6000 lineitems.  The
   // pull happens before BMJ construction so it doesn't show up in the
   // BMJ-driven stats (customers_scanned still reflects only the
   // baseline-scan path).
   std::vector<Integer> accepted_custkeys;
   {
      auto pre = customer.getScanner();
      while (auto kv = pre->next()) {
         if (q3_predicate_customer(kv->second, params)) {
            accepted_custkeys.push_back(kv->first.c_custkey);
         }
      }
   }
   // accepted_custkeys is in scanner order (= ascending custkey for the
   // customer primary index).

   // Map a custkey C to the smallest accepted custkey >= C (or +∞ if none).
   // Used by fetch_ord to compute its seek target on a miss.
   auto smallest_accepted_geq = [&](Integer C) -> Integer {
      auto it = std::lower_bound(accepted_custkeys.begin(),
                                  accepted_custkeys.end(), C);
      return it == accepted_custkeys.end()
                 ? std::numeric_limits<Integer>::max()
                 : *it;
   };

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
      for (;;) {
         auto kv = ord_scan->next();
         if (!kv) return std::nullopt;
         if (stats) stats->orders_scanned++;
         // Physical seek-skip: if the order's custkey has no accepted
         // customer, jump forward to the smallest accepted custkey >=
         // kv.custkey (or exhaust the scanner if none).  This is the
         // direct equivalent of S3's COLGroupWalk SkipGroup
         // (col_pipeline.tpp:289-302), which seeks the merged-adapter
         // scanner past failing custkeys on mktsegment-miss.
         Integer target = smallest_accepted_geq(kv->first.custkey);
         if (target != kv->first.custkey) {
            if (target == std::numeric_limits<Integer>::max()) {
               // No further accepted customers — drain the scanner.
               // Issuing a physical seek to a key past the end is the
               // analogue of S3's terminal advance.
               return std::nullopt;
            }
            orders_coli_t::Key skip_key{target, 0};
            ord_scan->seek(skip_key);
            if (stats) stats->s1_groups_skipped++;
            continue;
         }
         if (kv->second.o_orderdate < params.orderdate) {
            if (stats) stats->orders_passing_filter++;
            return kv;
         }
      }
   };

   // BMJ #1: customer ⋈ orders on custkey.
   BinaryMergeJoin<q3_cust_jk_t::Key, q3_jr1_t, customerh_t, orders_coli_t>
       bmj1(fetch_cust, fetch_ord);

   auto fetch_bmj1     = [&]() { return bmj1.next(); };
   auto fetch_lin_agg  = [&]() { return agg_lin.next(); };
   // NOTE on lineitem-aggregator seek-skip (deferred, mirrors Q3I §G6):
   // The natural extension — call agg_lin.seek_to_custkey(latest accepted
   // custkey) inside fetch_lin_agg — breaks parity.  BMJ#2's
   // refill_current_key() exhausts left-side jr1 buffering for jk before
   // refilling the right side, so by the time fetch_lin_agg is called for
   // jk=K, the latest accepted custkey may already be M>K (because
   // fetch_bmj1 has advanced ahead, which in turn pulled fetch_cust).  A
   // forward seek to M then loses lineitems for the orderkeys at custkey K
   // that BMJ#2 still needs.  Two seek-targets we tried, both unsafe:
   //   • min_ord_custkey (the latest accepted-customer custkey)
   //   • min_lin_custkey (the latest jr1 custkey from fetch_bmj1)
   // Both advance past K before BMJ#2 finishes K.  A clean fix needs an
   // intrusive BMJ hook ("group K is fully drained on both sides") — same
   // refactor cost that abandoned Q3I §G6.  Orders-side seek-skip works
   // because fetch_ord is BMJ#1's right side and BMJ#1's left (fetch_cust)
   // advances at the same per-custkey grain.  This still closes the bulk
   // of the I/O fairness gap to S3: at SF=1 the orders scanner is ~3× the
   // size of lineitems-per-group, and the seek mechanics are now
   // identical to S3's `seek<customer_coli_t>(next_key)` SkipGroup path.

   // BMJ #2: jr1 ⋈ lineitem_agg on (custkey, orderkey).
   BinaryMergeJoin<q3_family::lineitem_agg_t::Key, q3_jr2_t,
                   q3_jr1_t, q3_family::lineitem_agg_t>
       bmj2(fetch_bmj1, fetch_lin_agg);

   while (auto kv = bmj2.next()) {
      if (stats) stats->join_callbacks++;
      const q3_jr2_t& jr2 = kv->second;
      const q3_jr1_t& jr1 = jr2.jr1();
      const orders_coli_t& o = jr1.order();
      const q3_family::lineitem_agg_t& lagg = jr2.linagg();
      Integer orderkey = kv->first.jk.orderkey;
      out.push_back({orderkey, lagg.revenue, o.o_orderdate, o.o_shippriority});
   }

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   apply_topN(out, 10, q3_family::q3_agg_row_base_t::cmp);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q3Workload<Backend>::query_by_view(std::vector<q3_agg_row_t>& out)
{
   // S2: sequential scan over q3_pipeline_view_t (per-lineitem rows).
   //
   // Scan order is (custkey, orderkey, linenumber).  Mktsegment is evaluated
   // once per custkey transition (FD-attached, constant within a custkey
   // group); on a miss we seek past the entire custkey range — mirror of
   // S3's COLGroupWalk SkipGroup behaviour at on_customer.  Orderdate is
   // evaluated once per orderkey transition (constant within an order);
   // l_shipdate is applied per-lineitem inside the accumulator.  At each
   // orderkey boundary we emit the agg row if revenue > 0.
   //
   // OPERATORS.md §4 / §6: all parameterised filters applied live at query
   // time; none baked into the view at load time.
   out.clear();

   Integer   cur_custkey      = -1;
   Integer   cur_orderkey     = -1;
   Timestamp cur_orderdate    = 0;
   Integer   cur_shippriority = 0;
   bool      cur_order_ok     = false;  // passes mktsegment + orderdate
   q3_family::LineitemRevenueAccumulator<Params> rev;

   auto flush_order = [&]() {
      if (!cur_order_ok || cur_orderkey < 0) return;
      if (rev.revenue <= Numeric(0)) { rev.reset(); return; }
      if (stats) stats->join_callbacks++;
      out.push_back({cur_orderkey, rev.revenue, cur_orderdate, cur_shippriority});
      rev.reset();
   };

   auto vs = pipeline_view.getScanner();
   while (auto kv = vs->next()) {
      const q3_pipeline_view_t::Key& k   = kv->first;
      const q3_pipeline_view_t&      row = kv->second;
      if (stats) stats->lineitems_scanned++;

      if (k.custkey != cur_custkey) {
         // Custkey transition — flush prior order, re-evaluate mktsegment.
         flush_order();
         cur_custkey      = k.custkey;
         cur_orderkey     = -1;       // force orderdate re-check below
         cur_order_ok     = false;

         // Mktsegment filter — FD-attached, identical for every row sharing
         // this custkey, so checking the first row is sufficient.  On a miss
         // we seek past the entire custkey range to mirror S3's SkipGroup.
         auto sm  = std::string_view(row.c_mktsegment.data, row.c_mktsegment.length);
         auto psm = std::string_view(params.mktsegment.data, params.mktsegment.length);
         if (sm != psm) {
            if (stats) stats->view_groups_skipped++;
            // Seek to (cur_custkey + 1, 0, 0) — start of next custkey group.
            q3_pipeline_view_t::Key next_key{cur_custkey + 1, 0, 0};
            vs->seek(next_key);
            cur_custkey = -1;  // re-arm on next row
            continue;
         }
         if (stats) stats->customers_passing_filter++;
      }

      if (k.orderkey != cur_orderkey) {
         flush_order();
         cur_orderkey     = k.orderkey;
         cur_orderdate    = row.o_orderdate;
         cur_shippriority = row.o_shippriority;
         cur_order_ok     = false;

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

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   apply_topN(out, 10, q3_family::q3_agg_row_base_t::cmp);
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
   // After the walk, apply_topN enforces ORDER BY revenue DESC LIMIT 10
   // (OPERATORS.md §3 op 8–9), with o_orderdate ASC, o_orderkey ASC as
   // deterministic tiebreakers.
   out.clear();
   COLGroupWalkVisitor v(params, out, stats);
   col_group_walk<Backend>(col.merged_adapter(), v);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   apply_topN(out, 10, q3_family::q3_agg_row_base_t::cmp);
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
      // Index-NL seek pattern (mirror of S2 view custkey-seek and S3 COL
      // walker custkey-miss seek): extract qualifying orderkeys from the
      // orders_map, sort them, and physical-seek the lineitem scanner to
      // each (orderkey, 0).  Avoids streaming the full lineitem table and
      // filtering by hashmap probe.  Makes S1/S2/S3/S4 apples-to-apples on
      // access pattern, not just on filter pushdown.
      std::vector<Integer> qualifying_orderkeys;
      qualifying_orderkeys.reserve(orders_map.size());
      for (const auto& [ok, _slot] : orders_map) qualifying_orderkeys.push_back(ok);
      std::sort(qualifying_orderkeys.begin(), qualifying_orderkeys.end());

      auto sc = lineitem.getScanner();
      for (Integer ok : qualifying_orderkeys) {
         sc->seek(typename lineitem_t::Key{ok, 0});
         if (stats) stats->s4_orderkey_seeks++;
         auto it = orders_map.find(ok);  // guaranteed present
         while (auto kv = sc->next()) {
            if (kv->first.l_orderkey != ok) break;  // exited the orderkey group
            if (stats) stats->lineitems_scanned++;
            if (!q3_predicate_lineitem(kv->second, params)) continue;
            if (stats) stats->lineitems_passing_filter++;
            // Reuse LineitemRevenueAccumulator for the per-record arithmetic
            // (OPERATORS.md §6.1 comparison-integrity).  Fresh accumulator
            // so each consume() folds exactly one row's revenue into the slot.
            q3_family::LineitemRevenueAccumulator<Params> acc;
            acc.consume(kv->second, params);
            it->second.revenue += acc.revenue;
         }
      }
   }

   for (auto& [ok, slot] : orders_map) {
      if (slot.revenue > Numeric(0)) {
         if (stats) stats->join_callbacks++;
         out.push_back({ok, slot.revenue, slot.orderdate, slot.shippriority});
      }
   }

   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->topN_candidates     = static_cast<long>(out.size());
   }
   apply_topN(out, 10, q3_family::q3_agg_row_base_t::cmp);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q3
