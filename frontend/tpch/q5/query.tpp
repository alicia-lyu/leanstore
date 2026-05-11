// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)         — what each operator looks like in C++
//   §6 (comparison-integrity rules)    — read before changing join strategy
//   §7 (HashJoin downstream)           — dimension-table builds at query time
//
// Template method bodies for Q5Workload<Backend> query methods,
// predicate implementations, Params::defaults(), and agg_row_t::print().
//
// Phase 1 (cont.) status (2026-05-09 follow-up):
//   - Params::defaults()        — real body (ASIA / 1994-01-01).
//   - set_params_for_iter()     — real body (rotates Q5 PARAM_TABLE).
//   - q5_predicate_orders       — real body (orderdate window).
//   - q5_predicate_customer     — stays `true`; nation_set gate is
//                                  applied inline in query_by_* bodies.
//   - q5_predicate_lineitem     — stays `true` (no per-lineitem filter).
//   - query_by_*                — all four stubbed: out.clear(); return 0
//                                  (Phase 4 §7.x).
//   - q5_agg_row_t::print()     — real body.
//   - q5_pipeline_view_t::print() — real body.

#pragma once

#include <algorithm>
#include <limits>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "../../shared/merge-join/binary_merge_join.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../tpch_family/revenue.hpp"
#include "../tpch_family/walk_action.hpp"
#include "side_tables.hpp"
#include "views.hpp"

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// NNameRevenueAggregator — per-n_name HashAggregate for Q5.
//
// Q5's GROUP BY is keyed on n_name (~5 buckets, one per in-region nation).
// Unlike Q3's per-orderkey SortedAggregate, there is no natural sort order
// linking the lineitem stream to n_name, so a hash map is the right choice.
// Accumulate all qualifying lineitems, then call emit() once at the end.

struct NNameRevenueAggregator {
   std::unordered_map<std::string, Numeric> by_name;

   // Accumulate one lineitem's revenue into its nation's bucket.
   // L must expose l_extendedprice and l_discount (lineitem_t,
   // lineitem_col_t, and q5_pipeline_view_t all qualify).
   template <typename L>
   void accumulate(const L& l, const std::string& n_name)
   {
      by_name[n_name] += tpch::lineitem_revenue(l);
   }

   // emit — walk by_name, resolve each n_name back to its n_nationkey via
   // sides.n_name_map (reverse lookup: iterate the map to find the name),
   // and push one q5_agg_row_t per bucket into `out`.
   //
   // Reverse lookup is O(|nation_set|) per bucket; with ~5 buckets and
   // ~5 nations the total cost is O(25) — negligible.
   void emit(std::vector<q5_agg_row_t>& out, const Q5SideTables& sides) const
   {
      for (const auto& [name, revenue] : by_name) {
         // Reverse-lookup n_nationkey: scan n_name_map for matching name.
         // by_name's keys came from sides.n_name_map.at(...) inside
         // q5_admit_lineitem, so a miss here is structurally impossible.
         // Fail fast (PLAYBOOK §"Contract violations & fail-fast") rather
         // than silently emit nationkey=-1: any future caller path that
         // inserts through a different route gets a loud signal instead
         // of corrupted output.
         Integer nationkey = -1;
         for (const auto& [nk, nm] : sides.n_name_map) {
            if (nm == name) {
               nationkey = nk;
               break;
            }
         }
         if (nationkey == -1) {
            throw std::logic_error("NNameRevenueAggregator::emit: n_name '"
                                   + name + "' not present in n_name_map; "
                                   "aggregator and side-table built from "
                                   "different sources?");
         }
         q5_agg_row_t row;
         row.n_nationkey = nationkey;
         row.n_name      = Varchar<25>(name.c_str());
         row.revenue     = revenue;
         out.push_back(row);
      }
   }
};

// ---------------------------------------------------------------------------
// q5_sort_cmp — sort comparator for the final ORDER BY revenue DESC.
//
// TPC-H Q5 has no LIMIT; output is ~5 rows (one per in-region nation).
// n_name ASC is the tiebreaker for deterministic ordering across runs.

constexpr auto q5_sort_cmp = [](const q5_agg_row_t& a, const q5_agg_row_t& b) {
   if (a.revenue != b.revenue) return a.revenue > b.revenue;
   auto av = std::string_view(a.n_name.data, a.n_name.length);
   auto bv = std::string_view(b.n_name.data, b.n_name.length);
   return av < bv;
};

// ---------------------------------------------------------------------------
// q5_pipeline_view_t out-of-line method bodies.

inline void q5_pipeline_view_t::print(std::ostream& os) const
{
   // TODO Phase 1: implement real print body.
   os << "q5_view(extprice=" << l_extendedprice
      << ",disc=" << l_discount
      << ",suppkey=" << l_suppkey
      << ",nationkey=" << c_nationkey
      << ",orderdate=" << o_orderdate << ")";
}

// ---------------------------------------------------------------------------
// q5_agg_row_t out-of-line method bodies.
//
// Output format mirrors TPC-H spec §2.4.5 result: n_name | revenue.

inline void q5_agg_row_t::print(std::ostream& os) const
{
   os << n_name << " | " << revenue;
}

// ---------------------------------------------------------------------------
// Params

inline Params Params::defaults()
{
   return Params{
       Varchar<25>("ASIA"),
       DATE_1994_01_01,  // o_orderdate >= 1994-01-01
   };
}

// Rotate through the Q5 PARAM_TABLE so each TX iteration exercises a
// distinct (region, date) pair.  Mirrors Q3Workload::set_params_for_iter.
template <typename Backend>
void Q5Workload<Backend>::set_params_for_iter(long iter)
{
   const auto& e = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
   params.region       = Varchar<25>(e.region);
   params.orderdate_lo = e.date;
}

// ---------------------------------------------------------------------------
// Predicate bodies — stubbed to return true for Phase 0.5.
// Real filter logic lands in Phase 4 when query_by_* bodies are implemented.

inline bool q5_predicate_customer(const customerh_t& /*c*/, const Params& /*p*/)
{
   // Q5's spec-only customer predicate is `true` (no single-column
   // CUSTOMER filter in the SQL).  The runtime `c_nationkey ∈ nation_set`
   // gate is applied inline in each query_by_* body (Phase 4 §7.x), where
   // `nation_set` is a per-query in-process hashmap built from
   // REGION ⋈ NATION — not a Params field.
   return true;
}

inline bool q5_predicate_orders(const orders_t& o, const Params& p)
{
   // o_orderdate ∈ [orderdate_lo, orderdate_lo + 1y).  TPC-H §2.4.5
   // strict upper bound.  Timestamp is days since 1970-01-01; +365
   // approximates `INTERVAL '1' YEAR` (off-by-one across leap-year
   // boundaries; immaterial for SF=1/15 selectivity — see plan note).
   return o.o_orderdate >= p.orderdate_lo
       && o.o_orderdate <  p.orderdate_lo + 365;
}

inline bool q5_predicate_lineitem(const lineitem_t& /*l*/, const Params& /*p*/)
{
   // Q5 has no per-lineitem single-table filter before the SUPPLIER probe.
   // The cross-equality (c_nationkey = s_nationkey) fires after the probe.
   // This predicate exists for symmetry with Q3's shape.
   return true;
}

// ---------------------------------------------------------------------------
// q5_admit_lineitem — shared per-lineitem callback for S1, S3, and S4.
//
// Probes the SUPPLIER hashmap, applies the c_nationkey = s_nationkey
// cross-equality, resolves n_name, and accumulates revenue into the
// per-n_name bucket.  Returns `void`: Q5 has no per-lineitem skip case
// (skip semantics live at on_customer for nation_set membership and at
// on_order for the date window — see Q5GroupWalkVisitor below), so the
// callers always continue.  The S3 visitor wraps this call with an
// explicit `return WalkAction::Continue;` to make the no-skip contract
// visible at the call site rather than buried in this helper's
// signature.  S1 / S4 callers are void per-emit lambdas that simply
// invoke this helper.
//
// `cached_c_nationkey` is the calling customer's nationkey, cached from
// the on_customer arm (S3) or extracted from the join result (S1/S4).
//
// `lineitems_scanned` accounting convention (audit B.2 from the original
// Q5 Phase 4 review): the counter is bumped per **raw fetch (pre-filter)**
// at four sites that must stay in sync so the metric is comparable
// across paths:
//   - S2 view scan loop          — q5/query.tpp:~424 (every view row)
//   - S3 visitor on_lineitem     — q5/query.tpp:~287 (every walker emit)
//   - S1 BMJ fetch_lin lambda    — q5/query.tpp:~372 (every scanner row)
//   - S4 HJ  fetch_lin lambda    — q5/query.tpp:~573 (every scanner row)
// Do NOT bump the counter inside q5_admit_lineitem: the helper runs
// post-SUPPLIER-probe, after the cross-equality, so bumping here would
// double-count for S1/S3/S4 (which already bumped at fetch time) and
// also gate the count on filters the metric is supposed to be agnostic
// of.  `lineitems_passing_filter` (incremented below) is the post-filter
// counter; both together let the test harness print the funnel.

inline void q5_admit_lineitem(
    const lineitem_col_t&    l,
    Integer                  cached_c_nationkey,
    const Q5SideTables&      sides,
    NNameRevenueAggregator&  agg,
    Q5Stats*                 stats)
{
   // SUPPLIER probe: supplier must be in the pre-filtered hashmap.
   auto sit = sides.supplier_nation.find(l.l_suppkey);
   if (sit == sides.supplier_nation.end()) return;

   // Cross-equality: supplier's nation must match customer's nation.
   if (sit->second != cached_c_nationkey) return;

   if (stats) stats->lineitems_passing_filter++;

   // Resolve n_name and accumulate into the per-n_name bucket.
   const std::string& n_name = sides.n_name_map.at(sit->second);
   agg.accumulate(l, n_name);

   if (stats) {
      stats->join_callbacks++;
      stats->lineitems_admitted++;
   }
}

// ---------------------------------------------------------------------------
// Q5GroupWalkVisitor — group-walk visitor for the COL merged index (S3).
//
// Walks `customer → (orders → lineitems*)+` groups in byte-lex order.
// For each qualifying (customer, order, lineitem) triple it probes the
// SUPPLIER hashmap and applies the c_nationkey = s_nationkey cross-equality,
// then accumulates revenue into the per-n_name HashAggregate.
//
// This is a free struct rather than a Q3FamilyVisitor subclass because Q5's
// hook semantics differ significantly:
//   - on_customer: nation_set membership gate (not mktsegment string compare)
//   - on_order: orderdate window gate (not upper-bound only)
//   - on_lineitem: SUPPLIER probe + cross-eq + per-n_name accumulate
//                  (not per-orderkey revenue flush)
//   - on_group_end: no-op (no per-group flush; aggregate is global)
//
// Skip protocol: every walker hook returns tpch::WalkAction.  See
// tpch_family/walk_action.hpp for the contract.  In Q5:
//   on_customer  → SkipGroup when c_nationkey ∉ nation_set.
//   on_order     → SkipOrder when o_orderdate is outside the window.
//   on_lineitem  → always Continue (no per-lineitem skip case).
// The walker owns all skip mechanics; the visitor carries no skip flags.

struct Q5GroupWalkVisitor {
   const Params&             params;
   const Q5SideTables&       sides;
   NNameRevenueAggregator&   agg;
   Q5Stats*                  stats = nullptr;

   // Per-group state: customer's nationkey, needed by the cross-equality
   // (c_nationkey = s_nationkey) check inside on_lineitem.  Cleared in
   // on_group_end so a stale value never leaks into the next group.
   Integer cached_c_nationkey = -1;

   // on_record_visited: bump MI walk counter.
   void on_record_visited()
   {
      if (stats) stats->mi_records_visited++;
   }

   void on_group_skipped(Integer /*ck*/)
   {
      if (stats) stats->mi_groups_skipped++;
   }

   // Gate: c_nationkey must be in the in-region nation_set.
   ::tpch::WalkAction on_customer(Integer /*ck*/, const customer_coli_t& c)
   {
      if (stats) stats->customers_scanned++;
      if (sides.nation_set.count(c.c_nationkey) == 0) return ::tpch::WalkAction::SkipGroup;
      cached_c_nationkey = c.c_nationkey;
      if (stats) stats->customers_passing_filter++;
      return ::tpch::WalkAction::Continue;
   }

   // Gate: o_orderdate must fall in [orderdate_lo, orderdate_lo + 365).
   ::tpch::WalkAction on_order(const orders_coli_t::Key& /*k*/, const orders_coli_t& o)
   {
      if (stats) stats->orders_scanned++;
      if (o.o_orderdate < params.orderdate_lo
          || o.o_orderdate >= params.orderdate_lo + 365) {
         return ::tpch::WalkAction::SkipOrder;
      }
      if (stats) stats->orders_passing_filter++;
      return ::tpch::WalkAction::Continue;
   }

   // Per-lineitem: delegate to q5_admit_lineitem (shared with S1 / S4).
   // q5_admit_lineitem returns void — Q5 has no per-lineitem skip case;
   // skip semantics live at on_customer / on_order.  Always continue.
   ::tpch::WalkAction on_lineitem(const lineitem_col_t::Key& /*k*/, const lineitem_col_t& l)
   {
      if (stats) stats->lineitems_scanned++;
      q5_admit_lineitem(l, cached_c_nationkey, sides, agg, stats);
      return ::tpch::WalkAction::Continue;
   }

   // on_group_end: no per-group flush needed — the per-n_name aggregate
   // spans all groups.  Clear cached state to guard stale reads.
   void on_group_end(Integer /*ck*/)
   {
      cached_c_nationkey = -1;
   }
};

// ---------------------------------------------------------------------------
// Query bodies — all four stubbed for Phase 0.5.
//
// Each returns 0 admitted rows (empty result vector).  The XOR digest of an
// empty vector is 0x0; the test harness reports [OK] parity vacuously since
// all four paths agree on 0.

template <typename Backend>
long Q5Workload<Backend>::query_by_base(std::vector<q5_agg_row_t>& out)
{
   // S1: 2-BMJ chain over custkey-sorted COL split indexes.
   //
   //   BMJ #1: customerh_t  ⋈ orders_coli_t   on custkey       → q5_jr1_t
   //   BMJ #2: q5_jr1_t     ⋈ lineitem_col_t  on (custkey,
   //                                               orderkey)    → q5_jr2_t
   //
   // Per emit: q5_admit_lineitem probes SUPPLIER, applies the
   //   c_nationkey = s_nationkey cross-equality, and accumulates
   //   revenue into the per-n_name HashAggregate.
   //
   // Unlike Q3, we do NOT pre-aggregate lineitems per (custkey, orderkey)
   // because each lineitem needs l_suppkey for the SUPPLIER probe.
   // BMJ #2 keys on q5_sort_key_t (custkey, orderkey, linenumber) with
   // the WILDCARD_KEY convention: jr1 build-side rows project
   // linenumber=WILDCARD_KEY, so every lineitem in an order matches the
   // single jr1 entry via the wildcard-aware match() on the sort key.
   //
   // OPERATORS.md §6.1: same predicate logic as S3 — apples-to-apples.
   out.clear();

   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;

   auto cust_sc = customer.getScanner();
   auto ord_sc  = col.split_orders().getScanner();

   // Pre-scan customers to build the set of accepted custkeys for fetch_ord
   // seek-skip.  Mirrors Q3 S1 fairness fix (q3/query.tpp:169–241): a small
   // upfront linear pass over CUSTOMER produces an ascending vector of
   // custkeys whose c_nationkey ∈ nation_set; fetch_ord answers "is this
   // custkey valid? if not, what's the next valid one?" in O(log n) via
   // std::lower_bound.  Avoids the BMJ refill-race that abandoned Q3I §G6
   // and Q3's lineitem-side seek (see NOTE next to fetch_lin below).
   //
   // Customer-side cost: |customer| extra scanner pulls per query (~150 at
   // SF=1) — negligible against the orders/lineitem volume.
   std::vector<Integer> accepted_custkeys;
   {
      auto pre = customer.getScanner();
      while (auto kv = pre->next()) {
         if (sides.nation_set.count(kv->second.c_nationkey) != 0) {
            accepted_custkeys.push_back(kv->first.c_custkey);
         }
      }
   }
   auto smallest_accepted_geq = [&](Integer C) -> Integer {
      auto it = std::lower_bound(accepted_custkeys.begin(),
                                  accepted_custkeys.end(), C);
      return it == accepted_custkeys.end()
                 ? std::numeric_limits<Integer>::max()
                 : *it;
   };

   // fetch_cust: scan customers, gate by c_nationkey ∈ nation_set.
   auto fetch_cust = [&]() -> std::optional<std::pair<customerh_t::Key, customerh_t>> {
      while (auto kv = cust_sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_filter++;
         return kv;
      }
      return std::nullopt;
   };

   // fetch_ord: scan custkey-sorted orders split index, gate by orderdate
   // window.  Physical seek-skip: if the order's custkey has no accepted
   // customer (nation_set miss), jump forward to the smallest accepted
   // custkey >= kv.custkey.  Direct equivalent of S3's COLGroupWalk
   // SkipGroup (col_pipeline.tpp) and of S2's view seek on nation miss.
   auto fetch_ord = [&]() -> std::optional<std::pair<orders_coli_t::Key, orders_coli_t>> {
      for (;;) {
         auto kv = ord_sc->next();
         if (!kv) return std::nullopt;
         if (stats) stats->orders_scanned++;
         Integer target = smallest_accepted_geq(kv->first.custkey);
         if (target != kv->first.custkey) {
            if (target == std::numeric_limits<Integer>::max()) {
               // No further accepted customers — drain the scanner.
               return std::nullopt;
            }
            orders_coli_t::Key skip_key{target, 0};
            ord_sc->seek(skip_key);
            if (stats) stats->s1_groups_skipped++;
            continue;
         }
         const Timestamp od = kv->second.o_orderdate;
         if (od < params.orderdate_lo || od >= params.orderdate_lo + 365) continue;
         if (stats) stats->orders_passing_filter++;
         return kv;
      }
   };

   // BMJ #1: customer ⋈ split_orders on custkey → q5_jr1_t.
   BinaryMergeJoin<q5_cust_jk_t::Key, q5_jr1_t, customerh_t, orders_coli_t>
       bmj1(fetch_cust, fetch_ord);

   auto lin_sc = col.split_lineitem().getScanner();

   auto fetch_bmj1 = [&]() { return bmj1.next(); };

   // fetch_lin: scan custkey-sorted lineitem split index (no pre-filter;
   // SUPPLIER probe and cross-equality fire inside q5_admit_lineitem).
   auto fetch_lin = [&]() -> std::optional<std::pair<lineitem_col_t::Key, lineitem_col_t>> {
      auto kv = lin_sc->next();
      if (kv && stats) stats->lineitems_scanned++;
      return kv;
   };

   // BMJ #2: jr1 ⋈ split_lineitem on q5_sort_key_t → q5_jr2_t.
   // SKBuilder projects linenumber=WILDCARD_KEY on the build side
   // (q5_jr1_t) and forwards the real linenumber on the probe side
   // (lineitem_col_t).
   BinaryMergeJoin<q5_sort_key_t, q5_jr2_t, q5_jr1_t, lineitem_col_t>
       bmj2(fetch_bmj1, fetch_lin,
            [&](const q5_jr2_t::Key&, const q5_jr2_t& jr2) {
               // q5_admit_lineitem: SUPPLIER probe + cross-eq + accumulate.
               q5_admit_lineitem(jr2.lineitem(),
                                 jr2.jr1().cust().c_nationkey,
                                 sides, agg, stats);
            });
   bmj2.run();

   agg.emit(out, sides);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->agg_buckets         = static_cast<long>(out.size());
   }
   std::sort(out.begin(), out.end(), q5_sort_cmp);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q5Workload<Backend>::query_by_view(std::vector<q5_agg_row_t>& out)
{
   // S2: sequential scan over q5_pipeline_view_t (per-lineitem rows).
   //
   // The view is loaded unfiltered (predicate hoisting — no filter baked in
   // at load time so the view is reusable across all REGION / DATE param sets).
   // Every parameterised filter is applied live here:
   //   1. c_nationkey ∈ nation_set     — customer gate (replaces mktsegment)
   //   2. o_orderdate window           — inline date-range arithmetic
   //   3. SUPPLIER probe               — supplier_nation hashmap lookup
   //   4. c_nationkey = s_nationkey    — cross-equality (dominant pruner)
   //
   // No per-orderkey flush is needed: the per-n_name aggregate spans all
   // orders; accumulate all qualifying lineitems, then call agg.emit() once.
   out.clear();

   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;

   // Per-custkey seek-skip mirrors Q3 S2 (q3/query.tpp:365–386) and S3's
   // COLGroupWalk SkipGroup.  Within a custkey group, c_nationkey is
   // FD-attached and identical for every row; we evaluate the nation_set
   // gate once at the custkey transition and seek past the entire custkey
   // range on a miss instead of streaming every row through a per-row
   // hashmap probe.  This closes the access-pattern gap to S3 (apples-to-
   // apples physical-seek parity, not just filter pushdown).
   Integer cur_custkey       = -1;
   bool    cur_custkey_ok    = false;

   auto sc = pipeline_view.getScanner();
   while (auto kv = sc->next()) {
      const auto& k = kv->first;
      const auto& v = kv->second;
      if (stats) stats->lineitems_scanned++;

      if (k.custkey != cur_custkey) {
         // Custkey transition — re-evaluate nation_set membership.  On a
         // miss, physical-seek past the entire custkey range to mirror
         // S3's SkipGroup.
         cur_custkey = k.custkey;
         if (sides.nation_set.count(v.c_nationkey) == 0) {
            if (stats) stats->view_groups_skipped++;
            q5_pipeline_view_t::Key next_key{cur_custkey + 1, 0, 0};
            sc->seek(next_key);
            cur_custkey    = -1;  // re-arm on next row
            cur_custkey_ok = false;
            continue;
         }
         cur_custkey_ok = true;
         if (stats) stats->customers_passing_filter++;
      }
      if (!cur_custkey_ok) continue;

      // Orderdate window.  Inlined here (not delegated to q5_predicate_orders)
      // because that predicate takes orders_t and the view row is
      // q5_pipeline_view_t — same pattern as Q3's S2 inline orderdate check.
      if (v.o_orderdate < params.orderdate_lo
          || v.o_orderdate >= params.orderdate_lo + 365) {
         continue;
      }

      // SUPPLIER probe.
      auto sit = sides.supplier_nation.find(v.l_suppkey);
      if (sit == sides.supplier_nation.end()) continue;

      // Cross-equality: supplier's nation must match customer's nation.
      if (sit->second != v.c_nationkey) continue;

      if (stats) stats->lineitems_passing_filter++;

      const std::string& n_name = sides.n_name_map.at(sit->second);
      agg.accumulate(v, n_name);

      if (stats) {
         stats->join_callbacks++;
         stats->lineitems_admitted++;
      }
   }

   agg.emit(out, sides);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->agg_buckets         = static_cast<long>(out.size());
   }
   std::sort(out.begin(), out.end(), q5_sort_cmp);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q5Workload<Backend>::query_by_merged(std::vector<q5_agg_row_t>& out)
{
   // S3: col_group_walk over the 3-table COL MI using Q5GroupWalkVisitor.
   //
   // Byte-lex order within a custkey group:
   //   customer → (orders → lineitems*)+
   //
   // The visitor:
   //   • on_customer  — gates by c_nationkey ∈ nation_set; skips group on miss.
   //                    Caches cached_c_nationkey for the cross-equality check.
   //   • on_order     — gates by orderdate window; sets skip_order_pending on miss.
   //   • on_lineitem  — probes SUPPLIER map; applies cross-eq (c_nk == s_nk);
   //                    resolves n_name; accumulates revenue per n_name bucket.
   //   • on_group_end — clears cached state (no per-group flush needed).
   //
   // After the walk, agg.emit() pushes ~5 rows (one per in-region nation),
   // then std::sort orders them revenue DESC.
   out.clear();
   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;
   Q5GroupWalkVisitor v{params, sides, agg, stats};
   col_group_walk<Backend>(col.merged_adapter(), v);

   agg.emit(out, sides);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->agg_buckets         = static_cast<long>(out.size());
   }
   std::sort(out.begin(), out.end(), q5_sort_cmp);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q5Workload<Backend>::query_by_hash(std::vector<q5_agg_row_t>& out)
{
   // S4: HashJoin chain baseline rewritten for access-pattern fairness vs
   // S3 (mirrors the Q3 S4 fairness fix in q3/query.tpp:451–561).
   //
   //   1. Build qualifying-customer map (cust_nation_map: custkey → c_nationkey)
   //      with nation_set pushed down at the CUSTOMER scan.
   //   2. Build orders_map keyed by (custkey, orderkey) carrying c_nationkey,
   //      with orderdate filter + custkey-membership pushed down at the
   //      ORDERS scan.
   //   3. Extract qualifying (custkey, orderkey) pairs, sort lex, and
   //      physical-seek the lineitem scanner per pair (index-NL pattern,
   //      mirror of S2 view custkey-seek and S3 COL walker SkipGroup).
   //      Avoids streaming the full lineitem table — makes S1/S2/S3/S4
   //      apples-to-apples on access pattern, not just filter pushdown.
   //   4. Per-lineitem: invoke q5_admit_lineitem (SUPPLIER probe + cross-eq +
   //      n_name accumulate), with cached c_nationkey from orders_map.
   //
   // S4 is the *only* path allowed hash-aggregates inside the pipeline
   // (CONVENTIONS.md pitfall — the hashmap tax S4 pays for not having a
   // merged index).  s4_hashtable_bytes surfaces that empirically.
   out.clear();

   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;

   // (1) Qualifying customers: custkey → c_nationkey.
   std::unordered_map<Integer, Integer> cust_nation_map;
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_filter++;
         cust_nation_map.emplace(kv->first.c_custkey, kv->second.c_nationkey);
      }
   }

   // (2) Qualifying orders: (custkey, orderkey) → c_nationkey.
   //     Uses col.split_orders() scanner (carries custkey-extended key);
   //     filters by orderdate window AND custkey ∈ cust_nation_map.
   struct OrderSlot {
      Integer c_nationkey;
   };
   // Key by (custkey, orderkey) since S4 needs the custkey to drive the
   // custkey-prefixed lineitem seek in step 3.  Using a flat
   // unordered_map<pair, …> is cheaper than nested maps.
   std::unordered_map<long, OrderSlot> orders_map;
   auto pack_co = [](Integer custkey, Integer orderkey) -> long {
      return (static_cast<long>(custkey) << 32) | static_cast<unsigned long>(orderkey);
   };
   {
      auto sc = col.split_orders().getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         const Timestamp od = kv->second.o_orderdate;
         if (od < params.orderdate_lo || od >= params.orderdate_lo + 365) continue;
         auto cit = cust_nation_map.find(kv->first.custkey);
         if (cit == cust_nation_map.end()) continue;
         if (stats) stats->orders_passing_filter++;
         orders_map.emplace(pack_co(kv->first.custkey, kv->first.orderkey),
                            OrderSlot{cit->second});
      }
   }

   // (3) Sorted (custkey, orderkey) pairs drive the index-NL seek into
   //     col.split_lineitem().  Same Q3 fairness pattern; the only Q5
   //     specific bit is the 2-component lex sort (since split_lineitem's
   //     primary key is (custkey, orderkey, linenumber)).
   std::vector<std::pair<Integer, Integer>> qualifying_co;
   qualifying_co.reserve(orders_map.size());
   for (const auto& [packed, _slot] : orders_map) {
      Integer ck = static_cast<Integer>(packed >> 32);
      Integer ok = static_cast<Integer>(packed & 0xFFFFFFFFL);
      qualifying_co.emplace_back(ck, ok);
   }
   std::sort(qualifying_co.begin(), qualifying_co.end());

   // Transient hash-table working-set instrumentation (mirror of Q3's
   // s4_hashtable_bytes — see q3_family/stats.hpp).
   if (stats) {
      size_t bytes =
          cust_nation_map.size() * (sizeof(Integer) * 2 + 24)
        + orders_map.size() * (sizeof(long) + sizeof(OrderSlot) + 24)
        + qualifying_co.size() * sizeof(std::pair<Integer, Integer>);
      stats->s4_hashtable_bytes = static_cast<long>(bytes);
   }

   {
      auto sc = col.split_lineitem().getScanner();
      for (const auto& [ck, ok] : qualifying_co) {
         sc->seek(typename lineitem_col_t::Key{ck, ok, 0});
         if (stats) stats->s4_orderkey_seeks++;
         auto oit = orders_map.find(pack_co(ck, ok));  // guaranteed present
         const Integer c_nk = oit->second.c_nationkey;
         while (auto kv = sc->next()) {
            // Exited the (custkey, orderkey) group?  split_lineitem is sorted
            // (custkey, orderkey, linenumber), so the boundary is when either
            // custkey or orderkey changes.
            if (kv->first.custkey != ck || kv->first.orderkey != ok) break;
            if (stats) stats->lineitems_scanned++;
            q5_admit_lineitem(kv->second, c_nk, sides, agg, stats);
         }
      }
   }

   agg.emit(out, sides);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->agg_buckets         = static_cast<long>(out.size());
   }
   std::sort(out.begin(), out.end(), q5_sort_cmp);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q5
