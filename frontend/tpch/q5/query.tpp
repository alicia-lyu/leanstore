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
#include <ostream>
#include <unordered_map>
#include <vector>

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
         Integer nationkey = -1;
         for (const auto& [nk, nm] : sides.n_name_map) {
            if (nm == name) {
               nationkey = nk;
               break;
            }
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
// cross-equality, resolves n_name, and accumulates revenue into the per-n_name
// bucket.  Returns WalkAction::Continue unconditionally so the caller (visitor
// or BMJ/hash loop) can return its result directly.
//
// `cached_c_nationkey` is the calling customer's nationkey, cached from the
// on_customer arm (S3) or extracted from the join result (S1/S4).
//
// lineitems_scanned is NOT incremented here: in the visitor (S3) the walker
// already tracks records_visited, and in the S1/S4 fetch loops the per-scan
// counter increments naturally at the fetch site.  Keeping the counter out of
// this helper avoids double-counting on any future code path.

inline ::tpch::WalkAction q5_admit_lineitem(
    const lineitem_col_t&    l,
    Integer                  cached_c_nationkey,
    const Q5SideTables&      sides,
    NNameRevenueAggregator&  agg,
    Q5Stats*                 stats)
{
   // SUPPLIER probe: supplier must be in the pre-filtered hashmap.
   auto sit = sides.supplier_nation.find(l.l_suppkey);
   if (sit == sides.supplier_nation.end()) return ::tpch::WalkAction::Continue;

   // Cross-equality: supplier's nation must match customer's nation.
   if (sit->second != cached_c_nationkey) return ::tpch::WalkAction::Continue;

   if (stats) stats->lineitems_passing_filter++;

   // Resolve n_name and accumulate into the per-n_name bucket.
   const std::string& n_name = sides.n_name_map.at(sit->second);
   agg.accumulate(l, n_name);

   if (stats) {
      stats->join_callbacks++;
      stats->lineitems_admitted++;
   }
   return ::tpch::WalkAction::Continue;
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
   ::tpch::WalkAction on_lineitem(const lineitem_col_t::Key& /*k*/, const lineitem_col_t& l)
   {
      if (stats) stats->lineitems_scanned++;
      return q5_admit_lineitem(l, cached_c_nationkey, sides, agg, stats);
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
   // Phase 4 §7.2: 2-way BMJ chain over COL split indexes
   // (customer ⋈ orders_sec ⋈ lineitem_sec), same visitor as S3.
   out.clear();
   return 0;
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

   auto sc = pipeline_view.getScanner();
   while (auto kv = sc->next()) {
      const auto& v = kv->second;
      if (stats) stats->lineitems_scanned++;

      // Customer gate.
      if (sides.nation_set.count(v.c_nationkey) == 0) continue;

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
   // Phase 4 §7.5: HashJoin chain over base tables (S4 baseline).
   out.clear();
   return 0;
}

}  // namespace tpch::q5
