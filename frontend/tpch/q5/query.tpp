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
#include <cstring>
#include <limits>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
   // Keyed by n_name (the spec's GROUP BY column); ~5 buckets per query.
   // n_name is resolved upstream at the customer-survival point and carried
   // through q5_customer_rn_t / widened join intermediates — the aggregator
   // has no dependency on Q5SideTables or the NATION adapter.
   std::unordered_map<std::string, Numeric> by_name;

   // Accumulate one lineitem's revenue into its nation's bucket.
   // L must expose l_extendedprice and l_discount (lineitem_t,
   // lineitem_col_t, and q5_pipeline_view_t all qualify).
   template <typename L>
   void accumulate(const L& l, const std::string& n_name)
   {
      by_name[n_name] += tpch::lineitem_revenue(l);
   }

   // emit — iterate by_name (keyed directly by n_name) and push one
   // q5_agg_row_t per bucket into `out`.  No reverse-lookup needed:
   // n_name IS the bucket key (resolved upstream, not inside this sink).
   void emit(std::vector<q5_agg_row_t>& out) const
   {
      out.reserve(out.size() + by_name.size());
      for (const auto& [name, revenue] : by_name) {
         q5_agg_row_t row;
         row.n_nationkey = 0;           // not used downstream; output key is n_name
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
// q5_resolve_n_name — lazily resolves n_name from the NATION primary index.
//
// Called once per distinct nationkey at the customer-survival point of each
// query_by_* body.  Results are cached in `cache` (a per-query local map,
// ~5 entries); subsequent calls for the same nationkey are O(1) map hits.
// Total NATION PK lookups per query: bounded by |nation_set| ≈ 5.
//
// The returned reference is stable for the lifetime of `cache`.

template <typename NationAdapter>
inline const std::string& q5_resolve_n_name(
    Integer                                   nationkey,
    NationAdapter&                            nation,
    std::unordered_map<Integer, std::string>& cache)
{
   auto it = cache.find(nationkey);
   if (it != cache.end()) return it->second;
   std::string name;
   nation.lookup1(typename nation_t::Key{nationkey},
                  [&](const nation_t& n) {
                     name.assign(n.n_name.data,
                                 strnlen(n.n_name.data, sizeof(n.n_name.data)));
                  });
   return cache.emplace(nationkey, std::move(name)).first->second;
}

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
// q5_admit_lineitem — shared per-lineitem callback for all four paths.
//
// Performs ONE composite-key probe of supplier_nation_set with
// (cached_c_nationkey, l.l_suppkey), fusing the SUPPLIER semi-join,
// the cross-equality c_nationkey = s_nationkey, and the suppkey equi-join
// into a single hashset lookup.  On a hit, accumulates revenue into the
// per-n_name bucket using the already-resolved cached_n_name (no further
// NATION lookup inside this helper).
//
// Templated on L so S4 (base lineitem_t) and S1/S2/S3 (lineitem_col_t /
// q5_pipeline_view_t) share one helper — all record types expose
// l_suppkey, l_extendedprice, and l_discount.
//
// Counter convention (audit B.2): lineitems_scanned is bumped at four
// raw-fetch sites (S2 view loop, S3 on_lineitem, S1 fetch_lin, S4 inner
// loop) — NEVER inside this helper.  lineitems_passing_filter is
// post-probe; both together show the full filter funnel.

template <typename L>
inline void q5_admit_lineitem(
    const L&                 l,
    Integer                  cached_c_nationkey,
    const std::string&       cached_n_name,
    const Q5SideTables&      sides,
    NNameRevenueAggregator&  agg,
    Q5Stats*                 stats)
{
   // One composite-key probe: (c_nationkey, l_suppkey) ∈ supplier_nation_set.
   // Fuses SUPPLIER semi-join + cross-equality + suppkey equi-join.
   if (sides.supplier_nation_set.count(
           std::make_tuple(cached_c_nationkey, l.l_suppkey)) == 0)
      return;

   if (stats) stats->lineitems_passing_filter++;
   agg.accumulate(l, cached_n_name);
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

// Q5GroupWalkVisitor is a template so it can hold a reference to the
// nation adapter (needed for q5_resolve_n_name) without fixing the
// adapter type at header-include time.
template <typename NationAdapter>
struct Q5GroupWalkVisitor {
   const Params&                             params;
   const Q5SideTables&                       sides;
   NationAdapter&                            nation;
   std::unordered_map<Integer, std::string>& nationkey_to_name;  // per-query cache
   NNameRevenueAggregator&                   agg;
   Q5Stats*                                  stats = nullptr;

   // Per-group state: join-output of CUSTOMER ⋈ RN for the current custkey
   // group.  Populated at on_customer (customer-survival point); forwarded
   // into every on_lineitem call; cleared at on_group_end.
   q5_customer_rn_t cached_cust_rn{};

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
   // On survival, populate cached_cust_rn (including n_name via lazy NATION
   // PK lookup, cached in nationkey_to_name across the walk).
   ::tpch::WalkAction on_customer(Integer ck, const customer_coli_t& c)
   {
      if (stats) stats->customers_scanned++;
      if (sides.nation_set.count(c.c_nationkey) == 0) return ::tpch::WalkAction::SkipGroup;
      const std::string& n_name =
          q5_resolve_n_name(c.c_nationkey, nation, nationkey_to_name);
      cached_cust_rn = q5_customer_rn_t{ck, c.c_nationkey, n_name};
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

   // Per-lineitem: delegate to q5_admit_lineitem (shared with S1/S2/S4).
   // Passes cached_cust_rn.c_nationkey and cached_cust_rn.n_name —
   // both resolved once at on_customer, amortised over all lineitems in
   // the group.  Always continue (no per-lineitem skip case in Q5).
   ::tpch::WalkAction on_lineitem(const lineitem_col_t::Key& /*k*/, const lineitem_col_t& l)
   {
      if (stats) stats->lineitems_scanned++;
      q5_admit_lineitem(l, cached_cust_rn.c_nationkey, cached_cust_rn.n_name,
                        sides, agg, stats);
      return ::tpch::WalkAction::Continue;
   }

   // on_group_end: no per-group flush needed — the per-n_name aggregate
   // spans all groups.  Clear cached state to guard stale reads.
   void on_group_end(Integer /*ck*/)
   {
      cached_cust_rn = q5_customer_rn_t{};
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
   // n_name is resolved once per surviving customer via q5_resolve_n_name
   // and stored in q5_jr1_t.n_name (Phase 10B widening).  q5_jr2_t
   // propagates n_name from jr1.  Per-emit callback passes both
   // c_nationkey and n_name into q5_admit_lineitem — no further lookup.
   //
   // OPERATORS.md §6.1: same predicate logic as S3 — apples-to-apples.
   out.clear();

   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;
   std::unordered_map<Integer, std::string> nationkey_to_name;  // ~5 entries

   auto cust_sc = customer.getScanner();
   auto ord_sc  = col.split_orders().getScanner();

   // Pre-scan customers to build the set of accepted custkeys for fetch_ord
   // seek-skip.  Mirrors Q3 S1 fairness fix (q3/query.tpp:169–241): a small
   // upfront linear pass over CUSTOMER produces an ascending vector of
   // custkeys whose c_nationkey ∈ nation_set; fetch_ord answers "is this
   // custkey valid? if not, what's the next valid one?" in O(log n) via
   // std::lower_bound.  Avoids the BMJ refill-race that abandoned Q3I §G6
   // and Q3's lineitem-side seek.
   //
   // Customer-side cost: |customer| extra scanner pulls per query (~150K at
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
   // Resolves n_name at the customer-survival point via lazy NATION PK
   // lookup (~5 total across the full customer scan).
   auto fetch_cust = [&]() -> std::optional<std::pair<customerh_t::Key, customerh_t>> {
      while (auto kv = cust_sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_filter++;
         // Eagerly prime the cache so BMJ #1's emit callback can read
         // the n_name from nationkey_to_name without any new lookup.
         q5_resolve_n_name(kv->second.c_nationkey, nation, nationkey_to_name);
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
   // The BinaryMergeJoin emits q5_jr1_t via its default constructor +
   // payloads.  We intercept by wrapping fetch_bmj1 to inject n_name.
   BinaryMergeJoin<q5_cust_jk_t::Key, q5_jr1_t, customerh_t, orders_coli_t>
       bmj1(fetch_cust, fetch_ord);

   auto lin_sc = col.split_lineitem().getScanner();

   // fetch_bmj1: pull from bmj1 and attach n_name to each jr1 record.
   // n_name was cached in nationkey_to_name at fetch_cust time; the cache
   // hit is guaranteed because surviving customers primed it.
   auto fetch_bmj1 = [&]() -> std::optional<std::pair<q5_jr1_t::Key, q5_jr1_t>> {
      auto kv = bmj1.next();
      if (!kv) return std::nullopt;
      // Inject n_name into the q5_jr1_t record (Phase 10B widening).
      const Integer nationkey = kv->second.cust().c_nationkey;
      auto it = nationkey_to_name.find(nationkey);
      if (it != nationkey_to_name.end()) {
         kv->second.n_name = it->second;
      }
      return kv;
   };

   // fetch_lin: scan custkey-sorted lineitem split index (no pre-filter;
   // SUPPLIER probe fires inside q5_admit_lineitem).
   auto fetch_lin = [&]() -> std::optional<std::pair<lineitem_col_t::Key, lineitem_col_t>> {
      auto kv = lin_sc->next();
      if (kv && stats) stats->lineitems_scanned++;
      return kv;
   };

   // BMJ #2: jr1 ⋈ split_lineitem on q5_sort_key_t → q5_jr2_t.
   // q5_jr2_t propagates n_name from jr1 in its constructor (Phase 10B).
   // SKBuilder projects linenumber=WILDCARD_KEY on the build side (jr1)
   // and forwards the real linenumber on the probe side (lineitem_col_t).
   BinaryMergeJoin<q5_sort_key_t, q5_jr2_t, q5_jr1_t, lineitem_col_t>
       bmj2(fetch_bmj1, fetch_lin,
            [&](const q5_jr2_t::Key&, const q5_jr2_t& jr2) {
               // One composite-key probe; n_name already carried on jr2.
               q5_admit_lineitem(jr2.lineitem(),
                                 jr2.jr1().cust().c_nationkey,
                                 jr2.n_name,
                                 sides, agg, stats);
            });
   bmj2.run();

   agg.emit(out);
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
   // n_name is FD-attached at load time (1:1 with c_nationkey, not parameterised)
   // and read directly from v.n_name — no per-query NATION lookup needed.
   // Every parameterised filter is applied live here:
   //   1. c_nationkey ∈ nation_set     — customer gate (replaces mktsegment)
   //   2. o_orderdate window           — inline date-range arithmetic
   //   3. composite-key supplier probe — supplier_nation_set lookup
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
   Integer cur_custkey    = -1;
   bool    cur_custkey_ok = false;

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

      // Composite-key SUPPLIER probe + accumulate via shared helper.
      // v.n_name is FD-attached at load time — no runtime NATION lookup.
      q5_admit_lineitem(v, v.c_nationkey, v.n_name, sides, agg, stats);
   }

   agg.emit(out);
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
   // The visitor (Phase 10B):
   //   • on_customer  — gates by c_nationkey ∈ nation_set; skips group on miss.
   //                    Resolves n_name via q5_resolve_n_name (~5 NATION PK
   //                    lookups total, cached in nationkey_to_name); populates
   //                    cached_cust_rn for the current group.
   //   • on_order     — gates by orderdate window.
   //   • on_lineitem  — calls q5_admit_lineitem with cached_cust_rn fields.
   //                    One composite-key probe; no additional NATION lookup.
   //   • on_group_end — clears cached_cust_rn.
   //
   // After the walk, agg.emit() pushes ~5 rows (one per in-region nation),
   // then std::sort orders them revenue DESC.
   out.clear();
   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;
   std::unordered_map<Integer, std::string> nationkey_to_name;  // ~5 entries

   Q5GroupWalkVisitor<typename Backend::template Adapter<nation_t>>
       v{params, sides, nation, nationkey_to_name, agg, stats};
   col_group_walk<Backend>(col.merged_adapter(), v);

   agg.emit(out);
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
   // S4: Q3-isomorphic HashJoin chain on BASE tables (no col.split_*).
   //
   // Plan (Phase 10B — completes the fairness work started in 5312cbcd):
   //
   //   (1) cust_map<custkey, q5_customer_rn_t>
   //       Scan base CUSTOMER; gate by c_nationkey ∈ nation_set;
   //       resolve n_name lazily via NATION PK lookup (~5 total, cached).
   //       Payload IS the join-output of CUSTOMER ⋈ RN.
   //
   //   (2) orders_set<o_orderkey>   (PK-only — no payload)
   //       Scan base ORDERS; gate by orderdate window + o_custkey ∈ cust_map.
   //
   //   (3) sorted_orderkeys: sort orders_set into ascending vector.
   //       Drives a skip cursor into the base LINEITEM scan (seek-on-miss),
   //       mirroring S2's custkey-seek and S3's COL walker SkipGroup.
   //
   //   (4) Sequential LINEITEM scan with seek-on-miss:
   //       Per qualifying orderkey, ORDERS PK lookup → o_custkey →
   //       cust_map[custkey] yields q5_customer_rn_t (c_nationkey + n_name).
   //       Cached per orderkey (~4 lineitems per order).
   //       Per-lineitem: one composite-key supplier_nation_set probe.
   //
   // s4_hashtable_bytes accounts for cust_map + orders_set + sorted vector.
   // s4_orderkey_seeks counts one seek per sorted surviving orderkey
   // (including seek-on-miss advances through the sorted list).
   out.clear();

   Q5SideTables sides;
   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);

   NNameRevenueAggregator agg;
   std::unordered_map<Integer, std::string> nationkey_to_name;  // ~5 entries

   // ------------------------------------------------------------------
   // (1) cust_map: HashJoin(CUSTOMER ⋈ RN on c_nationkey = n_nationkey)
   // keyed by c_custkey, payload q5_customer_rn_t.
   std::unordered_map<Integer, q5_customer_rn_t> cust_map;
   {
      auto sc = customer.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->customers_scanned++;
         if (sides.nation_set.count(kv->second.c_nationkey) == 0) continue;
         if (stats) stats->customers_passing_filter++;
         const std::string& n_name =
             q5_resolve_n_name(kv->second.c_nationkey, nation, nationkey_to_name);
         cust_map.emplace(kv->first.c_custkey,
                          q5_customer_rn_t{kv->first.c_custkey,
                                           kv->second.c_nationkey,
                                           n_name});
      }
   }

   // ------------------------------------------------------------------
   // (2) orders_set: HashSemiJoin(ORDERS ⋉ cust_map on o_custkey = c_custkey)
   // PK-only build (orderkey only, no payload). Uses BASE orders adapter.
   std::unordered_set<Integer> orders_set;
   {
      auto sc = orders.getScanner();
      while (auto kv = sc->next()) {
         if (stats) stats->orders_scanned++;
         const Timestamp od = kv->second.o_orderdate;
         if (od < params.orderdate_lo || od >= params.orderdate_lo + 365) continue;
         if (cust_map.find(kv->second.o_custkey) == cust_map.end()) continue;
         if (stats) stats->orders_passing_filter++;
         orders_set.insert(kv->first.o_orderkey);
      }
   }

   // ------------------------------------------------------------------
   // (3) Sorted vector of qualifying orderkeys — drives the skip cursor.
   std::vector<Integer> sorted_orderkeys(orders_set.begin(), orders_set.end());
   std::sort(sorted_orderkeys.begin(), sorted_orderkeys.end());

   if (stats) {
      size_t bytes =
          cust_map.size()         * (sizeof(Integer) + sizeof(q5_customer_rn_t) + 24)
        + orders_set.size()       * (sizeof(Integer) + 24)
        + sorted_orderkeys.size() * sizeof(Integer);
      stats->s4_hashtable_bytes = static_cast<long>(bytes);
   }

   // ------------------------------------------------------------------
   // (4) Sequential LINEITEM scan over BASE lineitem table; probe orders_set
   // per row; on miss advance the cursor and seek to the next qualifying
   // orderkey.  Per orderkey transition, ORDERS PK lookup → o_custkey →
   // cust_map → q5_customer_rn_t (c_nationkey + n_name).
   {
      auto sc = lineitem.getScanner();
      size_t  cursor    = 0;   // index into sorted_orderkeys
      Integer cached_ok = -1;
      const q5_customer_rn_t* cached_cust = nullptr;

      // Seek to the first qualifying orderkey to skip any leading prefix.
      if (cursor < sorted_orderkeys.size()) {
         sc->seek(typename lineitem_t::Key{sorted_orderkeys[cursor], 0});
         if (stats) stats->s4_orderkey_seeks++;
      }

      while (cursor < sorted_orderkeys.size()) {
         auto kv = sc->next();
         if (!kv) break;
         if (stats) stats->lineitems_scanned++;
         const Integer ok = kv->first.l_orderkey;

         // Advance cursor past any orderkeys < ok (already consumed or absent).
         while (cursor < sorted_orderkeys.size() && sorted_orderkeys[cursor] < ok)
            ++cursor;
         if (cursor == sorted_orderkeys.size()) break;

         if (ok != sorted_orderkeys[cursor]) {
            // Miss: seek to the next qualifying orderkey.
            sc->seek(typename lineitem_t::Key{sorted_orderkeys[cursor], 0});
            if (stats) stats->s4_orderkey_seeks++;
            continue;
         }

         // Hit. Resolve the joined customer record on orderkey transition
         // (once per surviving order, amortised over ~4 lineitems per order).
         if (ok != cached_ok) {
            Integer custkey = -1;
            orders.lookup1(typename orders_t::Key{ok},
                           [&](const orders_t& o) { custkey = o.o_custkey; });
            cached_cust = &cust_map.at(custkey);
            cached_ok   = ok;
         }
         // One composite-key probe; n_name already resolved.
         q5_admit_lineitem(kv->second,
                           cached_cust->c_nationkey,
                           cached_cust->n_name,
                           sides, agg, stats);
      }
   }

   agg.emit(out);
   if (stats) {
      stats->aggregator_rows_out = static_cast<long>(out.size());
      stats->agg_buckets         = static_cast<long>(out.size());
   }
   std::sort(out.begin(), out.end(), q5_sort_cmp);
   return static_cast<long>(out.size());
}

}  // namespace tpch::q5
