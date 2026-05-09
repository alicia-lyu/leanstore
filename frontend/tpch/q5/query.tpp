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
   // Phase 4 §7.3: sequential scan of q5_pipeline_view_t; per-nation
   // HashAggregate over post-filter rows.
   out.clear();
   return 0;
}

template <typename Backend>
long Q5Workload<Backend>::query_by_merged(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.1: col_group_walk over MI[COL] with a Q5-specific Visitor;
   // SUPPLIER + NATION + REGION side hash builds pre-walk.
   out.clear();
   return 0;
}

template <typename Backend>
long Q5Workload<Backend>::query_by_hash(std::vector<q5_agg_row_t>& out)
{
   // Phase 4 §7.5: HashJoin chain over base tables (S4 baseline).
   out.clear();
   return 0;
}

}  // namespace tpch::q5
