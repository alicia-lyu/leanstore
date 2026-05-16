#pragma once

// Q5I post-pipeline OutClass — Rule 11 "first post-pipeline operator".
// Owned by query_by_* bodies; one instance per query. Receives
// q5i_pipeline_out_t rows from whichever in-pipeline operator the
// chosen storage variant uses (S3 visitor, S2 view scan, future S1
// BMJ chain, future S4 hash chain) and runs steps 1-2 of the shared
// post-pipeline chain documented in q5i/CLAUDE.md §Plan Descriptions:
//
//   1. supplier_nation_set probe (fuses SUPPLIER semi-join +
//      cross-equality c_nationkey = s_nationkey + l_suppkey = s_suppkey).
//   2. per-c_nationkey hash aggregate routed by i_status.
//
// Steps 3-4 (NATION PK lookup for n_name; sort by nominal_revenue DESC)
// live in q5i_resolve_n_names() + std::sort, called explicitly by each
// query_by_* body after the pipeline has finished feeding `out`.

#include <cstring>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../tpch_tables.hpp"
#include "views.hpp"

// Note: Q5SideTables is **not** forward-declared here; Q5IOutClass is
// templated on the Sides type so callers (`query.tpp`, where
// `q5::Params` and `q5::Q5SideTables` are already complete) instantiate
// with `q5::Q5SideTables`. This keeps `out_class.hpp` free of the
// q5/side_tables.hpp template body, which references a forward-declared
// `q5::Params` and would fail two-phase lookup here.

namespace tpch::q5i
{

// Pipeline output record — assembled by every storage variant's
// in-pipeline operator. In-memory only.
struct q5i_pipeline_out_t {
   Integer    c_nationkey;
   Integer    l_suppkey;
   Numeric    l_extendedprice;
   Numeric    l_discount;
   Varchar<1> i_status;
};

// 4-sum bucket: one per surviving c_nationkey in the OutClass aggregate.
struct q5i_revenue_quad_t {
   Numeric nominal  = 0;
   Numeric realised = 0;
   Numeric open     = 0;
   Numeric late     = 0;
};

// OutClass-proper: aggregate keyed by c_nationkey, fed via emit().
// No knowledge of NATION; no sort; no storage-variant dependency.
// Templated on Sides to avoid including q5/side_tables.hpp here (see
// header note); callers instantiate with `q5::Q5SideTables`.
template <typename Sides>
struct Q5IOutClass {
   const Sides& sides;
   std::unordered_map<Integer, q5i_revenue_quad_t> by_nationkey;

   explicit Q5IOutClass(const Sides& sides_) : sides(sides_) {}

   // Step 1: supplier_nation_set probe.
   // Step 2: per-nationkey hash aggregate routed by i_status.
   void emit(const q5i_pipeline_out_t& row)
   {
      if (!sides.supplier_nation_set.count(
              std::make_tuple(row.c_nationkey, row.l_suppkey))) {
         return;  // SUPPLIER semi-join + cross-equality miss.
      }
      const Numeric revenue =
          row.l_extendedprice * (Numeric{1} - row.l_discount);
      auto& q = by_nationkey[row.c_nationkey];
      q.nominal += revenue;
      const char s = row.i_status.data[0];
      if (s == 'P')      q.realised += revenue;
      else if (s == 'O') q.open     += revenue;
      else if (s == 'L') q.late     += revenue;
      // Any other status is data-generator bug; nominal still accounts
      // for it, the three split fields don't — drift surfaces in tests.
   }

   const auto& buckets() const { return by_nationkey; }
};

// Post-OutClass step 3: NATION PK lookup per surviving bucket, one
// q5i_agg_row_t per. Caller invokes std::sort afterwards (step 4).
// ≤5 buckets per query; cache inside this function is per-call (each
// bucket has a distinct c_nationkey so a cache adds nothing).
template <typename Sides, typename NationAdapter>
inline void q5i_resolve_n_names(
    const Q5IOutClass<Sides>&     out,
    NationAdapter&                nation,
    std::vector<q5i_agg_row_t>&   results)
{
   results.reserve(results.size() + out.buckets().size());
   for (const auto& [nationkey, quad] : out.buckets()) {
      q5i_agg_row_t row;
      nation.lookup1(typename nation_t::Key{nationkey},
                     [&](const nation_t& n) {
                        row.n_name.assign(
                            n.n_name.data,
                            strnlen(n.n_name.data, sizeof(n.n_name.data)));
                     });
      row.nominal_revenue  = quad.nominal;
      row.realised_revenue = quad.realised;
      row.open_revenue     = quad.open;
      row.late_revenue     = quad.late;
      results.push_back(std::move(row));
   }
}

}  // namespace tpch::q5i
