#pragma once

// Q9-specific record types layered on top of the shared ORDERS x LINEITEM
// pipeline types in ../views_ol.hpp.
//
// Three types are defined here:
//   - Params              : TPC-H §2.4.9 substitution parameters.
//   - q9_pipeline_view_t  : structure 2 intermediate view (aliases joined_ol_t).
//   - q9_agg_row_t        : final aggregate output row (one row per nation+year).
//
// Predicate function declarations live here; bodies are in query.tpp.

#include "../views_ol.hpp"

namespace tpch::q9
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.9).
// Default: COLOR=green  ->  name_pattern="%green%".

struct Params {
   Varchar<55> name_pattern;  // default "%green%"; applied as LIKE to p_name

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// The remaining 4 joins (PART, PARTSUPP, SUPPLIER, NATION) are performed at
// query time against base-table adapters, keeping the view schema minimal.
// Aliases joined_ol_t — no narrower projection needed at the OL pipeline level.

using q9_pipeline_view_t = ::tpch::joined_ol_t;

// ---------------------------------------------------------------------------
// Final aggregate output row: one per (n_name, o_year).

struct q9_agg_row_t {
   static constexpr int id = 33;

   struct Key {
      static constexpr int id = 33;
      Varchar<25> n_name;
      Integer     o_year;
      ADD_KEY_TRAITS(&Key::n_name, &Key::o_year)
   };

   Varchar<25> n_name;
   Integer     o_year;
   Numeric     sum_profit;  // SUM(l_extendedprice*(1-l_discount) - ps_supplycost*l_quantity)

   ADD_RECORD_TRAITS(q9_agg_row_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a fully-assembled joined_ol_t (structures 2 and 3) before the
// per-lineitem PART/PARTSUPP/SUPPLIER/NATION lookups.
// Currently a no-op at the OL level — the LIKE filter is on p_name (PART),
// which is only available after the PART join.
// See: frontend/tpch/q9/CLAUDE.md §Plan Descriptions.
bool q9_predicate_joined(const joined_ol_t& j, const Params& p);

}  // namespace tpch::q9
