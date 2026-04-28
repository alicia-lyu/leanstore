#pragma once

// Q12-specific record types layered on top of the shared ORDERS x LINEITEM
// pipeline types in ../views_ol.hpp.
//
// Three types are defined here:
//   - Params             : TPC-H §2.4.12 substitution parameters.
//   - q12_pipeline_view_t: structure 2 intermediate view (aliases joined_ol_t).
//   - q12_agg_row_t      : final aggregate output row (one row per shipmode).
//
// Predicate function declarations live here; bodies are in query.tpp.

#include "../views_ol.hpp"

namespace tpch::q12
{

// ---------------------------------------------------------------------------
// Substitution parameters (TPC-H §2.4.12).
// Defaults: SHIPMODE1=MAIL, SHIPMODE2=SHIP, DATE=1994-01-01.

struct Params {
   Varchar<10> shipmode1;    // default "MAIL"
   Varchar<10> shipmode2;    // default "SHIP"
   Timestamp receiptdate_lo; // default 1994-01-01 (days since epoch)
   Timestamp receiptdate_hi; // default 1995-01-01 (days since epoch)

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: ORDERS x LINEITEM join output, unfiltered.
// Predicate is hoisted to query time so the view is reusable across param sets.
// Aliases joined_ol_t — no narrower projection needed for Q12.

using q12_pipeline_view_t = ::tpch::joined_ol_t;

// ---------------------------------------------------------------------------
// Final aggregate output row: one per shipmode group.

struct q12_agg_row_t {
   static constexpr int id = 31;

   struct Key {
      static constexpr int id = 31;
      Varchar<10> l_shipmode;
      ADD_KEY_TRAITS(&Key::l_shipmode)
   };

   Varchar<10> l_shipmode;
   Integer high_line_count;
   Integer low_line_count;

   ADD_RECORD_TRAITS(q12_agg_row_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a raw lineitem before joining (structures 1 and 4).
// Encodes: l_shipmode IN (p.shipmode1, p.shipmode2)
//          AND l_shipdate < l_commitdate < l_receiptdate
//          AND l_receiptdate IN [p.receiptdate_lo, p.receiptdate_hi).
// See: frontend/tpch/q12/CLAUDE.md §Q12-Specific Operator Configurations.
bool q12_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// Same five conditions expressed over joined_ol_t fields.
// See: frontend/tpch/q12/CLAUDE.md §Q12-Specific Operator Configurations.
bool q12_predicate_joined(const joined_ol_t& j, const Params& p);

}  // namespace tpch::q12
