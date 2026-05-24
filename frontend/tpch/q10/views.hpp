#pragma once

// Q10-specific record types: Returned Item Reporting (per-customer top-20
// over returned lineitems within a 3-month order window). Row-shape types
// only. Query-time parameters and predicate declarations live in
// workload.hpp.
//
// Reuses the COL pipeline record types from tpch_family/views_col.hpp:
//   customer_coli_t : already carries the 7 wide output columns
//                     (c_name, c_address, c_nationkey, c_phone,
//                      c_acctbal, c_comment) — no widening.
//   orders_coli_t   : carries o_orderdate — no widening.
//   lineitem_col_t  : carries l_returnflag, l_extendedprice,
//                     l_discount — no widening.
//
// ID allocation:
//   q10_pipeline_view_t : id = 70
//   q10_agg_row_t       : id = 71 (reserved; type is in-memory only)

#include "../tpch_family/views_col.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q10
{

// Structure 2 pipeline view: one row per (custkey, orderkey, linenumber).
// Key mirrors lineitem_col_t's key shape so the view and the COL MI are
// sortable in the same order (PLAYBOOK §3.6 "Key shape").
//
// Predicate-hoisted: o_orderdate window is parameterised and applies live
// at query time (Decision D4 — view-scan plan rolls per-order before the
// orderdate filter). `l_returnflag` is spec-hardcoded but kept live (not
// baked) for view-reusability across a hypothetical no-returnflag variant.
//
// FD-attached customer columns ride on every lineitem row (Decision D3 —
// single scanner, duplicated per-lineitem). This carries the 7 output
// columns the per-customer SUM-then-TopN chain consumes downstream so S2
// has no separate customer scan.
//
// No n_name in the payload — NATION attaches uniformly as a per-customer
// INL on PK at record-assembly time (Decision D6). Deliberately NOT
// FD-attached at view-load time.
//
// All payload columns are Varchar<N> POD or numeric scalars; never
// std::string — record_traits use memcpy and libstdc++ std::string is
// non-standard-layout (cf. Q5 commit a1fbdc15).

struct q10_pipeline_view_t {
   static constexpr int id = 70;

   struct Key {
      static constexpr int id = 70;
      Integer custkey;
      Integer orderkey;
      Integer linenumber;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)
      auto operator<=>(const Key&) const = default;
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric    l_extendedprice;
   Numeric    l_discount;
   Varchar<1> l_returnflag;

   // FD-attached order column (parameterised filter applied above the
   // view scan; the S2-anomaly per-orderkey SUM groups by orderkey using
   // this).
   Timestamp  o_orderdate;

   // FD-attached customer payload (7 output columns + c_nationkey for
   // the NATION INL probe at record-assembly time).
   Varchar<25>  c_name;
   Varchar<40>  c_address;
   Integer      c_nationkey;
   Varchar<15>  c_phone;
   Numeric      c_acctbal;
   Varchar<117> c_comment;

   ADD_RECORD_TRAITS(q10_pipeline_view_t)

   void print(std::ostream& os) const;
};

// Conceptual post-assembly output row — materialised only at the TopN-sink
// boundary, never as a pipeline-internal intermediate. In-memory only;
// no ADD_RECORD_TRAITS, no stored schema. TopNSink consumes it directly
// (Phase 4 §7.1).
//
// FD output columns arrive differently per storage variant: in S1/S3 they
// come for free from the in-walker / in-BMJ customer record; in S2 they're
// carried in the view payload (D3); in S4 they're recovered via
// customer.lookup1 + NATION INL at record-assembly time (D5 + D6). The
// q10_agg_row_t shape is the *output* contract, not a join-build payload.
//
// Conceptual GROUP BY key is c_custkey (Decision E2) — a unique field per
// row, giving the TopNSink comparator (revenue DESC, c_custkey ASC) a
// built-in tiebreaker (CONVENTIONS §Post-pipeline OutClass).
struct q10_agg_row_t {
   // Group key (conceptual; not a nested Key struct because the row is
   // in-memory only).
   Integer    c_custkey;

   // Aggregate.
   Numeric    revenue;

   // FD output columns (resolved via INL at record-assembly time in S4;
   // pre-materialised in the view/walker payload in S1/S2/S3).
   Varchar<25>  c_name;
   Numeric      c_acctbal;
   Varchar<25>  n_name;          // from NATION INL (D6)
   Varchar<40>  c_address;
   Varchar<15>  c_phone;
   Varchar<117> c_comment;

   void print(std::ostream& os) const;

   // Comparator for the bounded top-20 sink (Phase 4 §7.1).
   //   revenue DESC, c_custkey ASC
   // The c_custkey tiebreaker is unique-per-row (each customer appears at
   // most once in the result) — satisfies the strict-weak-ordering
   // contract per CONVENTIONS §Post-pipeline OutClass.
   static bool cmp(const q10_agg_row_t& a, const q10_agg_row_t& b)
   {
      if (a.revenue != b.revenue) return a.revenue > b.revenue;
      return a.c_custkey < b.c_custkey;
   }
};

}  // namespace tpch::q10
