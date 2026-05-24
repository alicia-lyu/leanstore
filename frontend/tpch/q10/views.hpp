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
//
// Phase 1 (this file): pipeline-view shape lands with the leading-key
// shape (custkey, orderkey, linenumber); payload is finalised in
// commit 3. q10_agg_row_t lands flat (no nested Key struct) — c_custkey
// is the conceptual GROUP BY key per Decision E2, but the row is never
// stored in a B-tree adapter (TopNSink consumes it directly).

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
// orderdate filter). `l_returnflag` is spec-hardcoded but kept live for
// view-reusability across a hypothetical no-returnflag variant.
//
// No n_name in the payload — NATION attaches uniformly as a per-customer
// INL on PK at emit time (Decision D6). Deliberately NOT FD-attached at
// view-load time.

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
   Timestamp  o_orderdate;

   // FD-attached customer columns. Phase 1 commit 1 ships a placeholder
   // payload to keep load.tpp's view branch compilable; commit 3 widens
   // this to the full 7 output columns (c_custkey, c_name, c_address,
   // c_nationkey, c_phone, c_acctbal, c_comment). Varchar<N> (POD), never
   // std::string — record_traits use memcpy.

   ADD_RECORD_TRAITS(q10_pipeline_view_t)

   void print(std::ostream& os) const;
};

// Final aggregate output row: one per surviving customer; top-20 selected
// outside the pipeline. In-memory only — never stored in a B-tree or
// RocksDB adapter; TopNSink consumes it directly (Phase 4 §7.1).
//
// Conceptual GROUP BY key is c_custkey (Decision E2) — a unique field per
// row, giving the TopNSink comparator (revenue DESC, c_custkey ASC) a
// built-in tiebreaker. The 6 other GROUP BY columns are functional
// dependencies of c_custkey and ride in the payload.
struct q10_agg_row_t {
   // Group key (conceptual; not a nested Key struct because the row is
   // in-memory only).
   Integer    c_custkey;

   // Aggregate.
   Numeric    revenue;

   // FD-attached output columns (commit 3 widens to the full set; Phase 1
   // commit 1 ships placeholders so query.tpp's stub signature compiles).
   // Commit-3 additions: c_name, c_acctbal, c_address, c_phone, c_comment,
   // and n_name resolved per-customer via NATION INL at emit (Decision D6).

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
