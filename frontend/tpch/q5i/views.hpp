#pragma once

// Q5I-specific record types: Q5 extended with invoice payment-status split.
// Row-shape types only. Query-time parameters and predicate declarations live
// in workload.hpp alongside Q5IWorkload.

#include "../tpch_family/views_coli.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q5i
{

// Structure 2 pipeline view: one row per (custkey, orderkey, invoicekey,
// linenumber) — Key mirrors lineitem_coli_t's leading record-type key so
// the view and MI are sortable in the same order. Predicate-hoisted:
// o_orderdate and region/nation/supplier filters applied at query time.
// i_status resolved from invoice join at view-load time (Pattern B:
// view loader deferred to Phase 4a — reuses S3 walker with parameterised
// filters dropped; see PLAYBOOK.md §3.6).
//
// n_name is NOT in this payload: the COLI MI is 4-table (C,O,L,I), so
// storing n_name here would make S2 a 5-table substitute and unfair vs S3.
// n_name is resolved post-pipeline (≤5 NATION PK lookups per query) in
// both S2 and S3 — see q5i/out_class.hpp and CLAUDE.md §Plan Descriptions.
struct q5i_pipeline_view_t {
   static constexpr int id = 62;

   struct Key {
      static constexpr int id = 62;
      Integer custkey;
      Integer orderkey;
      Integer invoicekey;   // mirrors lineitem_coli_t key shape
      Integer linenumber;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey,
                     &Key::invoicekey, &Key::linenumber)
      auto operator<=>(const Key&) const = default;
   };

   Numeric     l_extendedprice;
   Numeric     l_discount;
   Integer     l_suppkey;
   Integer     c_nationkey;
   Timestamp   o_orderdate;
   Varchar<1>  i_status;

   ADD_RECORD_TRAITS(q5i_pipeline_view_t)

   void print(std::ostream& os) const;
};

// Final aggregate output row: one per nation in the chosen region (≤5 rows).
// In-memory only — never stored in a B-tree or RocksDB adapter.
struct q5i_agg_row_t {
   std::string n_name;
   Numeric     nominal_revenue;   // all qualifying lineitems
   Numeric     realised_revenue;  // i_status = 'P'
   Numeric     open_revenue;      // i_status = 'O'
   Numeric     late_revenue;      // i_status = 'L'

   void print(std::ostream& os) const;
};

}  // namespace tpch::q5i
