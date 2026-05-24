#pragma once

// Q10I-specific record types: Q10 extended with invoice return-payment split.
// Row-shape types only. Query-time parameters and predicate declarations live
// in workload.hpp alongside Q10IWorkload.
//
// ID allocation:
//   q10i_pipeline_view_t : id = 68
//   q10i_agg_row_t       : id = 69 (reserved; type is in-memory only)

#include <string>

#include "../../shared/view_templates.hpp"
#include "../tpch_family/views_coli.hpp"
#include "../tpch_tables.hpp"
#include "../q10/views.hpp"

namespace tpch::q10i
{

// Structure 2 pipeline view: one row per (custkey, orderkey, invoicekey,
// linenumber) — Key mirrors lineitem_coli_t's leading record-type key so
// the view and COLI MI are sortable in the same order.
// (PLAYBOOK §3.6 "Key shape"; memory feedback-view-key-mirrors-mi.)
//
// Predicate-hoisted: o_orderdate window and l_returnflag filter applied
// live at query time (D7) so the view is reusable across all DATE param sets.
// i_status resolved from the invoice_buf at view-load time (Pattern B:
// view loader deferred to Phase 4a — reuses S3 walker with parameterised
// filters dropped; see PLAYBOOK.md §3.6).
//
// FD-attached customer columns ride on every lineitem row (Decision D3 of
// Q10, inherited — single scanner, duplicated per-lineitem) so S2 needs no
// separate customer scan at query time.
//
// No n_name in the payload (D9 from Q10I CLAUDE.md): NATION attaches
// uniformly as a per-customer INL on PK at emit time. Deliberately NOT
// FD-attached at view-load time (would make S2 a 5-table substitute).
//
// All payload columns are Varchar<N> POD or numeric scalars; never
// std::string — record_traits use memcpy and libstdc++ std::string is
// non-standard-layout (PLAYBOOK PITFALL; cf. Q5 commit a1fbdc15).
struct q10i_pipeline_view_t {
   static constexpr int id = 68;

   struct Key {
      static constexpr int id = 68;
      Integer custkey;
      Integer orderkey;
      Integer invoicekey;   // mirrors lineitem_coli_t key shape (D14/E2)
      Integer linenumber;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey,
                     &Key::invoicekey, &Key::linenumber)
      auto operator<=>(const Key&) const = default;
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric    l_extendedprice;
   Numeric    l_discount;
   Varchar<1> l_returnflag;

   // FD-attached order column (parameterised filter applied above the view
   // scan; the S2-anomaly per-orderkey SUM groups by orderkey using this).
   Timestamp  o_orderdate;

   // Invoice dimension field — FD-attached at view-load time (Pattern B).
   Varchar<1> i_status;

   // FD-attached customer payload (7 output columns + c_nationkey for
   // the NATION INL probe at record-assembly time).
   Varchar<25>  c_name;
   Varchar<40>  c_address;
   Integer      c_nationkey;
   Varchar<15>  c_phone;
   Numeric      c_acctbal;
   Varchar<117> c_comment;

   ADD_RECORD_TRAITS(q10i_pipeline_view_t)

   void print(std::ostream& os) const;
};

// Conceptual post-assembly output row — materialised only at the TopN-sink
// boundary (at most 20 rows). In-memory only; no ADD_RECORD_TRAITS, no
// stored schema. Derives from q10_agg_row_t (D13) and adds three
// return-revenue partitions split by invoice payment status.
//
// Field naming follows the Q10I SQL column list:
//   paid_returns  = SUM(revenue WHERE i_status = 'P')
//   open_returns  = SUM(revenue WHERE i_status = 'O')
//   late_returns  = SUM(revenue WHERE i_status = 'L')
//
// The base `revenue` field (inherited) is the total return_revenue column
// used as the sort key for TopN(20) and as a parity check that
// paid + open + late == revenue.
struct q10i_agg_row_t : public tpch::q10::q10_agg_row_t {
   Numeric paid_returns = 0;  // i_status = 'P'
   Numeric open_returns = 0;  // i_status = 'O'
   Numeric late_returns = 0;  // i_status = 'L'

   void print(std::ostream& os) const;
};

}  // namespace tpch::q10i
