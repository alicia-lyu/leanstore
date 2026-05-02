#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Q12IWorkload<Backend>: Q12 extended with an INVOICE late-status aggregate.
// Replaces OrdersLineitemPipeline with COLIPipeline (4-table COLI MI) so a
// single PremergedJoin over MI(customer,orders,lineitem,invoice) can compute
// all three aggregates in one pass.
//
// Storage structures (--storage_structure flag):
//   1 = traditional indexes + merge join    -> query_by_base
//   2 = intermediate pipeline view          -> query_by_view
//   3 = MI[COLI] only (PremergedJoin)       -> query_by_merged
//   4 = traditional indexes + hash join     -> query_by_hash

#include <vector>

#include "../backend.hpp"
#include "../coli_pipeline.hpp"
#include "../tpch_workload.hpp"
#include "views.hpp"

namespace tpch::q12i
{

// ---------------------------------------------------------------------------
// Substitution parameters (Q12I SQL).
// Defaults: SHIPMODE1=MAIL, SHIPMODE2=SHIP, DATE=1994-01-01.

struct Params {
   Varchar<10> shipmode1;    // default "MAIL"
   Varchar<10> shipmode2;    // default "SHIP"
   Timestamp receiptdate_lo; // default DATE_1994_01_01
   Timestamp receiptdate_hi; // default DATE_1995_01_01

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to a raw lineitem before joining (structures 1 and 4).
// Encodes: l_shipmode IN (p.shipmode1, p.shipmode2)
//          AND l_shipdate < l_commitdate < l_receiptdate
//          AND l_receiptdate IN [p.receiptdate_lo, p.receiptdate_hi).
bool q12i_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t (structures 2 and 3).
// Same conditions expressed over joined_ol_t fields.
bool q12i_predicate_joined(const joined_ol_t& j, const Params& p);

// ---------------------------------------------------------------------------
// Optional per-query intermediate cardinality counters.

struct Q12IStats {
   long lineitems_scanned   = 0;
   long lineitems_passed    = 0;
   long variants_scanned    = 0;  // S3 only: total variant records from COLI MI scan
   long lineitems_admitted  = 0;  // S3 only
   long orders_built        = 0;  // S4 only
   long join_callbacks      = 0;
   long aggregator_rows_out = 0;

   void reset() { *this = Q12IStats{}; }
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q12IWorkload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<orders_t>&   orders;
   typename Backend::template Adapter<lineitem_t>& lineitem;
   typename Backend::template Adapter<invoice_t>&  invoice;

   // COLI 4-table pipeline: replaces the 2-table OL pipeline for Q12I.
   // Owns MI(customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t).
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q12i_pipeline_view_t>& pipeline_view;

  public:
   Params params;
   Q12IStats* stats = nullptr;

   Q12IWorkload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>& customer,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<invoice_t>& invoice,
       typename Backend::template Adapter<q12i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q12i_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q12i_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q12i_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q12i_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   void   load();
   double get_size() const;
};

}  // namespace tpch::q12i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
