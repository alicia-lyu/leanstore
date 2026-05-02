#pragma once

// Operator-translation reference: see ../OPERATORS.md
//   §3 (per-operator strategy)    — what each operator looks like in C++
//   §3 op 4 (load vs query)       — keep query-time joins on shared JoinState
//   §6 (comparison-integrity)     — read before changing join strategy
//
// Q3IWorkload<Backend>: Q3 extended with a per-customer outstanding-invoice
// sub-aggregate (cust_open_due = SUM(i_totaldue) WHERE i_status='O').
// Replaces OrdersLineitemPipeline with COLIPipeline (4-table COLI MI).
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

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// Substitution parameters (Q3I SQL).
// Defaults: SEGMENT=BUILDING, DATE=1995-03-15, THRESHOLD=0.

struct Params {
   Varchar<10> mktsegment;   // default "BUILDING"
   Timestamp   orderdate;    // default DATE '1995-03-15' — orders before this date
   Timestamp   shipdate;     // default DATE '1995-03-15' — lineitems shipped after
   Numeric     threshold;    // default 0 — minimum cust_open_due to include

   static Params defaults();
};

// ---------------------------------------------------------------------------
// Predicate declarations. Bodies live in query.tpp.

// Applied to an orders row: o_orderdate < params.orderdate.
bool q3i_predicate_orders(const orders_t& o, const Params& p);

// Applied to a raw lineitem: l_shipdate > params.shipdate.
bool q3i_predicate_lineitem(const lineitem_t& l, const Params& p);

// Applied to a fully-assembled joined_ol_t: checks both order and lineitem
// conditions simultaneously (structures 2 and 3).
bool q3i_predicate_joined(const joined_ol_t& j, const Params& p);

// Applied to an invoice row: i_status = 'O' (for open-due sub-aggregate).
bool q3i_predicate_invoice(const invoice_t& i);

// ---------------------------------------------------------------------------
// Optional per-query intermediate cardinality counters.

struct Q3IStats {
   long customers_scanned   = 0;
   long orders_scanned      = 0;
   long lineitems_scanned   = 0;
   long invoices_scanned    = 0;
   long join_callbacks      = 0;
   long aggregator_rows_out = 0;

   void reset() { *this = Q3IStats{}; }
};

// ---------------------------------------------------------------------------

template <typename Backend>
class Q3IWorkload
{
   // The full TPC-H base-table workload (owns part, supplier, etc.).
   TPCHWorkload<Backend::template Adapter>& tpch;

   // Direct references to the base adapters needed at query time.
   typename Backend::template Adapter<customerh_t>& customer;
   typename Backend::template Adapter<orders_t>&    orders;
   typename Backend::template Adapter<lineitem_t>&  lineitem;
   typename Backend::template Adapter<invoice_t>&   invoice;

   // COLI 4-table pipeline: owns MI(customer_coli_t, orders_coli_t,
   // lineitem_coli_t, invoice_coli_t).
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;

   // Structure 2: intermediate pipeline view (joined_ol_t rows, unfiltered).
   typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view;

  public:
   Params params;
   Q3IStats* stats = nullptr;

   Q3IWorkload(
       TPCHWorkload<Backend::template Adapter>& tpch,
       typename Backend::template Adapter<customerh_t>& customer,
       typename Backend::template Adapter<orders_t>& orders,
       typename Backend::template Adapter<lineitem_t>& lineitem,
       typename Backend::template Adapter<invoice_t>& invoice,
       typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli,
       typename Backend::template Adapter<orders_coli_t>&   orders_secondary,
       typename Backend::template Adapter<lineitem_coli_t>& lineitem_secondary,
       typename Backend::template Adapter<invoice_coli_t>&  invoice_secondary);

   // ------------------------------------------------------------------
   // Queries — one per storage structure.
   // Returns the number of result rows (at most 10, LIMIT 10 by revenue).
   // ------------------------------------------------------------------

   long query_by_base  (std::vector<q3i_agg_row_t>& out);  // structure 1
   long query_by_view  (std::vector<q3i_agg_row_t>& out);  // structure 2
   long query_by_merged(std::vector<q3i_agg_row_t>& out);  // structure 3
   long query_by_hash  (std::vector<q3i_agg_row_t>& out);  // structure 4

   // ------------------------------------------------------------------
   // Loading / sizing
   // ------------------------------------------------------------------

   void   load();
   double get_size() const;
};

}  // namespace tpch::q3i

#include "load.tpp"   // IWYU pragma: keep
#include "query.tpp"  // IWYU pragma: keep
