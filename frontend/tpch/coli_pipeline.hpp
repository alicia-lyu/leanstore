#pragma once

// CUSTOMER × ORDERS × LINEITEM × INVOICE merged-index substrate.
//
// This pipeline owns MI(customer_coli_t, orders_coli_t, lineitem_coli_t,
// invoice_coli_t) keyed by Calcite-style tagged keys and exposes only
// the truly-shared subset:
//   - populate_merged(): dual-write replay over the four base adapters
//   - get_merged_size(): estimated size in MiB
//
// Operator drivers (PremergedJoin / BinaryMergeJoin / HashJoin) and view
// loading are per-query and live in q{N}/query.tpp / q{N}/load.tpp.
//
// Design note: Lineitem is rekeyed from (orderkey, linenumber) to
// (custkey, orderkey, invoicekey, linenumber). The custkey is not stored
// in lineitem's PK, so populate_merged resolves it from the orders base table
// by scanning orders first and building an orderkey → custkey map, then
// replaying lineitems with the resolved custkey.

#include "backend.hpp"
#include "views_coli.hpp"
#include "tpch_tables.hpp"

namespace tpch
{

template <typename Backend>
class CustomerOrdersLineitemInvoicePipeline
{
   // Non-owning references; lifetime managed by the per-query workload.
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;
   typename Backend::template Adapter<invoice_t>&    invoice;
   typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                            lineitem_coli_t, invoice_coli_t>& merged_coli;

  public:
   CustomerOrdersLineitemInvoicePipeline(
       typename Backend::template Adapter<customerh_t>&  customer,
       typename Backend::template Adapter<orders_t>&     orders,
       typename Backend::template Adapter<lineitem_t>&   lineitem,
       typename Backend::template Adapter<invoice_t>&    invoice,
       typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                                lineitem_coli_t, invoice_coli_t>& merged_coli);

   // Dual-write replay: scans all four base tables and inserts each record
   // into merged_coli using *_coli_t tagged keys. Lineitem records are rekeyed
   // from (orderkey, linenumber) to include custkey resolved from orders.
   void populate_merged();

   // Returns the estimated size of merged_coli in MiB.
   double get_merged_size() const;
};

}  // namespace tpch

#include "coli_pipeline.tpp"  // IWYU pragma: keep
