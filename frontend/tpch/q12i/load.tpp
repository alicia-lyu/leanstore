// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading may use a manual two-pointer merge;
//                              query-time joins must use shared join primitives.
//   §4 (per-query view rationale) — unfiltered, predicate hoisted
//
// Template method bodies for Q12IWorkload<Backend> ctor / load / get_size,
// plus the free function populate_q12i_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>

DECLARE_int32(storage_structure);

namespace tpch::q12i
{

// ---------------------------------------------------------------------------
// View loading: materialise the ORDERS x LINEITEM join into a flat adapter.
//
// TODO: implement populate_q12i_view using a manual two-pointer merge over
// the orders and lineitem base scanners (same pattern as populate_q12_view in
// q12/load.tpp).  The view row type is currently aliased to joined_ol_t; widen
// it to include invoice fields once the COLI MI driver lands.

template <typename Backend>
static void populate_q12i_view(
    typename Backend::template Adapter<orders_t>&,
    typename Backend::template Adapter<lineitem_t>&,
    typename Backend::template Adapter<invoice_t>&,
    typename Backend::template Adapter<q12i_pipeline_view_t>&)
{
   // TODO: manual two-pointer merge over orders + lineitem scanners,
   //       with an invoice lookup per lineitem (l_invoicekey → i_status).
   //       Emit one q12i_pipeline_view_t row per lineitem.
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q12IWorkload<Backend>::Q12IWorkload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>& customer,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<invoice_t>& invoice,
    typename Backend::template Adapter<q12i_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& merged_coli)
    : tpch(tpch),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      coli(customer, orders, lineitem, invoice, merged_coli),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q12IWorkload<Backend>::load()
{
   // TODO: dispatch on FLAGS_storage_structure:
   //   case 1, 4: tpch.load() only (base tables)
   //   case 2:    tpch.load(); populate_q12i_view(orders, lineitem, invoice, pipeline_view)
   //   case 3:    tpch.load(); coli.populate_merged()
   tpch.load();
}

template <typename Backend>
double Q12IWorkload<Backend>::get_size() const
{
   // TODO: dispatch on FLAGS_storage_structure:
   //   case 1, 4: orders.size() + lineitem.size()
   //   case 2:    pipeline_view.size()
   //   case 3:    coli.get_merged_size()
   return 0.0;
}

}  // namespace tpch::q12i
