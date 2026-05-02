// Operator-translation reference: see ../OPERATORS.md
//   §3 op 4 (load vs query)  — view loading may use a manual two-pointer merge;
//                              query-time joins must use shared join primitives.
//   §4 (per-query view rationale) — unfiltered, predicate hoisted
//
// Template method bodies for Q3IWorkload<Backend> ctor / load / get_size,
// plus the free function populate_q3i_view.

#pragma once

#include <gflags/gflags.h>

#include <stdexcept>

DECLARE_int32(storage_structure);

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// View loading: materialise the ORDERS x LINEITEM join into a flat adapter.
//
// TODO Phase 2: implement populate_q3i_view using a manual two-pointer merge
// over the orders and lineitem base scanners (same pattern as populate_q12_view
// in q12/load.tpp). The view row type is currently aliased to joined_ol_t;
// widen it to include cust_open_due once the S2 path lands.

template <typename Backend>
static void populate_q3i_view(
    typename Backend::template Adapter<customerh_t>&,
    typename Backend::template Adapter<orders_t>&,
    typename Backend::template Adapter<lineitem_t>&,
    typename Backend::template Adapter<invoice_t>&,
    typename Backend::template Adapter<q3i_pipeline_view_t>&)
{
   // TODO Phase 2: manual two-pointer merge over orders + lineitem scanners.
   //       The cust_open_due sub-aggregate requires a prior invoice scan
   //       grouped by i_custkey with i_status='O' filter.
   //       Emit one q3i_pipeline_view_t row per (order, lineitem) pair.
}

// ---------------------------------------------------------------------------

template <typename Backend>
Q3IWorkload<Backend>::Q3IWorkload(
    TPCHWorkload<Backend::template Adapter>& tpch,
    typename Backend::template Adapter<customerh_t>& customer,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<invoice_t>& invoice,
    typename Backend::template Adapter<q3i_pipeline_view_t>& pipeline_view,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& merged_coli,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
    typename Backend::template Adapter<invoice_coli_t>&  split_invoice)
    : tpch(tpch),
      customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      coli(customer, orders, lineitem, invoice, merged_coli,
           split_orders, split_lineitem, split_invoice),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{
}

template <typename Backend>
void Q3IWorkload<Backend>::load()
{
   tpch.load();
   switch (FLAGS_storage_structure) {
      case 1: coli.populate_split(); break;
      case 4: break;  // base tables only
      case 2: /* TODO Phase 2: populate_q3i_view */ break;
      case 3: coli.populate_merged(); break;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

template <typename Backend>
double Q3IWorkload<Backend>::get_size() const
{
   double base = customer.size() + orders.size()
               + lineitem.size() + invoice.size();
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      case 4: return base;
      case 2: return 0.0;  // TODO Phase 2
      case 3: return coli.get_merged_size();
      default: throw std::runtime_error("invalid --storage_structure");
   }
}

}  // namespace tpch::q3i
