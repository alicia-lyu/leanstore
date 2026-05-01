// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>.
//
// STEP-3 STUB: The proxy record types (customerh_coli_t / orders_coli_t /
// lineitem_coli_t / invoice_coli_t) and their coli_proxy_key_t have been
// removed from views_coli.hpp as part of the tagged-key migration (step 3).
//
// populate_merged() is temporarily a no-op here. Step 4 will rewire it to
// use the new customer_coli_t / orders_coli_t / lineitem_coli_t /
// invoice_coli_t record types (and their from_base() / key_from_base()
// helpers) and change the MergedAdapter template parameter in
// coli_pipeline.hpp from the base types to the four coli_* types.
//
// get_merged_size() is unchanged — it delegates to the adapter's size()
// which does not depend on record type.

#pragma once

#include "views_coli.hpp"

namespace tpch
{

// ---------------------------------------------------------------------------
// Constructor

template <typename Backend>
CustomerOrdersLineitemInvoicePipeline<Backend>::CustomerOrdersLineitemInvoicePipeline(
    typename Backend::template Adapter<customerh_t>& customer,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<invoice_t>& invoice,
    typename Backend::template MergedAdapter<customerh_t, orders_t, lineitem_t, invoice_t>&
        merged_coli)
    : customer(customer), orders(orders), lineitem(lineitem), invoice(invoice),
      merged_coli(merged_coli)
{
}

// ---------------------------------------------------------------------------
// populate_merged — STUB (step-3 bridge; rewired in step 4)
//
// The old implementation used coli_proxy_key_t and SKBuilder<coli_sort_key_t>,
// both of which have been deleted. Step 4 will replace this body with direct
// tagged-key inserts using the new *_coli_t::from_base() helpers.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_merged()
{
   // no-op until step 4 rewires the tagged-key inserts
}

// ---------------------------------------------------------------------------
// get_merged_size: delegates to the adapter's CF-level size estimate.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_merged_size() const
{
   return merged_coli.size();
}

}  // namespace tpch
