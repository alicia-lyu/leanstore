// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>.

#pragma once

#include <unordered_map>

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
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>&
        merged_coli)
    : customer(customer), orders(orders), lineitem(lineitem), invoice(invoice),
      merged_coli(merged_coli)
{
}

// ---------------------------------------------------------------------------
// populate_merged: dual-write replay over the four base tables.
//
// Insertion order: customer → orders → lineitems → invoices.
// All four scans are forward (primary-key order); because merged_coli sorts
// on tagged keys, byte-lex order is automatically maintained by RocksDB.
//
// Lineitem rekey: lineitem's PK is (orderkey, linenumber); the COLI merged
// key needs (custkey, orderkey, invoicekey, linenumber).  We resolve custkey
// by scanning orders first and building an orderkey → custkey map, then
// replay lineitems with the resolved custkey.
//
// Invoice rekey: invoice's PK is (invoicekey); the COLI key needs
// (custkey, invoicekey).  i_custkey is already stored in the payload, so
// no auxiliary map is needed.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_merged()
{
   // --- Pass 0: scan customers ---
   {
      auto scanner = customer.getScanner();
      while (auto kv = scanner->next()) {
         merged_coli.template insert<customer_coli_t>(
             customer_coli_t::key_from_base(kv->first),
             customer_coli_t::from_base(kv->second));
      }
   }

   // --- Pass 1: scan orders + build orderkey → custkey map ---
   //
   // The map is needed by the lineitem pass.  At SF=1, orders ≈ 1.5M rows,
   // so the map fits comfortably in memory.
   std::unordered_map<Integer, Integer> orderkey_to_custkey;
   {
      auto scanner = orders.getScanner();
      while (auto kv = scanner->next()) {
         const orders_t::Key& ok = kv->first;
         const orders_t& ov      = kv->second;
         orderkey_to_custkey.emplace(ok.o_orderkey, ov.o_custkey);
         merged_coli.template insert<orders_coli_t>(
             orders_coli_t::key_from_base(ov.o_custkey, ok),
             orders_coli_t::from_base(ov));
      }
   }

   // --- Pass 2: scan lineitems, resolve custkey from map ---
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t& lv      = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end()
                && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         merged_coli.template insert<lineitem_coli_t>(
             lineitem_coli_t::key_from_base(custkey, lk, lv),
             lineitem_coli_t::from_base(lv));
      }
   }

   // --- Pass 3: scan invoices, rekey via payload i_custkey ---
   {
      auto scanner = invoice.getScanner();
      while (auto kv = scanner->next()) {
         const invoice_t::Key& ik = kv->first;
         const invoice_t& iv      = kv->second;
         merged_coli.template insert<invoice_coli_t>(
             invoice_coli_t::key_from_base(iv.i_custkey, ik),
             invoice_coli_t::from_base(iv));
      }
   }
}

// ---------------------------------------------------------------------------
// get_merged_size: delegates to the adapter's CF-level size estimate.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_merged_size() const
{
   return merged_coli.size();
}

}  // namespace tpch
