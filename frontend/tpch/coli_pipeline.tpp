// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>
// and the coli_group_walk free function template.

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
        merged_coli,
    typename Backend::template Adapter<orders_coli_t>&   orders_secondary,
    typename Backend::template Adapter<lineitem_coli_t>& lineitem_secondary,
    typename Backend::template Adapter<invoice_coli_t>&  invoice_secondary)
    : customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      merged_coli(merged_coli),
      orders_secondary(orders_secondary),
      lineitem_secondary(lineitem_secondary),
      invoice_secondary(invoice_secondary)
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
// populate_secondaries: build three custkey-sorted secondary indexes for S1.
//
// Mirrors populate_merged's scan order and the orderkey→custkey map pattern.
// Each record is inserted into its dedicated single-type secondary adapter
// using the same tagged-key encoding as in merged_coli, so the same
// key_from_base factories apply without duplication.
//
// Customer is already custkey-keyed via customerh_t, so no secondary for it.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_secondaries()
{
   // --- Pass 0: scan orders + build orderkey → custkey map ---
   std::unordered_map<Integer, Integer> orderkey_to_custkey;
   {
      auto scanner = orders.getScanner();
      while (auto kv = scanner->next()) {
         const orders_t::Key& ok = kv->first;
         const orders_t& ov      = kv->second;
         orderkey_to_custkey.emplace(ok.o_orderkey, ov.o_custkey);
         orders_secondary.insert(
             orders_coli_t::key_from_base(ov.o_custkey, ok),
             orders_coli_t::from_base(ov));
      }
   }

   // --- Pass 1: scan lineitems, resolve custkey from map ---
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t& lv      = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end()
                && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         lineitem_secondary.insert(
             lineitem_coli_t::key_from_base(custkey, lk, lv),
             lineitem_coli_t::from_base(lv));
      }
   }

   // --- Pass 2: scan invoices, rekey via payload i_custkey ---
   {
      auto scanner = invoice.getScanner();
      while (auto kv = scanner->next()) {
         const invoice_t::Key& ik = kv->first;
         const invoice_t& iv      = kv->second;
         invoice_secondary.insert(
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

// ---------------------------------------------------------------------------
// get_secondaries_size: sum of the three secondary adapter sizes in MiB.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_secondaries_size() const
{
   return orders_secondary.size() + lineitem_secondary.size() + invoice_secondary.size();
}

}  // namespace tpch

// ---------------------------------------------------------------------------
// coli_group_walk: free function template outside the tpch namespace so it
// mirrors the declaration in coli_pipeline.hpp.
//
// Pure dispatch — owns only:
//   1. Iterating the merged scanner.
//   2. Detecting custkey transitions and calling on_group_end.
//   3. Suppressing on_invoice/on_order/on_lineitem after on_customer→false.
//
// All query-specific record assembly, projection, and aggregation logic
// lives in the Visitor.  The walker never touches output containers.

namespace tpch
{

template <typename Backend, typename Visitor>
void coli_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& mi,
    Visitor& visitor)
{
   // Use customer_coli_t as the anchor JK/JR so the scanner starts at the
   // beginning and dispatches via coli_unfold_value_to_variant.
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   Integer cur_custkey = -1;
   bool    group_active = true;  // false after on_customer returned false

   while (auto kv = scanner->next()) {
      // Extract the custkey for this row from whichever variant arm it is.
      Integer row_custkey = std::visit(
          [](const auto& k) -> Integer { return k.custkey; }, kv->first);

      // Detect custkey group boundary.
      if (cur_custkey != -1 && row_custkey != cur_custkey) {
         if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
            visitor.on_group_end(cur_custkey);
         }
         group_active = true;  // reset for new group
      }
      cur_custkey = row_custkey;

      // Dispatch to the matching Visitor hook.
      std::visit(
          [&](auto&& val) {
             using V = std::decay_t<decltype(val)>;
             if constexpr (std::is_same_v<V, customer_coli_t>) {
                if constexpr (requires {
                                 { visitor.on_customer(row_custkey, val) } -> std::same_as<bool>;
                              }) {
                   group_active = visitor.on_customer(row_custkey, val);
                }
             } else if (group_active) {
                if constexpr (std::is_same_v<V, invoice_coli_t>) {
                   auto* ik = std::get_if<invoice_coli_t::Key>(&kv->first);
                   if constexpr (requires { visitor.on_invoice(*ik, val); }) {
                      if (ik) visitor.on_invoice(*ik, val);
                   }
                } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                   auto* ok = std::get_if<orders_coli_t::Key>(&kv->first);
                   if constexpr (requires { visitor.on_order(*ok, val); }) {
                      if (ok) visitor.on_order(*ok, val);
                   }
                } else if constexpr (std::is_same_v<V, lineitem_coli_t>) {
                   auto* lk = std::get_if<lineitem_coli_t::Key>(&kv->first);
                   if constexpr (requires { visitor.on_lineitem(*lk, val); }) {
                      if (lk) visitor.on_lineitem(*lk, val);
                   }
                }
             }
          },
          kv->second);
   }

   // Flush the final group.
   if (cur_custkey != -1) {
      if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
         visitor.on_group_end(cur_custkey);
      }
   }
}

}  // namespace tpch
