// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>
// and the coli_group_walk free function template.

#pragma once

#include <cstdint>
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
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_coli_t>& split_lineitem,
    typename Backend::template Adapter<invoice_coli_t>&  split_invoice)
    : customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      merged_coli(merged_coli),
      split_orders_ref(split_orders),
      split_lineitem_ref(split_lineitem),
      split_invoice_ref(split_invoice)
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
         invoice_coli_t converted  = invoice_coli_t::from_base(iv);
         merged_coli.template insert<invoice_coli_t>(
             invoice_coli_t::key_from_base(iv.i_custkey, ik),
             converted);
      }
   }
}

// ---------------------------------------------------------------------------
// populate_split: build three custkey-sorted split indexes for S1.
//
// Mirrors populate_merged's scan order and the orderkey→custkey map pattern.
// Each record is inserted into its dedicated single-type split adapter
// using the same tagged-key encoding as in merged_coli, so the same
// key_from_base factories apply without duplication.
//
// Customer is already custkey-keyed via customerh_t, so no split index for it.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_split()
{
   // --- Pass 0: scan orders + build orderkey → custkey map ---
   std::unordered_map<Integer, Integer> orderkey_to_custkey;
   {
      auto scanner = orders.getScanner();
      while (auto kv = scanner->next()) {
         const orders_t::Key& ok = kv->first;
         const orders_t& ov      = kv->second;
         orderkey_to_custkey.emplace(ok.o_orderkey, ov.o_custkey);
         split_orders_ref.insert(
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
         split_lineitem_ref.insert(
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
         split_invoice_ref.insert(
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
// get_split_size: sum of the three split adapter sizes in MiB.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_split_size() const
{
   return split_orders_ref.size() + split_lineitem_ref.size() + split_invoice_ref.size();
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

   Integer cur_custkey  = -1;
   Integer cur_orderkey = -1;    // latest orderkey seen; used by wants_skip_order
   bool    group_active = true;  // false after on_customer returned false
   bool    skip_pending = false; // set when we want to seek past current group

   while (auto kv = scanner->next()) {
      // Optional bytes-of-work counter hook for instrumentation.
      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }

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
                   if (!group_active) {
                      // Physical skip: don't iterate the rest of this custkey's
                      // invoices/orders/lineitems. The customer ALWAYS sorts
                      // first within its group (tag=1 customer < 2 invoice <
                      // 3 orders < 4 lineitem), so seeking to the customer
                      // record at custkey+1 lands at the next group's first
                      // row (or past-end). Saves up to ~98% of iterator cost
                      // on filter-rejected groups.
                      skip_pending = true;
                      if constexpr (requires { visitor.on_group_skipped(row_custkey); }) {
                         visitor.on_group_skipped(row_custkey);
                      }
                   }
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
                      if (ok) {
                         cur_orderkey = ok->orderkey;
                         visitor.on_order(*ok, val);
                      }
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

      // Visitor-driven custkey skip: lets a hook request a custkey-level skip
      // mid-group when the threshold filter finalises false on the first
      // on_order. Cheaper than waiting for the rest of the OL sub-tree
      // to scroll past and be filtered row-by-row.
      if constexpr (requires { { visitor.wants_skip_group() } -> std::convertible_to<bool>; }) {
         if (group_active && visitor.wants_skip_group()) {
            skip_pending = true;
            if constexpr (requires { visitor.on_group_skipped(cur_custkey); }) {
               visitor.on_group_skipped(cur_custkey);
            }
         }
      }

      // Visitor-driven order skip: when on_order rejects a date filter, the
      // visitor can request that the walker forward-iterate past the rejected
      // order's lineitems without dispatching on_lineitem. No physical Seek is
      // used (Seek invalidates the prefetch buffer — see SF=40 A/B finding for
      // the custkey-skip). Only fires when there is an open order group.
      if constexpr (requires { { visitor.wants_skip_order() } -> std::convertible_to<bool>; }) {
         if (group_active && cur_orderkey >= 0 && visitor.wants_skip_order()) {
            const Integer skip_orderkey = cur_orderkey;
            // Forward-iterate past lineitems for this (custkey, orderkey)
            // without dispatching on_lineitem. Break on any non-lineitem
            // record or a different (custkey, orderkey).
            while (auto kv2 = scanner->next()) {
               if constexpr (requires { visitor.on_record_visited(); }) {
                  visitor.on_record_visited();
               }
               auto* lk = std::get_if<lineitem_coli_t::Key>(&kv2->first);
               if (!lk || lk->custkey != cur_custkey || lk->orderkey != skip_orderkey) {
                  // Not a lineitem for this order — process it in the outer loop.
                  // We must re-dispatch this record. Re-enter the top of the loop
                  // by updating kv and breaking. Since we can't easily re-inject
                  // into a while(auto kv = ...) loop, we handle it inline here.
                  //
                  // Detect custkey boundary and dispatch appropriately.
                  Integer row_custkey2 = std::visit(
                      [](const auto& k) -> Integer { return k.custkey; }, kv2->first);
                  if (cur_custkey != -1 && row_custkey2 != cur_custkey) {
                     if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
                        visitor.on_group_end(cur_custkey);
                     }
                     group_active = true;
                  }
                  cur_custkey = row_custkey2;
                  std::visit(
                      [&](auto&& val2) {
                         using V2 = std::decay_t<decltype(val2)>;
                         if constexpr (std::is_same_v<V2, customer_coli_t>) {
                            if constexpr (requires {
                                             { visitor.on_customer(row_custkey2, val2) } -> std::same_as<bool>;
                                          }) {
                               group_active = visitor.on_customer(row_custkey2, val2);
                               if (!group_active) {
                                  skip_pending = true;
                                  if constexpr (requires { visitor.on_group_skipped(row_custkey2); }) {
                                     visitor.on_group_skipped(row_custkey2);
                                  }
                               }
                            }
                         } else if (group_active) {
                            if constexpr (std::is_same_v<V2, invoice_coli_t>) {
                               auto* ik2 = std::get_if<invoice_coli_t::Key>(&kv2->first);
                               if constexpr (requires { visitor.on_invoice(*ik2, val2); }) {
                                  if (ik2) visitor.on_invoice(*ik2, val2);
                               }
                            } else if constexpr (std::is_same_v<V2, orders_coli_t>) {
                               auto* ok2 = std::get_if<orders_coli_t::Key>(&kv2->first);
                               if constexpr (requires { visitor.on_order(*ok2, val2); }) {
                                  if (ok2) {
                                     cur_orderkey = ok2->orderkey;
                                     visitor.on_order(*ok2, val2);
                                  }
                               }
                            } else if constexpr (std::is_same_v<V2, lineitem_coli_t>) {
                               auto* lk2 = std::get_if<lineitem_coli_t::Key>(&kv2->first);
                               if constexpr (requires { visitor.on_lineitem(*lk2, val2); }) {
                                  if (lk2) visitor.on_lineitem(*lk2, val2);
                               }
                            }
                         }
                      },
                      kv2->second);
                  // Re-apply the group-level skip check for the re-dispatched record.
                  if constexpr (requires { { visitor.wants_skip_group() } -> std::convertible_to<bool>; }) {
                     if (group_active && visitor.wants_skip_group()) {
                        skip_pending = true;
                        if constexpr (requires { visitor.on_group_skipped(cur_custkey); }) {
                           visitor.on_group_skipped(cur_custkey);
                        }
                     }
                  }
                  break;
               }
               // Same order, lineitem — suppress dispatch and continue forward.
            }
         }
      }

      if (skip_pending) {
         // A/B EXPERIMENT (2026-05-02): the seek-based skip below trades
         // forward iteration past unwanted records for an explicit
         // RocksDB Seek per skipped custkey group. SF=40 dram=0.1
         // showed this is a net loss on disk (Seek invalidates the
         // iterator's prefetch buffer; SSTRead/TX rose 5×). Disabled by
         // default; re-enable to measure cache-resident behaviour.
         skip_pending = false;
         constexpr bool USE_PHYSICAL_SEEK_SKIP = false;
         if constexpr (USE_PHYSICAL_SEEK_SKIP) {
            typename customer_coli_t::Key next_key{cur_custkey + 1};
            scanner->template seek<customer_coli_t>(next_key);
            if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
               visitor.on_group_end(cur_custkey);
            }
            cur_custkey  = -1;
            group_active = true;
         }
         // When the physical skip is disabled, group_active stays false
         // (set by on_customer or wants_skip_group) and the loop simply
         // forward-iterates past the rest of the group; on_invoice /
         // on_order / on_lineitem are short-circuited by the existing
         // `else if (group_active)` guard.
      }
   }

   // Flush the final group.
   if (cur_custkey != -1) {
      if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
         visitor.on_group_end(cur_custkey);
      }
   }
}

}  // namespace tpch
