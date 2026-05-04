// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>
// and the coli_group_walk free function template.

#pragma once

#include <cstdint>
#include <unordered_map>

#include <gflags/gflags.h>

#include "views_coli.hpp"

// A3-Linux re-A/B: runtime override for Backend::USE_PHYSICAL_SEEK_SKIP.
// Defined in tpch_flags.hpp; declared here to avoid an include-order
// dependency on the per-executable TPCH_DEFINE_FLAGS pattern.
DECLARE_int32(use_seek_skip);

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
    typename Backend::template Adapter<invoice_coli_t>&  split_invoice,
    typename Backend::template MergedAdapter<customer_acoli_t, orders_acoli_t,
                                             lineitem_acoli_t>& acoli)
    : customer(customer),
      orders(orders),
      lineitem(lineitem),
      invoice(invoice),
      merged_coli(merged_coli),
      split_orders_ref(split_orders),
      split_lineitem_ref(split_lineitem),
      split_invoice_ref(split_invoice),
      acoli_adapter_ref(acoli)
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
   //
   // Phase 1 note: the base lineitem adapter still holds lineitem_t rows
   // (l_invoicekey removed from lineitem_t in schema split phase 1).
   // lineitem_coli_t::key_from_base expects lineitem_i_t; wrap with
   // l_invoicekey=0 here.  Phase 2 will replace Adapter<lineitem_t> with
   // Adapter<lineitem_i_t> so the real invoice key is available.
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t& lv      = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end()
                && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         lineitem_i_t::Key lik{lk.l_orderkey, lk.l_linenumber};
         lineitem_i_t li(lv, 0);  // l_invoicekey=0 until Phase 2 swaps adapter
         merged_coli.template insert<lineitem_coli_t>(
             lineitem_coli_t::key_from_base(custkey, lik, li),
             lineitem_coli_t::from_base(li));
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
   //
   // Phase 1 note: same l_invoicekey=0 placeholder as in populate_merged.
   // Phase 2 will replace Adapter<lineitem_t> with Adapter<lineitem_i_t>.
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t& lv      = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end()
                && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         lineitem_i_t::Key lik{lk.l_orderkey, lk.l_linenumber};
         lineitem_i_t li(lv, 0);  // l_invoicekey=0 until Phase 2 swaps adapter
         split_lineitem_ref.insert(
             lineitem_coli_t::key_from_base(custkey, lik, li),
             lineitem_coli_t::from_base(li));
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

// ---------------------------------------------------------------------------
// populate_aggregated: build the aCOLI 3-type MI.
//
// Stores customer_acoli_t (pre_open_due baked), orders_acoli_t (no pre-aggregated
// revenue), and lineitem_acoli_t (unaggregated lineitem payload).  Invoice rows
// are collapsed to the pre_open_due scalar (i_status='O' is hardcoded by spec —
// parameter-independent).  Lineitem revenue is computed at query time so S5
// remains reusable across all DATE param sets.
//
// Two-pass algorithm (Pass B — lineitem revenue map — removed 2026-05-03):
//   Pass A: scan invoice → build custkey → Numeric open_due map.
//   Pass C: scan customer + orders + lineitem, resolving custkey for lineitems
//           via the orderkey→custkey map built during the orders sub-pass.
//           Insert all three record types into acoli_adapter_ref.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_aggregated()
{
   // --- Pass A: invoice scan → per-custkey open_due (i_status='O' filter) ---
   std::unordered_map<Integer, Numeric> open_due_map;
   {
      auto scanner = invoice.getScanner();
      while (auto kv = scanner->next()) {
         const invoice_t& inv = kv->second;
         auto s = std::string_view(inv.i_status.data, inv.i_status.length);
         if (s == "O") open_due_map[inv.i_custkey] += inv.i_totaldue;
      }
   }

   // --- Pass C: customer + orders + lineitem ---
   //
   // Sub-pass C1: scan customers.
   {
      auto cust_scan = customer.getScanner();
      while (auto kv = cust_scan->next()) {
         Integer custkey  = kv->first.c_custkey;
         Numeric open_due = open_due_map.count(custkey) ? open_due_map.at(custkey) : Numeric(0);
         customer_acoli_t::Key ak{custkey};
         acoli_adapter_ref.template insert<customer_acoli_t>(
             ak, customer_acoli_t::from_customer(kv->second, open_due));
      }
   }

   // Sub-pass C2: scan orders, build orderkey→custkey map, insert orders_acoli_t.
   // The map is reused by sub-pass C3 (lineitems) to resolve custkey.
   std::unordered_map<Integer, Integer> orderkey_to_custkey;
   {
      auto ord_scan = orders.getScanner();
      while (auto kv = ord_scan->next()) {
         const orders_t::Key& ok = kv->first;
         const orders_t&      ov = kv->second;
         Integer custkey = ov.o_custkey;
         orderkey_to_custkey.emplace(ok.o_orderkey, custkey);
         orders_acoli_t::Key ak = orders_acoli_t::key_from_order(custkey, ok);
         acoli_adapter_ref.template insert<orders_acoli_t>(
             ak, orders_acoli_t::from_order(ov, custkey));
      }
   }

   // Sub-pass C3: scan lineitems, resolve custkey, insert lineitem_acoli_t.
   {
      auto lin_scan = lineitem.getScanner();
      while (auto kv = lin_scan->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t&      lv = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end() && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         lineitem_acoli_t::Key ak = lineitem_acoli_t::key_from_base(custkey, lk);
         acoli_adapter_ref.template insert<lineitem_acoli_t>(
             ak, lineitem_acoli_t::from_base(lv));
      }
   }
}

// ---------------------------------------------------------------------------
// get_aggregated_size: delegate to the aCOLI adapter's size estimate.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_aggregated_size() const
{
   return acoli_adapter_ref.size();
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
                   if constexpr (requires {
                                    { visitor.on_invoice(*ik, val) } -> std::same_as<bool>;
                                 }) {
                      if (ik) group_active = visitor.on_invoice(*ik, val);
                   } else if constexpr (requires { visitor.on_invoice(*ik, val); }) {
                      if (ik) visitor.on_invoice(*ik, val);
                   }
                } else if constexpr (std::is_same_v<V, orders_coli_t>) {
                   auto* ok = std::get_if<orders_coli_t::Key>(&kv->first);
                   if constexpr (requires {
                                    { visitor.on_order(*ok, val) } -> std::same_as<bool>;
                                 }) {
                      if (ok) {
                         cur_orderkey = ok->orderkey;
                         group_active = visitor.on_order(*ok, val);
                      }
                   } else if constexpr (requires { visitor.on_order(*ok, val); }) {
                      if (ok) {
                         cur_orderkey = ok->orderkey;
                         visitor.on_order(*ok, val);
                      }
                   }
                } else if constexpr (std::is_same_v<V, lineitem_coli_t>) {
                   auto* lk = std::get_if<lineitem_coli_t::Key>(&kv->first);
                   if constexpr (requires {
                                    { visitor.on_lineitem(*lk, val) } -> std::same_as<bool>;
                                 }) {
                      if (lk) group_active = visitor.on_lineitem(*lk, val);
                   } else if constexpr (requires { visitor.on_lineitem(*lk, val); }) {
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
            // Mark the group dead immediately so no further on_order /
            // on_lineitem dispatch happens while forward-iterating to the
            // next custkey boundary.
            group_active  = false;
            skip_pending  = true;
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
                               if constexpr (requires {
                                                { visitor.on_invoice(*ik2, val2) } -> std::same_as<bool>;
                                             }) {
                                  if (ik2) group_active = visitor.on_invoice(*ik2, val2);
                               } else if constexpr (requires { visitor.on_invoice(*ik2, val2); }) {
                                  if (ik2) visitor.on_invoice(*ik2, val2);
                               }
                            } else if constexpr (std::is_same_v<V2, orders_coli_t>) {
                               auto* ok2 = std::get_if<orders_coli_t::Key>(&kv2->first);
                               if constexpr (requires {
                                                { visitor.on_order(*ok2, val2) } -> std::same_as<bool>;
                                             }) {
                                  if (ok2) {
                                     cur_orderkey = ok2->orderkey;
                                     group_active = visitor.on_order(*ok2, val2);
                                  }
                               } else if constexpr (requires { visitor.on_order(*ok2, val2); }) {
                                  if (ok2) {
                                     cur_orderkey = ok2->orderkey;
                                     visitor.on_order(*ok2, val2);
                                  }
                               }
                            } else if constexpr (std::is_same_v<V2, lineitem_coli_t>) {
                               auto* lk2 = std::get_if<lineitem_coli_t::Key>(&kv2->first);
                               if constexpr (requires {
                                                { visitor.on_lineitem(*lk2, val2) } -> std::same_as<bool>;
                                             }) {
                                  if (lk2) group_active = visitor.on_lineitem(*lk2, val2);
                               } else if constexpr (requires { visitor.on_lineitem(*lk2, val2); }) {
                                  if (lk2) visitor.on_lineitem(*lk2, val2);
                               }
                            }
                         }
                      },
                      kv2->second);
                  // Re-apply the group-level skip check for the re-dispatched record.
                  if constexpr (requires { { visitor.wants_skip_group() } -> std::convertible_to<bool>; }) {
                     if (group_active && visitor.wants_skip_group()) {
                        group_active  = false;
                        skip_pending  = true;
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
         // Seek to the next customer.
         //
         // RocksDB SF=40 dram=0.1: net LOSS on disk. Seek invalidates the
         // iterator's prefetch buffer (RocksDB pre-reads the next 1-2 SST
         // blocks ahead of the current iterator position); SSTRead/TX
         // rose ~5×.
         //
         // LeanStore (B-tree): the same trade-off does NOT apply.
         // B-tree Seek is a tree descent of O(log N) pages — there is
         // no prefetch buffer to invalidate. Forward iteration through
         // a rejected custkey's invoices+orders+lineitems can touch
         // 10–50 records (avg ~10 invoices, ~10 orders, ~30 lineitems
         // at SF=40), each potentially in a different leaf page. A
         // direct Seek to the next customer should win on B-tree.
         //
         // TODO: gate this constexpr on the Backend trait so RocksDB
         // and LeanStore branches differ. See PERFORMANCE.md §H2.
         // **Per-order skip** (analogous flag in on_order's
         // `skip_pending=true` branch above) is probably NOT worth
         // doing on either backend: order groups are small (avg ~4
         // lineitems per order) and the tree descent cost likely
         // outweighs forward-iterating past a handful of records.
         skip_pending = false;
         // Customer-level seek-skip ONLY (not order-level). Gated by the
         // Backend trait so RocksDB stays on forward iteration (Seek
         // invalidates its prefetch buffer; archived A/B 2026-05-02 was
         // on macOS — A3-Linux re-A/B in flight) while LeanStore takes
         // the Seek branch (B-tree descent is O(log N) page touches
         // with no prefetch buffer to lose). See q3i/PERFORMANCE.md §3
         // A3 and backend.hpp. FLAGS_use_seek_skip overrides at runtime.
         const bool seek_skip =
             FLAGS_use_seek_skip < 0 ? Backend::USE_PHYSICAL_SEEK_SKIP
                                     : (FLAGS_use_seek_skip != 0);
         if (seek_skip) {
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

// ---------------------------------------------------------------------------
// coli_group_walk_fused_emit — A2c performance variant.
//
// Mirrors coli_group_walk exactly in semantics but bypasses std::variant
// construction and std::visit double-dispatch.  Instead it calls
// scanner->next_raw() to obtain a raw (idx_tag, key_slice, value_slice)
// triple, then dispatches via a switch on the trailing idx_id byte.
//
// Key layout reminder (views_coli.hpp):
//   customer : ... [t=0][idx=customer(0)]  trailing byte = 0
//   orders   : ... [t=0][idx=orders(1)]    trailing byte = 1
//   lineitem : ... [t=0][idx=lineitem(2)]  trailing byte = 2
//   invoice  : ... [t=0][idx=invoice(3)]   trailing byte = 3
//
// custkey is always the first field after the leading domain-tag byte:
//   byte 0 : domain tag (customer=1)
//   bytes 1-4 : custkey folded big-endian XOR-flipped
// decode_custkey reads these four bytes and reverses the fold.

namespace {

// Decode custkey from the first (tag, field) step of any COLI key.
// Layout: key[0] = domain_tag::customer (1), key[1..4] = custkey big-endian XOR-flipped.
// This mirrors the unfold() logic from Types.hpp for Integer (u32 big-endian flip).
inline Integer decode_custkey_from_coli_key(const u8* key_bytes)
{
   // Skip the leading tag byte (1 byte), then unfold the 4-byte big-endian
   // XOR-flipped Integer.  The fold/unfold for Integer is defined in Types.hpp:
   //   fold:   XOR the value with 0x80000000, then store big-endian.
   //   unfold: read big-endian, then XOR with 0x80000000.
   const u8* p = key_bytes + 1;  // skip the domain-tag byte
   uint32_t raw = (static_cast<uint32_t>(p[0]) << 24)
                | (static_cast<uint32_t>(p[1]) << 16)
                | (static_cast<uint32_t>(p[2]) <<  8)
                | (static_cast<uint32_t>(p[3]));
   return static_cast<Integer>(raw ^ 0x80000000u);
}

}  // anonymous namespace

template <typename Backend, typename Visitor>
void coli_group_walk_fused_emit(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_coli_t, invoice_coli_t>& mi,
    Visitor& visitor)
{
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   Integer cur_custkey  = -1;
   Integer cur_orderkey = -1;
   bool    group_active = true;
   bool    skip_pending = false;

   // coli_idx_id integer values (see views_coli.hpp coli_idx_id enum).
   constexpr u8 TAG_CUSTOMER = static_cast<u8>(coli_idx_id::customer);  // 0
   constexpr u8 TAG_ORDERS   = static_cast<u8>(coli_idx_id::orders);    // 1
   constexpr u8 TAG_LINEITEM = static_cast<u8>(coli_idx_id::lineitem);  // 2
   constexpr u8 TAG_INVOICE  = static_cast<u8>(coli_idx_id::invoice);   // 3

   // Lambda to dispatch a raw record to the visitor, given pre-decoded key bytes
   // and value bytes.  Keeps the inner loop tight: no variant, no type-erasure.
   // Returns false if the current group has been deactivated (skip_pending set).
   auto dispatch = [&](u8 tag, const u8* key_bytes, size_t key_len,
                        const u8* val_bytes, size_t val_len) -> void {
      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }

      Integer row_custkey = decode_custkey_from_coli_key(key_bytes);

      // Detect custkey group boundary.
      if (cur_custkey != -1 && row_custkey != cur_custkey) {
         if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
            visitor.on_group_end(cur_custkey);
         }
         group_active = true;
      }
      cur_custkey = row_custkey;

      switch (tag) {
         case TAG_CUSTOMER: {
            customer_coli_t rec;
            std::memcpy(&rec, val_bytes, sizeof(customer_coli_t));
            if constexpr (requires {
                             { visitor.on_customer(row_custkey, rec) } -> std::same_as<bool>;
                          }) {
               group_active = visitor.on_customer(row_custkey, rec);
               if (!group_active) {
                  skip_pending = true;
                  if constexpr (requires { visitor.on_group_skipped(row_custkey); }) {
                     visitor.on_group_skipped(row_custkey);
                  }
               }
            }
            break;
         }
         case TAG_INVOICE: {
            if (!group_active) break;
            invoice_coli_t::Key ik;
            invoice_coli_t::unfoldKey(key_bytes, ik);
            invoice_coli_t rec;
            std::memcpy(&rec, val_bytes, sizeof(invoice_coli_t));
            if constexpr (requires {
                             { visitor.on_invoice(ik, rec) } -> std::same_as<bool>;
                          }) {
               group_active = visitor.on_invoice(ik, rec);
            } else if constexpr (requires { visitor.on_invoice(ik, rec); }) {
               visitor.on_invoice(ik, rec);
            }
            break;
         }
         case TAG_ORDERS: {
            if (!group_active) break;
            orders_coli_t::Key ok;
            orders_coli_t::unfoldKey(key_bytes, ok);
            orders_coli_t rec;
            std::memcpy(&rec, val_bytes, sizeof(orders_coli_t));
            if constexpr (requires {
                             { visitor.on_order(ok, rec) } -> std::same_as<bool>;
                          }) {
               cur_orderkey = ok.orderkey;
               group_active = visitor.on_order(ok, rec);
            } else if constexpr (requires { visitor.on_order(ok, rec); }) {
               cur_orderkey = ok.orderkey;
               visitor.on_order(ok, rec);
            }
            break;
         }
         case TAG_LINEITEM: {
            if (!group_active) break;
            lineitem_coli_t::Key lk;
            lineitem_coli_t::unfoldKey(key_bytes, lk);
            lineitem_coli_t rec;
            std::memcpy(&rec, val_bytes, sizeof(lineitem_coli_t));
            if constexpr (requires {
                             { visitor.on_lineitem(lk, rec) } -> std::same_as<bool>;
                          }) {
               group_active = visitor.on_lineitem(lk, rec);
            } else if constexpr (requires { visitor.on_lineitem(lk, rec); }) {
               visitor.on_lineitem(lk, rec);
            }
            break;
         }
         default:
            // Unknown tag — skip silently.  Should not occur for well-formed COLI keys.
            break;
      }

      // Visitor-driven custkey skip (mirrors baseline walker logic exactly).
      if constexpr (requires { { visitor.wants_skip_group() } -> std::convertible_to<bool>; }) {
         if (group_active && visitor.wants_skip_group()) {
            group_active  = false;
            skip_pending  = true;
            if constexpr (requires { visitor.on_group_skipped(cur_custkey); }) {
               visitor.on_group_skipped(cur_custkey);
            }
         }
      }
   };

   while (auto raw = scanner->next_raw()) {
      auto [tag, k_slice, v_slice] = *raw;
      const u8* key_bytes = reinterpret_cast<const u8*>(k_slice.data());
      const u8* val_bytes = reinterpret_cast<const u8*>(v_slice.data());

      dispatch(tag, key_bytes, k_slice.size(), val_bytes, v_slice.size());

      // Visitor-driven order skip: forward-iterate past current order's lineitems
      // without dispatching on_lineitem.  Mirrors baseline walker logic.
      if constexpr (requires { { visitor.wants_skip_order() } -> std::convertible_to<bool>; }) {
         if (group_active && cur_orderkey >= 0 && visitor.wants_skip_order()) {
            const Integer skip_orderkey = cur_orderkey;
            while (auto raw2 = scanner->next_raw()) {
               auto [tag2, k2_slice, v2_slice] = *raw2;
               const u8* key2 = reinterpret_cast<const u8*>(k2_slice.data());
               const u8* val2 = reinterpret_cast<const u8*>(v2_slice.data());

               // Check if this is still a lineitem for the same (custkey, orderkey).
               // A lineitem key has tag TAG_LINEITEM and the same custkey + orderkey prefix.
               bool is_same_order_lineitem = false;
               if (tag2 == TAG_LINEITEM && k2_slice.size() >= 10) {
                  Integer ck2 = decode_custkey_from_coli_key(key2);
                  // orderkey is at bytes 6-9 (after [t=1][custkey:4][t=3][orderkey:4]).
                  const u8* op = key2 + 6;
                  uint32_t ok_raw = (static_cast<uint32_t>(op[0]) << 24)
                                  | (static_cast<uint32_t>(op[1]) << 16)
                                  | (static_cast<uint32_t>(op[2]) <<  8)
                                  | (static_cast<uint32_t>(op[3]));
                  Integer ok2 = static_cast<Integer>(ok_raw ^ 0x80000000u);
                  is_same_order_lineitem = (ck2 == cur_custkey && ok2 == skip_orderkey);
               }

               if (!is_same_order_lineitem) {
                  // Not a lineitem for this order — dispatch it via the main path.
                  dispatch(tag2, key2, k2_slice.size(), val2, v2_slice.size());
                  break;
               }
               // Same-order lineitem: suppress dispatch, count as visited.
               if constexpr (requires { visitor.on_record_visited(); }) {
                  visitor.on_record_visited();
               }
            }
         }
      }

      if (skip_pending) {
         skip_pending = false;
         // Customer-level seek-skip ONLY (not order-level). Backend-trait
         // gated with runtime override (FLAGS_use_seek_skip); see baseline
         // walker comment above and q3i/PERFORMANCE.md §3 A3.
         const bool seek_skip =
             FLAGS_use_seek_skip < 0 ? Backend::USE_PHYSICAL_SEEK_SKIP
                                     : (FLAGS_use_seek_skip != 0);
         if (seek_skip) {
            typename customer_coli_t::Key next_key{cur_custkey + 1};
            scanner->template seek<customer_coli_t>(next_key);
            if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
               visitor.on_group_end(cur_custkey);
            }
            cur_custkey  = -1;
            group_active = true;
         }
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
