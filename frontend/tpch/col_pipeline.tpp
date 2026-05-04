// Template method bodies for CustomerOrdersLineitemPipeline<Backend>
// and the col_group_walk free function template.

#pragma once

#include <cstdint>
#include <unordered_map>

#include <gflags/gflags.h>

#include "views_col.hpp"

// A3-Linux re-A/B: runtime override for Backend::USE_PHYSICAL_SEEK_SKIP.
// Defined in tpch_flags.hpp; declared here to avoid an include-order
// dependency on the per-executable TPCH_DEFINE_FLAGS pattern.
DECLARE_int32(use_seek_skip);

namespace tpch
{

// ---------------------------------------------------------------------------
// Constructor

template <typename Backend>
CustomerOrdersLineitemPipeline<Backend>::CustomerOrdersLineitemPipeline(
    typename Backend::template Adapter<customerh_t>&  customer,
    typename Backend::template Adapter<orders_t>&     orders,
    typename Backend::template Adapter<lineitem_t>&   lineitem,
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& merged_col,
    typename Backend::template Adapter<orders_coli_t>&   split_orders,
    typename Backend::template Adapter<lineitem_col_t>& split_lineitem)
    : customer(customer),
      orders(orders),
      lineitem(lineitem),
      merged_col(merged_col),
      split_orders_ref(split_orders),
      split_lineitem_ref(split_lineitem)
{
}

// ---------------------------------------------------------------------------
// populate_merged: 3-pass replay over the three base tables.
//
// Insertion order: customer → orders → lineitems.
// All three scans are forward (primary-key order); because merged_col sorts
// on tagged keys, byte-lex order is automatically maintained by RocksDB.
//
// Lineitem rekey: lineitem's PK is (orderkey, linenumber); the COL merged key
// needs (custkey, orderkey, linenumber). We resolve custkey by scanning orders
// first and building an orderkey → custkey map, then replay lineitems with the
// resolved custkey.

template <typename Backend>
void CustomerOrdersLineitemPipeline<Backend>::populate_merged()
{
   // --- Pass 0: scan customers ---
   {
      auto scanner = customer.getScanner();
      while (auto kv = scanner->next()) {
         merged_col.template insert<customer_coli_t>(
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
         merged_col.template insert<orders_coli_t>(
             orders_coli_t::key_from_base(ov.o_custkey, ok),
             orders_coli_t::from_base(ov));
      }
   }

   // --- Pass 2: scan lineitem rows, resolve custkey via map ---
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         const lineitem_t::Key& lk = kv->first;
         const lineitem_t& lv      = kv->second;
         auto it = orderkey_to_custkey.find(lk.l_orderkey);
         assert(it != orderkey_to_custkey.end()
                && "lineitem references unknown orderkey");
         Integer custkey = it->second;
         merged_col.template insert<lineitem_col_t>(
             lineitem_col_t::key_from_base(custkey, lk),
             lineitem_col_t::from_base(lv));
      }
   }
}

// ---------------------------------------------------------------------------
// populate_split: build two custkey-sorted split indexes for S1.
//
// Mirrors populate_merged's scan order and the orderkey→custkey map pattern.
// Each record is inserted into its dedicated single-type split adapter using
// the same tagged-key encoding as in merged_col.
//
// Customer is already custkey-keyed via customerh_t, so no split index for it.

template <typename Backend>
void CustomerOrdersLineitemPipeline<Backend>::populate_split()
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

   // --- Pass 1: scan lineitem rows, resolve custkey via map ---
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
             lineitem_col_t::key_from_base(custkey, lk),
             lineitem_col_t::from_base(lv));
      }
   }
}

// ---------------------------------------------------------------------------
// get_merged_size: delegates to the adapter's CF-level size estimate.

template <typename Backend>
double CustomerOrdersLineitemPipeline<Backend>::get_merged_size() const
{
   return merged_col.size();
}

// ---------------------------------------------------------------------------
// get_split_size: sum of the two split adapter sizes in MiB.

template <typename Backend>
double CustomerOrdersLineitemPipeline<Backend>::get_split_size() const
{
   return split_orders_ref.size() + split_lineitem_ref.size();
}

}  // namespace tpch

// ---------------------------------------------------------------------------
// col_group_walk: free function template outside the tpch namespace so it
// mirrors the declaration in col_pipeline.hpp.
//
// Pure dispatch — owns only:
//   1. Iterating the merged scanner.
//   2. Detecting custkey transitions and calling on_group_end.
//   3. Suppressing on_order/on_lineitem after on_customer→false.
//
// No on_invoice arm: COL has no invoice records.
// Mirrors coli_group_walk with the invoice dispatch arm deleted and
// lineitem_col_t substituted for lineitem_coli_t.

namespace tpch
{

template <typename Backend, typename Visitor>
void col_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& mi,
    Visitor& visitor)
{
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   Integer cur_custkey  = -1;
   Integer cur_orderkey = -1;   // latest orderkey seen; used by wants_skip_order
   bool    group_active = true;
   bool    skip_pending = false;

   while (auto kv = scanner->next()) {
      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }

      Integer row_custkey = std::visit(
          [](const auto& k) -> Integer { return k.custkey; }, kv->first);

      // Detect custkey group boundary.
      if (cur_custkey != -1 && row_custkey != cur_custkey) {
         if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
            visitor.on_group_end(cur_custkey);
         }
         group_active = true;
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
                      skip_pending = true;
                      if constexpr (requires { visitor.on_group_skipped(row_custkey); }) {
                         visitor.on_group_skipped(row_custkey);
                      }
                   }
                }
             } else if (group_active) {
                if constexpr (std::is_same_v<V, orders_coli_t>) {
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
                } else if constexpr (std::is_same_v<V, lineitem_col_t>) {
                   auto* lk = std::get_if<lineitem_col_t::Key>(&kv->first);
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

      // Visitor-driven custkey skip.
      if constexpr (requires { { visitor.wants_skip_group() } -> std::convertible_to<bool>; }) {
         if (group_active && visitor.wants_skip_group()) {
            group_active  = false;
            skip_pending  = true;
            if constexpr (requires { visitor.on_group_skipped(cur_custkey); }) {
               visitor.on_group_skipped(cur_custkey);
            }
         }
      }

      // Visitor-driven order skip: forward-iterate past the rejected order's
      // lineitems without dispatching on_lineitem.
      if constexpr (requires { { visitor.wants_skip_order() } -> std::convertible_to<bool>; }) {
         if (group_active && cur_orderkey >= 0 && visitor.wants_skip_order()) {
            const Integer skip_orderkey = cur_orderkey;
            while (auto kv2 = scanner->next()) {
               if constexpr (requires { visitor.on_record_visited(); }) {
                  visitor.on_record_visited();
               }
               auto* lk = std::get_if<lineitem_col_t::Key>(&kv2->first);
               if (!lk || lk->custkey != cur_custkey || lk->orderkey != skip_orderkey) {
                  // Not a lineitem for this order — re-dispatch inline.
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
                            if constexpr (std::is_same_v<V2, orders_coli_t>) {
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
                            } else if constexpr (std::is_same_v<V2, lineitem_col_t>) {
                               auto* lk2 = std::get_if<lineitem_col_t::Key>(&kv2->first);
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
         skip_pending = false;
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

// ---------------------------------------------------------------------------
// col_group_walk_fused_emit — A2c performance variant.
//
// Mirrors col_group_walk exactly in semantics but bypasses std::variant
// construction and std::visit double-dispatch. Calls scanner->next_raw() to
// obtain a raw (idx_tag, key_slice, value_slice) triple, then dispatches via
// a switch on the trailing idx_id byte.
//
// Key layout (views_col.hpp):
//   customer  : ... [t=0][idx=customer(0)]    trailing byte = 0
//   orders    : ... [t=0][idx=orders(1)]      trailing byte = 1
//   lineitem  : ... [t=0][idx=lineitem_col(36)] trailing byte = 36
//
// custkey is always the first field after the leading domain-tag byte:
//   byte 0 : domain tag (customer=1)
//   bytes 1-4 : custkey folded big-endian XOR-flipped

namespace {

// Decode custkey from the first (tag, field) step of any COL key.
// Layout: key[0] = domain_tag::customer (1), key[1..4] = custkey big-endian XOR-flipped.
inline Integer decode_custkey_from_col_key(const u8* key_bytes)
{
   const u8* p = key_bytes + 1;  // skip the domain-tag byte
   uint32_t raw = (static_cast<uint32_t>(p[0]) << 24)
                | (static_cast<uint32_t>(p[1]) << 16)
                | (static_cast<uint32_t>(p[2]) <<  8)
                | (static_cast<uint32_t>(p[3]));
   return static_cast<Integer>(raw ^ 0x80000000u);
}

}  // anonymous namespace

template <typename Backend, typename Visitor>
void col_group_walk_fused_emit(
    typename Backend::template MergedAdapter<customer_coli_t, orders_coli_t,
                                             lineitem_col_t>& mi,
    Visitor& visitor)
{
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   Integer cur_custkey  = -1;
   Integer cur_orderkey = -1;
   bool    group_active = true;
   bool    skip_pending = false;

   // coli_idx_id integer values for the three COL record types.
   constexpr u8 TAG_CUSTOMER = static_cast<u8>(coli_idx_id::customer);         // 0
   constexpr u8 TAG_ORDERS   = static_cast<u8>(coli_idx_id::orders);           // 1
   constexpr u8 TAG_LINEITEM = static_cast<u8>(LINEITEM_COL_IDX_ID);           // 36

   auto dispatch = [&](u8 tag, const u8* key_bytes, size_t key_len,
                        const u8* val_bytes, size_t /*val_len*/) -> void {
      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }

      Integer row_custkey = decode_custkey_from_col_key(key_bytes);

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
            lineitem_col_t::Key lk;
            lineitem_col_t::unfoldKey(key_bytes, lk);
            lineitem_col_t rec;
            std::memcpy(&rec, val_bytes, sizeof(lineitem_col_t));
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
            // Unknown tag — skip silently.
            break;
      }

      // Visitor-driven custkey skip.
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

      // Visitor-driven order skip: forward-iterate past current order's
      // lineitems without dispatching on_lineitem.
      if constexpr (requires { { visitor.wants_skip_order() } -> std::convertible_to<bool>; }) {
         if (group_active && cur_orderkey >= 0 && visitor.wants_skip_order()) {
            const Integer skip_orderkey = cur_orderkey;
            while (auto raw2 = scanner->next_raw()) {
               auto [tag2, k2_slice, v2_slice] = *raw2;
               const u8* key2 = reinterpret_cast<const u8*>(k2_slice.data());
               const u8* val2 = reinterpret_cast<const u8*>(v2_slice.data());

               // Check if this is still a lineitem for the same (custkey, orderkey).
               bool is_same_order_lineitem = false;
               if (tag2 == TAG_LINEITEM && k2_slice.size() >= 10) {
                  Integer ck2 = decode_custkey_from_col_key(key2);
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
                  dispatch(tag2, key2, k2_slice.size(), val2, v2_slice.size());
                  break;
               }
               if constexpr (requires { visitor.on_record_visited(); }) {
                  visitor.on_record_visited();
               }
            }
         }
      }

      if (skip_pending) {
         skip_pending = false;
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
