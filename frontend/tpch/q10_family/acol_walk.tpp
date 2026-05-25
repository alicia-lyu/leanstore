#pragma once

// acol_group_walk — hand-rolled single-pass walk over the Q10 aCOL MI
// (MergedAdapter<customer_coli_t, orders_acol_t>).
//
// THIS IS THE POINT OF THE aCOL. aCOLI (Q3I S5) drove its query with the
// generic merged scanner (getScanner + std::variant + std::visit inline); the
// per-record variant construction + double dispatch is why S5 lost to S3
// ("S3 > S5 anomaly traced to S5 lacking the hand-tuned walker"). This walk
// mirrors col_group_walk_fused_emit instead: scanner->next_raw() yields a raw
// (idx_tag, key_slice, value_slice) triple and we dispatch via a switch on the
// trailing idx byte + memcpy into the record — no std::variant.
//
// The aCOL has only two co-located levels (customer → orders_acol) and no
// lineitems, so there is no skip taxonomy (SkipOrder/SkipGroup) — the walk is
// strictly customer-record then its order-agg records, finalising each custkey
// group at on_group_end. Hooks return void.
//
// Visitor hook contract:
//   void on_customer(Integer custkey, const customer_coli_t&)
//   void on_order(const orders_acol_t::Key&, const orders_acol_t&)
//   void on_group_end(Integer custkey)              [optional]
//   void on_record_visited()                        [optional]

#include <cstring>

#include "../tpch_family/views_col.hpp"  // customer_coli_t, orders_acol_t, ORDERS_ACOL_IDX_ID

namespace tpch::q10
{

namespace acol_detail
{
// Decode custkey from any aCOL key. Both customer_coli_t and orders_acol_t keys
// open with [t=1(customer)][custkey:4 big-endian XOR-flipped], so one decoder
// serves both — same layout as col_group_walk_fused_emit's helper.
inline Integer decode_custkey(const u8* key_bytes)
{
   const u8* p = key_bytes + 1;  // skip the leading domain-tag byte
   uint32_t raw = (static_cast<uint32_t>(p[0]) << 24)
                | (static_cast<uint32_t>(p[1]) << 16)
                | (static_cast<uint32_t>(p[2]) <<  8)
                | (static_cast<uint32_t>(p[3]));
   return static_cast<Integer>(raw ^ 0x80000000u);
}
}  // namespace acol_detail

template <typename Backend, typename Visitor>
void acol_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_acol_t>& mi,
    Visitor& visitor)
{
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   constexpr u8 TAG_CUSTOMER = static_cast<u8>(coli_idx_id::customer);  // 0
   constexpr u8 TAG_ORDERS   = static_cast<u8>(ORDERS_ACOL_IDX_ID);     // 37

   Integer cur_custkey = -1;

   while (auto raw = scanner->next_raw()) {
      auto [tag, k_slice, v_slice] = *raw;
      const u8* key_bytes = reinterpret_cast<const u8*>(k_slice.data());
      const u8* val_bytes = reinterpret_cast<const u8*>(v_slice.data());

      const Integer row_custkey = acol_detail::decode_custkey(key_bytes);
      if (cur_custkey != -1 && row_custkey != cur_custkey) {
         if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
            visitor.on_group_end(cur_custkey);
         }
      }
      cur_custkey = row_custkey;

      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }

      switch (tag) {
         case TAG_CUSTOMER: {
            customer_coli_t rec;
            std::memcpy(&rec, val_bytes, sizeof(customer_coli_t));
            visitor.on_customer(row_custkey, rec);
            break;
         }
         case TAG_ORDERS: {
            orders_acol_t::Key ok;
            orders_acol_t::unfoldKey(key_bytes, ok);
            orders_acol_t rec;
            std::memcpy(&rec, val_bytes, sizeof(orders_acol_t));
            visitor.on_order(ok, rec);
            break;
         }
         default:
            break;  // unknown tag — skip
      }
   }

   // Flush the final group.
   if (cur_custkey != -1) {
      if constexpr (requires { visitor.on_group_end(cur_custkey); }) {
         visitor.on_group_end(cur_custkey);
      }
   }
}

}  // namespace tpch::q10
