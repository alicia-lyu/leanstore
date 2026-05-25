#pragma once

// acoli_group_walk — hand-rolled single-pass walk over the Q10I aCOLI MI
// (MergedAdapter<customer_coli_t, orders_acoli_q10i_t>). The COLI analogue of
// q10::acol_group_walk (q10_family/acol_walk.tpp).
//
// THIS IS THE POINT OF THE aCOLI. Q3I's aCOLI drove its query with the generic
// merged scanner (getScanner + std::variant + std::visit); the per-record
// variant construction + double dispatch is why its S5 lost to S3. This walk
// mirrors col_group_walk_fused_emit / acol_group_walk instead:
// scanner->next_raw() yields a raw (idx_tag, key_slice, value_slice) triple and
// we dispatch via a switch on the trailing idx byte + memcpy — no std::variant.
//
// Two co-located levels only (customer → orders_acoli_q10i) and no lineitems or
// invoices, so there is no skip taxonomy — strictly each customer record then
// its order-agg records, finalising each custkey group at on_group_end.
//
// Visitor hook contract:
//   void on_customer(Integer custkey, const customer_coli_t&)
//   void on_order(const orders_acoli_q10i_t::Key&, const orders_acoli_q10i_t&)
//   void on_group_end(Integer custkey)              [optional]
//   void on_record_visited()                        [optional]

#include <cstring>

#include "../tpch_family/views_coli.hpp"  // customer_coli_t, orders_acoli_q10i_t, coli_idx_id

namespace tpch::q10i
{

namespace acoli_detail
{
// Decode custkey from any aCOLI key. Both customer_coli_t and
// orders_acoli_q10i_t keys open with [t=1(customer)][custkey:4 big-endian
// XOR-flipped], so one decoder serves both (same as acol_group_walk's helper).
inline Integer decode_custkey(const u8* key_bytes)
{
   const u8* p = key_bytes + 1;  // skip the leading domain-tag byte
   uint32_t raw = (static_cast<uint32_t>(p[0]) << 24)
                | (static_cast<uint32_t>(p[1]) << 16)
                | (static_cast<uint32_t>(p[2]) <<  8)
                | (static_cast<uint32_t>(p[3]));
   return static_cast<Integer>(raw ^ 0x80000000u);
}
}  // namespace acoli_detail

template <typename Backend, typename Visitor>
void acoli_group_walk(
    typename Backend::template MergedAdapter<customer_coli_t, orders_acoli_q10i_t>& mi,
    Visitor& visitor)
{
   auto scanner = mi.template getScanner<customer_coli_t::Key, customer_coli_t>();

   constexpr u8 TAG_CUSTOMER = static_cast<u8>(coli_idx_id::customer);           // 0
   constexpr u8 TAG_ORDERS   = static_cast<u8>(coli_idx_id::orders_acoli_q10i);  // 54

   Integer cur_custkey = -1;

   while (auto raw = scanner->next_raw()) {
      auto [tag, k_slice, v_slice] = *raw;
      const u8* key_bytes = reinterpret_cast<const u8*>(k_slice.data());
      const u8* val_bytes = reinterpret_cast<const u8*>(v_slice.data());

      const Integer row_custkey = acoli_detail::decode_custkey(key_bytes);
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
            orders_acoli_q10i_t::Key ok;
            orders_acoli_q10i_t::unfoldKey(key_bytes, ok);
            orders_acoli_q10i_t rec;
            std::memcpy(&rec, val_bytes, sizeof(orders_acoli_q10i_t));
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

}  // namespace tpch::q10i
