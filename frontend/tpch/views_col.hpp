#pragma once

// CUSTOMER × ORDERS × LINEITEM (COL) merged-index record types.
//
// COL is a 3-table subset of the 4-table COLI family.  The relationship is at
// the *record-type* level: `customer_coli_t` and `orders_coli_t` are reused
// verbatim from views_coli.hpp (byte-identical keys and payloads).  Only
// `lineitem_col_t` is new, because COLI's `lineitem_coli_t::Key` carries
// `invoicekey` while COL has no invoice domain to interleave with.
//
// Type-identity rule (frontend/tpch/CLAUDE.md §Project pushdown):
//   Declare a new record type only when the Key shape or payload genuinely
//   diverges from an existing sibling.
//   - `customer_col_t` would be byte-identical to `customer_coli_t`. DO NOT
//     declare. Reuse `customer_coli_t`.
//   - `orders_col_t` would be byte-identical to `orders_coli_t`.  DO NOT
//     declare. Reuse `orders_coli_t`.
//   - `lineitem_col_t` genuinely differs: no `invoicekey` key segment.
//     DECLARE here.
//
// Sentinel ordering (§Open Question resolved 2026-05-03):
//   Strict-subset ordering — COL reuses coli_domain_tag verbatim:
//     index=0, customer=1, invoice=2 (reserved, unused), orders=3, lineitem=4.
//   The reserved invoice=2 slot keeps prefix bytes aligned with COLI, enabling
//   byte-compatible keys between the two families and future dual-format
//   scanner reuse if needed.
//
// MergedAdapter type for COL:
//   MergedAdapter<customer_coli_t, orders_coli_t, lineitem_col_t>
//
// Key byte layouts (integers fold to 4 bytes big-endian XOR-flipped; u8 = 1 byte):
//   customer   : [t=1][custkey:4]                          [t=0][idx=0]  =  7 B
//   orders     : [t=1][custkey:4][t=3][orderkey:4]         [t=0][idx=1]  = 12 B
//   lineitem   : [t=1][custkey:4][t=3][orderkey:4][t=4][linenumber:4]
//                [t=0][idx=36]  = 17 B
//
// Byte-lex sort order within a custkey group:
//   customer → (orders → lineitems*)+
//
// No aCOL / S5: Q3 has no parameter-independent aggregate to bake.  An aCOL
// MI would carry only the same 3 base record types as the COL MI (S3) with no
// additional information, so S5 collapses into S3 for Q3 and is omitted.

#include <cassert>
#include <cstdint>
#include <variant>

#include "../shared/Types.hpp"
#include "../shared/view_templates.hpp"
#include "tpch_tables.hpp"
#include "views_coli.hpp"  // reuse coli_domain_tag, coli_idx_id, tagged_path helpers,
                           // customer_coli_t, orders_coli_t

namespace tpch
{

// ---------------------------------------------------------------------------
// lineitem_col_t — stores lineitem_t payload under tagged key (no invoicekey).
//
// Key layout:
//   [t=1(cust)][custkey:4][t=3(ord)][orderkey:4][t=4(li)][linenumber:4]
//   [t=0(idx)][idx=lineitem_col(36)]  =  17 bytes
//
// Four bytes shorter than lineitem_coli_t (21 bytes) because there is no
// invoicekey segment.  idx=36 is distinct from all existing COLI idx values
// (0-3, 49, 53), guaranteeing clean merged-scanner accepts_key dispatch.
//
// Project-pushdown classification: co-located secondary in the COL MI (S3)
// and split adapter (S1). Carries only the columns required by Q3 consumers
// (the LineitemRevenueAccumulator read set). Widen in place when Q5/Q9 join
// this family.

// Register idx=36 in the coli_idx_id enum range.  Rather than extending the
// enum (which lives in views_coli.hpp), we define the value as a constexpr
// so it can be used in the tagged_path template argument without altering the
// upstream enum.
inline constexpr coli_idx_id LINEITEM_COL_IDX_ID = static_cast<coli_idx_id>(36);

struct lineitem_col_t {
   static constexpr int id = 36;

   struct Key {
      static constexpr int id = 36;

      Integer custkey;
      Integer orderkey;
      Integer linenumber;

      using path = tagged_path<
          LINEITEM_COL_IDX_ID,
          tag_field_step<coli_domain_tag::customer, &Key::custkey>,
          tag_field_step<coli_domain_tag::orders,   &Key::orderkey>,
          tag_field_step<coli_domain_tag::lineitem,  &Key::linenumber>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      // accepts_key: cheap type-dispatch hook for the merged-scanner variant.
      // Reads only total key length and the trailing idx_id byte.
      // Sufficient for well-formed COL keys produced by populate_merged.
      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(LINEITEM_COL_IDX_ID);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "lineitem_col_key(custkey=" << k.custkey
                   << ",orderkey=" << k.orderkey
                   << ",linenumber=" << k.linenumber << ")";
      }
   };

   // Payload: Q3-projected fields only (project-pushdown rule).
   // Matches the LineitemRevenueAccumulator read set.
   // l_orderkey and l_linenumber are in the Key; l_custkey is derived via FK.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Timestamp l_shipdate;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "lineitem_col(extprice=" << l_extendedprice
         << ",disc=" << l_discount << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const lineitem_col_t& r)
   {
      r.print(os);
      return os;
   }

   static lineitem_col_t from_base(const lineitem_t& l)
   {
      return {l.l_extendedprice, l.l_discount, l.l_shipdate};
   }

   static Key key_from_base(Integer custkey, const lineitem_t::Key& lk)
   {
      return Key{custkey, lk.l_orderkey, lk.l_linenumber};
   }
};

}  // namespace tpch

// ---------------------------------------------------------------------------
// SKMatcher specializations for COL-specific record pairs.
//
// Only pairs involving the new lineitem_col_t need to be declared here.
// The customer_coli_t self-pair, orders_coli_t self-pair, and
// customer_coli_t × orders_coli_t cross-pair already exist in views_coli.hpp.

// --- lineitem_col_t self-pair ---

template <>
struct SKMatcher<tpch::lineitem_col_t, tpch::lineitem_col_t> {
   static int match(const tpch::lineitem_col_t::Key& a, const tpch::lineitem_col_t&,
                    const tpch::lineitem_col_t::Key& b, const tpch::lineitem_col_t&)
   {
      if (a.custkey   != b.custkey)   return a.custkey   < b.custkey   ? -1 : 1;
      if (a.orderkey  != b.orderkey)  return a.orderkey  < b.orderkey  ? -1 : 1;
      if (a.linenumber != b.linenumber) return a.linenumber < b.linenumber ? -1 : 1;
      return 0;
   }
};

// --- customer_coli_t × lineitem_col_t — match on custkey only ---

template <>
struct SKMatcher<tpch::customer_coli_t, tpch::lineitem_col_t> {
   static int match(const tpch::customer_coli_t::Key& a, const tpch::customer_coli_t&,
                    const tpch::lineitem_col_t::Key&  b, const tpch::lineitem_col_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::lineitem_col_t, tpch::customer_coli_t> {
   static int match(const tpch::lineitem_col_t::Key&  a, const tpch::lineitem_col_t&,
                    const tpch::customer_coli_t::Key& b, const tpch::customer_coli_t&)
   {
      return -SKMatcher<tpch::customer_coli_t, tpch::lineitem_col_t>::match(b, {}, a, {});
   }
};

// --- orders_coli_t × lineitem_col_t — match on (custkey, orderkey) ---

template <>
struct SKMatcher<tpch::orders_coli_t, tpch::lineitem_col_t> {
   static int match(const tpch::orders_coli_t::Key&  a, const tpch::orders_coli_t&,
                    const tpch::lineitem_col_t::Key& b, const tpch::lineitem_col_t&)
   {
      if (a.custkey  != b.custkey)  return a.custkey  < b.custkey  ? -1 : 1;
      if (a.orderkey != b.orderkey) return a.orderkey < b.orderkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::lineitem_col_t, tpch::orders_coli_t> {
   static int match(const tpch::lineitem_col_t::Key& a, const tpch::lineitem_col_t&,
                    const tpch::orders_coli_t::Key&  b, const tpch::orders_coli_t&)
   {
      return -SKMatcher<tpch::orders_coli_t, tpch::lineitem_col_t>::match(b, {}, a, {});
   }
};
