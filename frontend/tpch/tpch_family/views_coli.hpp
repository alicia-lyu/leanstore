#pragma once

// CUSTOMER × ORDERS × LINEITEM × INVOICE merged-index record types.
//
// Encoding: Calcite-style interleaved domain tags + sentinel + trailing idx_id.
//
// Key byte layouts (all integers fold to 4 bytes big-endian XOR-flipped; u8 = 1 byte):
//
//   customer : [t=1][custkey:4]                                            [t=0][idx=0]  =  7 B
//   invoice  : [t=1][custkey:4][t=2][invoicekey:4]                         [t=0][idx=3]  = 12 B
//   orders   : [t=1][custkey:4][t=3][orderkey:4]                           [t=0][idx=1]  = 12 B
//   lineitem : [t=1][custkey:4][t=3][orderkey:4][t=4][invoicekey:4][lnum:4][t=0][idx=2]  = 21 B
//
// Byte-lex sort order within a custkey group: customer → invoice → orders → lineitems.
//   customer: after [1][custkey] the next byte is 0 (sentinel), smaller than any domain tag.
//   invoice (t=2) sorts before orders (t=3) and lineitem (t=3,t=4) — t=2 < t=3.
//   orders and lineitems share the [t=3][orderkey] prefix; orders' sentinel (t=0) sorts
//   before lineitems' tag (t=4) — lineitems nest after their parent order.
//
// §3.1.2 sibling sub-aggregate pattern: summary/sub-aggregate records with smaller
// cardinality (invoices) come first so consumers can finalize per-custkey state
// (e.g. cust_open_due) before the bulk O×L records stream by.
// §3.1.3 hierarchy layout falls out of byte-lex order automatically.

#include <cassert>
#include <cstdint>
#include <variant>

#include "../../shared/Types.hpp"
#include "../../shared/view_templates.hpp"
#include "../tpchi_tables.hpp"

namespace tpch
{

// ---------------------------------------------------------------------------
// Domain tag enum: identifies a key segment's owner, or the sentinel.

enum class coli_domain_tag : u8 {
   // Sentinel = 0 so it sorts STRICTLY BEFORE any field tag at the same
   // hierarchical level. This is what makes parent rows naturally sort
   // before child rows (e.g. customer before orders within a custkey,
   // orders before its lineitems within an orderkey): the parent row's
   // key terminates with [index=0][idx_id], while the child row continues
   // with [child_field_tag>0][child_field]... — and 0 < any positive tag.
   index    = 0,  // sentinel: end of (tag, field) pairs
   customer = 1,
   invoice  = 2,  // precedes orders/lineitems: summary records finalize first
   orders   = 3,  // was 2
   lineitem = 4,  // was 3
};

// Index identifier: trailing byte that names the leaf record type.
// Values 0–3 are the four COLI types; 49 and 53 are the two aCOLI-specific
// types (customer_acoli_t and lineitem_acoli_t).  orders_acoli_t was retired
// and collapsed into orders_coli_t (trailing byte = 1, same key bytes).
enum class coli_idx_id : u8 {
   customer        = 0,
   orders          = 1,
   lineitem        = 2,
   invoice         = 3,
   customer_acoli  = 49,  // customer_acoli_t — carries pre_open_due
   lineitem_acoli  = 53,  // lineitem_acoli_t — no invoicekey segment
};

// ---------------------------------------------------------------------------
// DRY helper: tagged_path<IdxId, Steps...>
//
// Each Step encodes one (domain-tag, field-or-fields) chunk.
// tagged_path appends the sentinel byte (coli_domain_tag::index) then the
// idx_id byte and provides foldKey / unfoldKey<Self> / maxFoldLength.
//
// tag_field_step<Tag, MemPtr>     — one (tag, field) pair
// tag_fields_step<Tag, MPs...>    — one tag followed by multiple fields (same domain)

// Extracts the member type from a pointer-to-member (e.g. &Key::custkey → Integer).
template <typename T>
struct member_type;
template <typename Class, typename Member>
struct member_type<Member Class::*> { using type = Member; };
template <auto MemPtr>
using member_type_t = typename member_type<decltype(MemPtr)>::type;

template <coli_domain_tag Tag, auto MemPtr>
struct tag_field_step {
   using field_t = member_type_t<MemPtr>;

   static constexpr unsigned stepLength()
   {
      // 1 byte for the tag + folded size of the field (sizeof for Integer/u8/Timestamp).
      return sizeof(u8) + sizeof(field_t);
   }

   template <typename Self>
   static unsigned foldStep(uint8_t* out, const Self& key)
   {
      unsigned pos = 0;
      u8 tag = static_cast<u8>(Tag);
      pos += fold(out + pos, tag);
      pos += fold(out + pos, key.*MemPtr);
      return pos;
   }

   template <typename Self>
   static unsigned unfoldStep(const uint8_t* in, Self& key)
   {
      unsigned pos = 0;
      u8 tag;
      pos += unfold(in + pos, tag);
      assert(tag == static_cast<u8>(Tag));
      pos += unfold(in + pos, key.*MemPtr);
      return pos;
   }
};

// Two-field variant: one tag byte followed by two field folds (e.g. lineitem's
// invoicekey + linenumber share the lineitem domain tag).
template <coli_domain_tag Tag, auto MemPtr1, auto MemPtr2>
struct tag_fields2_step {
   using field1_t = member_type_t<MemPtr1>;
   using field2_t = member_type_t<MemPtr2>;

   static constexpr unsigned stepLength()
   {
      return sizeof(u8) + sizeof(field1_t) + sizeof(field2_t);
   }

   template <typename Self>
   static unsigned foldStep(uint8_t* out, const Self& key)
   {
      unsigned pos = 0;
      u8 tag = static_cast<u8>(Tag);
      pos += fold(out + pos, tag);
      pos += fold(out + pos, key.*MemPtr1);
      pos += fold(out + pos, key.*MemPtr2);
      return pos;
   }

   template <typename Self>
   static unsigned unfoldStep(const uint8_t* in, Self& key)
   {
      unsigned pos = 0;
      u8 tag;
      pos += unfold(in + pos, tag);
      assert(tag == static_cast<u8>(Tag));
      pos += unfold(in + pos, key.*MemPtr1);
      pos += unfold(in + pos, key.*MemPtr2);
      return pos;
   }
};

template <coli_idx_id IdxId, typename... Steps>
struct tagged_path {
   static constexpr unsigned maxFoldLength()
   {
      return (Steps::stepLength() + ...)
           + sizeof(u8)   // sentinel byte (coli_domain_tag::index)
           + sizeof(u8);  // idx_id byte
   }

   template <typename Self>
   static unsigned foldKey(uint8_t* out, const Self& key)
   {
      unsigned pos = 0;
      ((pos += Steps::foldStep(out + pos, key)), ...);
      u8 sentinel = static_cast<u8>(coli_domain_tag::index);
      pos += fold(out + pos, sentinel);
      u8 idx = static_cast<u8>(IdxId);
      pos += fold(out + pos, idx);
      return pos;
   }

   template <typename Self>
   static unsigned unfoldKey(const uint8_t* in, Self& key)
   {
      unsigned pos = 0;
      ((pos += Steps::unfoldStep(in + pos, key)), ...);
      u8 sentinel;
      pos += unfold(in + pos, sentinel);
      assert(sentinel == static_cast<u8>(coli_domain_tag::index));
      u8 idx;
      pos += unfold(in + pos, idx);
      assert(idx == static_cast<u8>(IdxId));
      return pos;
   }
};

// ---------------------------------------------------------------------------
// Four record types

// customer_coli_t — stores customerh_t payload under tagged key.
//
// Key layout: [t=1(cust)][custkey:4][t=0(idx)][idx=customer]  = 7 bytes
struct customer_coli_t {
   static constexpr int id = 30;

   struct Key {
      static constexpr int id = 30;

      Integer custkey;

      using path = tagged_path<coli_idx_id::customer,
                               tag_field_step<coli_domain_tag::customer, &Key::custkey>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      // accepts_key: cheap type-dispatch hook for the merged-scanner
      // std::variant. Checks ONLY (a) total folded key length and (b) the
      // trailing idx_id byte. This works because keys and values are
      // stored separately, idx_id is constant-width (1 byte), and idx_id
      // values are unique per COLI type (see coli_idx_id enum).
      //
      // Limitation: a malformed key whose final byte coincidentally equals
      // our idx_id will be falsely accepted, and unfoldKey would then read
      // garbage from intermediate tag positions. The "ideal" implementation
      // would walk bytes step-by-step (domain tag, field, next tag, …)
      // sharing logic with unfoldKey, but that requires restructuring
      // unfoldKey to be try-or-fail. Deferred until we encounter actual
      // collisions; the present check is sufficient for well-formed COLI
      // keys produced by populate_merged.
      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::customer);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "customer_coli_key(custkey=" << k.custkey << ")";
      }
   };

   // Payload: all customerh_t fields.
   Varchar<25> c_name;
   Varchar<40> c_address;
   Integer     c_nationkey;
   Varchar<15> c_phone;
   Numeric     c_acctbal;
   Varchar<10> c_mktsegment;
   Varchar<117> c_comment;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "customer_coli(" << c_name << "," << c_nationkey << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const customer_coli_t& r)
   {
      r.print(os);
      return os;
   }

   // Conversion helpers
   static customer_coli_t from_base(const customerh_t& c)
   {
      return {c.c_name, c.c_address, c.c_nationkey, c.c_phone,
              c.c_acctbal, c.c_mktsegment, c.c_comment};
   }
   static Key key_from_base(const customerh_t::Key& k)
   {
      return Key{k.c_custkey};
   }
};

static_assert(sizeof(customer_coli_t) == sizeof(customerh_t),
              "customer_coli_t must be the same size as customerh_t");

// ---------------------------------------------------------------------------
// orders_coli_t — stores orders_t payload under tagged key.
//
// Key layout: [t=1(cust)][custkey:4][t=3(ord)][orderkey:4][t=0(idx)][idx=orders]  = 12 bytes
struct orders_coli_t {
   static constexpr int id = 31;

   struct Key {
      static constexpr int id = 31;

      Integer custkey;
      Integer orderkey;

      using path = tagged_path<coli_idx_id::orders,
                               tag_field_step<coli_domain_tag::customer, &Key::custkey>,
                               tag_field_step<coli_domain_tag::orders,   &Key::orderkey>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::orders);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "orders_coli_key(custkey=" << k.custkey
                   << ",orderkey=" << k.orderkey << ")";
      }
   };

   // Payload: Q3I-projected fields only (project-pushdown rule —
   // see frontend/tpch/CLAUDE.md §Project pushdown). orders_coli_t is a
   // co-located indexed view, not a primary index; widen in place when a
   // future query (Q5I/Q10I) reads more columns.
   Timestamp  o_orderdate;
   Integer    o_shippriority;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "orders_coli(orderdate=" << o_orderdate
         << ",shippri=" << o_shippriority << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const orders_coli_t& r)
   {
      r.print(os);
      return os;
   }

   static orders_coli_t from_base(const orders_t& o)
   {
      return {o.o_orderdate, o.o_shippriority};
   }
   static Key key_from_base(Integer custkey, const orders_t::Key& k)
   {
      return Key{custkey, k.o_orderkey};
   }
};

// orders_coli_t is now Q3I-projected; size will diverge from orders_t.

// ---------------------------------------------------------------------------
// lineitem_coli_t — stores lineitem_t payload under tagged key.
//
// Key layout:
//   [t=1(cust)][custkey:4][t=3(ord)][orderkey:4][t=4(li)][invoicekey:4][linenumber:4]
//   [t=0(idx)][idx=lineitem]  =  21 bytes
struct lineitem_coli_t {
   static constexpr int id = 32;

   struct Key {
      static constexpr int id = 32;

      Integer custkey;
      Integer orderkey;
      Integer invoicekey;
      Integer linenumber;

      using path = tagged_path<
          coli_idx_id::lineitem,
          tag_field_step<coli_domain_tag::customer, &Key::custkey>,
          tag_field_step<coli_domain_tag::orders,   &Key::orderkey>,
          tag_fields2_step<coli_domain_tag::lineitem, &Key::invoicekey, &Key::linenumber>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::lineitem);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "lineitem_coli_key(custkey=" << k.custkey
                   << ",orderkey=" << k.orderkey
                   << ",invoicekey=" << k.invoicekey
                   << ",linenumber=" << k.linenumber << ")";
      }
   };

   // Payload: Q3I-projected fields only (project-pushdown rule —
   // see frontend/tpch/CLAUDE.md §Project pushdown). lineitem_coli_t is a
   // co-located indexed view; widen in place when a future query reads
   // more columns. l_orderkey/l_invoicekey/l_linenumber are in the Key.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Timestamp l_shipdate;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "lineitem_coli(extprice=" << l_extendedprice
         << ",disc=" << l_discount << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const lineitem_coli_t& r)
   {
      r.print(os);
      return os;
   }

   static lineitem_coli_t from_base(const lineitem_i_t& l)
   {
      return {l.l_extendedprice, l.l_discount, l.l_shipdate};
   }
   static Key key_from_base(Integer custkey, const lineitem_i_t::Key& k, const lineitem_i_t& v)
   {
      return Key{custkey, k.l_orderkey, v.l_invoicekey, k.l_linenumber};
   }
};

// lineitem_coli_t is now Q3I-projected; size will diverge from lineitem_t.

// ---------------------------------------------------------------------------
// invoice_coli_t — stores invoice_t payload under tagged key.
//
// Key layout: [t=1(cust)][custkey:4][t=2(inv)][invoicekey:4][t=0(idx)][idx=invoice]  = 12 bytes
struct invoice_coli_t {
   static constexpr int id = 33;

   struct Key {
      static constexpr int id = 33;

      Integer custkey;
      Integer invoicekey;

      using path = tagged_path<coli_idx_id::invoice,
                               tag_field_step<coli_domain_tag::customer, &Key::custkey>,
                               tag_field_step<coli_domain_tag::invoice,  &Key::invoicekey>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::invoice);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "invoice_coli_key(custkey=" << k.custkey
                   << ",invoicekey=" << k.invoicekey << ")";
      }
   };

   // Payload: Q3I-projected fields only (project-pushdown rule —
   // see frontend/tpch/CLAUDE.md §Project pushdown). invoice_coli_t is a
   // co-located indexed view; widen in place when a future query reads
   // more columns. i_custkey is in the Key.
   Numeric    i_totaldue;
   Varchar<1> i_status;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "invoice_coli(totaldue=" << i_totaldue
         << ",status=" << i_status << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const invoice_coli_t& r)
   {
      r.print(os);
      return os;
   }

   static invoice_coli_t from_base(const invoice_t& i)
   {
      return {i.i_totaldue, i.i_status};
   }
   static Key key_from_base(Integer custkey, const invoice_t::Key& k)
   {
      return Key{custkey, k.i_invoicekey};
   }
};

// invoice_coli_t is now Q3I-projected; size will diverge from invoice_t.

// ---------------------------------------------------------------------------
// aCOLI (aggregated COLI) record types.
//
// These form a 3-type MergedAdapter<customer_acoli_t, orders_coli_t,
// lineitem_acoli_t> that pre-bakes the per-customer invoice sub-aggregate
// into customer_acoli_t and stores unaggregated lineitems in lineitem_acoli_t.
// Invoice rows are collapsed to the pre_open_due scalar (parameter-independent:
// i_status='O' is hardcoded by the TPC-H Q3I spec).  Lineitem revenue is
// computed at query time so S5 remains reusable across all DATE param sets.
//
// Schema (2026-05-03 redesign; tagged keys adopted 2026-05-03 Step 4b):
//   customer_acoli_t.pre_open_due  = SUM(i_totaldue WHERE i_status='O')
//   orders_coli_t   — retired orders_acoli_t (id=50); byte-identical key and
//                     payload after G8 projection, so collapsed to one type.
//   lineitem_acoli_t — full lineitem_t payload for per-query revenue computation.
//
// Tagged-key encoding (uniform with COLI — Step 4b):
//   customer_acoli_t::Key → [t=1][custkey:4][t=0][idx=49]   = 7 bytes
//   orders_coli_t::Key    → [t=1][custkey:4][t=3][orderkey:4][t=0][idx=1] = 12 bytes
//   lineitem_acoli_t::Key → [t=1][custkey:4][t=3][orderkey:4][t=4][lineno:4][t=0][idx=53] = 17 bytes
//
// All three trailing idx bytes (49, 1, 53) are distinct → merged-scanner
// variant dispatch via accepts_key works cleanly.
//
// Byte-lex sort order within a custkey group:
//   customer_acoli_t (7 B) → orders_coli_t (12 B) → lineitem_acoli_t (17 B)
// Sentinel byte (0) sorts before domain tags (1, 3, 4) so parent rows
// naturally precede child rows at each level.

// customer_acoli_t: customerh_t payload + pre_open_due aggregate.
//
// Key layout: [t=1(cust)][custkey:4][t=0(idx)][idx=customer_acoli(49)]  = 7 bytes
// Same domain-tag structure as customer_coli_t; distinguished at scan time
// by the trailing idx byte (49 vs 30) — the two types live in different
// MergedAdapters and never share a scanner.
struct customer_acoli_t {
   static constexpr int id = 49;

   struct Key {
      static constexpr int id = 49;

      Integer custkey;

      using path = tagged_path<coli_idx_id::customer_acoli,
                               tag_field_step<coli_domain_tag::customer, &Key::custkey>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::customer_acoli);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "customer_acoli_key(custkey=" << k.custkey << ")";
      }
   };

   // Full customerh_t payload (mirrored for cross-query reuse).
   Varchar<25>  c_name;
   Varchar<40>  c_address;
   Integer      c_nationkey;
   Varchar<15>  c_phone;
   Numeric      c_acctbal;
   Varchar<10>  c_mktsegment;
   Varchar<117> c_comment;

   // Pre-aggregated field baked in at populate_aggregated() time.
   Numeric pre_open_due;  // SUM(i_totaldue WHERE i_status='O') for this custkey

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "customer_acoli(" << c_name << ",open_due=" << pre_open_due << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const customer_acoli_t& r)
   {
      r.print(os);
      return os;
   }

   static customer_acoli_t from_customer(const customerh_t& c, Numeric open_due)
   {
      return {c.c_name, c.c_address, c.c_nationkey, c.c_phone,
              c.c_acctbal, c.c_mktsegment, c.c_comment, open_due};
   }
   static Key key_from_base(const customerh_t::Key& k) { return Key{k.c_custkey}; }
};

// orders_acoli_t — RETIRED (Step 4b, 2026-05-03).
//
// After switching aCOLI types to tagged-key encoding, orders_acoli_t::Key is
// byte-identical to orders_coli_t::Key (same domain tags, same idx byte = 1,
// same payload {o_orderdate, o_shippriority}).  The type is therefore
// collapsed: the aCOLI MergedAdapter stores orders_coli_t rows directly.
// id=50 is retired; do not reuse it.
//
// Consumers that previously referenced orders_acoli_t now use orders_coli_t.
// The existing orders_coli_t::key_from_base / from_base factories work
// unchanged because the payloads were already identical post-G8.
using orders_acoli_t = orders_coli_t;

// lineitem_acoli_t: projected lineitem mirror keyed by
//   (custkey, orderkey, linenumber).
//   id=53 (ids 51/52 were the now-retired projected aCOLI pair).
//
// Key layout:
//   [t=1(cust)][custkey:4][t=3(ord)][orderkey:4][t=4(li)][linenumber:4]
//   [t=0(idx)][idx=lineitem_acoli(53)]  =  17 bytes
//
// No invoicekey segment (S5 doesn't co-locate invoices — they are collapsed
// into customer_acoli_t.pre_open_due at load time).  This is what keeps the
// key shape distinct from lineitem_coli_t (21 bytes, with invoicekey).
//
// G8c (2026-05-03): payload projected to {l_extendedprice, l_discount,
// l_shipdate} — the three fields the revenue accumulator reads. Per
// CLAUDE.md §Project pushdown: aCOLI is a pre-aggregated secondary
// (customer is the S5 primary), so non-customer types carry only
// query-required columns. Widen in place when a future query needs more.
struct lineitem_acoli_t {
   static constexpr int id = 53;

   struct Key {
      static constexpr int id = 53;

      Integer custkey;
      Integer orderkey;
      Integer linenumber;

      using path = tagged_path<
          coli_idx_id::lineitem_acoli,
          tag_field_step<coli_domain_tag::customer, &Key::custkey>,
          tag_field_step<coli_domain_tag::orders,   &Key::orderkey>,
          tag_field_step<coli_domain_tag::lineitem,  &Key::linenumber>>;

      static constexpr unsigned maxFoldLength() { return path::maxFoldLength(); }

      static unsigned keyfold(uint8_t* out, const Key& k) { return path::foldKey(out, k); }
      static unsigned keyunfold(const uint8_t* in, Key& k) { return path::unfoldKey(in, k); }

      static bool accepts_key(const u8* key_bytes, size_t key_len)
      {
         return key_len == maxFoldLength()
             && key_bytes[key_len - 1] == static_cast<u8>(coli_idx_id::lineitem_acoli);
      }

      friend std::ostream& operator<<(std::ostream& os, const Key& k)
      {
         return os << "lineitem_acoli_key(custkey=" << k.custkey
                   << ",orderkey=" << k.orderkey
                   << ",linenumber=" << k.linenumber << ")";
      }
   };

   Numeric   l_extendedprice;
   Numeric   l_discount;
   Timestamp l_shipdate;

   static unsigned foldKey(uint8_t* out, const Key& k) { return Key::keyfold(out, k); }
   static unsigned unfoldKey(const uint8_t* in, Key& k) { return Key::keyunfold(in, k); }
   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   void print(std::ostream& os) const
   {
      os << "lineitem_acoli(ep=" << l_extendedprice << ",disc=" << l_discount
         << ",ship=" << l_shipdate << ")";
   }

   friend std::ostream& operator<<(std::ostream& os, const lineitem_acoli_t& r)
   {
      r.print(os);
      return os;
   }

   static lineitem_acoli_t from_base(const lineitem_t& l)
   {
      return {l.l_extendedprice, l.l_discount, l.l_shipdate};
   }
   static Key key_from_base(Integer custkey, const lineitem_t::Key& lk)
   {
      return Key{custkey, lk.l_orderkey, lk.l_linenumber};
   }
};

// Note: customer_acoli_q3i_t (id=51) and orders_acoli_q3i_t (id=52) —
// the Q3I-projected aCOLI pair — were retired 2026-05-03.  They embedded
// pre_revenue (parameterised by l_shipdate) and were never wired into
// production targets.  The ids 51/52 are reserved; do not reuse them.
// id=50 (orders_acoli_t) was retired Step 4b — collapsed into orders_coli_t.

// ---------------------------------------------------------------------------
// Byte-driven variant dispatcher.
//
// Reads the trailing idx_id byte (key_bytes[key_len - 1]), switches on it,
// delegates to that record type's unfoldKey, and returns the appropriate
// variant arm. Used by the merged-adapter scan path when record type is
// not known ahead of time.

using coli_key_variant   = std::variant<customer_coli_t::Key, orders_coli_t::Key,
                                        lineitem_coli_t::Key, invoice_coli_t::Key>;
using coli_value_variant = std::variant<customer_coli_t, orders_coli_t,
                                        lineitem_coli_t, invoice_coli_t>;

inline coli_key_variant coli_unfold_key_to_variant(const u8* key_bytes, size_t key_len)
{
   assert(key_len > 0);
   u8 idx = key_bytes[key_len - 1];
   switch (static_cast<coli_idx_id>(idx)) {
      case coli_idx_id::customer: {
         customer_coli_t::Key k;
         customer_coli_t::unfoldKey(key_bytes, k);
         return k;
      }
      case coli_idx_id::orders: {
         orders_coli_t::Key k;
         orders_coli_t::unfoldKey(key_bytes, k);
         return k;
      }
      case coli_idx_id::lineitem: {
         lineitem_coli_t::Key k;
         lineitem_coli_t::unfoldKey(key_bytes, k);
         return k;
      }
      case coli_idx_id::invoice: {
         invoice_coli_t::Key k;
         invoice_coli_t::unfoldKey(key_bytes, k);
         return k;
      }
      default:
         assert(false && "unknown coli_idx_id");
         __builtin_unreachable();
   }
}

inline coli_value_variant coli_unfold_value_to_variant(const u8* key_bytes, size_t key_len,
                                                        const u8* val_bytes)
{
   assert(key_len > 0);
   u8 idx = key_bytes[key_len - 1];
   switch (static_cast<coli_idx_id>(idx)) {
      case coli_idx_id::customer: {
         customer_coli_t v;
         std::memcpy(&v, val_bytes, sizeof(v));
         return v;
      }
      case coli_idx_id::orders: {
         orders_coli_t v;
         std::memcpy(&v, val_bytes, sizeof(v));
         return v;
      }
      case coli_idx_id::lineitem: {
         lineitem_coli_t v;
         std::memcpy(&v, val_bytes, sizeof(v));
         return v;
      }
      case coli_idx_id::invoice: {
         invoice_coli_t v;
         std::memcpy(&v, val_bytes, sizeof(v));
         return v;
      }
      default:
         assert(false && "unknown coli_idx_id");
         __builtin_unreachable();
   }
}

}  // namespace tpch

// ---------------------------------------------------------------------------
// SKMatcher specializations for COLI record pairs.
//
// Each specialization embodies ONE canonical FK-derived match predicate.
// Defined outside the tpch namespace so they sit beside the primary SKMatcher
// template in view_templates.hpp's namespace (global).
//
// Canonical ordering: lower coli_idx_id value is R1.
// Reversed directions are provided explicitly (symmetry helper TODO from step 2
// is not yet wired for explicit specializations, so we write both directions).

// --- self-pairs ---

template <>
struct SKMatcher<tpch::customer_coli_t, tpch::customer_coli_t> {
   static int match(const tpch::customer_coli_t::Key& a, const tpch::customer_coli_t&,
                    const tpch::customer_coli_t::Key& b, const tpch::customer_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::orders_coli_t, tpch::orders_coli_t> {
   static int match(const tpch::orders_coli_t::Key& a, const tpch::orders_coli_t&,
                    const tpch::orders_coli_t::Key& b, const tpch::orders_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      if (a.orderkey != b.orderkey) return a.orderkey < b.orderkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::lineitem_coli_t, tpch::lineitem_coli_t> {
   static int match(const tpch::lineitem_coli_t::Key& a, const tpch::lineitem_coli_t&,
                    const tpch::lineitem_coli_t::Key& b, const tpch::lineitem_coli_t&)
   {
      if (a.custkey   != b.custkey)   return a.custkey   < b.custkey   ? -1 : 1;
      if (a.orderkey  != b.orderkey)  return a.orderkey  < b.orderkey  ? -1 : 1;
      if (a.invoicekey != b.invoicekey) return a.invoicekey < b.invoicekey ? -1 : 1;
      if (a.linenumber != b.linenumber) return a.linenumber < b.linenumber ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::invoice_coli_t, tpch::invoice_coli_t> {
   static int match(const tpch::invoice_coli_t::Key& a, const tpch::invoice_coli_t&,
                    const tpch::invoice_coli_t::Key& b, const tpch::invoice_coli_t&)
   {
      if (a.custkey   != b.custkey)   return a.custkey   < b.custkey   ? -1 : 1;
      if (a.invoicekey != b.invoicekey) return a.invoicekey < b.invoicekey ? -1 : 1;
      return 0;
   }
};

// --- cross-pairs: customer × {orders, lineitem, invoice} — match on custkey only ---

template <>
struct SKMatcher<tpch::customer_coli_t, tpch::orders_coli_t> {
   static int match(const tpch::customer_coli_t::Key& a, const tpch::customer_coli_t&,
                    const tpch::orders_coli_t::Key&   b, const tpch::orders_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::orders_coli_t, tpch::customer_coli_t> {
   static int match(const tpch::orders_coli_t::Key&   a, const tpch::orders_coli_t&,
                    const tpch::customer_coli_t::Key& b, const tpch::customer_coli_t&)
   {
      return -SKMatcher<tpch::customer_coli_t, tpch::orders_coli_t>::match(b, {}, a, {});
   }
};

template <>
struct SKMatcher<tpch::customer_coli_t, tpch::lineitem_coli_t> {
   static int match(const tpch::customer_coli_t::Key&  a, const tpch::customer_coli_t&,
                    const tpch::lineitem_coli_t::Key&  b, const tpch::lineitem_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::lineitem_coli_t, tpch::customer_coli_t> {
   static int match(const tpch::lineitem_coli_t::Key&  a, const tpch::lineitem_coli_t&,
                    const tpch::customer_coli_t::Key&  b, const tpch::customer_coli_t&)
   {
      return -SKMatcher<tpch::customer_coli_t, tpch::lineitem_coli_t>::match(b, {}, a, {});
   }
};

template <>
struct SKMatcher<tpch::customer_coli_t, tpch::invoice_coli_t> {
   static int match(const tpch::customer_coli_t::Key& a, const tpch::customer_coli_t&,
                    const tpch::invoice_coli_t::Key&  b, const tpch::invoice_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::invoice_coli_t, tpch::customer_coli_t> {
   static int match(const tpch::invoice_coli_t::Key&  a, const tpch::invoice_coli_t&,
                    const tpch::customer_coli_t::Key& b, const tpch::customer_coli_t&)
   {
      return -SKMatcher<tpch::customer_coli_t, tpch::invoice_coli_t>::match(b, {}, a, {});
   }
};

// --- hierarchical: orders → lineitem — match on (custkey, orderkey) ---

template <>
struct SKMatcher<tpch::orders_coli_t, tpch::lineitem_coli_t> {
   static int match(const tpch::orders_coli_t::Key&  a, const tpch::orders_coli_t&,
                    const tpch::lineitem_coli_t::Key& b, const tpch::lineitem_coli_t&)
   {
      if (a.custkey  != b.custkey)  return a.custkey  < b.custkey  ? -1 : 1;
      if (a.orderkey != b.orderkey) return a.orderkey < b.orderkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::lineitem_coli_t, tpch::orders_coli_t> {
   static int match(const tpch::lineitem_coli_t::Key& a, const tpch::lineitem_coli_t&,
                    const tpch::orders_coli_t::Key&   b, const tpch::orders_coli_t&)
   {
      return -SKMatcher<tpch::orders_coli_t, tpch::lineitem_coli_t>::match(b, {}, a, {});
   }
};

// --- siblings: orders ↔ invoice — match on custkey only ---

template <>
struct SKMatcher<tpch::orders_coli_t, tpch::invoice_coli_t> {
   static int match(const tpch::orders_coli_t::Key&  a, const tpch::orders_coli_t&,
                    const tpch::invoice_coli_t::Key&  b, const tpch::invoice_coli_t&)
   {
      if (a.custkey != b.custkey) return a.custkey < b.custkey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::invoice_coli_t, tpch::orders_coli_t> {
   static int match(const tpch::invoice_coli_t::Key&  a, const tpch::invoice_coli_t&,
                    const tpch::orders_coli_t::Key&   b, const tpch::orders_coli_t&)
   {
      return -SKMatcher<tpch::orders_coli_t, tpch::invoice_coli_t>::match(b, {}, a, {});
   }
};

// --- lineitem ↔ invoice — match on (custkey, invoicekey) ---

template <>
struct SKMatcher<tpch::lineitem_coli_t, tpch::invoice_coli_t> {
   static int match(const tpch::lineitem_coli_t::Key& a, const tpch::lineitem_coli_t&,
                    const tpch::invoice_coli_t::Key&  b, const tpch::invoice_coli_t&)
   {
      if (a.custkey   != b.custkey)   return a.custkey   < b.custkey   ? -1 : 1;
      if (a.invoicekey != b.invoicekey) return a.invoicekey < b.invoicekey ? -1 : 1;
      return 0;
   }
};

template <>
struct SKMatcher<tpch::invoice_coli_t, tpch::lineitem_coli_t> {
   static int match(const tpch::invoice_coli_t::Key&  a, const tpch::invoice_coli_t&,
                    const tpch::lineitem_coli_t::Key& b, const tpch::lineitem_coli_t&)
   {
      return -SKMatcher<tpch::lineitem_coli_t, tpch::invoice_coli_t>::match(b, {}, a, {});
   }
};
