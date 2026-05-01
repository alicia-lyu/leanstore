#pragma once

// Shared types for the CUSTOMER x ORDERS x LINEITEM x INVOICE pipeline.
//
// This 4-table merged index demonstrates two §3.1 patterns simultaneously:
//   - §3.1.2 siblings: Orders and Invoice are independent children of Customer
//   - §3.1.3 hierarchy: Lineitem is nested under Orders within each custkey group
//
// Sort key shape (custkey, table_disc, orderkey, invoicekey, linenumber):
//   Customer  → (custkey,  0, 0,         0,          0)          leading per custkey
//   Orders    → (custkey,  1, orderkey,  0,          0)          interleaved with lineitems
//   Lineitem  → (custkey,  1, orderkey,  l_invoicekey, linenumber) invoicekey groups lineitems
//   Invoice   → (custkey,  2, 0,         invoicekey, 0)          trailing block per custkey

#include <limits>
#include <tuple>
#include <vector>

#include "../shared/view_templates.hpp"
#include "tpch_tables.hpp"

namespace tpch
{
// id range for tpch shared / per-query views: 30s+ (geo uses 10s/20s).

// ---------------------------------------------------------------------------
// Sort key over custkey: the CUSTOMER × ORDERS × LINEITEM × INVOICE join key.
//
// table_disc encodes the row kind:
//   0 = Customer, 1 = Orders or Lineitem (hierarchical pair), 2 = Invoice

struct coli_sort_key_t {
   Integer custkey;
   Integer table_disc;  // 0=Customer, 1=Orders/Lineitem, 2=Invoice
   Integer orderkey;    // 0 for Customer/Invoice rows
   Integer invoicekey;  // 0 for Customer/Orders rows; l_invoicekey for Lineitem
   Integer linenumber;  // 0 for Customer/Orders/Invoice rows

   using Key = coli_sort_key_t;
   ADD_KEY_TRAITS(&coli_sort_key_t::custkey, &coli_sort_key_t::table_disc,
                  &coli_sort_key_t::orderkey, &coli_sort_key_t::invoicekey,
                  &coli_sort_key_t::linenumber)

   auto operator<=>(const coli_sort_key_t&) const = default;

   static coli_sort_key_t max()
   {
      return {std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
              std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
              std::numeric_limits<Integer>::max()};
   }

   friend int operator%(const coli_sort_key_t& k, const int& n)
   {
      return static_cast<int>(k.custkey) % n;
   }

   // Returns all sort keys that should match during a hash probe.
   // A Customer row matches only itself; Orders matches itself; Lineitem
   // also needs its enclosing order-level key; Invoice matches only itself.
   std::vector<coli_sort_key_t> matching_keys() const
   {
      if (table_disc == 1 && linenumber > 0) {
         // Lineitem: also emit the parent Orders key
         return {{custkey, Integer(1), orderkey, Integer(0), Integer(0)}, *this};
      }
      return {*this};
   }

   // Hierarchical prefix comparison.
   // Within table_disc==1, orderkey==X with linenumber==0 acts as a wildcard
   // matching any lineitem under that orderkey.
   int match(const coli_sort_key_t& other) const
   {
      if (custkey != other.custkey)
         return static_cast<int>(custkey) - static_cast<int>(other.custkey);
      if (table_disc != other.table_disc)
         return static_cast<int>(table_disc) - static_cast<int>(other.table_disc);
      if (orderkey != other.orderkey)
         return static_cast<int>(orderkey) - static_cast<int>(other.orderkey);
      // Within table_disc==1: linenumber==0 means order-level wildcard
      if (table_disc == 1 && (linenumber == 0 || other.linenumber == 0))
         return 0;
      if (invoicekey != other.invoicekey)
         return static_cast<int>(invoicekey) - static_cast<int>(other.invoicekey);
      return static_cast<int>(linenumber) - static_cast<int>(other.linenumber);
   }

   // Returns the index of the first differing field, the other's value at
   // that field, and the signed difference (this - other). Used by
   // PremergedJoin::distance to compute seek distances.
   std::tuple<int, int, int> first_diff(const coli_sort_key_t& other) const
   {
      if (custkey != other.custkey)
         return {0, static_cast<int>(other.custkey),
                 static_cast<int>(custkey) - static_cast<int>(other.custkey)};
      if (table_disc != other.table_disc)
         return {1, static_cast<int>(other.table_disc),
                 static_cast<int>(table_disc) - static_cast<int>(other.table_disc)};
      if (orderkey != other.orderkey)
         return {2, static_cast<int>(other.orderkey),
                 static_cast<int>(orderkey) - static_cast<int>(other.orderkey)};
      if (invoicekey != other.invoicekey)
         return {3, static_cast<int>(other.invoicekey),
                 static_cast<int>(invoicekey) - static_cast<int>(other.invoicekey)};
      if (linenumber != other.linenumber)
         return {4, static_cast<int>(other.linenumber),
                 static_cast<int>(linenumber) - static_cast<int>(other.linenumber)};
      return {4, static_cast<int>(other.linenumber), 0};
   }

   // operator<< is provided by ADD_KEY_TRAITS above.
};

}  // namespace tpch

// std::hash specialization: hashes on custkey only so that all rows for the
// same customer collide into the same hash bucket (required by HashJoin).
namespace std
{
template <>
struct hash<tpch::coli_sort_key_t> {
   std::size_t operator()(const tpch::coli_sort_key_t& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.custkey));
   }
};
}  // namespace std

namespace tpch
{

// ---------------------------------------------------------------------------
// joined_coli_t: CUSTOMER || ORDERS || LINEITEM || INVOICE concatenated record.
//
// Analogous to joined_ol_t, but for four tables keyed by custkey.
// fold_pks=false: only the sort key is folded; constituent PKs are
// reconstructed by the explicit Key(coli_sort_key_t) constructor below.
//
// Per-query operator drivers (Q3I/Q9I/Q12I) emit rows of this type from
// their PremergedJoin / BinaryMergeJoin drivers.

struct joined_coli_t
    : public joined_t<31, coli_sort_key_t, false, customerh_t, orders_t, lineitem_t, invoice_t> {
   joined_coli_t() = default;

   joined_coli_t(const customerh_t& c, const orders_t& o, const lineitem_t& l, const invoice_t& i)
       : joined_t(c, o, l, i)
   {
   }

   struct Key : public joined_t::Key {
      Key() = default;

      // Reconstruct constituent keys from the join key (fold_pks=false path).
      // Sort key shape determines which PK fields are valid:
      //   table_disc==0 → Customer only  (orderkey/invoicekey/linenumber == 0)
      //   table_disc==1, linenumber==0 → Orders row
      //   table_disc==1, linenumber>0  → Lineitem row
      //   table_disc==2 → Invoice row   (orderkey/linenumber == 0)
      explicit Key(const coli_sort_key_t& jk)
          : joined_t::Key(jk,
                          customerh_t::Key{jk.custkey},
                          orders_t::Key{jk.orderkey},
                          lineitem_t::Key{jk.orderkey, jk.linenumber},
                          invoice_t::Key{jk.invoicekey})
      {
      }

      // Build the join key from constituent keys (lineitem is most specific).
      Key(const customerh_t::Key& ck,
          const orders_t::Key& ok,
          const lineitem_t::Key& lk,
          const invoice_t::Key& ik)
          : joined_t::Key(
                coli_sort_key_t{ck.c_custkey, Integer(1), lk.l_orderkey,
                                Integer(0), lk.l_linenumber},
                ck, ok, lk, ik)
      {
      }
   };

   // Convenience accessors
   const customerh_t& customer() const { return std::get<0>(payloads); }
   const orders_t&    order()    const { return std::get<1>(payloads); }
   const lineitem_t&  line()     const { return std::get<2>(payloads); }
   const invoice_t&   invoice()  const { return std::get<3>(payloads); }

   // Override unfoldKey: the generic fold_pks=false path would try
   // `typename Ts::Key{coli_sort_key_t}` which doesn't exist for these types.
   // Instead use Key(coli_sort_key_t) which knows how to split the sort key.
   template <typename K>
   static unsigned unfoldKey(const uint8_t* in, K& key)
   {
      coli_sort_key_t jk;
      unsigned read = coli_sort_key_t::keyunfold(in, jk);
      key = K(jk);
      return read;
   }
};

}  // namespace tpch

// ---------------------------------------------------------------------------
// SKBuilder<coli_sort_key_t>: derives a sort key from each participating
// record type. Required by PremergedJoin / BinaryMergeJoin.

template <>
struct SKBuilder<tpch::coli_sort_key_t> {
   // Customer: (custkey, 0, 0, 0, 0)
   static tpch::coli_sort_key_t create(const customerh_t::Key& k, const customerh_t&)
   {
      return {k.c_custkey, Integer(0), Integer(0), Integer(0), Integer(0)};
   }

   // Orders: (custkey, 1, orderkey, 0, 0)
   static tpch::coli_sort_key_t create(const orders_t::Key& k, const orders_t& v)
   {
      return {v.o_custkey, Integer(1), k.o_orderkey, Integer(0), Integer(0)};
   }

   // Lineitem: (custkey, 1, orderkey, l_invoicekey, linenumber)
   // Custkey must be resolved via the parent order; callers supply it via
   // the payload's l_invoicekey field. Orderkey is in the lineitem PK.
   // Note: custkey is not directly available in lineitem's PK or payload —
   // the merged index is loaded by replaying (orderkey → custkey) from
   // the orders base table, but at SKBuilder call sites the custkey has
   // already been resolved by the pipeline's populate_merged loop.
   // For the static create overload we default custkey to 0; the pipeline
   // uses the overload below that accepts an explicit custkey.
   static tpch::coli_sort_key_t create(const lineitem_t::Key& k, const lineitem_t& v)
   {
      return {Integer(0), Integer(1), k.l_orderkey, v.l_invoicekey, k.l_linenumber};
   }

   // Lineitem with explicit custkey (used by populate_merged where custkey
   // is known from the enclosing order).
   static tpch::coli_sort_key_t create_lineitem(Integer custkey,
                                                 const lineitem_t::Key& k,
                                                 const lineitem_t& v)
   {
      return {custkey, Integer(1), k.l_orderkey, v.l_invoicekey, k.l_linenumber};
   }

   // Invoice: (custkey, 2, 0, invoicekey, 0)
   static tpch::coli_sort_key_t create(const invoice_t::Key& k, const invoice_t& v)
   {
      return {v.i_custkey, Integer(2), Integer(0), k.i_invoicekey, Integer(0)};
   }

   static tpch::coli_sort_key_t create(const tpch::joined_coli_t::Key& k,
                                        const tpch::joined_coli_t&)
   {
      return k.jk;
   }

   // project<R>: project a full join key down to the granularity of record R.
   template <typename Record>
   static tpch::coli_sort_key_t project(const tpch::coli_sort_key_t& k);

   // to_key<R>: convert a sort key to the native primary key of record R.
   template <typename R>
   static typename R::Key to_key(const tpch::coli_sort_key_t& sk);
};

// --- project<R> explicit specializations ---

template <>
inline tpch::coli_sort_key_t
SKBuilder<tpch::coli_sort_key_t>::project<customerh_t>(const tpch::coli_sort_key_t& k)
{
   return {k.custkey, Integer(0), Integer(0), Integer(0), Integer(0)};
}

template <>
inline tpch::coli_sort_key_t
SKBuilder<tpch::coli_sort_key_t>::project<orders_t>(const tpch::coli_sort_key_t& k)
{
   return {k.custkey, Integer(1), k.orderkey, Integer(0), Integer(0)};
}

template <>
inline tpch::coli_sort_key_t
SKBuilder<tpch::coli_sort_key_t>::project<lineitem_t>(const tpch::coli_sort_key_t& k)
{
   return k;  // lineitem uses the full key
}

template <>
inline tpch::coli_sort_key_t
SKBuilder<tpch::coli_sort_key_t>::project<invoice_t>(const tpch::coli_sort_key_t& k)
{
   return {k.custkey, Integer(2), Integer(0), k.invoicekey, Integer(0)};
}

// --- to_key<R> explicit specializations ---

template <>
inline customerh_t::Key
SKBuilder<tpch::coli_sort_key_t>::to_key<customerh_t>(const tpch::coli_sort_key_t& sk)
{
   return customerh_t::Key{sk.custkey};
}

template <>
inline orders_t::Key
SKBuilder<tpch::coli_sort_key_t>::to_key<orders_t>(const tpch::coli_sort_key_t& sk)
{
   return orders_t::Key{sk.orderkey};
}

template <>
inline lineitem_t::Key
SKBuilder<tpch::coli_sort_key_t>::to_key<lineitem_t>(const tpch::coli_sort_key_t& sk)
{
   return lineitem_t::Key{sk.orderkey, sk.linenumber};
}

template <>
inline invoice_t::Key
SKBuilder<tpch::coli_sort_key_t>::to_key<invoice_t>(const tpch::coli_sort_key_t& sk)
{
   return invoice_t::Key{sk.invoicekey};
}

template <>
inline tpch::joined_coli_t::Key
SKBuilder<tpch::coli_sort_key_t>::to_key<tpch::joined_coli_t>(
    const tpch::coli_sort_key_t& sk)
{
   return tpch::joined_coli_t::Key{sk};
}
