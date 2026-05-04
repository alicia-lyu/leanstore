#pragma once

// Q3-specific record types for the COL pipeline (CUSTOMER × ORDERS × LINEITEM).
//
// Three types are defined here:
//   - q3_pipeline_view_t  : Structure 2 intermediate view — one row per
//                           (custkey, orderkey, linenumber), unaggregated
//                           lineitem fields + FD-attached order/customer cols.
//   - q3_agg_row_t        : aliases q3_family::q3_agg_row_base_t directly
//                           (Q3 has no invoice extension; the four base fields
//                           are everything Q3 needs).
//
// Params struct and predicate declarations live in workload.hpp.
// SKBuilder specialisation for q3_pipeline_view_t::Key lives below.

#include <functional>
#include <limits>

#include "../q3_family/agg_row.hpp"
#include "../q3_family/lineitem_agg.hpp"
#include "../views_col.hpp"
#include "../views_ol.hpp"

namespace tpch::q3
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey, linenumber) row.
//
// Schema: one row per lineitem, carrying unaggregated lineitem fields so
// query_by_view can apply the shipdate filter live and accumulate revenue
// per orderkey at query time.  No filters are baked in — predicate hoisting
// keeps the view reusable across all SEGMENT / DATE param sets.
//
// FD-attached customer / order columns (c_mktsegment, o_orderdate,
// o_shippriority) are copied into every lineitem row — the same
// "schema-faithful secondary" policy as the COL MI.

struct q3_pipeline_view_t {
   static constexpr int id = 35;

   struct Key {
      static constexpr int id = 35;
      Integer custkey;     // primary sort — groups custkey partitions
      Integer orderkey;    // secondary sort — unique within a custkey group
      Integer linenumber;  // tertiary sort — unique within an order
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Timestamp l_shipdate;

   // FD-attached customer / order columns (parameter-independent at load time).
   Varchar<10> c_mktsegment;    // customer market segment (filter at query time)
   Timestamp   o_orderdate;     // order date (filter at query time)
   Integer     o_shippriority;  // output column

   ADD_RECORD_TRAITS(q3_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: Q3 uses the base type directly (no extension).

using q3_agg_row_t = q3_family::q3_agg_row_base_t;

// ---------------------------------------------------------------------------
// Q3 S1 BMJ chain — intermediate types (Phase 4 §7.2).
//
// Track 1 chain (no Invoice; cf. Q3I's 3-BMJ chain):
//
//   BMJ #1: customerh_t ⋈ orders_coli_t   on custkey            → q3_jr1_t
//   BMJ #2: q3_jr1_t    ⋈ lineitem_agg_t  on (custkey, orderkey) → q3_jr2_t
//
// `q3_cust_jk_t` is a custkey-only join-key type used by BMJ #1.  Q3I uses
// `cust_open_due_t::Key` for the same role; we keep this type Q3-local so
// q3 does not depend on q3i headers.

// Wrapper carrying only a Key (custkey).  Mirrors the shape of q3i's
// cust_open_due_t (Key + payload) so SKBuilder / BMJ machinery can refer
// to `q3_cust_jk_t::Key` consistently with the rest of the family.
struct q3_cust_jk_t {
   static constexpr int id = 37;

   struct Key {
      static constexpr int id = 37;
      Integer custkey;
      ADD_KEY_TRAITS(&Key::custkey)

      int match(const Key& other) const
      {
         if (custkey != other.custkey) return custkey < other.custkey ? -1 : 1;
         return 0;
      }

      static Key max() { return Key{std::numeric_limits<Integer>::max()}; }

      std::vector<Key> matching_keys() const { return {*this}; }

      auto operator<=>(const Key&) const = default;

      friend int operator%(const Key& k, int n)
      {
         return static_cast<int>(k.custkey) % n;
      }
   };
};

// JR1: customerh_t ⋈ orders_coli_t on custkey.
//
// Mirrors q3i_jr1_t shape with orders_coli_t in the right slot
// (Q3I's right slot is cust_open_due_t).
struct q3_jr1_t : public joined_t<38, q3_cust_jk_t::Key, false,
                                   customerh_t, orders_coli_t>
{
   using Base = joined_t<38, q3_cust_jk_t::Key, false, customerh_t, orders_coli_t>;
   q3_jr1_t() = default;
   q3_jr1_t(const customerh_t& c, const orders_coli_t& o) : Base(c, o) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys — required by JoinState::join_current.
      Key(const customerh_t::Key& ck, const orders_coli_t::Key& ok)
          : Base::Key(q3_cust_jk_t::Key{ck.c_custkey}, ck, ok)
      {
      }

      // Constructor from JK — required by BMJ unfold path with fold_pks=false.
      explicit Key(const q3_cust_jk_t::Key& jk)
          : Base::Key(jk,
                      customerh_t::Key{jk.custkey},
                      orders_coli_t::Key{jk.custkey, Integer(0)})
      {
      }
   };

   const customerh_t&    cust()  const { return std::get<0>(payloads); }
   const orders_coli_t&  order() const { return std::get<1>(payloads); }
};

// JR2: q3_jr1_t ⋈ lineitem_agg_t on (custkey, orderkey).
struct q3_jr2_t : public joined_t<39, q3_family::lineitem_agg_t::Key, false,
                                   q3_jr1_t, q3_family::lineitem_agg_t>
{
   using Base = joined_t<39, q3_family::lineitem_agg_t::Key, false,
                          q3_jr1_t, q3_family::lineitem_agg_t>;
   q3_jr2_t() = default;
   q3_jr2_t(const q3_jr1_t& jr1, const q3_family::lineitem_agg_t& l) : Base(jr1, l) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys.  Build the JK
      // (custkey, orderkey) from the JR1 join-key (custkey) and
      // the orders sub-key (carries orderkey).
      Key(const q3_jr1_t::Key& jk1, const q3_family::lineitem_agg_t::Key& lk)
          : Base::Key(lk, jk1, lk)
      {
      }

      explicit Key(const q3_family::lineitem_agg_t::Key& jk)
          : Base::Key(jk,
                      q3_jr1_t::Key{q3_cust_jk_t::Key{jk.custkey}},
                      q3_family::lineitem_agg_t::Key{jk.custkey, jk.orderkey})
      {
      }
   };

   const q3_jr1_t&                  jr1()    const { return std::get<0>(payloads); }
   const q3_family::lineitem_agg_t& linagg() const { return std::get<1>(payloads); }
};

}  // namespace tpch::q3

namespace std
{

template <>
struct hash<tpch::q3::q3_cust_jk_t::Key> {
   std::size_t operator()(const tpch::q3::q3_cust_jk_t::Key& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.custkey));
   }
};

}  // namespace std

// SKBuilder specialisation for q3_cust_jk_t::Key — used by BMJ #1.
//
// Participating types: customerh_t (left), orders_coli_t (right), q3_jr1_t
// (left of BMJ #2 via JK projection — handled by lineitem_agg_t::Key
// SKBuilder below).
template <>
struct SKBuilder<tpch::q3::q3_cust_jk_t::Key> {
   using JK = tpch::q3::q3_cust_jk_t::Key;

   // customerh_t: join key is c_custkey.
   static JK create(const customerh_t::Key& k, const customerh_t&)
   {
      return JK{k.c_custkey};
   }

   // orders_coli_t: join key is custkey.
   static JK create(const tpch::orders_coli_t::Key& k, const tpch::orders_coli_t&)
   {
      return JK{k.custkey};
   }

   // q3_jr1_t (left side of BMJ #2): the embedded JK is itself custkey-only.
   static JK create(const tpch::q3::q3_jr1_t::Key& k, const tpch::q3::q3_jr1_t&)
   {
      return k.jk;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};

// SKBuilder specialisation for lineitem_agg_t::Key — Q3-specific overload of
// the family Key.  Provides the q3_jr1_t left-side `create` (different shape
// from Q3I's q3i_jr2_t).  The right-side `create` (lineitem_agg_t identity)
// is shared with Q3I; both definitions are ODR-equivalent.
template <>
struct SKBuilder<tpch::q3_family::lineitem_agg_t::Key> {
   using JK = tpch::q3_family::lineitem_agg_t::Key;

   // lineitem_agg_t: join key is (custkey, orderkey).
   static JK create(const JK& k, const tpch::q3_family::lineitem_agg_t&)
   {
      return k;
   }

   // q3_jr1_t (left side of BMJ #2): extract custkey from jk, orderkey
   // from the embedded orders_coli_t::Key.
   static JK create(const tpch::q3::q3_jr1_t::Key& k, const tpch::q3::q3_jr1_t&)
   {
      // k.keys = (customerh_t::Key, orders_coli_t::Key)
      const auto& ord_key = std::get<1>(k.keys);
      return JK{k.jk.custkey, ord_key.orderkey};
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};

// ---------------------------------------------------------------------------
// SKBuilder specialisation for q3_pipeline_view_t::Key.
//
// Required by BinaryMergeJoin / PremergedJoin callers that derive join keys
// from this record type.  Q3's view is not used in a join directly — the
// Phase-0.5 query_by_view is a stub — but the specialisation must exist for
// the template to instantiate cleanly when Q3Workload is compiled.

template <>
struct SKBuilder<tpch::q3::q3_pipeline_view_t::Key> {
   using JK = tpch::q3::q3_pipeline_view_t::Key;

   static JK create(const JK& k, const tpch::q3::q3_pipeline_view_t&)
   {
      return k;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
