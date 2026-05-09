#pragma once

// Q5-specific record types for the COL pipeline (CUSTOMER × ORDERS × LINEITEM).
//
// Types defined here:
//   - q5_pipeline_view_t  : Structure 2 intermediate view — one row per
//                           (custkey, orderkey, linenumber), unaggregated
//                           lineitem fields + FD-attached order/customer cols.
//   - q5_agg_row_t        : final aggregate output row, one per in-region
//                           nation (~5 rows).
//   - q5_cust_jk_t        : custkey-only join-key type used by S1 BMJ #1.
//   - q5_jr1_t            : result of BMJ #1: customerh_t ⋈ orders_coli_t
//                           on custkey.
//   - q5_jr2_t            : result of BMJ #2: q5_jr1_t ⋈ lineitem_col_t
//                           on (custkey, orderkey, linenumber).  Right side is
//                           per-lineitem (NOT pre-aggregated) because each
//                           lineitem needs l_suppkey for the SUPPLIER probe and
//                           c_nationkey = s_nationkey cross-equality.
//
// S1 BMJ chain shape:
//   BMJ #1: customerh_t ⋈ orders_coli_t  on custkey             → q5_jr1_t
//   BMJ #2: q5_jr1_t    ⋈ lineitem_col_t on (custkey, orderkey,
//                                             linenumber)         → q5_jr2_t
//
// The BMJ #2 join key reuses q5_pipeline_view_t::Key (same 3-field shape)
// rather than minting a fresh q5_lineitem_jk_t.
//
// ID allocation (verified 2026-05-08 against all existing record type IDs
// in the tpch directory — last used was 55 in tpchi_tables.hpp):
//   q5_pipeline_view_t : id = 56
//   q5_agg_row_t       : id = 57  (reserved; type is in-memory only)
//   q5_cust_jk_t       : id = 58
//   q5_jr1_t           : id = 59
//   q5_jr2_t           : id = 60
//
// Params struct and predicate declarations live in workload.hpp.
// SKBuilder specialisations live below.

#include <functional>
#include <limits>

#include "../tpch_family/views_col.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey, linenumber) row.
//
// Schema: one row per lineitem, carrying unaggregated lineitem fields so
// query_by_view can apply the orderdate filter live and accumulate revenue
// per n_name at query time.  No filters are baked in — predicate hoisting
// keeps the view reusable across all REGION / DATE param sets.
//
// FD-attached customer / order columns (c_nationkey, o_orderdate) are
// copied into every lineitem row — the same "schema-faithful secondary"
// policy as the COL MI.
//
// l_suppkey is included so query_by_view can perform the SUPPLIER hashmap
// probe and cross-equality check (c_nationkey = s_nationkey) without
// re-joining base tables.

struct q5_pipeline_view_t {
   static constexpr int id = 56;

   struct Key {
      static constexpr int id = 56;
      Integer custkey;     // primary sort — groups custkey partitions
      Integer orderkey;    // secondary sort — unique within a custkey group
      Integer linenumber;  // tertiary sort — unique within an order
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric   l_extendedprice;
   Numeric   l_discount;
   Integer   l_suppkey;      // supplier join key for SUPPLIER probe

   // FD-attached customer / order columns (parameter-independent at load time).
   Integer   c_nationkey;    // customer's nation (filter + GROUP BY driver)
   Timestamp o_orderdate;    // order date (filter at query time)

   ADD_RECORD_TRAITS(q5_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Final aggregate output row: one row per in-region nation (~5 rows).
//
// This is an in-memory result type — never stored in a B-tree or RocksDB
// column family — so it has no Key sub-struct, no SKBuilder, and no
// ADD_RECORD_TRAITS.  Fields are stored flat, analogous to q3_agg_row_base_t.
//
// Design note: the SQL groups by n_name, but n_nationkey is 1:1 with n_name
// so we track accumulation by nationkey internally and carry n_name for output.
// The result is sorted by revenue DESC (no LIMIT; bounded by |nation_set| ≈ 5).
//
// id = 57 is reserved here for future use (e.g. if this type is ever stored
// in a secondary structure).  It is not currently active.

struct q5_agg_row_t {
   Integer     n_nationkey;  // grouping key (1:1 with n_name)
   Varchar<25> n_name;       // output column (GROUP BY n_name per spec)
   Numeric     revenue;      // SUM(l_extendedprice * (1 - l_discount))

   void print(std::ostream& os) const;

   // Comparator for sorting: revenue DESC (no tiebreaker needed — ~5 rows).
   static bool cmp(const q5_agg_row_t& a, const q5_agg_row_t& b)
   {
      return a.revenue > b.revenue;
   }
};

// ---------------------------------------------------------------------------
// Q5 S1 BMJ chain — intermediate types (Phase 4 §7.2).
//
// Track 1 chain (no Invoice; mirrors Q3's 2-BMJ chain):
//
//   BMJ #1: customerh_t ⋈ orders_coli_t   on custkey                  → q5_jr1_t
//   BMJ #2: q5_jr1_t    ⋈ lineitem_col_t  on (custkey, orderkey,
//                                              linenumber)              → q5_jr2_t
//
// `q5_cust_jk_t` is a custkey-only join-key type used by BMJ #1.  Q5 keeps
// this type local (analogous to Q3's `q3_cust_jk_t`) so q5 does not depend
// on q3 headers.
//
// Key design difference from Q3: BMJ #2's right side is `lineitem_col_t`
// (per-lineitem), NOT a pre-aggregate.  Q5 must consult `supplier_nation_map
// [l_suppkey]` per lineitem and apply the cross-equality
// `c_nationkey = s_nationkey`; collapsing per-order lineitems into one
// revenue scalar before that probe would discard l_suppkey.

// Wrapper carrying only a Key (custkey).  Mirrors q3_cust_jk_t shape.
struct q5_cust_jk_t {
   static constexpr int id = 58;

   struct Key {
      static constexpr int id = 58;
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
struct q5_jr1_t : public joined_t<59, q5_cust_jk_t::Key, false,
                                   customerh_t, orders_coli_t>
{
   using Base = joined_t<59, q5_cust_jk_t::Key, false, customerh_t, orders_coli_t>;
   q5_jr1_t() = default;
   q5_jr1_t(const customerh_t& c, const orders_coli_t& o) : Base(c, o) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys — required by JoinState::join_current.
      Key(const customerh_t::Key& ck, const orders_coli_t::Key& ok)
          : Base::Key(q5_cust_jk_t::Key{ck.c_custkey}, ck, ok)
      {
      }

      // Constructor from JK — required by BMJ unfold path with fold_pks=false.
      explicit Key(const q5_cust_jk_t::Key& jk)
          : Base::Key(jk,
                      customerh_t::Key{jk.custkey},
                      orders_coli_t::Key{jk.custkey, Integer(0)})
      {
      }
   };

   const customerh_t&   cust()  const { return std::get<0>(payloads); }
   const orders_coli_t& order() const { return std::get<1>(payloads); }
};

// JR2: q5_jr1_t ⋈ lineitem_col_t on (custkey, orderkey, linenumber).
//
// The join key is q5_pipeline_view_t::Key (same 3-field shape), reused
// rather than minting a separate q5_lineitem_jk_t.
struct q5_jr2_t : public joined_t<60, q5_pipeline_view_t::Key, false,
                                   q5_jr1_t, lineitem_col_t>
{
   using Base = joined_t<60, q5_pipeline_view_t::Key, false,
                          q5_jr1_t, lineitem_col_t>;
   q5_jr2_t() = default;
   q5_jr2_t(const q5_jr1_t& jr1, const lineitem_col_t& l) : Base(jr1, l) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys.  Build the JK
      // (custkey, orderkey, linenumber) from the JR1 join-key (custkey),
      // the orders sub-key (carries orderkey), and the lineitem key
      // (carries linenumber).
      Key(const q5_jr1_t::Key& jk1, const lineitem_col_t::Key& lk)
          : Base::Key(q5_pipeline_view_t::Key{jk1.jk.custkey, lk.orderkey, lk.linenumber},
                      jk1, lk)
      {
      }

      explicit Key(const q5_pipeline_view_t::Key& jk)
          : Base::Key(jk,
                      q5_jr1_t::Key{q5_cust_jk_t::Key{jk.custkey}},
                      lineitem_col_t::Key{jk.custkey, jk.orderkey, jk.linenumber})
      {
      }
   };

   const q5_jr1_t&      jr1()     const { return std::get<0>(payloads); }
   const lineitem_col_t& lineitem() const { return std::get<1>(payloads); }
};

}  // namespace tpch::q5

namespace std
{

template <>
struct hash<tpch::q5::q5_cust_jk_t::Key> {
   std::size_t operator()(const tpch::q5::q5_cust_jk_t::Key& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.custkey));
   }
};

}  // namespace std

// ---------------------------------------------------------------------------
// SKBuilder specialisation for q5_pipeline_view_t::Key.
//
// Required so Backend::template Adapter<q5_pipeline_view_t> instantiates
// cleanly when Q5Workload is compiled.  Q5's view is not used in a join
// directly at Phase 0.5 — query_by_view is a stub — but the specialisation
// must exist for the template to compile.

template <>
struct SKBuilder<tpch::q5::q5_pipeline_view_t::Key> {
   using JK = tpch::q5::q5_pipeline_view_t::Key;

   static JK create(const JK& k, const tpch::q5::q5_pipeline_view_t&)
   {
      return k;
   }

   // q5_jr1_t (left side of BMJ #2): extract custkey from jk, orderkey from
   // the embedded orders_coli_t::Key.  linenumber is unknown at this level —
   // set to 0 so the BMJ seek positions correctly at the start of the order.
   static JK create(const tpch::q5::q5_jr1_t::Key& k, const tpch::q5::q5_jr1_t&)
   {
      const auto& ord_key = std::get<1>(k.keys);
      return JK{k.jk.custkey, ord_key.orderkey, Integer(0)};
   }

   // lineitem_col_t (right side of BMJ #2): the full 3-field key is the JK.
   static JK create(const tpch::lineitem_col_t::Key& k, const tpch::lineitem_col_t&)
   {
      return JK{k.custkey, k.orderkey, k.linenumber};
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};

// SKBuilder specialisation for q5_cust_jk_t::Key — used by BMJ #1.
//
// Participating types: customerh_t (left), orders_coli_t (right), q5_jr1_t
// (left of BMJ #2 via JK projection).
template <>
struct SKBuilder<tpch::q5::q5_cust_jk_t::Key> {
   using JK = tpch::q5::q5_cust_jk_t::Key;

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

   // q5_jr1_t (left side of BMJ #2): the embedded JK is itself custkey-only.
   static JK create(const tpch::q5::q5_jr1_t::Key& k, const tpch::q5::q5_jr1_t&)
   {
      return k.jk;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
