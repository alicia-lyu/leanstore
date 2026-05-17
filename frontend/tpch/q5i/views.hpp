#pragma once

// Q5I-specific record types: Q5 extended with invoice payment-status split.
// Row-shape types only. Query-time parameters and predicate declarations live
// in workload.hpp alongside Q5IWorkload.
//
// ID allocation (Phase 4b):
//   q5i_pipeline_view_t : id = 62
//   q5i_cust_jk_t       : id = 63  custkey-only JK for S1 BMJ #1
//   q5i_jr1_t           : id = 64  output of customerh_t ⋈ orders_coli_t
//   q5i_co_jk_t         : id = 65  (custkey, orderkey) JK for S1 BMJ #2
//   q5i_jr2_t           : id = 66  output of q5i_jr1_t ⋈ lineitem_coli_t
//
// jr1/jr2 and the jk types are in-memory only — no ADD_RECORD_TRAITS.

#include <functional>
#include <limits>

#include "../../shared/view_templates.hpp"
#include "../tpch_family/views_coli.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q5i
{

// Structure 2 pipeline view: one row per (custkey, orderkey, invoicekey,
// linenumber) — Key mirrors lineitem_coli_t's leading record-type key so
// the view and MI are sortable in the same order. Predicate-hoisted:
// o_orderdate and region/nation/supplier filters applied at query time.
// i_status resolved from invoice join at view-load time (Pattern B:
// view loader deferred to Phase 4a — reuses S3 walker with parameterised
// filters dropped; see PLAYBOOK.md §3.6).
//
// n_name is NOT in this payload: the COLI MI is 4-table (C,O,L,I), so
// storing n_name here would make S2 a 5-table substitute and unfair vs S3.
// n_name is resolved post-pipeline (≤5 NATION PK lookups per query) in
// both S2 and S3 — see q5i/out_class.hpp and CLAUDE.md §Plan Descriptions.
struct q5i_pipeline_view_t {
   static constexpr int id = 62;

   struct Key {
      static constexpr int id = 62;
      Integer custkey;
      Integer orderkey;
      Integer invoicekey;   // mirrors lineitem_coli_t key shape
      Integer linenumber;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey,
                     &Key::invoicekey, &Key::linenumber)
      auto operator<=>(const Key&) const = default;
   };

   Numeric     l_extendedprice;
   Numeric     l_discount;
   Integer     l_suppkey;
   Integer     c_nationkey;
   Timestamp   o_orderdate;
   Varchar<1>  i_status;

   ADD_RECORD_TRAITS(q5i_pipeline_view_t)

   void print(std::ostream& os) const;
};

// Final aggregate output row: one per nation in the chosen region (≤5 rows).
// In-memory only — never stored in a B-tree or RocksDB adapter.
struct q5i_agg_row_t {
   std::string n_name;
   Numeric     nominal_revenue;   // all qualifying lineitems
   Numeric     realised_revenue;  // i_status = 'P'
   Numeric     open_revenue;      // i_status = 'O'
   Numeric     late_revenue;      // i_status = 'L'

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// S1 BMJ chain intermediate types (Phase 4b).
//
//   BMJ #1: customerh_t ⋈ orders_coli_t  on custkey            → q5i_jr1_t
//   BMJ #2: q5i_jr1_t   ⋈ lineitem_coli_t on (custkey, orderkey) → q5i_jr2_t
//
// Plain 2-field JK for BMJ #2 (no wildcard sort-key abstraction): per
// (custkey, orderkey) group we have 1 jr1 × N lineitems = N emits;
// BMJ's per-JK buffering handles the Cartesian.
//
// No `n_name` carried anywhere — resolved post-pipeline in Q5IOutClass
// (Phase 4a commit 0). Per-emit callback assembles q5i_pipeline_out_t
// and feeds out.emit().

struct q5i_cust_jk_t {
   static constexpr int id = 63;

   struct Key {
      static constexpr int id = 63;
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

struct q5i_co_jk_t {
   static constexpr int id = 65;

   struct Key {
      static constexpr int id = 65;
      Integer custkey;
      Integer orderkey;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)

      int match(const Key& other) const
      {
         if (custkey  != other.custkey)  return custkey  < other.custkey  ? -1 : 1;
         if (orderkey != other.orderkey) return orderkey < other.orderkey ? -1 : 1;
         return 0;
      }

      static Key max()
      {
         return Key{std::numeric_limits<Integer>::max(),
                    std::numeric_limits<Integer>::max()};
      }

      std::vector<Key> matching_keys() const { return {*this}; }

      auto operator<=>(const Key&) const = default;

      friend int operator%(const Key& k, int n)
      {
         std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
         h ^= std::hash<int>{}(static_cast<int>(k.orderkey))
              + 0x9e3779b9u + (h << 6) + (h >> 2);
         return static_cast<int>(h % static_cast<std::size_t>(n));
      }
   };
};

// JR1: customerh_t ⋈ orders_coli_t on custkey.
struct q5i_jr1_t : public joined_t<64, q5i_cust_jk_t::Key, false,
                                    customerh_t, orders_coli_t>
{
   using Base = joined_t<64, q5i_cust_jk_t::Key, false,
                          customerh_t, orders_coli_t>;

   q5i_jr1_t() = default;
   q5i_jr1_t(const customerh_t& c, const orders_coli_t& o) : Base(c, o) {}

   struct Key : public Base::Key {
      Key() = default;

      Key(const customerh_t::Key& ck, const orders_coli_t::Key& ok)
          : Base::Key(q5i_cust_jk_t::Key{ck.c_custkey}, ck, ok)
      {
      }

      explicit Key(const q5i_cust_jk_t::Key& jk)
          : Base::Key(jk,
                      customerh_t::Key{jk.custkey},
                      orders_coli_t::Key{jk.custkey, 0})
      {
      }
   };

   const customerh_t&    cust()  const { return std::get<0>(payloads); }
   const orders_coli_t&  order() const { return std::get<1>(payloads); }
};

// JR2: q5i_jr1_t ⋈ lineitem_coli_t on (custkey, orderkey).
struct q5i_jr2_t : public joined_t<66, q5i_co_jk_t::Key, false,
                                    q5i_jr1_t, lineitem_coli_t>
{
   using Base = joined_t<66, q5i_co_jk_t::Key, false,
                          q5i_jr1_t, lineitem_coli_t>;

   q5i_jr2_t() = default;
   q5i_jr2_t(const q5i_jr1_t& jr1, const lineitem_coli_t& l) : Base(jr1, l) {}

   struct Key : public Base::Key {
      Key() = default;

      Key(const q5i_jr1_t::Key& jk1, const lineitem_coli_t::Key& lk)
          : Base::Key(q5i_co_jk_t::Key{jk1.jk.custkey, lk.orderkey}, jk1, lk)
      {
      }

      explicit Key(const q5i_co_jk_t::Key& jk)
          : Base::Key(jk,
                      q5i_jr1_t::Key{q5i_cust_jk_t::Key{jk.custkey}},
                      lineitem_coli_t::Key{jk.custkey, jk.orderkey, 0, 0})
      {
      }
   };

   const q5i_jr1_t&       jr1()      const { return std::get<0>(payloads); }
   const lineitem_coli_t& lineitem() const { return std::get<1>(payloads); }
};

}  // namespace tpch::q5i

namespace std
{

template <>
struct hash<tpch::q5i::q5i_cust_jk_t::Key> {
   std::size_t operator()(const tpch::q5i::q5i_cust_jk_t::Key& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.custkey));
   }
};

template <>
struct hash<tpch::q5i::q5i_co_jk_t::Key> {
   std::size_t operator()(const tpch::q5i::q5i_co_jk_t::Key& k) const
   {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
      h ^= std::hash<int>{}(static_cast<int>(k.orderkey))
           + 0x9e3779b9u + (h << 6) + (h >> 2);
      return h;
   }
};

}  // namespace std

// ---------------------------------------------------------------------------
// SKBuilder specialisations.

template <>
struct SKBuilder<tpch::q5i::q5i_cust_jk_t::Key> {
   using JK = tpch::q5i::q5i_cust_jk_t::Key;

   static JK create(const customerh_t::Key& k, const customerh_t&)
   {
      return JK{k.c_custkey};
   }

   static JK create(const tpch::orders_coli_t::Key& k, const tpch::orders_coli_t&)
   {
      return JK{k.custkey};
   }

   static JK create(const tpch::q5i::q5i_jr1_t::Key& k, const tpch::q5i::q5i_jr1_t&)
   {
      return k.jk;
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};

template <>
struct SKBuilder<tpch::q5i::q5i_co_jk_t::Key> {
   using JK = tpch::q5i::q5i_co_jk_t::Key;

   static JK create(const tpch::q5i::q5i_jr1_t::Key& k, const tpch::q5i::q5i_jr1_t&)
   {
      return JK{k.jk.custkey, std::get<1>(k.keys).orderkey};
   }

   static JK create(const tpch::lineitem_coli_t::Key& k, const tpch::lineitem_coli_t&)
   {
      return JK{k.custkey, k.orderkey};
   }

   template <typename R>
   static JK project(const JK& k) { return k; }

   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
