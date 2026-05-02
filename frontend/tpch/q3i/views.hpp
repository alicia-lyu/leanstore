#pragma once

// Q3I-specific record types: Q3 extended with a per-customer open-invoice-due
// aggregate joined from the INVOICE table via the COLI 4-table merged index.
//
// Row-shape types only.  Query-time parameters and predicate declarations live
// in workload.hpp alongside Q3IWorkload.
//
// This file also provides the SKBuilder specializations and std::hash
// specializations required by BinaryMergeJoin / HashJoin for the two
// intermediate join key types (cust_open_due_t::Key, lineitem_agg_t::Key).
// These live here (not in a separate header) because both the key types and
// their join machinery are Q3I-specific.

#include <functional>
#include <limits>

#include "../views_coli.hpp"
#include "../views_ol.hpp"

namespace tpch::q3i
{

// ---------------------------------------------------------------------------
// Structure 2 pipeline view row: per-(custkey, orderkey) post-aggregate row.
//
// Rationale for widening from joined_ol_t alias:
//   The family logical plan's inside-pipeline SortedAggregate collapses all
//   lineitems per orderkey into one revenue sum.  The view therefore has one
//   row per (custkey, orderkey) — roughly |orders| rows — which is far smaller
//   than joined_ol_t × N_lineitems_per_order.  Carrying cust_open_due,
//   c_mktsegment, o_orderdate, and o_shippriority in the view row means S2's
//   query time is a single sequential scan with per-row mktsegment + threshold
//   filters, with no secondary invoice lookup needed.  This makes S2 a fair
//   comparison point against S1/S3 (both of which build cust_open_due in a
//   single streaming pass) while keeping the view UNFILTERED on parametrised
//   predicates so it stays reusable across param sets (predicate hoisting).

struct q3i_pipeline_view_t {
   static constexpr int id = 43;

   struct Key {
      static constexpr int id = 43;
      Integer custkey;   // primary sort — groups custkey partitions
      Integer orderkey;  // secondary sort — unique within a custkey group
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)
   };

   Numeric     revenue;         // SUM(l_extendedprice * (1 - l_discount)) per orderkey
   Numeric     cust_open_due;   // SUM(i_totaldue WHERE i_status='O') per custkey
   Varchar<10> c_mktsegment;    // customer market segment (filter at query time)
   Timestamp   o_orderdate;     // order date (filter at query time)
   Integer     o_shippriority;  // output column

   ADD_RECORD_TRAITS(q3i_pipeline_view_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Intermediate row types used by S1/S2 query drivers (Phase 2).

// Per-customer open-invoice-due aggregate: one row per custkey in the
// custkey-sorted COLI secondary invoice index.
struct cust_open_due_t {
   static constexpr int id = 44;

   struct Key {
      static constexpr int id = 44;
      Integer custkey;
      ADD_KEY_TRAITS(&Key::custkey)

      // Join-operator contract: match returns negative/zero/positive like strcmp.
      // Two cust_open_due_t keys are in the same group when their custkeys agree.
      // customerh_t::Key carries c_custkey; we compare against custkey directly.
      int match(const Key& other) const
      {
         if (custkey != other.custkey) return custkey < other.custkey ? -1 : 1;
         return 0;
      }

      static Key max()
      {
         return Key{std::numeric_limits<Integer>::max()};
      }

      // For HashJoin: a custkey-level key matches only itself (no hierarchy).
      std::vector<Key> matching_keys() const { return {*this}; }

      auto operator<=>(const Key&) const = default;

      friend int operator%(const Key& k, int n) { return static_cast<int>(k.custkey) % n; }
   };

   Numeric cust_open_due;  // SUM(i_totaldue WHERE i_status='O')

   ADD_RECORD_TRAITS(cust_open_due_t)

   void print(std::ostream& os) const;
};

// Per-(custkey, orderkey) lineitem revenue aggregate: output of the
// LineitemRevenueAggregator scanner-wrapper (OPERATORS.md §3 op 6).
// One row per orderkey after SUM(l_extendedprice * (1 - l_discount)).
struct lineitem_agg_t {
   static constexpr int id = 45;

   struct Key {
      static constexpr int id = 45;
      Integer custkey;
      Integer orderkey;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)

      // Match on (custkey, orderkey). The left side of BMJ #3 is JR2 whose
      // join key is cust_open_due_t::Key (custkey only); BMJ uses the
      // SKBuilder::create overloads to derive a lineitem_agg_t::Key from JR2,
      // so the two-field key always has both components set at match time.
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

      friend int operator%(const Key& k, int n) { return static_cast<int>(k.custkey) % n; }
   };

   Numeric revenue;  // SUM(l_extendedprice * (1 - l_discount))

   ADD_RECORD_TRAITS(lineitem_agg_t)

   void print(std::ostream& os) const;
};

// ---------------------------------------------------------------------------
// Explicit join result types for the S1 3-BMJ chain.
//
// JoinState::join_current() constructs JR::Key as JR::Key{R1::Key, R2::Key, ...}
// (variadic pack from cartesian product pairs). The generic joined_t::Key does
// not have such a constructor — it expects (JK, Ts::Key...). We therefore
// provide explicit structs with the right Key constructor, mirroring the
// joined_ol_t pattern from views_ol.hpp.

// JR1: customer ⋈ cust_open_due on custkey.
struct q3i_jr1_t : public joined_t<46, cust_open_due_t::Key, false,
                                    customerh_t, cust_open_due_t>
{
   using Base = joined_t<46, cust_open_due_t::Key, false, customerh_t, cust_open_due_t>;
   q3i_jr1_t() = default;
   q3i_jr1_t(const customerh_t& c, const cust_open_due_t& d) : Base(c, d) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys — required by JoinState::join_current.
      Key(const customerh_t::Key& ck, const cust_open_due_t::Key& dk)
          : Base::Key(dk, ck, dk)
      {
      }

      // Constructor from JK (used by BMJ unfold path with fold_pks=false).
      explicit Key(const cust_open_due_t::Key& jk)
          : Base::Key(jk,
                      customerh_t::Key{jk.custkey},
                      cust_open_due_t::Key{jk.custkey})
      {
      }
   };

   const customerh_t&    cust() const { return std::get<0>(payloads); }
   const cust_open_due_t& due()  const { return std::get<1>(payloads); }
};

// JR2: q3i_jr1_t ⋈ orders_coli_t on custkey.
struct q3i_jr2_t : public joined_t<47, cust_open_due_t::Key, false,
                                    q3i_jr1_t, orders_coli_t>
{
   using Base = joined_t<47, cust_open_due_t::Key, false, q3i_jr1_t, orders_coli_t>;
   q3i_jr2_t() = default;
   q3i_jr2_t(const q3i_jr1_t& jr1, const orders_coli_t& o) : Base(jr1, o) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys — required by JoinState::join_current.
      // orderkey is in orders_coli_t::Key, custkey in q3i_jr1_t::Key.jk.
      Key(const q3i_jr1_t::Key& jk1, const orders_coli_t::Key& ok)
          : Base::Key(jk1.jk, jk1, ok)
      {
      }

      explicit Key(const cust_open_due_t::Key& jk)
          : Base::Key(jk,
                      q3i_jr1_t::Key{jk},
                      orders_coli_t::Key{jk.custkey, Integer(0)})
      {
      }
   };

   const q3i_jr1_t&    jr1()   const { return std::get<0>(payloads); }
   const orders_coli_t& order() const { return std::get<1>(payloads); }
};

// JR3: q3i_jr2_t ⋈ lineitem_agg_t on (custkey, orderkey).
struct q3i_jr3_t : public joined_t<48, lineitem_agg_t::Key, false,
                                    q3i_jr2_t, lineitem_agg_t>
{
   using Base = joined_t<48, lineitem_agg_t::Key, false, q3i_jr2_t, lineitem_agg_t>;
   q3i_jr3_t() = default;
   q3i_jr3_t(const q3i_jr2_t& jr2, const lineitem_agg_t& l) : Base(jr2, l) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys.
      Key(const q3i_jr2_t::Key& jk2, const lineitem_agg_t::Key& lk)
          : Base::Key(lk, jk2, lk)
      {
      }

      explicit Key(const lineitem_agg_t::Key& jk)
          : Base::Key(jk,
                      q3i_jr2_t::Key{cust_open_due_t::Key{jk.custkey}},
                      lineitem_agg_t::Key{jk.custkey, jk.orderkey})
      {
      }
   };

   const q3i_jr2_t&    jr2()    const { return std::get<0>(payloads); }
   const lineitem_agg_t& linagg() const { return std::get<1>(payloads); }
};

// ---------------------------------------------------------------------------
// Final aggregate output row: one per (orderkey, orderdate, shippriority,
// cust_open_due) group, ordered by revenue DESC LIMIT 10.

struct q3i_agg_row_t {
   static constexpr int id = 42;

   struct Key {
      static constexpr int id = 42;
      Integer o_orderkey;
      ADD_KEY_TRAITS(&Key::o_orderkey)
   };

   Integer   o_orderkey;
   Numeric   revenue;        // SUM(l_extendedprice * (1 - l_discount))
   Timestamp o_orderdate;
   Integer   o_shippriority;
   Numeric   cust_open_due;  // SUM(i_totaldue) WHERE i_status='O' per custkey

   ADD_RECORD_TRAITS(q3i_agg_row_t)

   void print(std::ostream& os) const;
};

}  // namespace tpch::q3i

// ---------------------------------------------------------------------------
// std::hash specializations — required by HashJoin's unordered_multimap.

namespace std
{

template <>
struct hash<tpch::q3i::cust_open_due_t::Key> {
   std::size_t operator()(const tpch::q3i::cust_open_due_t::Key& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.custkey));
   }
};

template <>
struct hash<tpch::q3i::lineitem_agg_t::Key> {
   std::size_t operator()(const tpch::q3i::lineitem_agg_t::Key& k) const
   {
      // Combine custkey and orderkey via the standard xor-shift pattern.
      std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
      h ^= std::hash<int>{}(static_cast<int>(k.orderkey)) + 0x9e3779b9 + (h << 6) + (h >> 2);
      return h;
   }
};

}  // namespace std

// ---------------------------------------------------------------------------
// SKBuilder specializations for the Q3I intermediate join key types.
//
// BinaryMergeJoin/JoinState call:
//   SKBuilder<JK>::create(k, v)          — derive JK from a (Key, Value) pair
//   SKBuilder<JK>::project<R>(jk)        — project JK to granularity of R
//   SKBuilder<JK>::to_key<R>(jk)         — convert JK to R's primary Key
//
// The S1 3-BMJ chain uses two join key types:
//   JK1 = cust_open_due_t::Key  (custkey only)    — BMJ #1 (cust ⋈ inv_agg)
//                                                   and BMJ #2 (JR1 ⋈ ord)
//   JK3 = lineitem_agg_t::Key   (custkey,orderkey) — BMJ #3 (JR2 ⋈ lin_agg)

template <>
struct SKBuilder<tpch::q3i::cust_open_due_t::Key> {
   using JK = tpch::q3i::cust_open_due_t::Key;

   // customerh_t: join key is c_custkey.
   static JK create(const customerh_t::Key& k, const customerh_t&)
   {
      return JK{k.c_custkey};
   }

   // cust_open_due_t: join key is custkey.
   static JK create(const JK& k, const tpch::q3i::cust_open_due_t&)
   {
      return k;
   }

   // orders_coli_t (right side of BMJ #2): join key is custkey.
   static JK create(const tpch::orders_coli_t::Key& k, const tpch::orders_coli_t&)
   {
      return JK{k.custkey};
   }

   // q3i_jr1_t (left side of BMJ #2): extract custkey from jk.
   static JK create(const tpch::q3i::q3i_jr1_t::Key& k,
                    const tpch::q3i::q3i_jr1_t&)
   {
      return k.jk;
   }

   // project<R>: all participating types are custkey-granularity — identity.
   template <typename R>
   static JK project(const JK& k) { return k; }

   // to_key<R>: not needed for BMJ (only used by PremergedJoin), but required
   // by the SKBuilder contract.  Provided as a no-op stub.
   template <typename R>
   static JK to_key(const JK& k) { return k; }
};

template <>
struct SKBuilder<tpch::q3i::lineitem_agg_t::Key> {
   using JK = tpch::q3i::lineitem_agg_t::Key;

   // lineitem_agg_t: join key is (custkey, orderkey).
   static JK create(const JK& k, const tpch::q3i::lineitem_agg_t&)
   {
      return k;
   }

   // q3i_jr2_t (left side of BMJ #3): extract custkey from jk,
   // orderkey from the embedded orders_coli_t::Key.
   static JK create(const tpch::q3i::q3i_jr2_t::Key& k,
                    const tpch::q3i::q3i_jr2_t&)
   {
      // k.keys = (q3i_jr1_t::Key, orders_coli_t::Key)
      const auto& ord_key = std::get<1>(k.keys);
      return JK{k.jk.custkey, ord_key.orderkey};
   }

   // project<R>: identity — all participating types carry (custkey, orderkey).
   template <typename R>
   static JK project(const JK& k) { return k; }

   // to_key stub (not used by BMJ).
   template <typename R>
   static JK to_key(const JK& k) { return k; }
};
