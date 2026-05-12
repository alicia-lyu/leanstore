#pragma once

// Q5-specific record types for the COL pipeline (CUSTOMER × ORDERS × LINEITEM).
//
// Types defined here:
//   - q5_pipeline_view_t  : Structure 2 intermediate view — one row per
//                           (custkey, orderkey, linenumber), unaggregated
//                           lineitem fields + FD-attached order/customer cols
//                           including n_name (FD-attached at load time).
//   - q5_agg_row_t        : final aggregate output row, one per in-region
//                           nation (~5 rows).
//   - q5_cust_jk_t        : custkey-only join-key type used by S1 BMJ #1.
//   - q5_jr1_t            : result of BMJ #1: customerh_t ⋈ orders_coli_t
//                           on custkey; carries n_name resolved at customer-
//                           survival point.
//   - q5_jr2_t            : result of BMJ #2: q5_jr1_t ⋈ lineitem_col_t
//                           on (custkey, orderkey, linenumber).  Right side is
//                           per-lineitem (NOT pre-aggregated) because each
//                           lineitem needs l_suppkey for the SUPPLIER probe and
//                           c_nationkey = s_nationkey cross-equality.  Carries
//                           n_name propagated from jr1.
//
// S1 BMJ chain shape:
//   BMJ #1: customerh_t ⋈ orders_coli_t  on custkey             → q5_jr1_t
//   BMJ #2: q5_jr1_t    ⋈ lineitem_col_t on (custkey, orderkey) → q5_jr2_t
//
// BMJ #2 join key is q5_sort_key_t (custkey, orderkey, linenumber) —
// a dedicated shared sort-key type that is its own Key (mirrors
// geo::sort_key_t).  Wildcard semantics live in match() / matching_keys()
// on q5_sort_key_t; SKBuilder<q5_sort_key_t>::project<q5_jr1_t> strips
// linenumber to WILDCARD_KEY so join_state preserves the jr1 buffer
// across per-lineitem cartesian flushes within an orderkey group.
// The canonical analogue is ol_sort_key_t in
// frontend/tpch/tpch_family/views_ol.hpp.
//
// ID allocation (verified 2026-05-08 against all existing record type IDs
// in the tpch directory — last used was 55 in tpchi_tables.hpp):
//   q5_pipeline_view_t : id = 56
//   q5_agg_row_t       : id = 57  (reserved; type is in-memory only)
//   q5_cust_jk_t       : id = 58
//   q5_jr1_t           : id = 59
//   q5_jr2_t           : id = 60
//   q5_sort_key_t      : id = 61  shared (custkey, orderkey, linenumber)
//                                   sort-key abstraction; JK for BMJ #2 /
//                                   HJ stage 2.  Replaced retired q5_co_jk_t.
//
// Params struct and predicate declarations live in workload.hpp.
// SKBuilder specialisations live below.

#include <functional>
#include <limits>
#include <string>

#include "../../shared/wildcard_key.hpp"
#include "../tpch_family/views_col.hpp"
#include "../tpch_tables.hpp"

namespace tpch::q5
{

// ---------------------------------------------------------------------------
// q5_sort_key_t — shared sort/join-key abstraction for the (custkey,
// orderkey, linenumber) hierarchy.  Used as the JK by BMJ #2 / HJ stage 2
// and any future operator that joins along this hierarchy.
//
// Design mirrors geo::sort_key_t (frontend/geo/views.hpp):
//   - Self-referential `using Key = q5_sort_key_t` — the type IS its own
//     key; not the primary key of any single table.
//   - Wildcard semantics live entirely in match() and matching_keys().
//     match() uses wildcard_match per field; matching_keys() enumerates
//     each prefix-anchor key a probe-side row must look up.
//   - Hash (operator% / std::hash) is wildcard-blind: it hashes all three
//     fields uniformly.  HashJoin probe walks matching_keys() to consult
//     each anchor bucket explicitly.
//   - Per-table records (q5_jr1_t, lineitem_col_t, q5_pipeline_view_t)
//     keep their own native ::Key types.  SKBuilder<q5_sort_key_t> below
//     extracts the sort key from each, projecting trailing fields to
//     WILDCARD_KEY where the record has no value at that level
//     (e.g. jr1 carries no linenumber so its sort key is
//     (custkey, orderkey, WILDCARD_KEY)).

struct q5_sort_key_t {
   static constexpr int id = 61;  // reuses retired q5_co_jk_t id slot

   Integer custkey;
   Integer orderkey;
   Integer linenumber;

   using Key = q5_sort_key_t;
   ADD_KEY_TRAITS(&q5_sort_key_t::custkey, &q5_sort_key_t::orderkey, &q5_sort_key_t::linenumber)

   auto operator<=>(const q5_sort_key_t&) const = default;

   static q5_sort_key_t max()
   {
      return q5_sort_key_t{std::numeric_limits<Integer>::max(),
                           std::numeric_limits<Integer>::max(),
                           std::numeric_limits<Integer>::max()};
   }

   // Wildcard-aware match — WILDCARD_KEY in any field on either side
   // matches any value in that field.  All wildcard semantics live here
   // and in matching_keys(); hash is wildcard-blind.
   int match(const q5_sort_key_t& other) const
   {
      if (int c = wildcard_match(custkey,    other.custkey);    c) return c;
      if (int c = wildcard_match(orderkey,   other.orderkey);   c) return c;
      if (int c = wildcard_match(linenumber, other.linenumber); c) return c;
      return 0;
   }

   // Enumerate every prefix-anchor key a probe-side row must look up,
   // walking up the (custkey, orderkey, linenumber) hierarchy.  Mirrors
   // geo::sort_key_t::matching_keys.
   std::vector<q5_sort_key_t> matching_keys() const
   {
      std::vector<q5_sort_key_t> result;
      if (linenumber != WILDCARD_KEY) {
         result.push_back(q5_sort_key_t{custkey, orderkey, WILDCARD_KEY});
      }
      if (orderkey != WILDCARD_KEY) {
         result.push_back(q5_sort_key_t{custkey, WILDCARD_KEY, WILDCARD_KEY});
      }
      result.push_back(*this);
      return result;
   }

   friend int operator%(const q5_sort_key_t& k, int n)
   {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
      h ^= std::hash<int>{}(static_cast<int>(k.orderkey))   + 0x9e3779b9u + (h << 6) + (h >> 2);
      h ^= std::hash<int>{}(static_cast<int>(k.linenumber)) + 0x9e3779b9u + (h << 6) + (h >> 2);
      return static_cast<int>(h % static_cast<std::size_t>(n));
   }
};

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
//
// q5_pipeline_view_t::Key is the primary key of the S2 view: a plain
// (custkey, orderkey, linenumber) triple, one row per lineitem.  All
// join semantics — wildcard match, prefix-anchor enumeration, hashing —
// live in the dedicated q5_sort_key_t (defined below), not on this Key.
// The view loader builds a sort key per row via SKBuilder<q5_sort_key_t>
// when needed for joining; for storage it just folds via ADD_KEY_TRAITS.

struct q5_pipeline_view_t {
   static constexpr int id = 56;

   struct Key {
      static constexpr int id = 56;
      Integer custkey;     // primary sort — groups custkey partitions
      Integer orderkey;    // secondary sort — unique within a custkey group
      Integer linenumber;  // tertiary sort — unique within an order
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey, &Key::linenumber)

      auto operator<=>(const Key&) const = default;
   };

   // Unaggregated lineitem fields — revenue computed at query time.
   Numeric     l_extendedprice;
   Numeric     l_discount;
   Integer     l_suppkey;      // supplier join key for SUPPLIER probe

   // FD-attached customer / order columns (parameter-independent at load time).
   Integer     c_nationkey;    // customer's nation (filter + GROUP BY driver)
   std::string n_name;         // FD-attached at load time (1:1 with c_nationkey)
   Timestamp   o_orderdate;    // order date (filter at query time)

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
// BMJ #2 / HJ stage 2 keys on `q5_sort_key_t` — a dedicated 3-field
// (custkey, orderkey, linenumber) shared sort-key type defined above
// (mirrors `geo::sort_key_t`).  All wildcard semantics (per-field
// match, full prefix-anchor enumeration, wildcard-blind hash) live on
// q5_sort_key_t; per-input projection (e.g. linenumber=WILDCARD_KEY for
// the build side jr1) lives in `SKBuilder<q5_sort_key_t>`.  See
// q5_sort_key_t header comment for the full convention.
//
// Key design difference from Q3: BMJ #2's right side is `lineitem_col_t`
// (per-lineitem), NOT a pre-aggregate.  Q5 must probe `supplier_nation_set`
// with the composite key `(c_nationkey, l_suppkey)` per lineitem (fuses
// SUPPLIER semi-join + cross-equality + suppkey equi-join); collapsing
// per-order lineitems into one revenue scalar before that probe would
// discard l_suppkey.

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
//
// Carries n_name resolved at the customer-survival point (Phase 10B
// widening): the CUSTOMER ⋈ RN inner join resolves n_name lazily via
// NATION PK lookup (~5 total) and the string is forwarded into q5_jr1_t
// so q5_jr2_t and the per-emit callback can pass it to q5_admit_lineitem
// without any further lookup.
struct q5_jr1_t : public joined_t<59, q5_cust_jk_t::Key, false,
                                   customerh_t, orders_coli_t>
{
   using Base = joined_t<59, q5_cust_jk_t::Key, false, customerh_t, orders_coli_t>;

   // n_name resolved at the customer-survival point of BMJ #1.
   // Not part of the key; carried as a plain member alongside the joined_t payload.
   std::string n_name;

   q5_jr1_t() = default;
   q5_jr1_t(const customerh_t& c, const orders_coli_t& o, std::string name = {})
       : Base(c, o), n_name(std::move(name)) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys — required by JoinState::join_current.
      Key(const customerh_t::Key& ck, const orders_coli_t::Key& ok)
          : Base::Key(q5_cust_jk_t::Key{ck.c_custkey}, ck, ok)
      {
      }

      // Constructor from JK — required by BMJ unfold path with fold_pks=false.
      // The JK (q5_cust_jk_t) carries only custkey, so the orders placeholder's
      // orderkey slot is filled with WILDCARD_KEY.
      explicit Key(const q5_cust_jk_t::Key& jk)
          : Base::Key(jk,
                      customerh_t::Key{jk.custkey},
                      orders_coli_t::Key{jk.custkey, WILDCARD_KEY})
      {
      }
   };

   const customerh_t&   cust()  const { return std::get<0>(payloads); }
   const orders_coli_t& order() const { return std::get<1>(payloads); }
};

// JR2: q5_jr1_t ⋈ lineitem_col_t on (custkey, orderkey, linenumber).
//
// The join key is q5_sort_key_t — the dedicated shared sort-key
// abstraction defined above.  linenumber follows the WILDCARD_KEY
// convention: jr1 build-side rows project linenumber=WILDCARD_KEY,
// lineitem probe-side rows project their real linenumber.
//
// n_name is propagated from jr1 (Phase 10B widening) so the per-emit
// callback can pass it directly to q5_admit_lineitem without any
// additional lookup.
struct q5_jr2_t : public joined_t<60, q5_sort_key_t, false,
                                   q5_jr1_t, lineitem_col_t>
{
   using Base = joined_t<60, q5_sort_key_t, false,
                          q5_jr1_t, lineitem_col_t>;

   // n_name propagated from jr1 at join-emit time.
   std::string n_name;

   q5_jr2_t() = default;
   q5_jr2_t(const q5_jr1_t& jr1, const lineitem_col_t& l)
       : Base(jr1, l), n_name(jr1.n_name) {}

   struct Key : public Base::Key {
      Key() = default;

      // Constructor from constituent Keys.  Carry the lineitem's real
      // linenumber into the JK; the JK's wildcard-aware match() handles
      // the build-side WILDCARD_KEY case symmetrically.
      Key(const q5_jr1_t::Key& jk1, const lineitem_col_t::Key& lk)
          : Base::Key(q5_sort_key_t{jk1.jk.custkey, lk.orderkey, lk.linenumber},
                      jk1, lk)
      {
      }

      // Reverse-construct constituent keys from JK only — taken by the BMJ
      // unfold/seek logic where only the join key is available.
      explicit Key(const q5_sort_key_t& jk)
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

// std::hash<q5_sort_key_t> — used by HashJoin in S4 stage 2 and any
// future hashmap keyed on the sort-key abstraction.  Hashes all three
// fields uniformly; wildcard handling lives in match() / matching_keys()
// only.  HashJoin probe walks matching_keys() to consult each anchor
// bucket explicitly.
template <>
struct hash<tpch::q5::q5_sort_key_t> {
   std::size_t operator()(const tpch::q5::q5_sort_key_t& k) const
   {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
      h ^= std::hash<int>{}(static_cast<int>(k.orderkey))   + 0x9e3779b9u + (h << 6) + (h >> 2);
      h ^= std::hash<int>{}(static_cast<int>(k.linenumber)) + 0x9e3779b9u + (h << 6) + (h >> 2);
      return h;
   }
};

}  // namespace std

// ---------------------------------------------------------------------------
// SKBuilder specialisation for q5_sort_key_t.
//
// Used by BMJ #2 / HJ stage 2 (q5_jr1_t ⋈ lineitem_col_t).  Three concerns:
//
//   - create<R>: extract a sort key from each participating record type.
//     q5_jr1_t (build side) projects linenumber=WILDCARD_KEY because jr1
//     represents a (customer, order) pair with no per-lineitem component.
//     lineitem_col_t (probe side) carries its real linenumber.
//
//   - project<R>: project a full sort key down to record R's natural
//     granularity.  q5_jr1_t lives at orderkey-only granularity within a
//     customer, so project<q5_jr1_t> strips linenumber to WILDCARD_KEY.
//     This is what makes wildcard-keyed BMJ work: join_state's
//     join_and_clear() compares next_jk.match(project<R>(jk_to_join)) per
//     record-type buffer; with linenumber stripped to wildcard, the jr1
//     buffer survives across per-lineitem cartesian flushes within the
//     same orderkey group.  lineitem_col_t lives at full-key granularity
//     so its project is identity.
//
//   - to_key<R>: convert a sort key back to record R's native primary key.

template <>
struct SKBuilder<tpch::q5::q5_sort_key_t> {
   using JK = tpch::q5::q5_sort_key_t;

   // q5_jr1_t (build side): extract custkey from the embedded q5_cust_jk_t,
   // orderkey from the embedded orders_coli_t::Key, project linenumber to
   // WILDCARD_KEY (jr1 has no per-lineitem component).
   static JK create(const tpch::q5::q5_jr1_t::Key& k, const tpch::q5::q5_jr1_t&)
   {
      const auto& ord_key = std::get<1>(k.keys);
      return JK{k.jk.custkey, ord_key.orderkey, WILDCARD_KEY};
   }

   // lineitem_col_t (probe side): forward custkey, orderkey, and the real
   // linenumber from the lineitem's primary key.
   static JK create(const tpch::lineitem_col_t::Key& k, const tpch::lineitem_col_t&)
   {
      return JK{k.custkey, k.orderkey, k.linenumber};
   }

   // q5_pipeline_view_t (S2 view): primary key shape matches the sort key
   // shape; forward all three fields with the lineitem's real linenumber.
   static JK create(const tpch::q5::q5_pipeline_view_t::Key& k,
                    const tpch::q5::q5_pipeline_view_t&)
   {
      return JK{k.custkey, k.orderkey, k.linenumber};
   }

   // project<R>: see header comment.
   template <typename R>
   static JK project(const JK& k);

   // to_key<R>: convert a sort key to record R's native primary key.
   template <typename R>
   static typename R::Key to_key(const JK& k);
};

// project<R> explicit specialisations.
template <>
inline tpch::q5::q5_sort_key_t
SKBuilder<tpch::q5::q5_sort_key_t>::project<tpch::q5::q5_jr1_t>(const tpch::q5::q5_sort_key_t& k)
{
   // jr1 lives at orderkey-only granularity within a customer.
   return tpch::q5::q5_sort_key_t{k.custkey, k.orderkey, WILDCARD_KEY};
}

template <>
inline tpch::q5::q5_sort_key_t
SKBuilder<tpch::q5::q5_sort_key_t>::project<tpch::lineitem_col_t>(const tpch::q5::q5_sort_key_t& k)
{
   return k;
}

template <>
inline tpch::q5::q5_sort_key_t
SKBuilder<tpch::q5::q5_sort_key_t>::project<tpch::q5::q5_pipeline_view_t>(const tpch::q5::q5_sort_key_t& k)
{
   return k;
}

// to_key<R> explicit specialisations.
template <>
inline tpch::q5::q5_jr1_t::Key
SKBuilder<tpch::q5::q5_sort_key_t>::to_key<tpch::q5::q5_jr1_t>(const tpch::q5::q5_sort_key_t& k)
{
   return tpch::q5::q5_jr1_t::Key{tpch::q5::q5_cust_jk_t::Key{k.custkey}};
}

template <>
inline tpch::lineitem_col_t::Key
SKBuilder<tpch::q5::q5_sort_key_t>::to_key<tpch::lineitem_col_t>(const tpch::q5::q5_sort_key_t& k)
{
   return tpch::lineitem_col_t::Key{k.custkey, k.orderkey, k.linenumber};
}

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
