#pragma once

// Shared types for the ORDERS x LINEITEM pipeline used by Q12, Q3, Q9.
//
// All three Tier 1 queries' single MI is MergedIndex(ORDERS, LINEITEM) keyed by
// orderkey. They all produce join results from this MI (structure 3) or an
// intermediate pipeline view materializing the same join (structure 2). The
// downstream operators differ (filter/project/aggregate per query), but the
// join key, the MI shape, and the ORDERS||LINEITEM concatenated record type
// are identical — they belong here, not in any one query directory.

#include <limits>
#include <tuple>
#include <vector>

#include "../../shared/view_templates.hpp"
#include "../../shared/wildcard_key.hpp"
#include "../tpch_tables.hpp"

namespace tpch
{
// id range for tpch shared / per-query views: 30s+ (geo uses 10s/20s).

// ---------------------------------------------------------------------------
// Sort key over orderkey: the ORDERS x LINEITEM join key.
//
// OLD DESIGN — slated for retirement.  Pre-dates the WILDCARD_KEY /
// wildcard_match convention codified in `frontend/shared/wildcard_key.hpp`.
// Two divergences from the canonical pattern (used by `q5_sort_key_t` and
// `geo::sort_key_t`):
//
//   1. Hash is wildcard-AWARE (orderkey-only) — see operator% below and
//      std::hash<ol_sort_key_t>.  The canonical pattern hashes all fields
//      uniformly and lets `matching_keys()` enumerate the anchor buckets
//      a probe must visit.  The orderkey-only hash works only because no
//      consumer probes with an orderkey-only anchor today.
//
//   2. `matching_keys()` enumerates only the linenumber-wildcard anchor
//      (returns 2 entries).  The canonical pattern walks every prefix
//      anchor up the hierarchy.  Single-anchor enumeration is fine here
//      only because no current consumer needs more.
//
// Kept working as-is — Q12, Q3, Q9 all depend on this.  Retire to the
// canonical pattern the next time a consumer or behaviour forces the
// question (e.g. a join introducing an orderkey-only anchor probe).

struct ol_sort_key_t {
   Integer orderkey;
   Integer linenumber;  // WILDCARD_KEY for ORDERS-only granularity, >0 for LINEITEM rows

   using Key = ol_sort_key_t;
   ADD_KEY_TRAITS(&ol_sort_key_t::orderkey, &ol_sort_key_t::linenumber)

   auto operator<=>(const ol_sort_key_t&) const = default;

   static ol_sort_key_t max()
   {
      return {std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()};
   }

   friend int operator%(const ol_sort_key_t& k, const int& n) { return static_cast<int>(k.orderkey) % n; }

   // Returns all sort keys that should match during a hash probe.
   // An ORDERS row (linenumber == WILDCARD_KEY) matches only itself; a
   // LINEITEM row (linenumber > 0) also needs to match the enclosing
   // order-level key.
   std::vector<ol_sort_key_t> matching_keys() const
   {
      if (linenumber != WILDCARD_KEY) {
         return {{orderkey, WILDCARD_KEY}, *this};
      }
      return {*this};
   }

   // Hierarchical prefix comparison. linenumber == WILDCARD_KEY on either
   // side acts as a prefix wildcard matching any linenumber under the same
   // orderkey — see frontend/shared/wildcard_key.hpp.
   int match(const ol_sort_key_t& other) const
   {
      if (orderkey != other.orderkey)
         return static_cast<int>(orderkey) - static_cast<int>(other.orderkey);
      return wildcard_match(linenumber, other.linenumber);
   }

   // Returns the index of the first differing field, the other's value at
   // that field, and the signed difference (this - other). Used by
   // PremergedJoin::distance to compute seek distances.
   std::tuple<int, int, int> first_diff(const ol_sort_key_t& other) const
   {
      if (orderkey != other.orderkey)
         return {0, static_cast<int>(other.orderkey), static_cast<int>(orderkey) - static_cast<int>(other.orderkey)};
      if (linenumber != other.linenumber)
         return {1, static_cast<int>(other.linenumber), static_cast<int>(linenumber) - static_cast<int>(other.linenumber)};
      return {1, static_cast<int>(other.linenumber), 0};
   }

   // operator<< is provided by ADD_KEY_TRAITS above.
};

}  // namespace tpch

// std::hash specialization for use in HashJoin.  Hashes on orderkey only
// so order-level (linenumber=WILDCARD_KEY) and lineitem-level keys collide
// into the same bucket — the wildcard-AWARE hash divergence flagged in the
// OLD DESIGN note above ol_sort_key_t.
namespace std
{
template <>
struct hash<tpch::ol_sort_key_t> {
   std::size_t operator()(const tpch::ol_sort_key_t& k) const
   {
      return std::hash<int>{}(static_cast<int>(k.orderkey));
   }
};
}  // namespace std

namespace tpch
{

// ---------------------------------------------------------------------------
// joined_ol_t: ORDERS || LINEITEM concatenated record. Required by every
// merge-join template in frontend/shared/merge-join/ — these all need a
// JR type with JR::Key(Rs::Key...) and JR(Rs...) constructors. Reuses the
// generic `joined_t` template from view_templates.hpp.
//
// Structure 2 (intermediate pipeline view) for Q12/Q3/Q9 stores rows of this
// type. Structure 3 (PremergedJoin) emits rows of this type at query time.

struct joined_ol_t : public joined_t<30, ol_sort_key_t, false, orders_t, lineitem_t> {
   joined_ol_t() = default;

   joined_ol_t(const orders_t& o, const lineitem_t& l) : joined_t(o, l) {}

   struct Key : public joined_t::Key {
      Key() = default;

      // Reconstruct constituent keys from the join key (fold_pks=false path).
      explicit Key(const ol_sort_key_t& jk)
          : joined_t::Key(jk, orders_t::Key{jk.orderkey}, lineitem_t::Key{jk.orderkey, jk.linenumber})
      {
      }

      // Build the join key from the most-specific constituent key (lineitem).
      Key(const orders_t::Key& ok, const lineitem_t::Key& lk)
          : joined_t::Key(ol_sort_key_t{lk.l_orderkey, lk.l_linenumber}, ok, lk)
      {
      }
   };

   // Convenience accessors that hide std::get<N>(payloads).
   const orders_t& order() const { return std::get<0>(payloads); }
   const lineitem_t& line() const { return std::get<1>(payloads); }

   // Override the generic unfoldKey: the base joined_t::unfoldKeyHelper for
   // fold_pks=false tries `typename Ts::Key{key.jk}` which would require
   // orders_t::Key{ol_sort_key_t} and lineitem_t::Key{ol_sort_key_t} — those
   // constructors don't exist. Instead we reconstruct via the custom
   // joined_ol_t::Key(ol_sort_key_t) ctor that knows how to split the sort key.
   template <typename K>
   static unsigned unfoldKey(const uint8_t* in, K& key)
   {
      ol_sort_key_t jk;
      unsigned read = ol_sort_key_t::keyunfold(in, jk);
      key = K(jk);
      return read;
   }
};

}  // namespace tpch

// SortKeyFor specializations: register orders_t and lineitem_t as participants
// in the ol_sort_key_t join key. This enables the default SKMatcher<R1, R2>
// specialization (HasSharedSKBuilder) to resolve automatically for these types.
template <>
struct SortKeyFor<orders_t> {
   using type = tpch::ol_sort_key_t;
};

template <>
struct SortKeyFor<lineitem_t> {
   using type = tpch::ol_sort_key_t;
};

// SKBuilder specialization: how to derive ol_sort_key_t from each
// participating record type. Required by PremergedJoin / BinaryMergeJoin.
template <>
struct SKBuilder<tpch::ol_sort_key_t> {
   static tpch::ol_sort_key_t create(const orders_t::Key& k, const orders_t&)
   {
      return {k.o_orderkey, Integer(0)};
   }

   static tpch::ol_sort_key_t create(const lineitem_t::Key& k, const lineitem_t&)
   {
      return {k.l_orderkey, k.l_linenumber};
   }

   static tpch::ol_sort_key_t create(const tpch::joined_ol_t::Key& k, const tpch::joined_ol_t&)
   {
      return k.jk;
   }

   // project<R>: project a full join key down to the granularity of record R.
   // ORDERS rows are keyed at orderkey-only (linenumber=0); LINEITEM rows keep
   // the full (orderkey, linenumber) key.
   template <typename Record>
   static tpch::ol_sort_key_t project(const tpch::ol_sort_key_t& k);

   // to_key<R>: convert a sort key to the native primary key of record R.
   template <typename R>
   static typename R::Key to_key(const tpch::ol_sort_key_t& sk);
};

// --- project<R> explicit specializations ---

template <>
inline tpch::ol_sort_key_t SKBuilder<tpch::ol_sort_key_t>::project<orders_t>(const tpch::ol_sort_key_t& jk)
{
   return {jk.orderkey, Integer(0)};
}

template <>
inline tpch::ol_sort_key_t SKBuilder<tpch::ol_sort_key_t>::project<lineitem_t>(const tpch::ol_sort_key_t& jk)
{
   return jk;
}

// --- to_key<R> explicit specializations ---

template <>
inline orders_t::Key SKBuilder<tpch::ol_sort_key_t>::to_key<orders_t>(const tpch::ol_sort_key_t& sk)
{
   return orders_t::Key{sk.orderkey};
}

template <>
inline lineitem_t::Key SKBuilder<tpch::ol_sort_key_t>::to_key<lineitem_t>(const tpch::ol_sort_key_t& sk)
{
   return lineitem_t::Key{sk.orderkey, sk.linenumber};
}

template <>
inline tpch::joined_ol_t::Key SKBuilder<tpch::ol_sort_key_t>::to_key<tpch::joined_ol_t>(const tpch::ol_sort_key_t& sk)
{
   return tpch::joined_ol_t::Key{sk};
}
