#pragma once

// Per-(custkey, orderkey) lineitem revenue aggregate row.
//
// Output row of LineitemRevenueAggregator: one row per orderkey after
// SUM(l_extendedprice * (1 - l_discount)) over qualifying lineitems.
//
// Shared by Q3 and Q3I: both queries' S1 BMJ chain consumes this same
// aggregate row from a custkey-sorted split lineitem secondary.  Hoisted
// to q3_family so the aggregate's Key shape, match semantics, hash, and
// SKBuilder specialisation live in exactly one place.

#include <functional>
#include <limits>
#include <ostream>

#include "../tpch_tables.hpp"

namespace tpch::q3_family
{

struct lineitem_agg_t {
   static constexpr int id = 45;

   struct Key {
      static constexpr int id = 45;
      Integer custkey;
      Integer orderkey;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)

      // Match on (custkey, orderkey).  The left side of BMJ #N (Q3I = #3,
      // Q3 = #2) carries a custkey-only join key; SKBuilder::create
      // overloads (per-query) derive a lineitem_agg_t::Key from the
      // left-side row, so the two-field key always has both components
      // set at match time.
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

   inline void print(std::ostream& os) const
   {
      os << "lineitem_agg(" << revenue << ")\n";
   }
};

}  // namespace tpch::q3_family

namespace std
{

template <>
struct hash<tpch::q3_family::lineitem_agg_t::Key> {
   std::size_t operator()(const tpch::q3_family::lineitem_agg_t::Key& k) const
   {
      std::size_t h = std::hash<int>{}(static_cast<int>(k.custkey));
      h ^= std::hash<int>{}(static_cast<int>(k.orderkey)) + 0x9e3779b9 + (h << 6) + (h >> 2);
      return h;
   }
};

}  // namespace std
