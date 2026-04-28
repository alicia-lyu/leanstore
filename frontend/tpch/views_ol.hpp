#pragma once

// Shared types for the ORDERS x LINEITEM pipeline used by Q12, Q3, Q9.
//
// All three Tier 1 queries' single MI is MergedIndex(ORDERS, LINEITEM) keyed by
// orderkey. They all produce join results from this MI (structure 3) or an
// intermediate pipeline view materializing the same join (structure 2). The
// downstream operators differ (filter/project/aggregate per query), but the
// join key, the MI shape, and the ORDERS||LINEITEM concatenated record type
// are identical — they belong here, not in any one query directory.

#include <variant>
#include <limits>

#include "../shared/variant_tuple_utils.hpp"
#include "../shared/view_templates.hpp"
#include "tpch_tables.hpp"

namespace tpch
{
// id range for tpch shared / per-query views: 30s+ (geo uses 10s/20s).

// ---------------------------------------------------------------------------
// Sort key over orderkey: the ORDERS x LINEITEM join key.

struct ol_sort_key_t {
   Integer orderkey;
   Integer linenumber;  // 0 for ORDERS-only granularity, >0 for LINEITEM rows

   using Key = ol_sort_key_t;
   ADD_KEY_TRAITS(&ol_sort_key_t::orderkey, &ol_sort_key_t::linenumber)

   auto operator<=>(const ol_sort_key_t&) const = default;

   static ol_sort_key_t max();
   friend int operator%(const ol_sort_key_t& k, const int& n);

   // Subset matching analogous to geo's sort_key_t::matching_keys/match.
   // Used when MergeJoin / PremergedJoin probes by an orderkey-only prefix.
   std::vector<ol_sort_key_t> matching_keys() const;
   int match(const ol_sort_key_t& other) const;
};

}  // namespace tpch

// std::hash specialization for use in HashJoin.
namespace std
{
template <>
struct hash<tpch::ol_sort_key_t> {
   std::size_t operator()(const tpch::ol_sort_key_t& k) const;
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
   joined_ol_t(const orders_t& o, const lineitem_t& l);

   struct Key : public joined_t::Key {
      Key() = default;
      Key(const ol_sort_key_t& jk);
      Key(const orders_t::Key& ok, const lineitem_t::Key& lk);
   };

   // Convenience accessors that hide std::get<orders_t>(payloads).
   const orders_t& order() const;
   const lineitem_t& line() const;
};

}  // namespace tpch

// SKBuilder specialization: how to derive ol_sort_key_t from each
// participating record type. Required by PremergedJoin / BinaryMergeJoin.
template <>
struct SKBuilder<tpch::ol_sort_key_t> {
   static tpch::ol_sort_key_t create(const orders_t::Key& k, const orders_t&);
   static tpch::ol_sort_key_t create(const lineitem_t::Key& k, const lineitem_t&);
   static tpch::ol_sort_key_t create(const tpch::joined_ol_t::Key& k, const tpch::joined_ol_t&);
   static tpch::ol_sort_key_t create(const std::variant<orders_t::Key, lineitem_t::Key>& k,
                                     const std::variant<orders_t, lineitem_t>& v);

   template <typename Record>
   static tpch::ol_sort_key_t get(const tpch::ol_sort_key_t& k);
};
