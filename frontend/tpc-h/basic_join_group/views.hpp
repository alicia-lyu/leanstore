#pragma once
// SELECT o.orderkey, o.orderdate, COUNT(*)
// FROM Orders o, Lineitem l
// WHERE o.orderkey = l.orderkey
// GROUP BY o.orderkey, o.orderdate;

#include "../randutils.hpp"
#include "../tables.hpp"
#include "../view_templates.hpp"

// id range: 30s

namespace basic_join_group
{
struct sort_key_t {
   Integer orderkey;
   Timestamp orderdate;
   using Key = sort_key_t;
   ADD_KEY_TRAITS(&sort_key_t::orderkey, &sort_key_t::orderdate)

   auto operator<=>(const sort_key_t&) const = default;

   static sort_key_t max() { return sort_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Timestamp>::max()}; }

   friend int operator%(const sort_key_t& jk, const int& n) { return (jk.orderkey + jk.orderdate) % n; }

   int match(const sort_key_t& other) const
   {
      // {0, 0} cannot be used as wildcard
      if (*this == sort_key_t{} && other == sort_key_t{})
         return 0;
      else if (*this == sort_key_t{})
         return -1;
      else if (other == sort_key_t{})
         return 1;

      if (orderkey != 0 && other.orderkey != 0 && orderkey != other.orderkey)
         return orderkey - other.orderkey;
      if (orderdate != 0 && other.orderdate != 0)
         return orderdate - other.orderdate;
      return 0;
   }
};

struct view_t {
   static constexpr int id = 30;
   struct Key {
      static constexpr int id = 30;
      Integer o_orderkey;
      Timestamp o_orderdate;
      ADD_KEY_TRAITS(&Key::o_orderkey, &Key::o_orderdate)
   };

   Integer count_lineitem;

   ADD_RECORD_TRAITS(view_t)

   static view_t generateRandomRecord() { return view_t({randutils::urand(1, 10000)}); }
};

struct merged_view_t : public merged_t<31, view_t, sort_key_t, ExtraID::PKID> {
   merged_view_t() = default;
   template <typename... Args>
   explicit merged_view_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer orderkey, Timestamp orderdate) : merged_t::Key{sort_key_t{orderkey, orderdate}, view_t::Key{orderkey, orderdate}} {}
      Key(const view_t::Key& pk) : merged_t::Key{sort_key_t{pk.o_orderkey, pk.o_orderdate}, pk} {}
      Key(const sort_key_t& sk, const view_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, view_t::Key{sk.orderkey, sk.orderdate}} {}
      Key(const merged_t::Key& k) : merged_t::Key{k.jk, view_t::Key{k.jk.orderkey, k.jk.orderdate}} {}
   };
};

struct merged_orders_t : public merged_t<31, orders_t, sort_key_t, ExtraID::PKID> {
   merged_orders_t() = default;
   template <typename... Args>
   explicit merged_orders_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer orderkey, Timestamp orderdate) : merged_t::Key{sort_key_t{orderkey, orderdate}, orders_t::Key{orderkey}} {}
      Key(const sort_key_t& sk, const orders_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const orders_t::Key& pk) : merged_t::Key{sort_key_t{pk.o_orderkey, Timestamp{0}}, pk} {}
      Key(const merged_t::Key& k) : merged_t::Key{k.jk, orders_t::Key{k.jk.orderkey}} {}
   };
};

struct merged_lineitem_t : public merged_t<31, lineitem_t, sort_key_t, ExtraID::PK> {
   merged_lineitem_t() = default;
   template <typename... Args>
   explicit merged_lineitem_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer orderkey, Timestamp orderdate) : merged_t::Key{sort_key_t{orderkey, orderdate}, lineitem_t::Key{orderkey, 1}} {}
      Key(const sort_key_t& sk, const lineitem_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const lineitem_t::Key& pk) : merged_t::Key{sort_key_t{pk.l_orderkey, Timestamp{0}}, pk} {}
   };
};

}  // namespace basic_join_group

using namespace basic_join_group;

template <>
struct SKBuilder<sort_key_t> {
   static sort_key_t create(const orders_t::Key& k, const orders_t& v) { return sort_key_t{k.o_orderkey, v.o_orderdate}; }

   static sort_key_t create(const lineitem_t::Key& k, const lineitem_t&) { return sort_key_t{k.l_orderkey, Timestamp{0}}; }

   static sort_key_t create(const view_t::Key& k, const view_t&) { return sort_key_t{k.o_orderkey, k.o_orderdate}; }

   static sort_key_t create(const merged_orders_t::Key& k, const merged_orders_t&) { return k.jk; }

   static sort_key_t create(const merged_lineitem_t::Key& k, const merged_lineitem_t&) { return k.jk; }
};