#pragma once

#include "../tables.hpp"
#include "../view_templates.hpp"

// id range: 10s + 20s (only one such namespace are included in each executable)

namespace geo_join
{
using namespace randutils;
struct sort_key_t {
   Integer nationkey;
   Integer statekey;
   Integer countykey;
   Integer citykey;
   using Key = sort_key_t;
   ADD_KEY_TRAITS(&sort_key_t::nationkey, &sort_key_t::statekey, &sort_key_t::countykey, &sort_key_t::citykey)
   auto operator<=>(const sort_key_t&) const = default;

   static sort_key_t max()
   {
      return sort_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
                        std::numeric_limits<Integer>::max()};
   }

   friend int operator%(const sort_key_t& jk, const int& n) { return (jk.nationkey + jk.statekey + jk.countykey + jk.citykey) % n; }

   int match(const sort_key_t& other) const
   {
      // {0, 0, 0, 0, 0} cannot be used as wildcard
      if (*this == sort_key_t{} && other == sort_key_t{})
         return 0;
      else if (*this == sort_key_t{})
         return -1;
      else if (other == sort_key_t{})
         return 1;

      if (nationkey != 0 && other.nationkey != 0 && nationkey != other.nationkey)
         return nationkey - other.nationkey;
      if (statekey != 0 && other.statekey != 0 && statekey != other.statekey)
         return statekey - other.statekey;
      if (countykey != 0 && other.countykey != 0 && countykey != other.countykey)
         return countykey - other.countykey;
      if (citykey != 0 && other.citykey != 0 && citykey != other.citykey)
         return citykey - other.citykey;
      return 0;
   }
};

struct nation2_t {
   static constexpr int id = 12;
   nation2_t() = default;
   nation2_t(const Varchar<25>& n_name, const Varchar<152>& n_comment, Integer last_statekey)
       : n_name(n_name), n_comment(n_comment), last_statekey(last_statekey)
   {
   }
   nation2_t(const nation_t& n) : n_name(n.n_name), n_comment(n.n_comment), last_statekey(0) {}
   struct Key {
      static constexpr int id = 12;
      Integer nationkey;
      ADD_KEY_TRAITS(&Key::nationkey)

      sort_key_t get_jk() const { return sort_key_t{nationkey, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> n_name;
   Varchar<152> n_comment;
   Integer last_statekey;

   ADD_RECORD_TRAITS(nation2_t)

   static nation2_t generateRandomRecord(int state_cnt) { return nation2_t{randomastring<25>(1, 25), randomastring<152>(0, 152), state_cnt}; }
};

struct states_t {
   static constexpr int id = 13;
   struct Key {
      static constexpr int id = 14;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey)

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, 0, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   Integer last_countykey;
   ADD_RECORD_TRAITS(states_t)

   static states_t generateRandomRecord(int county_cnt) { return states_t{randomastring<25>(1, 25), randomastring<152>(0, 152), county_cnt}; }
};

struct ns_t : public joined_t<21, sort_key_t, nation2_t, states_t> {
   template <typename... Args>
   explicit ns_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() : joined_t::Key() {}
      Key(const nation2_t::Key& nk, const states_t::Key& sk) : joined_t::Key{sort_key_t{nk.nationkey, sk.statekey, 0, 0}, std::make_tuple(nk, sk)} {}

      std::tuple<nation2_t::Key, states_t::Key> flatten() const { return keys; }
   };

   std::tuple<nation2_t, states_t> flatten() const { return payloads; }
};

struct county_t {
   static constexpr int id = 15;
   struct Key {
      static constexpr int id = 15;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey)

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   Integer last_citykey;
   ADD_RECORD_TRAITS(county_t)

   static county_t generateRandomRecord(int city_cnt) { return county_t{randomastring<25>(1, 25), randomastring<152>(0, 152), city_cnt}; }
};

struct nsc_t : public joined_t<20, sort_key_t, ns_t, county_t> {
   template <typename... Args>
   explicit nsc_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const ns_t::Key& nsk, const county_t::Key& ck)
          : joined_t::Key{sort_key_t{nsk.jk.nationkey, nsk.jk.statekey, ck.countykey, 0}, std::make_tuple(nsk, ck)}
      {
      }

      std::tuple<nation2_t::Key, states_t::Key, county_t::Key> flatten() const
      {
         return std::tuple_cat(std::get<0>(keys).flatten(), std::make_tuple(std::get<1>(keys)));
      }
   };

   std::tuple<nation2_t, states_t, county_t> flatten() const
   {
      std::tuple<nation2_t, states_t> ns_flattened = std::get<0>(payloads).flatten();
      return std::tuple_cat(ns_flattened, std::make_tuple(std::get<1>(payloads)));
   }
};

struct city_t {
   static constexpr int id = 16;
   struct Key {
      static constexpr int id = 16;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      Integer citykey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey, &Key::citykey)

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, citykey}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(city_t)

   static city_t generateRandomRecord() { return city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

template <int id>
struct group_key_t {
   Integer nationkey;
   Integer statekey;
   Integer countykey;
   using Key = group_key_t<id>;
   ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey)

   group_key_t() = default;
   group_key_t(Integer n, Integer s, Integer c) : nationkey(n), statekey(s), countykey(c) {}
   group_key_t(const county_t::Key& ck) : nationkey(ck.nationkey), statekey(ck.statekey), countykey(ck.countykey) {}
   group_key_t(const city_t::Key& cik) : nationkey(cik.nationkey), statekey(cik.statekey), countykey(cik.countykey) {}
   group_key_t(const county_t::Key& ck, const group_key_t<17>&) : nationkey(ck.nationkey), statekey(ck.statekey), countykey(ck.countykey) {}

   auto operator<=>(const Key&) const = default;

   group_key_t get_jk() const { return *this; }
   Key get_pk() const { return *this; }

   static group_key_t max()
   {
      return group_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()};
   }

   friend int operator%(const group_key_t& jk, const int& n) { return (jk.nationkey + jk.statekey + jk.countykey) % n; }

   int match(const group_key_t& other) const
   {
      if (nationkey != 0 && other.nationkey != 0 && nationkey != other.nationkey)
         return nationkey - other.nationkey;
      if (statekey != 0 && other.statekey != 0 && statekey != other.statekey)
         return statekey - other.statekey;
      if (countykey != 0 && other.countykey != 0 && countykey != other.countykey)
         return countykey - other.countykey;
      return 0;
   }
};

struct city_count_per_county_t {
   static constexpr int id = 17;

   Integer city_count;

   using Key = group_key_t<17>;

   ADD_RECORD_TRAITS(city_count_per_county_t)
};

struct mixed_view_t {
   static constexpr int id = 18;

   Varchar<25> name;
   Integer city_count;

   mixed_view_t() = default;

   mixed_view_t(const Varchar<25>& n_name, Integer city_count) : name(n_name), city_count(city_count) {}

   mixed_view_t(const county_t& c, const city_count_per_county_t& cc) : name(c.name), city_count(cc.city_count) {}

   using Key = group_key_t<18>;
};

struct view_t : public joined_t<15, sort_key_t, nation2_t, states_t, county_t, city_t> {
   view_t() = default;
   view_t(const nsc_t& nsc, const city_t& c) : joined_t{std::tuple_cat(nsc.flatten(), std::make_tuple(c))} {}
   // template <typename... Args>
   // explicit view_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   // {
   // }
   view_t(const nation2_t& n, const states_t& s, const county_t& c, const city_t& ci) : joined_t{std::make_tuple(n, s, c, ci)} {}
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const nation2_t::Key& nk, const states_t::Key& sk, const county_t::Key& ck, const city_t::Key& ci)
          : joined_t::Key{sort_key_t{nk.nationkey, sk.statekey, ck.countykey, ci.citykey}, std::make_tuple(nk, sk, ck, ci)}
      {
      }

      Key(const nsc_t::Key& nsck, const city_t::Key& ck)
          : joined_t::Key{sort_key_t{nsck.jk.nationkey, nsck.jk.statekey, nsck.jk.countykey, ck.citykey},
                          std::tuple_cat(nsck.flatten(), std::make_tuple(ck))}
      {
      }

      Key(int n, int s, int c, int ci)
          : joined_t::Key{sort_key_t{n, s, c, ci}, nation2_t::Key{n}, states_t::Key{n, s}, county_t::Key{n, s, c}, city_t::Key{n, s, c, ci}}
      {
      }

      Key(const sort_key_t& jk)
          : joined_t::Key{jk, nation2_t::Key{jk.nationkey}, states_t::Key{jk.nationkey, jk.statekey},
                          county_t::Key{jk.nationkey, jk.statekey, jk.countykey}, city_t::Key{jk.nationkey, jk.statekey, jk.countykey, jk.citykey}}
      {
      }
   };

   static view_t generateRandomRecord(int state_cnt, int county_cnt, int city_cnt)
   {
      return view_t{nation2_t::generateRandomRecord(state_cnt), states_t::generateRandomRecord(county_cnt), county_t::generateRandomRecord(city_cnt),
                    city_t::generateRandomRecord()};
   }
};

}  // namespace geo_join

using namespace geo_join;

template <>
struct SKBuilder<sort_key_t> {
   static sort_key_t inline create(const nation2_t::Key& k, const nation2_t&) { return sort_key_t{k.nationkey, 0, 0, 0}; }
   static sort_key_t inline create(const states_t::Key& k, const states_t&) { return sort_key_t{k.nationkey, k.statekey, 0, 0}; }
   static sort_key_t inline create(const ns_t::Key& k, const ns_t&) { return k.jk; }
   static sort_key_t inline create(const county_t::Key& k, const county_t&) { return sort_key_t{k.nationkey, k.statekey, k.countykey, 0}; }
   static sort_key_t inline create(const nsc_t::Key& k, const nsc_t&) { return k.jk; }
   static sort_key_t inline create(const city_t::Key& k, const city_t&) { return sort_key_t{k.nationkey, k.statekey, k.countykey, k.citykey}; }
   static sort_key_t inline create(const view_t::Key& k, const view_t&) { return k.jk; }

   template <typename Record>
   static sort_key_t inline get(const sort_key_t& k)
   {
      return k;
   }
};

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<nation2_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, 0, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<states_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<county_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, 0};
}