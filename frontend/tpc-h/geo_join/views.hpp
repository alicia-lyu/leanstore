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
      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key(std::forward<Args>(args)...)
      {
      }

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
      ADD_KEY_TRAITS(&Key::statekey, &Key::countykey)

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
      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key(std::forward<Args>(args)...)
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
      ADD_KEY_TRAITS(&Key::countykey, &Key::citykey)

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, citykey}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(city_t)

   static city_t generateRandomRecord() { return city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct view_t : public joined_t<15, sort_key_t, nation2_t, states_t, county_t, city_t> {
   view_t() = default;
   view_t(const nsc_t& nsc, const city_t& c) : joined_t{std::tuple_cat(nsc.flatten(), std::make_tuple(c))} {}
   template <typename... Args>
   explicit view_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const nsc_t::Key& nsck, const city_t::Key& ck)
          : joined_t::Key(sort_key_t{nsck.jk.nationkey, nsck.jk.statekey, nsck.jk.countykey, ck.citykey},
                          std::tuple_cat(nsck.flatten(), std::make_tuple(ck)))
      {
      }

      Key(int n, int s, int c, int ci)
          : joined_t::Key(sort_key_t{n, s, c, ci}, nation2_t::Key{n}, states_t::Key{n, s}, county_t::Key{n, s, c}, city_t::Key{n, s, c, ci})
      {
      }

      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key(std::forward<Args>(args)...)
      {
      }
   };

   static view_t generateRandomRecord(int state_cnt, int county_cnt, int city_cnt)
   {
      return view_t{nation2_t::generateRandomRecord(state_cnt), states_t::generateRandomRecord(county_cnt),
                    county_t::generateRandomRecord(city_cnt), city_t::generateRandomRecord()};
   }
};

}  // namespace geo_join