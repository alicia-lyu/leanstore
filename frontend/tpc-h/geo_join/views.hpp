#pragma once

#include "../view_templates.hpp"
#include "../tables.hpp"

// id range: 10s (only one such namespace are included in each executable)

namespace geo_join
{
using namespace randutils;
struct sort_key_t {
   Integer regionkey;
   Integer nationkey;
   Integer statekey;
   Integer countykey;
   Integer citykey;
   using Key = sort_key_t;
   ADD_KEY_TRAITS(&sort_key_t::regionkey, &sort_key_t::nationkey, &sort_key_t::statekey, &sort_key_t::countykey, &sort_key_t::citykey)
   auto operator<=>(const sort_key_t&) const = default;

   static sort_key_t max()
   {
      return sort_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
                        std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()};
   }

   friend int operator%(const sort_key_t& jk, const int& n) { return (jk.regionkey + jk.nationkey + jk.statekey + jk.countykey + jk.citykey) % n; }

   int match(const sort_key_t& other) const
   {
      // {0, 0, 0, 0, 0} cannot be used as wildcard
      if (*this == sort_key_t{} && other == sort_key_t{})
         return 0;
      else if (*this == sort_key_t{})
         return -1;
      else if (other == sort_key_t{})
         return 1;

      if (regionkey != 0 && other.regionkey != 0 && regionkey != other.regionkey)
         return regionkey - other.regionkey;
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

struct region2_t {
   static constexpr int id = 7;
   region2_t() = default;
   region2_t(const Varchar<25>& r_name, const Varchar<152>& r_comment) : r_name(r_name), r_comment(r_comment) {}
   region2_t(const region_t& r) : r_name(r.r_name), r_comment(r.r_comment) {}
   struct Key {
      static constexpr int id = 7;
      Integer r_regionkey;
      ADD_KEY_TRAITS(&Key::r_regionkey)
      sort_key_t get_jk() const { return sort_key_t{r_regionkey, 0, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> r_name;
   Varchar<152> r_comment;

   ADD_RECORD_TRAITS(region2_t)

   static region2_t generateRandomRecord() { return region2_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct nation2_t {
   static constexpr int id = 6;
   nation2_t() = default;
   nation2_t(const Varchar<25>& n_name, const Varchar<152>& n_comment) : n_name(n_name), n_comment(n_comment) {}
   nation2_t(const nation_t& n) : n_name(n.n_name), n_comment(n.n_comment) {}
   struct Key {
      static constexpr int id = 6;
      Integer n_regionkey;
      Integer n_nationkey;
      ADD_KEY_TRAITS(&Key::n_regionkey, &Key::n_nationkey)

      sort_key_t get_jk() const { return sort_key_t{n_regionkey, n_nationkey, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> n_name;
   Varchar<152> n_comment;

   ADD_RECORD_TRAITS(nation2_t)

   static nation2_t generateRandomRecord()
   {
      return nation2_t{randomastring<25>(1, 25), randomastring<152>(0, 152)};
   }
};

struct states_t {
   static constexpr int id = 11;
   struct Key {
      static constexpr int id = 11;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey)

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, 0, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(states_t)

   static states_t generateRandomRecord() { return states_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct county_t {
   static constexpr int id = 12;
   struct Key {
      static constexpr int id = 12;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      ADD_KEY_TRAITS(&Key::statekey, &Key::countykey)

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, countykey, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(county_t)

   static county_t generateRandomRecord() { return county_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct city_t {
   static constexpr int id = 13;
   struct Key {
      static constexpr int id = 13;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      Integer citykey;
      ADD_KEY_TRAITS(&Key::countykey, &Key::citykey)

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, countykey, citykey}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(city_t)

   static city_t generateRandomRecord() { return city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct view_t : public joined_t<15, sort_key_t, region_t, nation_t, states_t, county_t, city_t> {
   view_t() = default;
   template <typename... Args>
   explicit view_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const sort_key_t& jk,
          const region_t::Key& rk,
          const nation_t::Key& nk,
          const states_t::Key& sk,
          const county_t::Key& ck,
          const city_t::Key& cik)
          : joined_t::Key{jk, std::make_tuple(rk, nk, sk, ck, cik)}
      {
      }

      Key(const sort_key_t& jk)
          : joined_t::Key{jk, std::make_tuple(region_t::Key{jk.regionkey},
                                              nation_t::Key{jk.regionkey, jk.nationkey},
                                              states_t::Key{jk.regionkey, jk.nationkey, jk.statekey},
                                              county_t::Key{jk.regionkey, jk.nationkey, jk.statekey, jk.countykey},
                                              city_t::Key{jk.regionkey, jk.nationkey, jk.statekey, jk.countykey, jk.citykey})}
      {
      }
   };
};

}  // namespace geo_join