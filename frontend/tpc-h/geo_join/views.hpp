#pragma once

#include "../tables.hpp"
#include "../view_templates.hpp"

// id range: 10s + 20s (only one such namespace are included in each executable)

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
   static constexpr int id = 11;
   region2_t() = default;
   region2_t(const Varchar<25>& r_name, const Varchar<152>& r_comment) : r_name(r_name), r_comment(r_comment) {}
   region2_t(const region_t& r) : r_name(r.r_name), r_comment(r.r_comment) {}
   struct Key {
      static constexpr int id = 11;
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
   static constexpr int id = 12;
   nation2_t() = default;
   nation2_t(const Varchar<25>& n_name, const Varchar<152>& n_comment, Integer last_statekey)
       : n_name(n_name), n_comment(n_comment), last_statekey(last_statekey)
   {
   }
   nation2_t(const nation_t& n) : n_name(n.n_name), n_comment(n.n_comment), last_statekey(0) {}
   struct Key {
      static constexpr int id = 12;
      Integer n_regionkey;
      Integer n_nationkey;
      ADD_KEY_TRAITS(&Key::n_regionkey, &Key::n_nationkey)

      sort_key_t get_jk() const { return sort_key_t{n_regionkey, n_nationkey, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> n_name;
   Varchar<152> n_comment;
   Integer last_statekey;

   ADD_RECORD_TRAITS(nation2_t)

   static nation2_t generateRandomRecord() { return nation2_t{randomastring<25>(1, 25), randomastring<152>(0, 152), 0}; }
};

struct rn_t {
   static constexpr int id = 21;

   rn_t() = default;
   rn_t(const Varchar<25>& r_name, const Varchar<25>& n_name, const Varchar<152>& r_comment, const Varchar<152>& n_comment, Integer last_statekey)
       : region_name(r_name), nation_name(n_name), region_comment(r_comment), nation_comment(n_comment), last_statekey(last_statekey)
   {
   }

   rn_t(const region2_t& r, const nation2_t& n)
       : region_name(r.r_name), nation_name(n.n_name), region_comment(r.r_comment), nation_comment(n.n_comment), last_statekey(n.last_statekey)
   {
   }

   struct Key {
      static constexpr int id = 21;
      Integer regionkey;
      Integer nationkey;
      ADD_KEY_TRAITS(&Key::regionkey, &Key::nationkey)

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> region_name;
   Varchar<25> nation_name;
   Varchar<152> region_comment;
   Varchar<152> nation_comment;
   Integer last_statekey;
   ADD_RECORD_TRAITS(rn_t)

   static rn_t generateRandomRecord() { return rn_t{region2_t::generateRandomRecord(), nation2_t::generateRandomRecord()}; }
};

struct states_t {
   static constexpr int id = 13;
   struct Key {
      static constexpr int id = 14;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey)

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, 0, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   Integer last_countykey;
   ADD_RECORD_TRAITS(states_t)

   static states_t generateRandomRecord() { return states_t{randomastring<25>(1, 25), randomastring<152>(0, 152), 0}; }
};

struct rns_t {
   static constexpr int id = 22;

   rns_t() = default;
   rns_t(const Varchar<25>& r_name,
         const Varchar<25>& n_name,
         const Varchar<25>& s_name,
         const Varchar<152>& r_comment,
         const Varchar<152>& n_comment,
         const Varchar<152>& s_comment,
         Integer last_statekey,
         Integer last_countykey)
       : region_name(r_name),
         nation_name(n_name),
         state_name(s_name),
         region_comment(r_comment),
         nation_comment(n_comment),
         state_comment(s_comment),
         last_statekey(last_statekey),
         last_countykey(last_countykey)
   {
   }

   rns_t(const rn_t& rn, const states_t& s)
       : region_name(rn.region_name),
         nation_name(rn.nation_name),
         state_name(s.name),
         region_comment(rn.region_comment),
         nation_comment(rn.nation_comment),
         state_comment(s.comment),
         last_statekey(rn.last_statekey),
         last_countykey(s.last_countykey)
   {
   }

   struct Key {
      static constexpr int id = 22;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::regionkey, &Key::nationkey, &Key::statekey)

      Key() = default;
      Key(Integer r, Integer n, Integer s) : regionkey(r), nationkey(n), statekey(s) {}
      Key(const rn_t::Key& rnk, const states_t::Key& sk) : regionkey(rnk.regionkey), nationkey(rnk.nationkey), statekey(sk.statekey) {}

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> region_name;
   Varchar<25> nation_name;
   Varchar<25> state_name;
   Varchar<152> region_comment;
   Varchar<152> nation_comment;
   Varchar<152> state_comment;
   Integer last_statekey;
   Integer last_countykey;
   ADD_RECORD_TRAITS(rns_t)
   static rns_t generateRandomRecord() { return rns_t{rn_t::generateRandomRecord(), states_t::generateRandomRecord()}; }
};

struct county_t {
   static constexpr int id = 15;
   struct Key {
      static constexpr int id = 15;
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
   Integer last_citykey;
   ADD_RECORD_TRAITS(county_t)

   static county_t generateRandomRecord() { return county_t{randomastring<25>(1, 25), randomastring<152>(0, 152), 0}; }
};

struct rnsc_t {
   static constexpr int id = 23;

   rnsc_t() = default;
   rnsc_t(const Varchar<25>& r_name,
          const Varchar<25>& n_name,
          const Varchar<25>& s_name,
          const Varchar<25>& c_name,
          const Varchar<152>& r_comment,
          const Varchar<152>& n_comment,
          const Varchar<152>& s_comment,
          const Varchar<152>& c_comment,
          Integer last_statekey,
          Integer last_countykey,
          Integer last_citykey)
       : region_name(r_name),
         nation_name(n_name),
         state_name(s_name),
         county_name(c_name),
         region_comment(r_comment),
         nation_comment(n_comment),
         state_comment(s_comment),
         county_comment(c_comment),
         last_statekey(last_statekey),
         last_countykey(last_countykey),
         last_citykey(last_citykey)
   {
   }

   rnsc_t(const rns_t& rns, const county_t& c)
       : region_name(rns.region_name),
         nation_name(rns.nation_name),
         state_name(rns.state_name),
         county_name(c.name),
         region_comment(rns.region_comment),
         nation_comment(rns.nation_comment),
         state_comment(rns.state_comment),
         county_comment(c.comment)
   {
   }

   struct Key {
      static constexpr int id = 23;
      Integer regionkey;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      ADD_KEY_TRAITS(&Key::regionkey, &Key::nationkey, &Key::statekey, &Key::countykey)

      Key() = default;
      Key(Integer r, Integer n, Integer s, Integer c) : regionkey(r), nationkey(n), statekey(s), countykey(c) {}
      Key(const rns_t::Key& rnsk, const county_t::Key& ck)
          : regionkey(rnsk.regionkey), nationkey(rnsk.nationkey), statekey(rnsk.statekey), countykey(ck.countykey)
      {
      }

      sort_key_t get_jk() const { return sort_key_t{regionkey, nationkey, statekey, countykey, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> region_name;
   Varchar<25> nation_name;
   Varchar<25> state_name;
   Varchar<25> county_name;
   Varchar<152> region_comment;
   Varchar<152> nation_comment;
   Varchar<152> state_comment;
   Varchar<152> county_comment;
   Integer last_statekey;
   Integer last_countykey;
   Integer last_citykey;
   ADD_RECORD_TRAITS(rnsc_t)

   static rnsc_t generateRandomRecord() { return rnsc_t{rns_t::generateRandomRecord(), county_t::generateRandomRecord()}; }
};

struct city_t {
   static constexpr int id = 16;
   struct Key {
      static constexpr int id = 16;
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

struct view_t : public joined_t<15, sort_key_t, region2_t, nation2_t, states_t, county_t, city_t> {
   view_t() = default;
   view_t(const rnsc_t& rns, const city_t& c)
       : joined_t{region2_t{rns.region_name, rns.region_comment}, nation2_t{rns.nation_name, rns.nation_comment, rns.last_statekey},
                  states_t{rns.state_name, rns.state_comment, rns.last_countykey}, county_t{rns.county_name, rns.county_comment, rns.last_citykey},
                  city_t{c.name, c.comment}}
   {
   }
   template <typename... Args>
   explicit view_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const rnsc_t::Key& rnsk, const city_t::Key& ck)
          : joined_t::Key(sort_key_t{rnsk.regionkey, rnsk.nationkey, rnsk.statekey, rnsk.countykey, ck.citykey})
      {
      }

      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key(std::forward<Args>(args)...)
      {
      }
   };
};

}  // namespace geo_join