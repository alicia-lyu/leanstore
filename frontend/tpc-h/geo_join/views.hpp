#pragma once

#include "../tables.hpp"
#include "../view_templates.hpp"

// id range: 10s (only one such namespace are included in each executable)

namespace geo_join
{

struct states_t {
   static constexpr int id = 11;
   struct Key {
      static constexpr int id = 11;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey)
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
      Integer statekey;
      Integer countykey;
      ADD_KEY_TRAITS(&Key::statekey, &Key::countykey)
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
      Integer countykey;
      Integer citykey;
      ADD_KEY_TRAITS(&Key::countykey, &Key::citykey)
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(city_t)

   static city_t generateRandomRecord() { return city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

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

struct merged_region_t : public merged_t<14, region_t, sort_key_t, ExtraID::NONE> {
   merged_region_t() = default;
   template <typename... Args>
   explicit merged_region_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer regionkey) : merged_t::Key{sort_key_t{regionkey, 0, 0, 0, 0}, region_t::Key{regionkey}} {}
      Key(const sort_key_t& sk, const region_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, region_t::Key{sk.regionkey}} {}
   };

   static unsigned foldKey(uint8_t* out, const Key& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.jk.regionkey);
      return pos;
   }

   static unsigned unfoldKey(const uint8_t* in, Key& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.jk.regionkey);
      key.jk.nationkey = 0;
      key.jk.statekey = 0;
      key.jk.countykey = 0;
      key.jk.citykey = 0;
      key.pk.r_regionkey = key.jk.regionkey;
      return pos;
   }

   static unsigned maxFoldLength() { return sizeof(Integer); }
};

struct merged_nation_t : public merged_t<15, nation_t, sort_key_t, ExtraID::NONE> {
   merged_nation_t() = default;
   // also serves as joined result of region and nation
   merged_nation_t(const region_t&, const nation_t& nation)
       : merged_t{nation}
   {
   }
   template <typename... Args>
   explicit merged_nation_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer regionkey, Integer nationkey) : merged_t::Key{sort_key_t{regionkey, nationkey, 0, 0, 0}, nation_t::Key{regionkey, nationkey}} {}
      Key(const sort_key_t& sk, const nation_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, nation_t::Key{sk.regionkey, sk.nationkey}} {}

      // also serves as joined result of region and nation
      Key(const region_t::Key& rk, const nation_t::Key& nk) : merged_t::Key{sort_key_t{rk.r_regionkey, nk.n_regionkey, 0, 0, 0}, nk} {}
   };

   static unsigned foldKey(uint8_t* out, const Key& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.jk.regionkey);
      pos += fold(out + pos, key.jk.nationkey);
      return pos;
   }

   static unsigned unfoldKey(const uint8_t* in, Key& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.jk.regionkey);
      pos += unfold(in + pos, key.jk.nationkey);
      key.jk.statekey = 0;
      key.jk.countykey = 0;
      key.jk.citykey = 0;
      key.pk.n_regionkey = key.jk.regionkey;
      key.pk.n_nationkey = key.jk.nationkey;
      return pos;
   }

   static unsigned maxFoldLength() { return sizeof(Integer) * 2; }
};

struct merged_states_t : public merged_t<16, states_t, sort_key_t, ExtraID::NONE> {
   merged_states_t() = default;
   // also serves as joined result of merged_nation and states
   merged_states_t(const merged_nation_t&, const states_t& states)
       : merged_t{states}
   {
   }
   template <typename... Args>
   explicit merged_states_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer regionkey, Integer nationkey, Integer statekey)
          : merged_t::Key{sort_key_t{regionkey, nationkey, statekey, 0, 0}, states_t::Key{regionkey, statekey}}
      {
      }
      Key(const sort_key_t& sk, const states_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, states_t::Key{sk.nationkey, sk.statekey}} {}

      // also serves as joined result of merged_nation and states
      Key(const merged_nation_t::Key& nk , const states_t::Key& sk) : merged_t::Key{sort_key_t{nk.jk.regionkey, nk.jk.nationkey, sk.statekey, 0, 0}, sk} {}
   };

   static unsigned foldKey(uint8_t* out, const Key& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.jk.regionkey);
      pos += fold(out + pos, key.jk.nationkey);
      pos += fold(out + pos, key.jk.statekey);
      return pos;
   }

   static unsigned unfoldKey(const uint8_t* in, Key& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.jk.regionkey);
      pos += unfold(in + pos, key.jk.nationkey);
      pos += unfold(in + pos, key.jk.statekey);
      key.jk.countykey = 0;
      key.jk.citykey = 0;
      key.pk.nationkey = key.jk.nationkey;
      key.pk.statekey = key.jk.statekey;
      return pos;
   }
};

struct merged_county_t : public merged_t<17, county_t, sort_key_t, ExtraID::NONE> {
   merged_county_t() = default;
   // also serves as joined result of merged_states and county
   merged_county_t(const merged_states_t&, const county_t& county)
       : merged_t{county}
   {
   }
   template <typename... Args>
   explicit merged_county_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer regionkey, Integer nationkey, Integer statekey, Integer countykey)
          : merged_t::Key{sort_key_t{regionkey, nationkey, statekey, countykey, 0}, county_t::Key{statekey, countykey}}
      {
      }
      Key(const sort_key_t& sk, const county_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, county_t::Key{sk.statekey, sk.countykey}} {}

      // also serves as joined result of merged_states and county
      Key(const merged_states_t::Key& sk, const county_t::Key& ck) : merged_t::Key{sort_key_t{sk.jk.regionkey, sk.jk.nationkey, sk.jk.statekey, ck.countykey, 0}, ck} {}
   };

   static unsigned foldKey(uint8_t* out, const Key& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.jk.regionkey);
      pos += fold(out + pos, key.jk.nationkey);
      pos += fold(out + pos, key.jk.statekey);
      pos += fold(out + pos, key.jk.countykey);
      return pos;
   }

   static unsigned unfoldKey(const uint8_t* in, Key& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.jk.regionkey);
      pos += unfold(in + pos, key.jk.nationkey);
      pos += unfold(in + pos, key.jk.statekey);
      pos += unfold(in + pos, key.jk.countykey);
      key.jk.citykey = 0;
      key.pk.statekey = key.jk.statekey;
      key.pk.countykey = key.jk.countykey;
      return pos;
   }

   static unsigned maxFoldLength() { return sizeof(Integer) * 4; }
};

struct merged_city_t : public merged_t<18, city_t, sort_key_t, ExtraID::NONE> {
   merged_city_t() = default;
   // also serves as joined result of merged_county and city
   merged_city_t(const merged_county_t&, const city_t& city)
       : merged_t{city}
   {
   }
   template <typename... Args>
   explicit merged_city_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer regionkey, Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
          : merged_t::Key{sort_key_t{regionkey, nationkey, statekey, countykey, citykey}, city_t::Key{countykey, citykey}}
      {
      }
      Key(const sort_key_t& sk, const city_t::Key& pk) : merged_t::Key{sk, pk} {}
      Key(const sort_key_t& sk) : merged_t::Key{sk, city_t::Key{sk.countykey, sk.citykey}} {}
      // also serves as joined result of merged_county and city
      Key(const merged_county_t::Key& ck, const city_t::Key& ci) : merged_t::Key{sort_key_t{ck.jk.regionkey, ck.jk.nationkey, ck.jk.statekey, ck.jk.countykey, ci.citykey}, ci} {}
   };
   // use default implementation of foldKey and unfoldKey defined in merged_t
};

struct view_t : public joined_t<15, sort_key_t, region_t, nation_t, states_t, county_t, city_t> {
    view_t() = default;
   template <typename... Args>
   explicit view_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   // From all possible join inputs other than the base tables
   view_t(merged_region_t r, merged_nation_t n, merged_states_t s, merged_county_t c, merged_city_t ci)
       : joined_t(std::make_tuple(r.payload, n.payload, s.payload, c.payload, ci.payload))
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;
      // From all possible join inputs other than the base tables
      Key(const merged_region_t::Key& rk,
          const merged_nation_t::Key& nk,
          const merged_states_t::Key& sk,
          const merged_county_t::Key& ck,
          const merged_city_t::Key& cik)
          : joined_t::Key{sort_key_t{rk.jk.regionkey, nk.jk.nationkey, sk.jk.statekey, ck.jk.countykey, cik.jk.citykey},
                          std::make_tuple(rk.pk, nk.pk, sk.pk, ck.pk, cik.pk)}
      {
      }

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
                                              states_t::Key{jk.nationkey, jk.statekey},
                                              county_t::Key{jk.statekey, jk.countykey},
                                              city_t::Key{jk.countykey, jk.citykey})}
      {
      }
   };
};

}  // namespace geo_join