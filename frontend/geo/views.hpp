#pragma once

#include <variant>
#include "../shared/variant_tuple_utils.hpp"
#include "../shared/view_templates.hpp"
#include "tpch_tables.hpp"

// id range: 10s + 20s (only one such namespace are included in each executable)
namespace geo_join
{
using namespace randutils;
struct sort_key_t {
   Integer nationkey;
   Integer statekey;
   Integer countykey;
   Integer citykey;
   Integer custkey;
   using Key = sort_key_t;
   ADD_KEY_TRAITS(&sort_key_t::nationkey, &sort_key_t::statekey, &sort_key_t::countykey, &sort_key_t::citykey, &sort_key_t::custkey)
   auto operator<=>(const sort_key_t&) const = default;

   static sort_key_t max()
   {
      return sort_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max(),
                        std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()};
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
      if (custkey != 0 && other.custkey != 0 && custkey != other.custkey)
         return custkey - other.custkey;
      return 0;
   }

   std::tuple<int, int, int> first_diff(const sort_key_t& base) const
   {
      if (nationkey != base.nationkey)
         return std::make_tuple(0, base.nationkey, nationkey - base.nationkey);
      if (statekey != base.statekey)
         return std::make_tuple(1, base.statekey, statekey - base.statekey);
      if (countykey != base.countykey)
         return std::make_tuple(2, base.countykey, countykey - base.countykey);
      if (citykey != base.citykey)
         return std::make_tuple(3, base.citykey, citykey - base.citykey);
      if (custkey != base.custkey)
         return std::make_tuple(4, base.custkey, custkey - base.custkey);
      return std::make_tuple(5, 0, 0);  // no difference
   }
};

struct customer2_t {
   static constexpr int id = 11;
   customer2_t() = default;
   customer2_t(const Varchar<25>& c_name,
               const Varchar<40>& c_address,
               const Varchar<15>& c_phone,
               Numeric c_acctbal,
               const Varchar<10>& c_mktsegment,
               const Varchar<117>& c_comment)
       : c_name(c_name), c_address(c_address), c_phone(c_phone), c_acctbal(c_acctbal), c_mktsegment(c_mktsegment), c_comment(c_comment)
   {
   }
   customer2_t(const customerh_t& old_v)
       : c_name(old_v.c_name),
         c_address(old_v.c_address),
         c_phone(old_v.c_phone),
         c_acctbal(old_v.c_acctbal),
         c_mktsegment(old_v.c_mktsegment),
         c_comment(old_v.c_comment)
   {
   }

   struct Key {
      static constexpr int id = 11;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      Integer citykey;
      Integer custkey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey, &Key::citykey, &Key::custkey)

      Key() = default;
      Key(Integer nationkey, Integer statekey, Integer countykey, Integer citykey, Integer custkey)
          : nationkey(nationkey), statekey(statekey), countykey(countykey), citykey(citykey), custkey(custkey)
      {
      }
      Key(const customerh_t::Key& old_k, Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
          : nationkey(nationkey), statekey(statekey), countykey(countykey), citykey(citykey), custkey(old_k.c_custkey)
      {
      }
      Key(const sort_key_t& sk) : nationkey(sk.nationkey), statekey(sk.statekey), countykey(sk.countykey), citykey(sk.citykey), custkey(sk.custkey) {}

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, citykey, custkey}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> c_name;
   Varchar<40> c_address;
   Varchar<15> c_phone;
   Numeric c_acctbal;
   Varchar<10> c_mktsegment;
   Varchar<117> c_comment;

   ADD_RECORD_TRAITS(customer2_t)

   void print(std::ostream& os) const
   {
      os << c_name << "|" << c_address << "|" << c_phone << "|" << c_acctbal << "|" << c_mktsegment << "|" << c_comment;
   }

   static customer2_t generateRandomRecord()
   {
      return customer2_t{randomastring<25>(25, 25),       randomastring<40>(0, 40),  randomastring<15>(15, 15),
                         randomNumeric(0.0000, 100.0000), randomastring<10>(10, 10), randomastring<117>(117, 117)};
   }
   static customer2_t generateRandomRecord(const Varchar<25>& state_name, const Varchar<25>& county_name, const Varchar<25>& city_name)
   {
      // fill address with state, county, city names
      std::string address_str = state_name.toString() + ", " + county_name.toString() + ", " + city_name.toString();
      if (address_str.length() > 40) {
         address_str = address_str.substr(0, 40);
      } else if (address_str.length() < 40) {
         // append random characters
         for (int i = address_str.length(); i < 40; ++i) {
            address_str += char('a' + (rand() % 26));  // random lowercase letter
         }
      }
      return customer2_t{randomastring<25>(1, 25),        Varchar<40>(address_str.c_str()), randomastring<15>(15, 15),
                         randomNumeric(0.0000, 100.0000), randomastring<10>(10, 10),        randomastring<117>(117, 117)};
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

      Key() = default;
      Key(Integer nationkey) : nationkey(nationkey) {}
      Key(const sort_key_t& sk) : nationkey(sk.nationkey) {}

      sort_key_t get_jk() const { return sort_key_t{nationkey, 0, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };

   Varchar<25> n_name;
   Varchar<152> n_comment;
   Integer last_statekey;

   ADD_RECORD_TRAITS(nation2_t)

   void print(std::ostream& os) const { os << n_name << "|" << n_comment << "|" << last_statekey; }

   static nation2_t generateRandomRecord(int state_cnt) { return nation2_t{randomastring<25>(1, 25), randomastring<152>(0, 152), state_cnt}; }
};

struct states_t {
   static constexpr int id = 13;
   struct Key {
      static constexpr int id = 13;
      Integer nationkey;
      Integer statekey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey)

      Key() = default;
      Key(Integer nationkey, Integer statekey) : nationkey(nationkey), statekey(statekey) {}
      Key(const sort_key_t& sk) : nationkey(sk.nationkey), statekey(sk.statekey) {}

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, 0, 0, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   Integer last_countykey;
   ADD_RECORD_TRAITS(states_t)

   void print(std::ostream& os) const { os << name << "|" << comment << "|" << last_countykey; }

   static states_t generateRandomRecord(int county_cnt) { return states_t{randomastring<25>(1, 25), randomastring<152>(0, 152), county_cnt}; }
};

struct ns_t : public joined_t<21, sort_key_t, false, nation2_t, states_t> {
   ns_t() = default;
   ns_t(const nation2_t& n, const states_t& s) : joined_t{std::make_tuple(n, s)} {}
   struct Key : public joined_t::Key {
      Key() : joined_t::Key() {}
      Key(const nation2_t::Key& nk, const states_t::Key& sk) : joined_t::Key{sort_key_t{nk.nationkey, sk.statekey, 0, 0, 0}, std::make_tuple(nk, sk)}
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
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey)

      Key() = default;
      Key(Integer nationkey, Integer statekey, Integer countykey) : nationkey(nationkey), statekey(statekey), countykey(countykey) {}
      Key(const sort_key_t& sk) : nationkey(sk.nationkey), statekey(sk.statekey), countykey(sk.countykey) {}

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, 0, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   Integer last_citykey;
   ADD_RECORD_TRAITS(county_t)

   void print(std::ostream& os) const { os << name << "|" << comment << "|" << last_citykey; }

   static county_t generateRandomRecord(int city_cnt) { return county_t{randomastring<25>(1, 25), randomastring<152>(0, 152), city_cnt}; }
};

struct nsc_t : public joined_t<20, sort_key_t, false, ns_t, county_t> {
   template <typename... Args>
   explicit nsc_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const ns_t::Key& nsk, const county_t::Key& ck)
          : joined_t::Key{sort_key_t{nsk.jk.nationkey, nsk.jk.statekey, ck.countykey, 0, 0}, std::make_tuple(nsk, ck)}
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

      Key() = default;
      Key(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
          : nationkey(nationkey), statekey(statekey), countykey(countykey), citykey(citykey)
      {
      }

      Key(const sort_key_t& sk) : nationkey(sk.nationkey), statekey(sk.statekey), countykey(sk.countykey), citykey(sk.citykey) {}

      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, citykey, 0}; }
      Key get_pk() const { return *this; }
   };
   Varchar<25> name;
   Varchar<152> comment;
   ADD_RECORD_TRAITS(city_t)

   void print(std::ostream& os) const { os << name << "|" << comment; }

   static city_t generateRandomRecord() { return city_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

struct ccc_t : public joined_t<23, sort_key_t, false, county_t, city_t, customer2_t> {
   ccc_t() = default;
   ccc_t(const county_t& c, const city_t& ci, const customer2_t& cu) : joined_t{std::make_tuple(c, ci, cu)} {}
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const county_t::Key& ck, const city_t::Key& cik, const customer2_t::Key& cuk)
          : joined_t::Key{sort_key_t{ck.nationkey, ck.statekey, ck.countykey, cik.citykey, cuk.custkey}, std::make_tuple(ck, cik, cuk)}
      {
      }

      std::tuple<county_t::Key, city_t::Key, customer2_t::Key> flatten() const { return this->keys; }
   };
   std::tuple<county_t, city_t, customer2_t> flatten() const { return this->payloads; }
};

struct nscci_t : public joined_t<22, sort_key_t, false, nsc_t, city_t> {
   template <typename... Args>
   explicit nscci_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public joined_t::Key {
      Key() = default;

      Key(const nsc_t::Key& nck, const city_t::Key& cik)
          : joined_t::Key{sort_key_t{nck.jk.nationkey, nck.jk.statekey, nck.jk.countykey, cik.citykey, 0}, std::make_tuple(nck, cik)}
      {
      }

      std::tuple<nation2_t::Key, states_t::Key, county_t::Key, city_t::Key> flatten() const
      {
         return std::tuple_cat(std::get<0>(keys).flatten(), std::make_tuple(std::get<1>(keys)));
      }
   };
   std::tuple<nation2_t, states_t, county_t, city_t> flatten() const
   {
      std::tuple<nation2_t, states_t, county_t> nsc_flattened = std::get<0>(payloads).flatten();
      return std::tuple_cat(nsc_flattened, std::make_tuple(std::get<1>(payloads)));
   }
};

struct customer_count_t {
   static constexpr int id = 26;
   struct Key {
      static constexpr int id = 26;
      Integer nationkey;
      Integer statekey;
      Integer countykey;
      Integer citykey;
      ADD_KEY_TRAITS(&Key::nationkey, &Key::statekey, &Key::countykey, &Key::citykey)

      Key() = default;
      Key(Integer nationkey, Integer statekey, Integer countykey, Integer citykey)
          : nationkey(nationkey), statekey(statekey), countykey(countykey), citykey(citykey)
      {
      }

      Key(const customer2_t::Key cuk) : nationkey(cuk.nationkey), statekey(cuk.statekey), countykey(cuk.countykey), citykey(cuk.citykey) {}

      Key(const sort_key_t& sk) : nationkey(sk.nationkey), statekey(sk.statekey), countykey(sk.countykey), citykey(sk.citykey) {}
      sort_key_t get_jk() const { return sort_key_t{nationkey, statekey, countykey, citykey, 0}; }
      Key get_pk() const { return *this; }
   };

   Integer customer_count;

   ADD_RECORD_TRAITS(customer_count_t);
   void print(std::ostream& os) const { os << customer_count; }
};

struct mixed_view_t : public joined_t<25, sort_key_t, false, nation2_t, states_t, county_t, city_t, customer_count_t> {
   mixed_view_t() = default;
   mixed_view_t(const nation2_t& n, const states_t& s, const county_t& c, const city_t& ci, const customer_count_t& cuc)
       : joined_t{std::make_tuple(n, s, c, ci, cuc)}
   {
   }

   mixed_view_t(const nscci_t& nscci, const customer_count_t& cuc) : joined_t{std::tuple_cat(nscci.flatten(), std::make_tuple(cuc))} {}

   struct Key : public joined_t::Key {
      Key() = default;

      Key(const sort_key_t& jk)
          : joined_t::Key{sort_key_t{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, 0},
                          nation2_t::Key{jk},
                          states_t::Key{jk},
                          county_t::Key{jk},
                          city_t::Key{jk},
                          customer_count_t::Key{jk}}
      {
      }

      Key(const customer2_t::Key& cuk)
          : joined_t::Key{sort_key_t{cuk.nationkey, cuk.statekey, cuk.countykey, cuk.citykey, cuk.custkey},
                          nation2_t::Key{cuk.nationkey},
                          states_t::Key{cuk.nationkey, cuk.statekey},
                          county_t::Key{cuk.nationkey, cuk.statekey, cuk.countykey},
                          city_t::Key{cuk.nationkey, cuk.statekey, cuk.countykey, cuk.citykey},
                          customer_count_t::Key{cuk}}
      {
      }

      Key(const nation2_t::Key& n, const states_t::Key& s, const county_t::Key& c, const city_t::Key& ci, const customer_count_t::Key& cuc)
          : joined_t::Key{sort_key_t{n.nationkey, s.statekey, c.countykey, ci.citykey, 0}, std::make_tuple(n, s, c, ci, cuc)}
      {
      }

      Key(const nscci_t::Key& nscci, const customer_count_t::Key& cuc)
          : joined_t::Key{sort_key_t{nscci.jk.nationkey, nscci.jk.statekey, nscci.jk.countykey, nscci.jk.citykey, 0},
                          std::tuple_cat(nscci.flatten(), std::make_tuple(cuc))}
      {
      }
   };
};

struct view_t : public joined_t<24, sort_key_t, false, nation2_t, states_t, county_t, city_t, customer2_t> {
   view_t() = default;
   // view_t(const nsc_t& nsc, const city_t& c) : joined_t{std::tuple_cat(nsc.flatten(), std::make_tuple(c))} {}
   view_t(const nscci_t& nscci, const customer2_t& cu) : joined_t{std::tuple_cat(nscci.flatten(), std::make_tuple(cu))} {}

   view_t(const nation2_t& n, const states_t& s, const county_t& c, const city_t& ci, const customer2_t& cu)
       : joined_t{std::make_tuple(n, s, c, ci, cu)}
   {
   }

   view_t(const ns_t& ns, const ccc_t& ccc) : joined_t{std::tuple_cat(ns.flatten(), ccc.flatten())} {}

   struct Key : public joined_t::Key {
      Key() = default;

      Key(const nation2_t::Key& nk, const states_t::Key& sk, const county_t::Key& ck, const city_t::Key& ci, const customer2_t::Key& cu)
          : joined_t::Key{sort_key_t{nk.nationkey, sk.statekey, ck.countykey, ci.citykey, cu.custkey}, std::make_tuple(nk, sk, ck, ci, cu)}
      {
      }

      Key(const nscci_t::Key& nscci, const customer2_t::Key& cu)
          : joined_t::Key{sort_key_t{cu.get_jk()}, std::tuple_cat(nscci.flatten(), std::make_tuple(cu))}
      {
      }

      Key(const ns_t::Key& ns, const ccc_t::Key& ccc)
          : joined_t::Key{sort_key_t{ns.jk.nationkey, ns.jk.statekey, ccc.jk.countykey, ccc.jk.citykey, ccc.jk.custkey},
                          std::tuple_cat(ns.flatten(), ccc.flatten())}
      {
      }

      Key(int n, int s, int c, int ci, int cu)
          : joined_t::Key{sort_key_t{n, s, c, ci, cu}, nation2_t::Key{n},        states_t::Key{n, s},
                          county_t::Key{n, s, c},      city_t::Key{n, s, c, ci}, customer2_t::Key{n, s, c, ci, cu}}
      {
      }

      Key(const customer2_t::Key& cu)
          : joined_t::Key{sort_key_t{cu.nationkey, cu.statekey, cu.countykey, cu.citykey, cu.custkey},
                          nation2_t::Key{cu.nationkey},
                          states_t::Key{cu.nationkey, cu.statekey},
                          county_t::Key{cu.nationkey, cu.statekey, cu.countykey},
                          city_t::Key{cu.nationkey, cu.statekey, cu.countykey, cu.citykey},
                          customer2_t::Key{cu}}
      {
      }

      Key(const sort_key_t& jk)
          : joined_t::Key{jk,
                          nation2_t::Key{jk.nationkey},
                          states_t::Key{jk.nationkey, jk.statekey},
                          county_t::Key{jk.nationkey, jk.statekey, jk.countykey},
                          city_t::Key{jk.nationkey, jk.statekey, jk.countykey, jk.citykey},
                          customer2_t::Key{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, jk.custkey}}
      {
      }
   };

   static view_t generateRandomRecord(int state_cnt, int county_cnt, int city_cnt)
   {
      return view_t{nation2_t::generateRandomRecord(state_cnt), states_t::generateRandomRecord(county_cnt), county_t::generateRandomRecord(city_cnt),
                    city_t::generateRandomRecord(), customer2_t::generateRandomRecord()};
   }
};

};  // namespace geo_join

using namespace geo_join;

template <>
struct SKBuilder<sort_key_t> {
   static sort_key_t inline create(const nation2_t::Key& k, const nation2_t&) { return sort_key_t{k.nationkey, 0, 0, 0, 0}; }
   static sort_key_t inline create(const states_t::Key& k, const states_t&) { return sort_key_t{k.nationkey, k.statekey, 0, 0, 0}; }
   static sort_key_t inline create(const ns_t::Key& k, const ns_t&) { return k.jk; }
   static sort_key_t inline create(const county_t::Key& k, const county_t&) { return sort_key_t{k.nationkey, k.statekey, k.countykey, 0, 0}; }
   static sort_key_t inline create(const nsc_t::Key& k, const nsc_t&) { return k.jk; }
   static sort_key_t inline create(const city_t::Key& k, const city_t&) { return sort_key_t{k.nationkey, k.statekey, k.countykey, k.citykey, 0}; }
   static sort_key_t inline create(const ccc_t::Key& k, const ccc_t&) { return k.jk; }
   static sort_key_t inline create(const view_t::Key& k, const view_t&) { return k.jk; }
   static sort_key_t inline create(const nscci_t::Key& k, const nscci_t&) { return k.jk; }
   static sort_key_t inline create(const customer2_t::Key& k, const customer2_t&)
   {
      return sort_key_t{k.nationkey, k.statekey, k.countykey, k.citykey, k.custkey};
   }
   static sort_key_t inline create(const customer_count_t::Key& k, const customer_count_t&)
   {
      return sort_key_t{k.nationkey, k.statekey, k.countykey, k.citykey, 0};
   }
   static sort_key_t inline create(const mixed_view_t::Key& k, const mixed_view_t&) { return k.jk; }
   static sort_key_t inline create(const std::variant<nation2_t::Key, states_t::Key, county_t::Key, city_t::Key, customer2_t::Key>& k,
                                   const std::variant<nation2_t, states_t, county_t, city_t, customer2_t>& v)
   {
      return std::visit(overloaded{[&](const nation2_t::Key& nk) {
                                      const nation2_t* n = std::get_if<nation2_t>(&v);
                                      return create(nk, *n);
                                   },
                                   [&](const states_t::Key& sk) {
                                      const states_t* s = std::get_if<states_t>(&v);
                                      return create(sk, *s);
                                   },
                                   [&](const county_t::Key& ck) {
                                      const county_t* c = std::get_if<county_t>(&v);
                                      return create(ck, *c);
                                   },
                                   [&](const city_t::Key& ci) {
                                      const city_t* c = std::get_if<city_t>(&v);
                                      return create(ci, *c);
                                   },
                                   [&](const customer2_t::Key& cu) {
                                      const customer2_t* c = std::get_if<customer2_t>(&v);
                                      return create(cu, *c);
                                   }},
                        k);
   }
   static sort_key_t inline create(const std::variant<nation2_t::Key, states_t::Key, county_t::Key, city_t::Key, customer_count_t::Key>& k,
                                   const std::variant<nation2_t, states_t, county_t, city_t, customer_count_t>& v)
   {
      return std::visit(overloaded{[&](const nation2_t::Key& nk) {
                                      const nation2_t* n = std::get_if<nation2_t>(&v);
                                      return create(nk, *n);
                                   },
                                   [&](const states_t::Key& sk) {
                                      const states_t* s = std::get_if<states_t>(&v);
                                      return create(sk, *s);
                                   },
                                   [&](const county_t::Key& ck) {
                                      const county_t* c = std::get_if<county_t>(&v);
                                      return create(ck, *c);
                                   },
                                   [&](const city_t::Key& ci) {
                                      const city_t* c = std::get_if<city_t>(&v);
                                      return create(ci, *c);
                                   },
                                   [&](const customer_count_t::Key& cu) {
                                      const customer_count_t* c = std::get_if<customer_count_t>(&v);
                                      return create(cu, *c);
                                   }},
                        k);
   }
   static sort_key_t inline create(const std::variant<nation2_t::Key, states_t::Key>& k, const std::variant<nation2_t, states_t>& v)
   {
      return std::visit(overloaded{[&](const nation2_t::Key& nk) {
                                      const nation2_t* n = std::get_if<nation2_t>(&v);
                                      return create(nk, *n);
                                   },
                                   [&](const states_t::Key& sk) {
                                      const states_t* s = std::get_if<states_t>(&v);
                                      return create(sk, *s);
                                   }},
                        k);
   }
   static sort_key_t inline create(const std::variant<county_t::Key, city_t::Key, customer2_t::Key>& k,
                                   const std::variant<county_t, city_t, customer2_t>& v)
   {
      return std::visit(overloaded{[&](const county_t::Key& ck) {
                                      const county_t* c = std::get_if<county_t>(&v);
                                      return create(ck, *c);
                                   },
                                   [&](const city_t::Key& ci) {
                                      const city_t* c = std::get_if<city_t>(&v);
                                      return create(ci, *c);
                                   },
                                   [&](const customer2_t::Key& cu) {
                                      const customer2_t* c = std::get_if<customer2_t>(&v);
                                      return create(cu, *c);
                                   }},
                        k);
   }

   template <typename Record>
   static sort_key_t inline get(const sort_key_t& k)
   {
      return k;
   }
};

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<nation2_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, 0, 0, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<states_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, 0, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<ns_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, 0, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<county_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<nsc_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, 0, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<city_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<nscci_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<mixed_view_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, 0};
}

template <>
inline sort_key_t SKBuilder<sort_key_t>::get<customer_count_t>(const sort_key_t& jk)
{
   return sort_key_t{jk.nationkey, jk.statekey, jk.countykey, jk.citykey, 0};
}