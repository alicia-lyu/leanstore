#pragma once
#include <functional>
#include "../shared/randutils.hpp"
#include "../shared/table_traits.hpp"

using namespace randutils;

// id range: (0s) 0--7

struct part_t {
   static constexpr int id = 0;
   Varchar<55> p_name;
   Varchar<25> p_mfgr;
   Varchar<10> p_brand;
   Varchar<25> p_type;
   Integer p_size;
   Varchar<10> p_container;
   Numeric p_retailprice;
   Varchar<23> p_comment;

   struct Key {
      static constexpr int id = 0;
      Integer p_partkey;
      ADD_KEY_TRAITS(&Key::p_partkey)
   };

   ADD_RECORD_TRAITS(part_t)

   static part_t generateRandomRecord()
   {
      return part_t{randomastring<55>(0, 55), randomastring<25>(25, 25), randomastring<10>(10, 10),       randomastring<25>(0, 25),
                    urand(1, 50) * 5,         randomastring<10>(10, 10), randomNumeric(0.0000, 100.0000), randomastring<23>(0, 23)};
   }
};

struct supplier_t {
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      Integer s_suppkey;
      ADD_KEY_TRAITS(&Key::s_suppkey)
   };

   Varchar<25> s_name;
   Varchar<40> s_address;
   Integer s_nationkey;
   Varchar<15> s_phone;
   Numeric s_acctbal;
   Varchar<101> s_comment;

   ADD_RECORD_TRAITS(supplier_t)

   static supplier_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return supplier_t{randomastring<25>(25, 25), randomastring<40>(0, 40),        generate_nationkey(),
                        randomastring<15>(15, 15), randomNumeric(0.0000, 100.0000), randomastring<101>(0, 101)};
   }
};

struct partsupp_t {
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      Integer ps_partkey;
      Integer ps_suppkey;
      ADD_KEY_TRAITS(&Key::ps_partkey, &Key::ps_suppkey)
   };

   Integer ps_availqty;
   Numeric ps_supplycost;
   Varchar<199> ps_comment;

   ADD_RECORD_TRAITS(partsupp_t)

   static partsupp_t generateRandomRecord() { return partsupp_t{urand(1, 100000), randomNumeric(0.0000, 100.0000), randomastring<199>(0, 199)}; }
};

struct customerh_t {
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      Integer c_custkey;
      ADD_KEY_TRAITS(&Key::c_custkey)
   };

   Varchar<25> c_name;
   Varchar<40> c_address;
   Integer c_nationkey;
   Varchar<15> c_phone;
   Numeric c_acctbal;
   Varchar<10> c_mktsegment;
   Varchar<117> c_comment;

   ADD_RECORD_TRAITS(customerh_t)

   static customerh_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return customerh_t{randomastring<25>(0, 25),        randomastring<40>(0, 40),  generate_nationkey(),      randomastring<15>(15, 15),
                         randomNumeric(0.0000, 100.0000), randomastring<10>(10, 10), randomastring<117>(0, 117)};
   }
};

struct orders_t {
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      Integer o_orderkey;
      ADD_KEY_TRAITS(&Key::o_orderkey)
   };

   Integer o_custkey;
   Varchar<1> o_orderstatus;
   Numeric o_totalprice;
   Timestamp o_orderdate;
   Varchar<15> o_orderpriority;
   Varchar<15> o_clerk;
   Integer o_shippriority;
   Varchar<79> o_comment;

   ADD_RECORD_TRAITS(orders_t)

   static orders_t generateRandomRecord(std::function<int()> generate_custkey)
   {
      return orders_t{generate_custkey(), randomastring<1>(1, 1),    randomNumeric(0.0000, 100.0000),
                      urand(1, 10000),    randomastring<15>(15, 15), randomastring<15>(15, 15),
                      urand(0, 5),        randomastring<79>(0, 79)};
   }
};

struct lineitem_t {
   static constexpr int id = 5;
   struct Key {
      static constexpr int id = 5;
      Integer l_orderkey;
      Integer l_linenumber;
      ADD_KEY_TRAITS(&Key::l_orderkey, &Key::l_linenumber)
   };

   Integer l_partkey;
   Integer l_suppkey;

   Numeric l_quantity;
   Numeric l_extendedprice;
   Numeric l_discount;
   Numeric l_tax;
   Varchar<1> l_returnflag;
   Varchar<1> l_linestatus;
   Timestamp l_shipdate;
   Timestamp l_commitdate;
   Timestamp l_receiptdate;
   Varchar<25> l_shipinstruct;
   Varchar<10> l_shipmode;
   Varchar<44> l_comment;

   ADD_RECORD_TRAITS(lineitem_t)

   static lineitem_t generateRandomRecord(std::function<int()> generate_partkey, std::function<int()> generate_suppkey)
   {
      return lineitem_t{generate_partkey(),
                        generate_suppkey(),
                        randomNumeric(0.0000, 100.0000),
                        randomNumeric(0.0000, 100.0000),
                        randomNumeric(0.0000, 100.0000),
                        randomNumeric(0.0000, 100.0000),
                        randomastring<1>(1, 1),
                        randomastring<1>(1, 1),
                        urand(1, 10000),
                        urand(1, 10000),
                        urand(1, 10000),
                        randomastring<25>(25, 25),
                        randomastring<10>(10, 10),
                        randomastring<44>(44, 44)};
   }
};

struct nation_t {
   static constexpr int id = 6;
   struct Key {
      static constexpr int id = 6;
      Integer n_nationkey;
      ADD_KEY_TRAITS(&Key::n_nationkey)
   };

   Integer n_regionkey;
   Varchar<25> n_name;
   Varchar<152> n_comment;

   ADD_RECORD_TRAITS(nation_t)

   static nation_t generateRandomRecord(std::function<int()> generate_regionkey)
   {
      return nation_t{generate_regionkey(), randomastring<25>(1, 25), randomastring<152>(0, 152)};
   }
};

struct region_t {
   static constexpr int id = 7;
   struct Key {
      static constexpr int id = 7;
      Integer r_regionkey;
      ADD_KEY_TRAITS(&Key::r_regionkey)
   };

   Varchar<25> r_name;
   Varchar<152> r_comment;

   ADD_RECORD_TRAITS(region_t)

   static region_t generateRandomRecord() { return region_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};