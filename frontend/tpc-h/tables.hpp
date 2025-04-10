#pragma once
#include <functional>
#include "randutils.hpp"
#include "table_traits.hpp"

using namespace randutils;

// id range: (0s) 0--7

struct part_base {
   static constexpr int id = 0;
   struct key_base {
      static constexpr int id = 0;
      Integer p_partkey;
   };

   Varchar<55> p_name;
   Varchar<25> p_mfgr;
   Varchar<10> p_brand;
   Varchar<25> p_type;
   Integer p_size;
   Varchar<10> p_container;
   Numeric p_retailprice;
   Varchar<23> p_comment;
};

struct part_t : public part_base, public record_traits<part_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;

   struct Key : public key_base, public key_traits<key_base, &key_base::p_partkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   part_t() = default;  // Defining a custom constructor (below) prevents the compiler from generating a default constructor

   template <typename... Args>
   explicit part_t(Args&&... args) : part_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static part_t generateRandomRecord()
   {
      return part_t{randomastring<55>(0, 55), randomastring<25>(25, 25), randomastring<10>(10, 10),       randomastring<25>(0, 25),
                    urand(1, 50) * 5,         randomastring<10>(10, 10), randomNumeric(0.0000, 100.0000), randomastring<23>(0, 23)};
   }
};

struct supplier_base {
   static constexpr int id = 1;
   struct key_base {
      static constexpr int id = 1;
      Integer s_suppkey;
   };

   Varchar<25> s_name;
   Varchar<40> s_address;
   Integer s_nationkey;
   Varchar<15> s_phone;
   Numeric s_acctbal;
   Varchar<101> s_comment;
};

struct supplier_t : public supplier_base, public record_traits<supplier_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::s_suppkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   supplier_t() = default;

   template <typename... Args>
   explicit supplier_t(Args&&... args) : supplier_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static supplier_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return supplier_t{randomastring<25>(25, 25), randomastring<40>(0, 40),        generate_nationkey(),
                        randomastring<15>(15, 15), randomNumeric(0.0000, 100.0000), randomastring<101>(0, 101)};
   }
};

struct partsupp_base {
   static constexpr int id = 2;
   struct key_base {
      static constexpr int id = 2;
      Integer ps_partkey;
      Integer ps_suppkey;
   };

   Integer ps_availqty;
   Numeric ps_supplycost;
   Varchar<199> ps_comment;
};

struct partsupp_t : public partsupp_base, public record_traits<partsupp_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::ps_partkey, &key_base::ps_suppkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   partsupp_t() = default;

   template <typename... Args>
   explicit partsupp_t(Args&&... args) : partsupp_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static partsupp_t generateRandomRecord() { return partsupp_t{urand(1, 100000), randomNumeric(0.0000, 100.0000), randomastring<199>(0, 199)}; }
};

struct customer_base {
   static constexpr int id = 3;
   struct key_base {
      static constexpr int id = 3;
      Integer c_custkey;
   };

   Varchar<25> c_name;
   Varchar<40> c_address;
   Integer c_nationkey;
   Varchar<15> c_phone;
   Numeric c_acctbal;
   Varchar<10> c_mktsegment;
   Varchar<117> c_comment;
};

struct customerh_t : public customer_base, public record_traits<customer_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::c_custkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   customerh_t() = default;

   template <typename... Args>
   explicit customerh_t(Args&&... args) : customer_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static customerh_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return customerh_t{randomastring<25>(0, 25),        randomastring<40>(0, 40),  generate_nationkey(),      randomastring<15>(15, 15),
                         randomNumeric(0.0000, 100.0000), randomastring<10>(10, 10), randomastring<117>(0, 117)};
   }
};

struct orders_base {
   static constexpr int id = 4;
   struct key_base {
      static constexpr int id = 4;
      Integer o_orderkey;
   };

   Integer o_custkey;
   Varchar<1> o_orderstatus;
   Numeric o_totalprice;
   Timestamp o_orderdate;
   Varchar<15> o_orderpriority;
   Varchar<15> o_clerk;
   Integer o_shippriority;
   Varchar<79> o_comment;
};

struct orders_t : public orders_base, public record_traits<orders_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::o_orderkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   orders_t() = default;

   template <typename... Args>
   explicit orders_t(Args&&... args) : orders_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static orders_t generateRandomRecord(std::function<int()> generate_custkey)
   {
      return orders_t{generate_custkey(), randomastring<1>(1, 1),    randomNumeric(0.0000, 100.0000),
                      urand(1, 10000),    randomastring<15>(15, 15), randomastring<15>(15, 15),
                      urand(0, 5),        randomastring<79>(0, 79)};
   }
};

struct lineitem_base {
   static constexpr int id = 5;
   struct key_base {
      static constexpr int id = 5;
      Integer l_orderkey;
      Integer l_linenumber;
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
};

struct lineitem_t : public lineitem_base, public record_traits<lineitem_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::l_orderkey, &key_base::l_linenumber> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   lineitem_t() = default;

   template <typename... Args>
   explicit lineitem_t(Args&&... args) : lineitem_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

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

struct nation_base {
   static constexpr int id = 6;
   struct key_base {
      static constexpr int id = 6;
      Integer n_nationkey;
   };

   Varchar<25> n_name;
   Integer n_regionkey;
   Varchar<152> n_comment;
};

struct nation_t : public nation_base, public record_traits<nation_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::n_nationkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   nation_t() = default;

   template <typename... Args>
   explicit nation_t(Args&&... args) : nation_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static nation_t generateRandomRecord(std::function<int()> generate_regionkey)
   {
      return nation_t{randomastring<25>(0, 25), generate_regionkey(), randomastring<152>(0, 152)};
   }
};

struct region_base {
   static constexpr int id = 7;
   struct key_base {
      static constexpr int id = 7;
      Integer r_regionkey;
   };

   Varchar<25> r_name;
   Varchar<152> r_comment;
};

struct region_t : public region_base, public record_traits<region_base> {
   using record_traits::fromBytes;
   using record_traits::toBytes;
   struct Key : public key_base, public key_traits<key_base, &key_base::r_regionkey> {
      template <typename... Args>
      explicit Key(Args&&... args) : key_base{std::forward<Args>(args)...}
      {
      }
   };

   region_t() = default;

   template <typename... Args>
   explicit region_t(Args&&... args) : region_base{std::forward<Args>(args)...}
   {
   }

   static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

   static region_t generateRandomRecord() { return region_t{randomastring<25>(0, 25), randomastring<152>(0, 152)}; }
};