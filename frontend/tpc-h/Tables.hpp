#pragma once
#include <ostream>
#include "../shared/Types.hpp"

template <typename K, auto K::* ...Members>
class KeyPrototype {
    friend std::ostream& operator<<(std::ostream& os, const K& record)
    {
        ((os << Members << ": " << record.*Members << ", "), ...);
        return os;
    }
};

template <typename T, auto T::* ...Members>
struct RecordPrototype {
    static constexpr unsigned rowSize() { return (sizeof(std::declval<T>().*Members) + ...); }
    
    template <class K, auto K::* ...KMembers>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        unsigned pos = 0;
        ((pos += fold(out + pos, key.*KMembers)), ...);
        return pos;
    }

    template <class K, auto K::* ...KMembers>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        unsigned pos = 0;
        ((pos += unfold(in + pos, key.*KMembers)), ...);
        return pos;
    }

    static constexpr unsigned maxFoldLength() { return (sizeof(std::declval<T>().*Members) + ...); }
};

struct part_base {
    static constexpr int id = 0;
    struct key_base {
        static constexpr int id = 0;
        Integer p_partkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {};

    Varchar<55> p_name;
    Varchar<25> p_mfgr;
    Varchar<10> p_brand;
    Varchar<25> p_type;
    Integer p_size;
    Varchar<10> p_container;
    Numeric p_retailprice;
    Varchar<23> p_comment;
};

struct part_t : public part_base, public RecordPrototype<part_base,
    &part_base::p_name,
    &part_base::p_mfgr,
    &part_base::p_brand,
    &part_base::p_type,
    &part_base::p_size,
    &part_base::p_container,
    &part_base::p_retailprice,
    &part_base::p_comment>
{};

struct supplier_base {
    static constexpr int id = 1;
    struct key_base {
        static constexpr int id = 1;
        Integer s_suppkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::s_suppkey> {};

    Varchar<25> s_name;
    Varchar<40> s_address;
    Integer s_nationkey;
    Varchar<15> s_phone;
    Numeric s_acctbal;
    ;Varchar<101> s_comment;
};

struct supplier_t : public supplier_base, public RecordPrototype<supplier_base, &supplier_base::s_name, &supplier_base::s_address, &supplier_base::s_nationkey, &supplier_base::s_phone, &supplier_base::s_acctbal, &supplier_base::s_comment> {};

struct partsupp_base {
    static constexpr int id = 2;
    struct key_base {
        static constexpr int id = 2;
        Integer ps_partkey;
        Integer ps_suppkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::ps_partkey, &key_base::ps_suppkey> {};

    Integer ps_availqty;
    Numeric ps_supplycost;
    Varchar<199> ps_comment;
};

struct partsupp_t : public partsupp_base, public RecordPrototype<partsupp_base, &partsupp_base::ps_availqty, &partsupp_base::ps_supplycost, &partsupp_base::ps_comment> {};

struct customer_base {
    static constexpr int id = 3;
    struct key_base {
        static constexpr int id = 3;
        Integer c_custkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::c_custkey> {};

    Varchar<25> c_name;
    Varchar<40> c_address;
    Integer c_nationkey;
    Varchar<15> c_phone;
    Numeric c_acctbal;
    Varchar<10> c_mktsegment;
    Varchar<117> c_comment;
};

struct customerh_t : public customer_base, public RecordPrototype<customer_base, &customer_base::c_name, &customer_base::c_address, &customer_base::c_nationkey, &customer_base::c_phone, &customer_base::c_acctbal, &customer_base::c_mktsegment, &customer_base::c_comment> {};

struct orders_base {
    static constexpr int id = 4;
    struct key_base {
        static constexpr int id = 4;
        Integer o_orderkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::o_orderkey> {};

    Integer o_custkey;
    Varchar<1> o_orderstatus;
    Numeric o_totalprice;
    Timestamp o_orderdate;
    Varchar<15> o_orderpriority;
    Varchar<15> o_clerk;
    Integer o_shippriority;
    Varchar<79> o_comment;
};

struct orders_t : public orders_base, public RecordPrototype<orders_base, &orders_base::o_custkey, &orders_base::o_orderstatus, &orders_base::o_totalprice, &orders_base::o_orderdate, &orders_base::o_orderpriority, &orders_base::o_clerk, &orders_base::o_shippriority, &orders_base::o_comment> {};

struct lineitem_base {
    static constexpr int id = 5;
    struct key_base {
        static constexpr int id = 5;
        Integer l_orderkey;
        Integer l_linenumber;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::l_orderkey, &key_base::l_linenumber> {};

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

struct lineitem_t : public lineitem_base, public RecordPrototype<lineitem_base, &lineitem_base::l_partkey, &lineitem_base::l_suppkey, &lineitem_base::l_quantity, &lineitem_base::l_extendedprice, &lineitem_base::l_discount, &lineitem_base::l_tax, &lineitem_base::l_returnflag, &lineitem_base::l_linestatus, &lineitem_base::l_shipdate, &lineitem_base::l_commitdate, &lineitem_base::l_receiptdate, &lineitem_base::l_shipinstruct, &lineitem_base::l_shipmode, &lineitem_base::l_comment> {};

struct nation_base {
    static constexpr int id = 6;
    struct key_base {
        static constexpr int id = 6;
        Integer n_nationkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::n_nationkey> {};

    Varchar<25> n_name;
    Integer n_regionkey;
    Varchar<152> n_comment;
};

struct nation_t : public nation_base, public RecordPrototype<nation_base, &nation_base::n_name, &nation_base::n_regionkey, &nation_base::n_comment> {};

struct region_base {
    static constexpr int id = 7;
    struct key_base {
        static constexpr int id = 7;
        Integer r_regionkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::r_regionkey> {};

    Varchar<25> r_name;
    Varchar<152> r_comment;
};

struct region_t : public region_base, public RecordPrototype<region_base, &region_base::r_name, &region_base::r_comment> {};