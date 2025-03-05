#pragma once
#include <ostream>
#include "../shared/Types.hpp"

struct part_t {
    static constexpr int id = 0;
    struct Key {
        static constexpr int id = 0;
        Integer p_partkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "partkey: " << record.p_partkey;
            return os;
        }
    }

    Varchar<55> p_name;
    Varchar<25> p_mfgr;
    Varchar<10> p_brand;
    Varchar<25> p_type;
    Integer p_size;
    Varchar<10> p_container;
    Numeric p_retailprice;
    Varchar<23> p_comment;
};

struct supplier_t {
    static constexpr int id = 1;
    struct Key {
        static constexpr int id = 1;
        Integer s_suppkey

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "s_id: " << record.s_suppkey;
            return os;
        }
    }

    Varchar<25> s_name;
    Varchar<40> s_address;
    Integer s_nationkey;
    Varchar<15> s_phone;
    Numeric s_acctbal;
    ;Varchar<101> s_comment;
}

struct partsupp_t {
    static constexpr int id = 2;
    struct Key {
        static constexpr int id = 2;
        Integer ps_partkey;
        Integer ps_suppkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "ps_partkey: " << record.ps_partkey << ", ps_suppkey: " << record.ps_suppkey;
            return os;
        }
    }

    Integer ps_availqty;
    Numeric ps_supplycost;
    Varchar<199> ps_comment;
};

struct customer_t {
    static constexpr int id = 3;
    struct Key {
        static constexpr int id = 3;
        Integer c_custkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "c_custkey: " << record.c_custkey;
            return os;
        }
    }

    Varchar<25> c_name;
    Varchar<40> c_address;
    Integer c_nationkey;
    Varchar<15> c_phone;
    Numeric c_acctbal;
    Varchar<10> c_mktsegment;
    Varchar<117> c_comment;
};

struct orders_t {
    static constexpr int id = 4;
    struct Key {
        static constexpr int id = 4;
        Integer o_orderkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "o_orderkey: " << record.o_orderkey;
            return os;
        }
    }

    Integer o_custkey;
    Varchar<1> o_orderstatus;
    Numeric o_totalprice;
    Timestamp o_orderdate;
    Varchar<15> o_orderpriority;
    Varchar<15> o_clerk;
    Integer o_shippriority;
    Varchar<79> o_comment;
};

struct lineitem_t {
    static constexpr int id = 5;
    struct Key {
        static constexpr int id = 5;
        Integer l_orderkey;
        Integer l_linenumber;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "l_orderkey: " << record.l_orderkey << ", l_linenumber: " << record.l_linenumber;
            return os;
        }
    }

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

struct nation_t {
    static constexpr int id = 6;
    struct Key {
        static constexpr int id = 6;
        Integer n_nationkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "n_nationkey: " << record.n_nationkey;
            return os;
        }
    }

    Varchar<25> n_name;
    Integer n_regionkey;
    Varchar<152> n_comment;
};

struct region_t {
    static constexpr int id = 7;
    struct Key {
        static constexpr int id = 7;
        Integer r_regionkey;

        friend std::ostream& operator<<(std::ostream& os, const Key& record)
        {
            os << "r_regionkey: " << record.r_regionkey;
            return os;
        }
    }

    Varchar<25> r_name;
    Varchar<152> r_comment;
};