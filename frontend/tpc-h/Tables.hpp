#pragma once
#include <functional>
#include <ostream>
#include "../shared/Types.hpp"
#include "randutils.hpp"

using namespace randutils;

template <typename T>
inline T bytes_to_struct(const std::vector<std::byte>& s) {
    static_assert(std::is_standard_layout_v<T>, "Must be standard-layout");
    if (sizeof(T) != s.size()) {
        throw std::runtime_error("Size mismatch");
    }
    T t;
    std::memcpy(&t, s.data(), sizeof(T));
    return t;
}

template <typename K, auto K::* ...Members>
struct KeyPrototype {
    
    friend std::ostream& operator<<(std::ostream& os, const K& k)
    {
        ((os << k.*Members << ", "), ...);
        return os;
    }

    static constexpr unsigned maxFoldLength() { return (0 + ... + sizeof(std::declval<K>().*Members)); }
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

    friend std::ostream& operator<<(std::ostream& os, const T& record)
    {
        ((os << record.*Members << ", "), ...);
        return os;
    }
};

struct part_base {
    static constexpr int id = 0;
    struct key_base {
        static constexpr int id = 0;
        Integer p_partkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
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

struct part_t : public part_base, public RecordPrototype<part_base,
    &part_base::p_name,
    &part_base::p_mfgr,
    &part_base::p_brand,
    &part_base::p_type,
    &part_base::p_size,
    &part_base::p_container,
    &part_base::p_retailprice,
    &part_base::p_comment>
{
    explicit part_t(part_base base) : part_base(base) {}

    part_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::p_partkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::p_partkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static part_t generateRandomRecord()
    {
        return part_t({randomastring<55>(0, 55), randomastring<25>(25, 25), randomastring<10>(10, 10), randomastring<25>(0, 25),
            urand(1, 50) * 5, randomastring<10>(10, 10), randomNumeric(0.0000, 100.0000), randomastring<23>(0, 23)});
    }
};

struct supplier_base {
    static constexpr int id = 1;
    struct key_base {
        static constexpr int id = 1;
        Integer s_suppkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::s_suppkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
    };

    Varchar<25> s_name;
    Varchar<40> s_address;
    Integer s_nationkey;
    Varchar<15> s_phone;
    Numeric s_acctbal;
    Varchar<101> s_comment;
};

struct supplier_t : public supplier_base, public RecordPrototype<supplier_base, &supplier_base::s_name, &supplier_base::s_address, &supplier_base::s_nationkey, &supplier_base::s_phone, &supplier_base::s_acctbal, &supplier_base::s_comment> {
    explicit supplier_t(supplier_base base) : supplier_base(base) {}

    supplier_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::s_suppkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::s_suppkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static supplier_t generateRandomRecord(std::function<int()> generate_nationkey)
    {
        return supplier_t({randomastring<25>(25, 25), randomastring<40>(0, 40), generate_nationkey(), randomastring<15>(15, 15),
            randomNumeric(0.0000, 100.0000), randomastring<101>(0, 101)});
    }
};

struct partsupp_base {
    static constexpr int id = 2;
    struct key_base {
        static constexpr int id = 2;
        Integer ps_partkey;
        Integer ps_suppkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::ps_partkey, &key_base::ps_suppkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
    };

    Integer ps_availqty;
    Numeric ps_supplycost;
    Varchar<199> ps_comment;
};

struct partsupp_t : public partsupp_base, public RecordPrototype<partsupp_base, &partsupp_base::ps_availqty, &partsupp_base::ps_supplycost, &partsupp_base::ps_comment> {
    explicit partsupp_t(partsupp_base base) : partsupp_base(base) {}

    partsupp_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::ps_partkey, &K::key_base::ps_suppkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::ps_partkey, &K::key_base::ps_suppkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static partsupp_t generateRandomRecord()
    {
        return partsupp_t({urand(1, 100000), randomNumeric(0.0000, 100.0000), randomastring<199>(0, 199)});
    }
};

struct customer_base {
    static constexpr int id = 3;
    struct key_base {
        static constexpr int id = 3;
        Integer c_custkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::c_custkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
    };

    Varchar<25> c_name;
    Varchar<40> c_address;
    Integer c_nationkey;
    Varchar<15> c_phone;
    Numeric c_acctbal;
    Varchar<10> c_mktsegment;
    Varchar<117> c_comment;
};

struct customerh_t : public customer_base, public RecordPrototype<customer_base, &customer_base::c_name, &customer_base::c_address, &customer_base::c_nationkey, &customer_base::c_phone, &customer_base::c_acctbal, &customer_base::c_mktsegment, &customer_base::c_comment> {
    explicit customerh_t(customer_base base) : customer_base(base) {}

    customerh_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::c_custkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::c_custkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static customerh_t generateRandomRecord(std::function<int()> generate_nationkey)
    {
        return customerh_t({randomastring<25>(0, 25), randomastring<40>(0, 40), generate_nationkey(), randomastring<15>(15, 15),
            randomNumeric(0.0000, 100.0000), randomastring<10>(10, 10), randomastring<117>(0, 117)});
    }
};

struct orders_base {
    static constexpr int id = 4;
    struct key_base {
        static constexpr int id = 4;
        Integer o_orderkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::o_orderkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
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

struct orders_t : public orders_base, public RecordPrototype<orders_base, &orders_base::o_custkey, &orders_base::o_orderstatus, &orders_base::o_totalprice, &orders_base::o_orderdate, &orders_base::o_orderpriority, &orders_base::o_clerk, &orders_base::o_shippriority, &orders_base::o_comment> {

    explicit orders_t(orders_base base) : orders_base(base) {}

    orders_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::o_orderkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::o_orderkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static orders_t generateRandomRecord(std::function<int()> generate_custkey)
    {
        return orders_t({generate_custkey(),
            randomastring<1>(1, 1), randomNumeric(0.0000, 100.0000), urand(1, 10000), randomastring<15>(15, 15),
            randomastring<15>(15, 15), urand(0, 5), randomastring<79>(0, 79)});
    }
};

struct lineitem_base {
    static constexpr int id = 5;
    struct key_base {
        static constexpr int id = 5;
        Integer l_orderkey;
        Integer l_linenumber;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::l_orderkey, &key_base::l_linenumber> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
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

struct lineitem_t : public lineitem_base, public RecordPrototype<lineitem_base, &lineitem_base::l_partkey, &lineitem_base::l_suppkey, &lineitem_base::l_quantity, &lineitem_base::l_extendedprice, &lineitem_base::l_discount, &lineitem_base::l_tax, &lineitem_base::l_returnflag, &lineitem_base::l_linestatus, &lineitem_base::l_shipdate, &lineitem_base::l_commitdate, &lineitem_base::l_receiptdate, &lineitem_base::l_shipinstruct, &lineitem_base::l_shipmode, &lineitem_base::l_comment> {
    explicit lineitem_t(lineitem_base base) : lineitem_base(base) {}

    lineitem_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::l_orderkey, &K::key_base::l_linenumber>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::l_orderkey, &K::key_base::l_linenumber>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static lineitem_t generateRandomRecord(std::function<int()> generate_partkey, std::function<int()> generate_suppkey)
    {
        return lineitem_t({generate_partkey(), generate_suppkey(), randomNumeric(0.0000, 100.0000), randomNumeric(0.0000, 100.0000), randomNumeric(0.0000, 100.0000), randomNumeric(0.0000, 100.0000),
            randomastring<1>(1, 1), randomastring<1>(1, 1), urand(1, 10000), urand(1, 10000), urand(1, 10000),
            randomastring<25>(25, 25), randomastring<10>(10, 10), randomastring<44>(44, 44)});
    }
};

struct nation_base {
    static constexpr int id = 6;
    struct key_base {
        static constexpr int id = 6;
        Integer n_nationkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::n_nationkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
    };

    Varchar<25> n_name;
    Integer n_regionkey;
    Varchar<152> n_comment;
};

struct nation_t : public nation_base, public RecordPrototype<nation_base, &nation_base::n_name, &nation_base::n_regionkey, &nation_base::n_comment> {
    explicit nation_t(nation_base base) : nation_base(base) {}

    nation_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::n_nationkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::n_nationkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static nation_t generateRandomRecord(std::function<int()> generate_regionkey)
    {
        return nation_t({randomastring<25>(0, 25), generate_regionkey(), randomastring<152>(0, 152)});
    }
};

struct region_base {
    static constexpr int id = 7;
    struct key_base {
        static constexpr int id = 7;
        Integer r_regionkey;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::r_regionkey> {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
    };

    Varchar<25> r_name;
    Varchar<152> r_comment;
};

struct region_t : public region_base, public RecordPrototype<region_base, &region_base::r_name, &region_base::r_comment> {
    explicit region_t(region_base base) : region_base(base) {}

    region_t() = default;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key)
    {
        return RecordPrototype::foldKey<typename K::key_base, &K::key_base::r_regionkey>(out, key);
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key)
    {
        return RecordPrototype::unfoldKey<typename K::key_base, &K::key_base::r_regionkey>(in, key);
    }

    static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

    static region_t generateRandomRecord()
    {
        return region_t({randomastring<25>(0, 25), randomastring<152>(0, 152)});
    }
};