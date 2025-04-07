#pragma once

// For more complex group views, we may need generic functions for GK. Here, for each view, its key is identical to GK.

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

// id range: (20s)

#include "TableTemplates.hpp"

namespace basic_group {
    struct count_partsupp_base {
        static constexpr int id = 20;
        struct key_base {
            static constexpr int id = 20;
            Integer p_partkey;
        };
    
        struct Key: public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {
            Key() = default;
            Key(const key_base& k): key_base(k) {}
        };
    
        Integer count;
    };

    struct count_partsupp_t: public count_partsupp_base, public RecordPrototype<count_partsupp_base, &count_partsupp_base::count> {
        explicit count_partsupp_t(count_partsupp_base base): count_partsupp_base(base) {}
        count_partsupp_t() = default;
    };
    
    struct sum_supplycost_base {
        static constexpr int id = 21;
        struct key_base {
            static constexpr int id = 21;
            Integer p_partkey;
        };
    
        struct Key: public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {
            Key() = default;
            Key(const key_base& k): key_base(k) {}
        };
    
        Numeric sum_supplycost;
    };

    struct sum_supplycost_t: public sum_supplycost_base, public RecordPrototype<sum_supplycost_base, &sum_supplycost_base::sum_supplycost> {
        explicit sum_supplycost_t(sum_supplycost_base base): sum_supplycost_base(base) {}
        sum_supplycost_t() = default;
    };

    struct view_base {
        static constexpr int id = 22;
        struct key_base {
            static constexpr int id = 22;
            Integer p_partkey;
        };
    
        struct Key: public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {
            Key() = default;
            Key(const key_base& k): key_base(k) {}
        };
    
        Integer count_partsupp;
        Numeric sum_supplycost;
    };

    struct view_t: public view_base, public RecordPrototype<view_base, &view_base::count_partsupp, &view_base::sum_supplycost> {
        explicit view_t(view_base base): view_base(base) {}
        view_t() = default;
    };
}