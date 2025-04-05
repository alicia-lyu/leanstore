#pragma once

// For more complex group views, we may need generic functions for GK. Here, for each view, its key is identical to GK.

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

// id range: (20s)

#include "TableTemplates.hpp"

namespace basic_group {
    struct count_part_base {
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

    struct count_part_t: public count_part_base, public RecordPrototype<count_part_base, &count_part_base::count> {
        explicit count_part_t(count_part_base base): count_part_base(base) {}
        count_part_t() = default;
    };
    
    struct avg_supplycost_base {
        static constexpr int id = 21;
        struct key_base {
            static constexpr int id = 21;
            Integer p_partkey;
        };
    
        struct Key: public key_base, public KeyPrototype<key_base, &key_base::p_partkey> {
            Key() = default;
            Key(const key_base& k): key_base(k) {}
        };
    
        Numeric avg_supplycost;
    };

    struct avg_supplycost_t: public avg_supplycost_base, public RecordPrototype<avg_supplycost_base, &avg_supplycost_base::avg_supplycost> {
        explicit avg_supplycost_t(avg_supplycost_base base): avg_supplycost_base(base) {}
        avg_supplycost_t() = default;
    };
}