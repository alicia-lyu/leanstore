#pragma once

// For more complex group views, we may need generic functions for GK. Here, for each view, its key is identical to GK.

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

// id range: (20s)

#include "TableTemplates.hpp"
#include "Tables.hpp"
#include "ViewTemplates.hpp"
#include "randutils.hpp"

namespace basic_group {
    struct sort_key_base {
        Integer partkey;
    };

    struct sort_key_t : public sort_key_base, public KeyPrototype<sort_key_base, &sort_key_base::partkey> {
        sort_key_t() = default;
        sort_key_t(Integer partkey): sort_key_base{partkey} {}
    };
    
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

        using count_partsupp_base::Key;

        static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

        static count_partsupp_t generateRandomRecord()
        {
            return count_partsupp_t({randutils::urand(1, 10000)});
        }
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

        using sum_supplycost_base::Key;

        static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

        static sum_supplycost_t generateRandomRecord()
        {
            return sum_supplycost_t({randutils::randomNumeric(0.0000, 100.0000)});
        }
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

        using view_base::Key;

        static constexpr unsigned maxFoldLength() { return Key::maxFoldLength(); }

        static view_t generateRandomRecord()
        {
            return view_t({randutils::urand(1, 10000), randutils::randomNumeric(0.0000, 100.0000)});
        }
    };

    using merged_sum_supplycost_t = merged_t<23, sum_supplycost_t, sort_key_t, ExtraID::PKID>;
    using merged_count_partsupp_t = merged_t<23, count_partsupp_t, sort_key_t, ExtraID::PKID>;
    struct merged_partsupp_t: public partsupp_t {
        using partsupp_t::partsupp_t;
        using partsupp_t::Key;
        static constexpr int id = 23;
    };
}