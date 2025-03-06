#pragma once

#include "Tables.hpp"

#define DEFINE_PROJECTED_TYPE(ProjectedName, BaseType, ...) \
    struct ProjectedName##base : { \
        using Base = BaseType; \
        (void)std::initializer_list<int>{((void)__VA_ARGS__, 0)...}; /* Ensure members exist */ \
    }; \
    struct ProjectedName : public RecordPrototype<ProjectedName, __VA_ARGS__> {};

DEFINE_PROJECTED_TYPE(part_proj, part_t, &part_t::p_name)

#define JoinedViewName(T1, T2) join_##T1##_##T2

template <typename T1, typename T2, typename K1, typename K2>
class JoinedViewName(T1, T2) {
   public:
    struct key_base {
        K1 key1;
        K2 key2;
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::key1, &key_base::key2> {};

    T1 record1;
    T2 record2;

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key) {
        unsigned pos = 0;
        pos += T1::foldKey(out + pos, key.key1);
        pos += T2::foldKey(out + pos, key.key2);
        return pos;
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key) {
        unsigned pos = 0;
        pos += T1::unfoldKey(in + pos, key.key1);
        pos += T2::unfoldKey(in + pos, key.key2);
        return pos;
    }

    static constexpr unsigned maxFoldLength() { return T1::maxFoldLength() + T2::maxFoldLength(); }

    static constexpr unsigned rowSize() { return T1::rowSize() + T2::rowSize(); }
    
};