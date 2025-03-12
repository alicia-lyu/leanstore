#pragma once

#include <cstdint>
#include "Tables.hpp"

template <int TID, typename JK, typename... Ts>
class Joined {
   public:
    static constexpr int id = TID;
    struct key_base {
        static constexpr int id = TID;
        JK jk;
        std::tuple<typename Ts::Key...> keys; // LATER: use a boolean array to indicate which keys should be folded
    };

    struct Key : public key_base, public KeyPrototype<key_base, &key_base::jk, &key_base::keys> {};

    std::tuple<Ts...> payloads;

    explicit Joined(std::tuple<Ts...> tuple) : payloads(std::move(tuple)) {}

    template <typename K, size_t... Is>
    static unsigned foldKeyHelper(uint8_t* out, const K& key, std::index_sequence<Is...>) {
        unsigned pos = 0;
        ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::foldKey(out + pos, key)), ...);
        return pos;
    }

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key) {
        return foldKeyHelper(out, key, std::index_sequence_for<Ts...>{});
    }

    template <typename K, size_t... Is>
    static unsigned unfoldKeyHelper(const uint8_t* in, K& key, std::index_sequence<Is...>) {
        unsigned pos = 0;
        ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::unfoldKey(in + pos, key)), ...);
        return pos;
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key) {
        return unfoldKeyHelper(in, key, std::index_sequence_for<Ts...>{});
    }

    static constexpr unsigned maxFoldLength() {
        return (0 + ... + Ts::maxFoldLength());
    }

    static constexpr unsigned rowSize() {
        return (0 + ... + Ts::rowSize());
    }
};

template <int TID, typename T, typename JK, bool foldPK>
struct merged {
    static constexpr int id = TID;
    struct key_base {
        static constexpr int id = TID;
        JK jk;
        T::Key pk;
    };
    struct Key: public key_base, public KeyPrototype<key_base, &key_base::jk, &key_base::pk> {};

    T payload;

    static constexpr unsigned rowSize() { return T::rowSize(); }

    static unsigned foldKey(uint8_t* out, const Key& key) {
        unsigned pos = 0;
        pos += JK::keyfold(out + pos, key.jk);
        if (foldPK)
            pos += T::foldKey(out + pos, key.pk);
        return pos;
    }

    static unsigned unfoldKey(const uint8_t* in, Key& key) {
        unsigned pos = 0;
        pos += JK::keyfold(in + pos, key.jk);
        if (foldPK)
            pos += T::unfoldKey(in + pos, key.pk);
        return pos;
    }

    static constexpr unsigned maxFoldLength() { return 0 + JK::maxFoldLength() + (foldPK ? T::maxFoldLength() : 0); }
};

struct PPsL_JK {
    Integer l_partkey;
    Integer l_partsuppkey;

    static unsigned keyfold(uint8_t* out, const PPsL_JK& key) {
        unsigned pos = 0;
        pos += fold(out + pos, key.l_partkey); // TODO: Only fold the first field for part_t
        pos += fold(out + pos, key.l_partsuppkey);
        return pos;
    };

    static constexpr unsigned maxFoldLength() {
        return sizeof(Integer) + sizeof(Integer);
    }

    bool operator==(const PPsL_JK& other) const {
        return l_partkey == other.l_partkey && l_partsuppkey == other.l_partsuppkey;
    }
};

using merged_part_t = merged<12, part_t, PPsL_JK, false>;

using merged_partsupp_t = merged<12, partsupp_t, PPsL_JK, false>;

using merged_lineitem_t = merged<12, lineitem_t, PPsL_JK, true>;

struct joinedPPs_t : public Joined<11, PPsL_JK, part_t, partsupp_t> {
    joinedPPs_t(merged_part_t p, merged_partsupp_t ps): Joined<11, PPsL_JK, part_t, partsupp_t>(std::make_tuple(p.payload, ps.payload)) {};
};

struct joinedPPsL_t : public Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t> {
    joinedPPsL_t(merged_part_t p, merged_partsupp_t ps, merged_lineitem_t l): Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t>(std::make_tuple(p.payload, ps.payload, l.payload)) {}
};