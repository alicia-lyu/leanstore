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

    struct Key : public key_base {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}

        friend std::ostream& operator<<(std::ostream& os, const Key& key) {
            os << "JoinedKey(" << key.jk << ", ";
            std::apply([&](const auto&... args) { ((os << args << ", "), ...); }, key.keys);
            os << ")";
            return os;
        }
    };

    std::tuple<Ts...> payloads;

    explicit Joined(std::tuple<Ts...> tuple) : payloads(std::move(tuple)) {}

    explicit Joined(Ts... records):
        payloads(std::make_tuple(records...)) {}

    template <typename K, size_t... Is>
    static unsigned foldKeyHelper(uint8_t* out, const K& key, std::index_sequence<Is...>) {
        unsigned pos = 0;
        pos += JK::keyfold(out + pos, key.jk);
        ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::foldKey(out + pos, std::get<Is>(key.keys))), ...);
        return pos;
    }

    template <typename K>
    static unsigned foldKey(uint8_t* out, const K& key) {
        return foldKeyHelper(out, key, std::index_sequence_for<Ts...>{});
    }

    template <typename K, size_t... Is>
    static unsigned unfoldKeyHelper(const uint8_t* in, K& key, std::index_sequence<Is...>) {
        unsigned pos = 0;
        pos += JK::keyunfold(in + pos, key.jk);
        ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::unfoldKey(in + pos, std::get<Is>(key.keys))), ...);
        return pos;
    }

    template <typename K>
    static unsigned unfoldKey(const uint8_t* in, K& key) {
        return unfoldKeyHelper(in, key, std::index_sequence_for<Ts...>{});
    }

    static constexpr unsigned maxFoldLength() {
        return (JK::maxFoldLength() + ... + Ts::maxFoldLength());
    }

    friend std::ostream& operator<<(std::ostream& os, const Joined& j) {
        os << "Joined(";
        std::apply([&](const auto&... args) { ((os << args << ", "), ...); }, j.payloads);
        os << ")";
        return os;
    }

    static JK getJK(const JK& jk) {
        return jk;
    }
};

template <int TID, typename T, typename JK, bool foldPK>
struct merged {
    static constexpr int id = TID;
    struct key_base {
        static constexpr int id = TID;
        JK jk;
        typename T::Key pk;
    };
    struct Key: public key_base {
        Key() = default;
        Key(const key_base& k) : key_base(k) {}
        Key(const typename T::Key& pk, const T& v) : key_base({JK(pk, v), pk}) {}
        Key(const JK& jk, const typename T::Key& pk) : key_base({jk, pk}) {}
        Key(const JK&, const Key& k) : key_base(k) {}

        friend std::ostream& operator<<(std::ostream& os, const Key& key) {
            os << "mergedKey(" << key.jk;
            if (foldPK) {
                os << ", " << key.pk;
            }
            os << ")";
            return os;
        }
    };

    T payload;

    merged(T payload): payload(std::move(payload)) {}

    merged() = default;

    static unsigned foldKey(uint8_t* out, const Key& key) {
        unsigned pos = 0;
        pos += JK::keyfold(out + pos, key.jk);
        if (foldPK)
            pos += T::foldKey(out + pos, key.pk);
        return pos;
    }

    static unsigned unfoldKey(const uint8_t* in, Key& key) {
        unsigned pos = 0;
        pos += JK::keyunfold(in + pos, key.jk);
        if (foldPK)
            pos += T::unfoldKey(in + pos, key.pk);
        return pos;
    }

    static constexpr unsigned maxFoldLength() { return 0 + JK::maxFoldLength() + (foldPK ? T::maxFoldLength() : 0); }

    template <typename Type>
    static std::vector<std::byte> toBytes(const Type& keyOrRec)
    {
        std::vector<std::byte> bytes(sizeof(keyOrRec));
        std::memcpy(bytes.data(), &keyOrRec, sizeof(keyOrRec));
        return bytes;
    }

    template <typename Type>
    static Type fromBytes(const std::vector<std::byte>& s)
    {
        return bytes_to_struct<Type>(s);
    }

    friend std::ostream& operator<<(std::ostream& os, const merged& m) {
        os << "merged(" << m.payload << ")";
        return os;
    }

    static JK getJK(const JK& jk) {
        return jk;
    }
};

struct joinedPPs_t;

struct PPsL_JK {
    Integer l_partkey;
    Integer l_partsuppkey;

    PPsL_JK() = default;
    PPsL_JK(Integer partkey, Integer partsuppkey): l_partkey(partkey), l_partsuppkey(partsuppkey) {}
    PPsL_JK(const PPsL_JK& jk): l_partkey(jk.l_partkey), l_partsuppkey(jk.l_partsuppkey) {}

    static PPsL_JK max() {
        return PPsL_JK({std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()});
    }

    static unsigned keyfold(uint8_t* out, const PPsL_JK& key) {
        unsigned pos = 0;
        pos += fold(out + pos, key.l_partkey); // TODO: Only fold the first field for part_t
        pos += fold(out + pos, key.l_partsuppkey);
        return pos;
    };

    static unsigned keyunfold(const uint8_t* in, PPsL_JK& key) {
        unsigned pos = 0;
        pos += unfold(in + pos, key.l_partkey);
        pos += unfold(in + pos, key.l_partsuppkey);
        return pos;
    };

    static constexpr unsigned maxFoldLength() {
        return sizeof(Integer) + sizeof(Integer);
    }

    friend std::ostream& operator<<(std::ostream& os, const PPsL_JK& jk) {
        os << "PPsL_JK(" << jk.l_partkey << ", " << jk.l_partsuppkey << ")";
        return os;
    }

    auto operator<=>(const PPsL_JK&) const = default;

    int match(const PPsL_JK& other) const {
        // {0, 0} cannot be used as wildcard
        if (*this == PPsL_JK{} && other == PPsL_JK{})
            return 0;
        else if (*this == PPsL_JK{})
            return -1;
        else if (other == PPsL_JK{})
            return 1;

        if (l_partkey != 0 && other.l_partkey != 0 && l_partkey != other.l_partkey)
            return l_partkey - other.l_partkey;
        if (l_partsuppkey != 0 && other.l_partsuppkey != 0)
            return l_partsuppkey - other.l_partsuppkey;
        return 0;
    }
};

struct merged_part_t : public merged<12, part_t, PPsL_JK, false> {
    using merged::merged;

    static PPsL_JK getJK(const PPsL_JK& jk) {
        return {jk.l_partkey, 0};
    }
};

using merged_partsupp_t = merged<12, partsupp_t, PPsL_JK, false>;

using merged_lineitem_t = merged<12, lineitem_t, PPsL_JK, true>;

struct joinedPPs_t : public Joined<11, PPsL_JK, part_t, partsupp_t> {
    using Joined::Joined;

    joinedPPs_t(merged_part_t p, merged_partsupp_t ps): Joined<11, PPsL_JK, part_t, partsupp_t>(std::make_tuple(p.payload, ps.payload)) {}

    struct Key: public Joined::Key {
        Key() = default;
        Key(const Joined::Key& k): Joined::Key(k) {}
        Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk): Joined::Key({psk.jk, std::tuple_cat(std::make_tuple(pk.pk), std::make_tuple(psk.pk))}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk): Joined::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk)}) {}
    };
};

struct joinedPPsL_t : public Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t> {
    using Joined::Joined;
    joinedPPsL_t(merged_part_t p, merged_partsupp_t ps, merged_lineitem_t l): Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t>(std::make_tuple(p.payload, ps.payload, l.payload)) {}

    joinedPPsL_t(part_t p, partsupp_t ps, merged_lineitem_t l): Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t>(std::make_tuple(p, ps, l.payload)) {}

    joinedPPsL_t(joinedPPs_t j, merged_lineitem_t l): Joined<12, PPsL_JK, part_t, partsupp_t, lineitem_t>(std::tuple_cat(j.payloads, std::make_tuple(l.payload))) {}

    struct Key: public Joined::Key {
        Key() = default;
        Key(const Joined::Key& k): Joined::Key(k) {}
        Key(const joinedPPs_t::Key& j1k, const merged_lineitem_t::Key& lk): Joined::Key({j1k.jk, std::tuple_cat(j1k.keys, std::make_tuple(lk.pk))}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk, const lineitem_t::Key& lk): Joined::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk, lk)}) {}
        Key(const part_t::Key& pk, const partsupp_t::Key& psk, const merged_lineitem_t::Key& lk): Joined::Key({PPsL_JK{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk, lk.pk)}) {}
        Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk, const merged_lineitem_t::Key& lk): Joined::Key({PPsL_JK{pk.jk.l_partkey, psk.jk.l_partsuppkey}, std::make_tuple(pk.pk, psk.pk, lk.pk)}) {}

    };
};

template <typename JK>
struct JKBuilder;
template <>
struct JKBuilder<PPsL_JK> {

    static PPsL_JK inline create(const part_t::Key& k, const part_t&){
        return PPsL_JK(k.p_partkey, 0);
    }

    static PPsL_JK inline create(const partsupp_t::Key& k, const partsupp_t&) {
        return PPsL_JK(k.ps_partkey, k.ps_suppkey);
    }

    static PPsL_JK inline create(const lineitem_t::Key&, const lineitem_t& v) {
        return PPsL_JK(v.l_partkey, v.l_suppkey);
    }

    static PPsL_JK inline create(const joinedPPs_t::Key& k, const joinedPPs_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const joinedPPsL_t::Key& k, const joinedPPsL_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_part_t::Key& k, const merged_part_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_partsupp_t::Key& k, const merged_partsupp_t&) {
        return PPsL_JK(k.jk);
    }

    static PPsL_JK inline create(const merged_lineitem_t::Key& k, const merged_lineitem_t&) {
        return PPsL_JK(k.jk);
    }

    template <typename Record>
    static PPsL_JK inline get(const PPsL_JK& k) {
        return k;
    }
};

template <>
inline PPsL_JK JKBuilder<PPsL_JK>::get<part_t>(const PPsL_JK& jk) {
    return {jk.l_partkey, 0};
}

template <>
inline PPsL_JK JKBuilder<PPsL_JK>::get<merged_part_t>(const PPsL_JK& jk) {
    return {jk.l_partkey, 0};
}