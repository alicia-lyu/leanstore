#pragma once

#include <cstdint>
#include <cstring>
#include <ostream>
#include <tuple>
#include <vector>
#include "TableTemplates.hpp"

template <int TID, typename JK, typename... Ts>
struct joined_t {
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

    explicit joined_t(std::tuple<Ts...> tuple) : payloads(std::move(tuple)) {}

    explicit joined_t(Ts... records):
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

    friend std::ostream& operator<<(std::ostream& os, const joined_t& j) {
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
struct merged_t {
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

    merged_t(T payload): payload(std::move(payload)) {}

    merged_t() = default;

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

    friend std::ostream& operator<<(std::ostream& os, const merged_t& m) {
        os << "merged(" << m.payload << ")";
        return os;
    }

    static JK getJK(const JK& jk) {
        return jk;
    }
};

template <typename SK>
struct SKBuilder {}; // sort key builder