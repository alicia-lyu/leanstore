#pragma once
#include <ostream>
#include <vector>
#include <cstring>
#include "../shared/Types.hpp"


template <typename T>
inline T bytes_to_struct(const std::vector<std::byte>& s) {
    static_assert(std::is_standard_layout_v<T>, "Must be standard-layout");
    if (sizeof(T) != s.size()) {
        throw std::runtime_error("Size mismatch: expected " + std::to_string(sizeof(T)) + ", got " + std::to_string(s.size()));
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

    friend std::ostream& operator<<(std::ostream& os, const T& record)
    {
        ((os << record.*Members << ", "), ...);
        return os;
    }
};