#pragma once
#include <cstring>
#include <ostream>
#include <type_traits>
#include <vector>
#include "../shared/Types.hpp"

template <typename K, auto K::*... Members>
struct key_traits {
   static std::ostream& print(std::ostream& os, const K& k)
   {
      ((os << k.*Members << ","), ...);
      return os;
   }

   static constexpr unsigned maxFoldLength() { return (0 + ... + sizeof(std::declval<K>().*Members)); }

   static unsigned keyfold(uint8_t* out, const K& key)
   {
      unsigned pos = 0;
      ((pos += fold(out + pos, key.*Members)), ...);
      return pos;
   }

   static unsigned keyunfold(const uint8_t* in, K& key)
   {
      unsigned pos = 0;
      ((pos += unfold(in + pos, key.*Members)), ...);
      return pos;
   }
};

inline std::vector<std::byte> struct_to_bytes(const void* s, size_t size)
{
   std::vector<std::byte> v(size);
   std::memcpy(v.data(), s, size);
   return v;
}

template <typename T>
inline T struct_from_bytes(const std::vector<std::byte>& s)
{
   static_assert(std::is_standard_layout_v<T>, "Must be standard-layout");
   if (sizeof(T) != s.size()) {
      throw std::runtime_error("Size mismatch: expected " + std::to_string(sizeof(T)) + ", got " + std::to_string(s.size()));
   }
   T t;
   std::memcpy(&t, s.data(), sizeof(T));
   return t;
}

template <typename K, typename T>
struct record_traits {
   static unsigned foldKey(uint8_t* out, const K& key) { return K::keyfold(out, key); }

   static unsigned unfoldKey(const uint8_t* in, K& key) { return K::keyunfold(in, key); }

   template <typename Type>
   static std::vector<std::byte> toBytes(Type s)
   {
      return struct_to_bytes(&s, sizeof(Type));
   }

   template <typename Type>
   static Type fromBytes(const std::vector<std::byte>& s)
   {
      return struct_from_bytes<Type>(s);
   }

   static std::ostream& print(std::ostream& os, const T&)
   {
      os << "Record(size " << sizeof(T) << "): ";
      return os;
   }
};

// With part_base (aggregate) and part_t (with traits) defined separately
// The only way to inherit aggregate initialization (either with braces or parentheses) without restating all the members
// is to use parameter forwarding: using part_base::part_base directive does not inherit aggregate initialization
// But with user-defined constructors, parameter forwarding becomes a universal fallback and shadows useful compiling info.
// Then, we must restate all the members

// So, for base tables and their keys, do not have a saparate part_base and use composition for traits
// for derived classes of joined_t and merged_t, use parameter forwarding
// for derived classes of joined_t::Key and merged_t::Key, restate all the members

// With inheritance, keeping aggregate-like structure is possible in C++ 20 but still awkward
// It indeed becomes impossible with user-defined constructors

#define ADD_KEY_TRAITS(...)                                          \
   using traits = key_traits<Key, __VA_ARGS__>;                      \
   static unsigned keyfold(uint8_t* out, const Key& key)             \
   {                                                                 \
      return traits::keyfold(out, key);                              \
   }                                                                 \
   static unsigned keyunfold(const uint8_t* in, Key& key)            \
   {                                                                 \
      return traits::keyunfold(in, key);                             \
   }                                                                 \
   static constexpr unsigned maxFoldLength()                         \
   {                                                                 \
      return traits::maxFoldLength();                                \
   }                                                                 \
   friend std::ostream& operator<<(std::ostream& os, const Key& key) \
   {                                                                 \
      return traits::print(os, key);                                 \
   }

#define ADD_RECORD_TRAITS(RECORD_TYPE)                                       \
   using traits = record_traits<Key, RECORD_TYPE>;                           \
   static unsigned foldKey(uint8_t* out, const Key& key)                     \
   {                                                                         \
      return traits::foldKey(out, key);                                      \
   }                                                                         \
   static unsigned unfoldKey(const uint8_t* in, Key& key)                    \
   {                                                                         \
      return traits::unfoldKey(in, key);                                     \
   }                                                                         \
   template <typename Type>                                                  \
   static Type fromBytes(const std::vector<std::byte>& s)                    \
   {                                                                         \
      return traits::fromBytes<Type>(s);                                     \
   }                                                                         \
   static std::vector<std::byte> toBytes(const RECORD_TYPE& rec)             \
   {                                                                         \
      return traits::toBytes(rec);                                           \
   }                                                                         \
   static std::vector<std::byte> toBytes(const Key& key)                     \
   {                                                                         \
      return traits::toBytes(key);                                           \
   }                                                                         \
   static constexpr unsigned maxFoldLength()                                 \
   {                                                                         \
      return Key::maxFoldLength();                                           \
   }                                                                         \
   friend std::ostream& operator<<(std::ostream& os, const RECORD_TYPE& rec) \
   {                                                                         \
      rec.print(os);                                                         \
      return os;                                                             \
   }