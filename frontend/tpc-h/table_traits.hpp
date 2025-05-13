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
      ((os << k.*Members << ", "), ...);
      return os;
   }

   static constexpr unsigned maxFoldLength() { return (0 + ... + sizeof(std::declval<K>().*Members)); }

   static unsigned keyfold(uint8_t* out, const K& key)
   {
      unsigned pos = 0;
      ((key.*Members == 0 ? void() : pos += fold(out + pos, key.*Members)), ...);
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