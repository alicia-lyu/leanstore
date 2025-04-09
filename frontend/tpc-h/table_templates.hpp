#pragma once
#include <cstring>
#include <ostream>
#include <vector>
#include "../shared/Types.hpp"

template <typename T>
inline T bytes_to_struct(const std::vector<std::byte>& s)
{
   static_assert(std::is_standard_layout_v<T>, "Must be standard-layout");
   if (sizeof(T) != s.size()) {
      throw std::runtime_error("Size mismatch: expected " + std::to_string(sizeof(T)) + ", got " + std::to_string(s.size()));
   }
   T t;
   std::memcpy(&t, s.data(), sizeof(T));
   return t;
}

template <typename K, auto K::*... Members>
struct KeyPrototype {
   friend std::ostream& operator<<(std::ostream& os, const K& k)
   {
      ((os << k.*Members << ", "), ...);
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

template <typename T, auto T::*... Members>
struct RecordPrototype {
   template <typename K>
   static unsigned foldKey(uint8_t* out, const K& key)
   {
      return K::keyfold(out, key);
   }

   template <typename K>
   static unsigned unfoldKey(const uint8_t* in, K& key)
   {
      return K::keyunfold(in, key);
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