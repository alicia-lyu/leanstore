#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <ostream>
#include <tuple>
#include <vector>
#include "table_traits.hpp"

template <int TID, typename JK, bool fold_pks, typename... Ts>
struct joined_t {
   static constexpr int id = TID;

   struct Key {
      static constexpr int id = TID;
      JK jk;
      std::tuple<typename Ts::Key...> keys;  // LATER: use a boolean array to indicate which keys should be folded

      Key() = default;
      explicit Key(JK jk, std::tuple<typename Ts::Key...> keys) : jk(jk), keys(std::move(keys)) {}
      explicit Key(JK jk, typename Ts::Key... keys) : jk(jk), keys(std::make_tuple(keys...)) {}

      friend std::ostream& operator<<(std::ostream& os, const Key& key)
      {
         os << "JoinedKey(" << key.jk << ", ";
         std::apply([&](const auto&... args) { ((os << args << ", "), ...); }, key.keys);
         os << ")";
         return os;
      }
   };

   std::tuple<Ts...> payloads;

   joined_t() = default;

   explicit joined_t(std::tuple<Ts...> tuple) : payloads(std::move(tuple)) {}

   explicit joined_t(Ts... records) : payloads(std::make_tuple(records...)) {}

   template <typename K, size_t... Is>
   static unsigned foldKeyHelper(uint8_t* out, const K& key, std::index_sequence<Is...>)
   {
      unsigned pos = 0;
      pos += JK::keyfold(out + pos, key.jk);
      if (fold_pks) {
         ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::foldKey(out + pos, std::get<Is>(key.keys))), ...);
      }
      return pos;
   }

   template <typename K>
   static unsigned foldKey(uint8_t* out, const K& key)
   {
      return foldKeyHelper(out, key, std::index_sequence_for<Ts...>{});
   }

   template <typename K, size_t... Is>
   static unsigned unfoldKeyHelper(const uint8_t* in, K& key, std::index_sequence<Is...>)
   {
      unsigned pos = 0;
      pos += JK::keyunfold(in + pos, key.jk);
      if (fold_pks) {
         ((pos += std::tuple_element_t<Is, std::tuple<Ts...>>::unfoldKey(in + pos, std::get<Is>(key.keys))), ...);
      } else { // reconstruct pks from jk
         ((std::get<Is>(key.keys) = typename Ts::Key{key.jk}), ...);
      }
      return pos;
   }

   template <typename K>
   static unsigned unfoldKey(const uint8_t* in, K& key)
   {
      return unfoldKeyHelper(in, key, std::index_sequence_for<Ts...>{});
   }

   static constexpr unsigned maxFoldLength() { return (JK::maxFoldLength() + ... + Ts::maxFoldLength()); }

   friend std::ostream& operator<<(std::ostream& os, const joined_t& j)
   {
      os << "Joined(";
      std::apply([&](const auto&... args) { ((os << args << ", "), ...); }, j.payloads);
      os << ")";
      return os;
   }

   static JK getJK(const JK& jk) { return jk; }
};

enum class ExtraID {
   NONE,  // When the sort key is a superset of the primary key, and the key length & payload length can distinguish different sources
   PK,    // When the sort key is a subset of the primary key
   PKID   // When the sort key is a superset ofto the primary key, but the key length & payload length cannot distinguish different sources
};

template <int TID, typename T, typename JK, ExtraID extra_id>
struct merged_t {
   static constexpr int id = TID;
   struct Key {
      static constexpr int id = TID;
      JK jk;
      typename T::Key pk;

      friend std::ostream& operator<<(std::ostream& os, const Key& key)
      {
         os << "mergedKey(" << key.jk;
         if (extra_id == ExtraID::PK) {
            os << ", PK " << key.pk;
         } else if (extra_id == ExtraID::PKID) {
            os << ", PKID " << key.pk.id;
         }
         os << ")";
         return os;
      }

      JK get_jk() const { return jk; }
      typename T::Key get_pk() const { return pk; }
   };

   T payload;

   merged_t() = default;

   explicit merged_t(const T& t) : payload(t) {}

   static unsigned foldKey(uint8_t* out, const Key& key)
   {
      unsigned pos = 0;
      pos += JK::keyfold(out + pos, key.jk);
      if constexpr (extra_id == ExtraID::PK)
         pos += T::foldKey(out + pos, key.pk);
      else if constexpr (extra_id == ExtraID::PKID) {
         if (key.pk.id < 0 || key.pk.id > std::numeric_limits<u8>::max())
            throw std::runtime_error("pk.id out of range");
         u8 id = static_cast<u8>(key.pk.id);
         pos += fold(out + pos, id);
      }
      return pos;
   }

   static unsigned unfoldKey(const uint8_t* in, Key& key)
   {
      unsigned pos = 0;
      pos += JK::keyunfold(in + pos, key.jk);
      if (extra_id == ExtraID::PK)
         pos += T::unfoldKey(in + pos, key.pk);
      // else {
      //    key = Key{key.jk};
      // }

      if (extra_id == ExtraID::PKID) {
         u8 id;
         pos += unfold(in + pos, id);
         assert(key.pk.id == id);
      }

      return pos;
   }

   static constexpr unsigned maxFoldLength()
   {
      return 0 + JK::maxFoldLength() + (extra_id == ExtraID::PK ? T::maxFoldLength() : 0) + (extra_id == ExtraID::PKID ? sizeof(u8) : 0);
   }

   template <typename Type>
   static std::vector<std::byte> toBytes(const Type& keyOrRec)
   {
      return struct_to_bytes(&keyOrRec, sizeof(Type));
   }

   template <typename Type>
   static Type fromBytes(const std::vector<std::byte>& s)
   {
      return struct_from_bytes<Type>(s);
   }

   friend std::ostream& operator<<(std::ostream& os, const merged_t& m)
   {
      os << "merged(" << m.payload << ")";
      return os;
   }

   static JK getJK(const JK& jk) { return jk; }
};

template <typename SK>
struct SKBuilder {
   // template <typename K, typename V>
   // static SK inline create(const K&, const V&)
   // {
   //    UNREACHABLE();
   // }
};  // sort key builder