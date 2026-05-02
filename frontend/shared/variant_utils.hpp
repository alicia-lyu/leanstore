#pragma once

#include <cstring>
#include <variant>
#include "leanstore/KVInterface.hpp"
#include <rocksdb/slice.h>

// -------------------------------------------------------------------------------------
// Opt-in discrimination hook.
//
// A record type may define:
//   static bool accepts_key(const u8* key_bytes, size_t key_len);
//
// When present, toType() calls it *before* the legacy (maxFoldLength, sizeof)
// check.  If it returns true the record wins; no other record is tried.
// Records that do not define `accepts_key` fall through to the legacy path unchanged.
// -------------------------------------------------------------------------------------
template <typename R>
concept HasAcceptsKey = requires(const u8* key_bytes, size_t key_len) {
   { R::accepts_key(key_bytes, key_len) } -> std::same_as<bool>;
};

// Dispatch helper: try the accepts_key() hook if present, otherwise fall back to
// the legacy (maxFoldLength == key_len && sizeof(R) == val_len) check.
// key_bytes is accepted as const void* to avoid deduction failures when the
// caller's pointer element type differs (u8 vs char).
template <typename R>
inline bool record_matches(const void* key_bytes, size_t key_len, size_t val_len)
{
   if constexpr (HasAcceptsKey<R>) {
      return R::accepts_key(reinterpret_cast<const u8*>(key_bytes), key_len);
   } else {
      return key_len == R::maxFoldLength() && val_len == sizeof(R);
   }
}

template <typename... Records>
inline std::pair<std::variant<typename Records::Key...>, std::variant<Records...>> toType(const leanstore::Slice& k, const leanstore::Slice& v)
{
   bool matched = false;
   std::variant<typename Records::Key...> result_key;
   std::variant<Records...> result_rec;

   (([&]() {
       if (!matched && record_matches<Records>(k.data(), k.size(), v.size())) {
          typename Records::Key key;
          Records::unfoldKey(k.data(), key);
          // RocksDB does not guarantee 8-byte alignment of value buffers;
          // reinterpret_cast on misaligned data is UB and the fields contain doubles.
          Records rec;
          std::memcpy(&rec, v.data(), sizeof(Records));
          matched = true;
          result_key = key;
          result_rec = rec;
       }
    })(),
    ...);
   assert(matched);
   return std::make_pair(result_key, result_rec);
}

template <typename... Records>
inline std::pair<std::variant<typename Records::Key...>, std::variant<Records...>> toType(const rocksdb::Slice& k, const rocksdb::Slice& v)
{
   bool matched = false;
   std::variant<typename Records::Key...> result_key;
   std::variant<Records...> result_rec;

   (([&]() {
       if (!matched && record_matches<Records>(k.data(), k.size(), v.size())) {
          typename Records::Key key;
          Records::unfoldKey(reinterpret_cast<const u8*>(k.data()), key);
          // RocksDB does not guarantee 8-byte alignment of value buffers;
          // reinterpret_cast on misaligned data is UB and the fields contain doubles.
          Records rec;
          std::memcpy(&rec, v.data(), sizeof(Records));
          matched = true;
          result_key = key;
          result_rec = rec;
       }
    })(),
    ...);
   assert(matched);
   return std::make_pair(result_key, result_rec);
}