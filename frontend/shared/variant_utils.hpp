#pragma once

#include <cstring>
#include <variant>
#include "leanstore/KVInterface.hpp"
#include <rocksdb/slice.h>

// -------------------------------------------------------------------------------------
// Opt-in discrimination hook.
//
// A record type may expose the hook in either of two equivalent locations:
//   static bool R::accepts_key(const u8* key_bytes, size_t key_len);
//   static bool R::Key::accepts_key(const u8* key_bytes, size_t key_len);
//
// The COLI tagged-key types (views_coli.hpp) place the hook on the inner
// `Key` because that is the type that owns the byte layout. Earlier
// versions of this file only checked `R::accepts_key`; for the COLI
// types that hook is invisible, so dispatch fell through to the legacy
// (maxFoldLength, sizeof) heuristic — which mis-routes records whose
// `maxFoldLength()` and `sizeof(R)` happen to coincide with another
// co-resident type (e.g. orders_coli_t and invoice_coli_t both have
// 12-byte tagged keys and 16-byte payloads, so the first one in the
// `Records...` pack wins for both record types). This produced the
// `tag == static_cast<u8>(Tag)` assertion failure in views_coli.hpp:110
// when the wrong unfoldKey was invoked.
//
// When EITHER hook is present, toType() calls it *before* the legacy
// (maxFoldLength, sizeof) check. If it returns true the record wins;
// no other record is tried. Records that do not define `accepts_key`
// at either level fall through to the legacy path unchanged.
// -------------------------------------------------------------------------------------
template <typename R>
concept HasAcceptsKey = requires(const u8* key_bytes, size_t key_len) {
   { R::accepts_key(key_bytes, key_len) } -> std::same_as<bool>;
};

template <typename R>
concept HasKeyAcceptsKey = requires(const u8* key_bytes, size_t key_len) {
   { R::Key::accepts_key(key_bytes, key_len) } -> std::same_as<bool>;
};

// Dispatch helper: try the accepts_key() hook if present (on R or R::Key),
// otherwise fall back to the legacy (maxFoldLength == key_len &&
// sizeof(R) == val_len) check. key_bytes is accepted as const void* to
// avoid deduction failures when the caller's pointer element type differs
// (u8 vs char).
template <typename R>
inline bool record_matches(const void* key_bytes, size_t key_len, size_t val_len)
{
   if constexpr (HasAcceptsKey<R>) {
      return R::accepts_key(reinterpret_cast<const u8*>(key_bytes), key_len);
   } else if constexpr (HasKeyAcceptsKey<R>) {
      return R::Key::accepts_key(reinterpret_cast<const u8*>(key_bytes), key_len);
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