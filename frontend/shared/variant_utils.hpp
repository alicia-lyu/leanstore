#pragma once

#include <variant>
#include "leanstore/KVInterface.hpp"
#include <rocksdb/slice.h>

template <typename... Records>
inline std::pair<std::variant<typename Records::Key...>, std::variant<Records...>> toType(const leanstore::Slice& k, const leanstore::Slice& v)
{
   bool matched = false;
   std::variant<typename Records::Key...> result_key;
   std::variant<Records...> result_rec;

   (([&]() {
       if (!matched && k.size() == Records::maxFoldLength() && v.size() == sizeof(Records)) {
          typename Records::Key key;
          Records::unfoldKey(k.data(), key);
          const Records& rec = *reinterpret_cast<const Records*>(v.data());
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
       if (!matched && k.size() == Records::maxFoldLength() && v.size() == sizeof(Records)) {
          typename Records::Key key;
          Records::unfoldKey(reinterpret_cast<const u8*>(k.data()), key);
          const Records& rec = *reinterpret_cast<const Records*>(v.data());
          matched = true;
          result_key = key;
          result_rec = rec;
       }
    })(),
    ...);
   assert(matched);
   return std::make_pair(result_key, result_rec);
}