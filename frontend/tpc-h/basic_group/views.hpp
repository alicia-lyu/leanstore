#pragma once

// For more complex group views, we may need generic functions for GK. Here, for each view, its key is identical to GK.

// SELECT partkey, COUNT(*), AVG(supplycost)
// FROM PartSupp
// GROUP BY partkey;

// id range: (20s)

#include "../randutils.hpp"
#include "../tables.hpp"
#include "../view_templates.hpp"

namespace basic_group
{

struct sort_key_t {
   Integer partkey;
   using Key = sort_key_t;
   ADD_KEY_TRAITS(&sort_key_t::partkey)
};

struct sort_key_variant_t {
   u8 id;
   Integer partkey;
   using Key = sort_key_variant_t;
   ADD_KEY_TRAITS(&sort_key_variant_t::id, &sort_key_variant_t::partkey)
};

struct view_t {
   static constexpr int id = 22;
   struct Key {
      static constexpr int id = 22;
      Integer p_partkey;
      ADD_KEY_TRAITS(&Key::p_partkey)
   };

   Integer count_partsupp;
   Numeric sum_supplycost;

   ADD_RECORD_TRAITS(view_t)

   static view_t generateRandomRecord() { return view_t({randutils::urand(1, 10000), randutils::randomNumeric(0.0000, 100.0000)}); }
};

struct merged_view_t : public merged_t<22, view_t, sort_key_t, ExtraID::NONE> {
   template <typename... Args>
   explicit merged_view_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer partkey) : merged_t::Key{sort_key_t{partkey}, view_t::Key{partkey}} {}
      Key(const view_t::Key& pk) : merged_t::Key{sort_key_t{pk.p_partkey}, pk} {}
      Key(const sort_key_t& sk, view_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_partsupp_t : public merged_t<23, partsupp_t, sort_key_t, ExtraID::PK> {
   template <typename... Args>
   explicit merged_partsupp_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer partkey, Integer supplierkey) : merged_t::Key{sort_key_t{partkey}, partsupp_t::Key{partkey, supplierkey}} {}
      Key(const partsupp_t::Key& pk) : merged_t::Key{sort_key_t{pk.ps_partkey}, pk} {}
      Key(const sort_key_t& sk, partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_view_variant_t : public merged_t<22, view_t, sort_key_variant_t, ExtraID::NONE> {
   template <typename... Args>
   explicit merged_view_variant_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer partkey) : merged_t::Key{sort_key_variant_t{view_t::id, partkey}, view_t::Key{partkey}} {}
      Key(const view_t::Key& pk) : merged_t::Key{sort_key_variant_t{view_t::id, pk.p_partkey}, pk} {}
      Key(const sort_key_variant_t& sk, view_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_partsupp_variant_t : public merged_t<23, partsupp_t, sort_key_variant_t, ExtraID::PK> {
   template <typename... Args>
   explicit merged_partsupp_variant_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(Integer partkey, Integer supplierkey) : merged_t::Key{sort_key_variant_t{partsupp_t::id, partkey}, partsupp_t::Key{partkey, supplierkey}} {}
      Key(const partsupp_t::Key& pk) : merged_t::Key{sort_key_variant_t{partsupp_t::id, pk.ps_partkey}, pk} {}
      Key(const sort_key_variant_t& sk, partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};
}  // namespace basic_group