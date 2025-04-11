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

struct count_partsupp_t {
   static constexpr int id = 20;
   struct Key {
      static constexpr int id = 20;
      Integer p_partkey;
      ADD_KEY_TRAITS(&Key::p_partkey)
   };

   Integer count;

   ADD_RECORD_TRAITS(count_partsupp_t)

   static count_partsupp_t generateRandomRecord() { return count_partsupp_t({randutils::urand(1, 10000)}); }
};

struct sum_supplycost_t {
   static constexpr int id = 21;
   struct Key {
      static constexpr int id = 21;
      Integer p_partkey;
      ADD_KEY_TRAITS(&Key::p_partkey)
   };

   Numeric sum_supplycost;

   ADD_RECORD_TRAITS(sum_supplycost_t)

   static sum_supplycost_t generateRandomRecord() { return sum_supplycost_t({randutils::randomNumeric(0.0000, 100.0000)}); }
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

struct merged_sum_supplycost_t : public merged_t<23, sum_supplycost_t, sort_key_t, ExtraID::PKID> {
   template <typename... Args>
   explicit merged_sum_supplycost_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const sum_supplycost_t::Key& pk) : merged_t::Key{sort_key_t{pk.p_partkey}, pk} {}
      template <typename... Args>
      Key(const sort_key_t& sk, sum_supplycost_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_count_partsupp_t : public merged_t<23, count_partsupp_t, sort_key_t, ExtraID::PKID> {
   template <typename... Args>
   explicit merged_count_partsupp_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const count_partsupp_t::Key& pk) : merged_t::Key{sort_key_t{pk.p_partkey}, pk} {}
      Key(const sort_key_t& sk, count_partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_partsupp_t : public merged_t<23, partsupp_t, sort_key_t, ExtraID::PK> {
   template <typename... Args>
   explicit merged_partsupp_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const partsupp_t::Key& pk) : merged_t::Key{sort_key_t{pk.ps_partkey}, pk} {}
      Key(const sort_key_t& sk, partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};

struct merged_count_variant_t : public merged_t<23, count_partsupp_t, sort_key_variant_t, ExtraID::NONE> {
   template <typename... Args>
   explicit merged_count_variant_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const count_partsupp_t::Key& pk) : merged_t::Key{sort_key_variant_t{count_partsupp_t::id, pk.p_partkey}, pk} {}
      Key(const sort_key_variant_t& sk, count_partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};
struct merged_sum_variant_t : public merged_t<23, sum_supplycost_t, sort_key_variant_t, ExtraID::NONE> {
   template <typename... Args>
   explicit merged_sum_variant_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const sum_supplycost_t::Key& pk) : merged_t::Key{sort_key_variant_t{sum_supplycost_t::id, pk.p_partkey}, pk} {}
      Key(const sort_key_variant_t& sk, sum_supplycost_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};
struct merged_partsupp_variant_t : public merged_t<23, partsupp_t, sort_key_variant_t, ExtraID::PK> {
   template <typename... Args>
   explicit merged_partsupp_variant_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }
   struct Key : public merged_t::Key {
      Key() = default;
      Key(const partsupp_t::Key& pk) : merged_t::Key{sort_key_variant_t{partsupp_t::id, pk.ps_partkey}, pk} {}
      Key(const sort_key_variant_t& sk, partsupp_t::Key&& pk) : merged_t::Key{sk, pk} {}
   };
};
}  // namespace basic_group