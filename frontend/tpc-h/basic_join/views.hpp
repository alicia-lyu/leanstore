#pragma once

#include "../tables.hpp"
#include "../view_templates.hpp"

// id range: (10s) 11-14

namespace basic_join
{
struct join_key_base {
   Integer l_partkey;
   Integer suppkey;

   auto operator<=>(const join_key_base&) const = default;
};

struct join_key_t : public join_key_base, public key_traits<join_key_base, &join_key_base::l_partkey, &join_key_base::suppkey> {
   join_key_t() = default;

   template <typename... Args>
   explicit join_key_t(Args&&... args) : join_key_base{std::forward<Args>(args)...}
   {
   }

   static join_key_t max() { return join_key_t{std::numeric_limits<Integer>::max(), std::numeric_limits<Integer>::max()}; }

   auto operator<=>(const join_key_t& other) const { return join_key_base::operator<=>(static_cast<const join_key_base&>(other)); }

   friend int operator%(const join_key_t& jk, const int& n) { return (jk.suppkey + jk.l_partkey) % n; }

   int match(const join_key_t& other) const
   {
      // {0, 0} cannot be used as wildcard
      if (*this == join_key_t{} && other == join_key_t{})
         return 0;
      else if (*this == join_key_t{})
         return -1;
      else if (other == join_key_t{})
         return 1;

      if (l_partkey != 0 && other.l_partkey != 0 && l_partkey != other.l_partkey)
         return l_partkey - other.l_partkey;
      if (suppkey != 0 && other.suppkey != 0)
         return suppkey - other.suppkey;
      return 0;
   }
};

struct merged_part_t : public merged_t<13, part_t, join_key_t, ExtraID::NONE> {
   using merged_t::merged_t;

   static join_key_t getJK(const join_key_t& jk) { return join_key_t{jk.l_partkey, 0}; }

   merged_part_t() = default;

   template <typename... Args>
   explicit merged_part_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }

   struct Key : public merged_t::Key {
      Key() = default;
      Key(const typename part_t::Key& pk, const part_t&) : merged_t::Key{join_key_t{pk.p_partkey, 0}, pk} {}

      template <typename... Args>
      explicit Key(Args&&... args) : merged_t::Key{std::forward<Args>(args)...}
      {
      }
   };
};

struct merged_partsupp_t : public merged_t<13, partsupp_t, join_key_t, ExtraID::NONE> {
   using merged_t::merged_t;

   merged_partsupp_t() = default;

   template <typename... Args>
   explicit merged_partsupp_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }

   struct Key : public merged_t::Key {
      using merged_t::Key::Key;

      Key(const typename partsupp_t::Key& pk, const partsupp_t&) : merged_t::Key{join_key_t{pk.ps_partkey, pk.ps_suppkey}, pk} {}

      template <typename... Args>
      explicit Key(Args&&... args) : merged_t::Key{std::forward<Args>(args)...}
      {
      }
   };
};

struct merged_lineitem_t : public merged_t<13, lineitem_t, join_key_t, ExtraID::PK> {
   using merged_t::merged_t;

   merged_lineitem_t() = default;

   template <typename... Args>
   explicit merged_lineitem_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }

   struct Key : public merged_t::Key {
      using merged_t::Key::Key;

      Key(const typename lineitem_t::Key& pk, const lineitem_t& pv) : merged_t::Key{join_key_t{pv.l_partkey, pv.l_suppkey}, pk} {}

      template <typename... Args>
      explicit Key(Args&&... args) : merged_t::Key{std::forward<Args>(args)...}
      {
      }
   };
};

struct sorted_lineitem_t : public merged_t<14, lineitem_t, join_key_t, ExtraID::PK> {
   using merged_t::merged_t;

   sorted_lineitem_t() = default;

   template <typename... Args>
   explicit sorted_lineitem_t(Args&&... args) : merged_t{std::forward<Args>(args)...}
   {
   }

   operator merged_lineitem_t() const { return merged_lineitem_t{this->payload}; }

   struct Key : public merged_t::Key {
      Key(const typename lineitem_t::Key& pk, const lineitem_t& pv) : merged_t::Key{join_key_t{pv.l_partkey, pv.l_suppkey}, pk} {}

      template <typename... Args>
      explicit Key(Args&&... args) : merged_t::Key{std::forward<Args>(args)...}
      {
      }

      operator merged_lineitem_t::Key() const { return merged_lineitem_t::Key{this->jk, this->pk}; }
   };
};

struct joinedPPs_t : public joined_t<11, join_key_t, part_t, partsupp_t> {
   joinedPPs_t() = default;

   template <typename... Args>
   explicit joinedPPs_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }

   // From all possible join inputs
   // joined_t(part_t, partsupp_t) supported in joined_t
   joinedPPs_t(merged_part_t p, merged_partsupp_t ps) : joined_t(std::make_tuple(p.payload, ps.payload)) {}

   struct Key : public joined_t::Key {
      Key() = default;

      // From all possible join inputs
      Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk)
          : joined_t::Key{psk.jk, std::tuple_cat(std::make_tuple(pk.pk), std::make_tuple(psk.pk))}
      {
      }

      Key(const part_t::Key& pk, const partsupp_t::Key& psk) : joined_t::Key{join_key_t{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk)} {}

      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key{std::forward<Args>(args)...}
      {
      }
   };
};

struct joinedPPsL_t : public joined_t<12, join_key_t, part_t, partsupp_t, lineitem_t> {
   joinedPPsL_t() = default;

   // From all possible join inputs
   joinedPPsL_t(merged_part_t p, merged_partsupp_t ps, merged_lineitem_t l) : joined_t(std::make_tuple(p.payload, ps.payload, l.payload)) {}

   joinedPPsL_t(part_t p, partsupp_t ps, sorted_lineitem_t l) : joined_t(std::make_tuple(p, ps, l.payload)) {}

   joinedPPsL_t(joinedPPs_t j, sorted_lineitem_t l) : joined_t(std::tuple_cat(j.payloads, std::make_tuple(l.payload))) {}

   template <typename... Args>
   explicit joinedPPsL_t(Args&&... args) : joined_t{std::forward<Args>(args)...}
   {
   }

   struct Key : public joined_t::Key {
      Key() = default;

      // From all possible join inputs
      Key(const joinedPPs_t::Key& j1k, const sorted_lineitem_t::Key& lk) : joined_t::Key{j1k.jk, std::tuple_cat(j1k.keys, std::make_tuple(lk.pk))} {}
      Key(const part_t::Key& pk, const partsupp_t::Key& psk, const sorted_lineitem_t::Key& lk)
          : joined_t::Key{join_key_t{pk.p_partkey, psk.ps_suppkey}, std::make_tuple(pk, psk, lk.pk)}
      {
      }
      Key(const merged_part_t::Key& pk, const merged_partsupp_t::Key& psk, const merged_lineitem_t::Key& lk)
          : joined_t::Key{join_key_t{psk.jk}, std::make_tuple(pk.pk, psk.pk, lk.pk)}
      {
      }

      template <typename... Args>
      explicit Key(Args&&... args) : joined_t::Key{std::forward<Args>(args)...}
      {
      }
   };
};
}  // namespace basic_join

using namespace basic_join;

template <>
struct SKBuilder<join_key_t> {
   static join_key_t inline create(const part_t::Key& k, const part_t&) { return join_key_t(k.p_partkey, 0); }

   static join_key_t inline create(const partsupp_t::Key& k, const partsupp_t&) { return join_key_t(k.ps_partkey, k.ps_suppkey); }

   static join_key_t inline create(const lineitem_t::Key&, const lineitem_t& v) { return join_key_t(v.l_partkey, v.l_suppkey); }

   static join_key_t inline create(const joinedPPs_t::Key& k, const joinedPPs_t&) { return join_key_t(k.jk); }

   static join_key_t inline create(const joinedPPsL_t::Key& k, const joinedPPsL_t&) { return join_key_t(k.jk); }

   static join_key_t inline create(const merged_part_t::Key& k, const merged_part_t&) { return join_key_t(k.jk); }

   static join_key_t inline create(const merged_partsupp_t::Key& k, const merged_partsupp_t&) { return join_key_t(k.jk); }

   static join_key_t inline create(const merged_lineitem_t::Key& k, const merged_lineitem_t&) { return join_key_t(k.jk); }

   template <typename Record>
   static join_key_t inline get(const join_key_t& k)
   {
      return k;
   }
};

template <>
inline join_key_t SKBuilder<join_key_t>::get<part_t>(const join_key_t& jk)
{
   return join_key_t{jk.l_partkey, 0};
}

template <>
inline join_key_t SKBuilder<join_key_t>::get<merged_part_t>(const join_key_t& jk)
{
   return join_key_t{jk.l_partkey, 0};
}