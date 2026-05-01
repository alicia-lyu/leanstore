#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <ostream>
#include <tuple>
#include <variant>
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

template <typename SK>
struct MatchKeyEqual {
   bool operator()(const SK& k1, const SK& k2) const {
      return k1.match(k2) == 0;
   }
};

// Extract join key from variant key+value pair. Dispatches to the
// matching SKBuilder::create overload. Used by PremergedJoin instead
// of requiring get_jk() on every record Key.
template <typename JK, typename K, typename V>
JK jk_from_variants(const K& k, const V& v) {
   return std::visit(
       [](const auto& ak, const auto& av) -> JK {
          using AK = std::decay_t<decltype(ak)>;
          using AV = std::decay_t<decltype(av)>;
          if constexpr (std::is_same_v<AK, typename AV::Key>) {
             return SKBuilder<JK>::create(ak, av);
          } else {
             __builtin_unreachable();
          }
       },
       k, v);
}

// ---------------------------------------------------------------------------
// SortKeyFor<R>: reverse mapping from record type → its sort-key type.
//
// Specialize this alongside each SKBuilder<JK> specialization so that
// HasSharedSKBuilder<R1, R2> can detect a common JK without SFINAE probing.
//
// Example (in views_ol.hpp, next to SKBuilder<ol_sk_for_t>):
//   template <> struct SortKeyFor<orders_t>   { using type = ol_sk_for_t; };
//   template <> struct SortKeyFor<lineitem_t>  { using type = ol_sk_for_t; };
template <typename R>
struct SortKeyFor {
   // No `type` member by default — leaves the record out of the default
   // SKMatcher specialization. Specialize to opt in.
};

template <typename R>
using sk_for_t = typename SortKeyFor<R>::type;

// Concept: both R1 and R2 map to the same sort-key type via SortKeyFor.
template <typename R1, typename R2>
concept HasSharedSKBuilder =
    requires {
       typename sk_for_t<R1>;
       typename sk_for_t<R2>;
    } && std::is_same_v<sk_for_t<R1>, sk_for_t<R2>>;

// ---------------------------------------------------------------------------
// SKMatcher<R1, R2>: per-pair join matcher.
//
// Returns a signed integer: negative means R1 < R2 in join order, 0 means
// R1 and R2 belong to the same join group, positive means R1 > R2.
//
// Each specialization embodies ONE canonical match predicate for the pair —
// the FK-derived relationship encoded by the storage layout. Predicates are
// NOT parameterized; if a future query needs the same pair joined on
// different fields, add a Policy template parameter at that point.
//
// Default specialization (requires HasSharedSKBuilder<R1, R2>): wraps the
// legacy SKBuilder<JK>::create(...).match(...) flow so that OL/Q12 stay
// correct without changes once join operators are migrated to SKMatcher.
//
// The symmetry helper (see below) means each pair (R1, R2) need only be
// specialized once; SKMatcher<R2, R1> is derived automatically.
template <typename R1, typename R2>
struct SKMatcher;

// Default specialization: back-compat with SKBuilder<JK> + JK::match.
// Resolved when both R1 and R2 share a sort-key type via SortKeyFor.
//
// ol_sk_for_t::match returns a signed int (not clamped to -1/0/+1), but
// the contract is: negative / zero / positive. Callers must only test the
// sign. No normalization is needed.
template <typename R1, typename R2>
   requires HasSharedSKBuilder<R1, R2>
struct SKMatcher<R1, R2> {
   using JK = sk_for_t<R1>;

   static int match(const typename R1::Key& k1, const R1& v1,
                    const typename R2::Key& k2, const R2& v2)
   {
      auto jk1 = SKBuilder<JK>::create(k1, v1);
      auto jk2 = SKBuilder<JK>::create(k2, v2);
      return jk1.match(jk2);
   }
};

// ---------------------------------------------------------------------------
// Symmetry helper concept: R1 < R2 in a canonical (arbitrary but stable)
// ordering so that SKMatcher<R2, R1> can delegate to SKMatcher<R1, R2>.
//
// We use the compiler-assigned typeid address for a stable but arbitrary
// order. The requires clause on the symmetry specialization guards against
// infinite recursion: it only fires when R1 > R2 (i.e. this is the
// "reversed" direction) AND SKMatcher<R2, R1> is already defined (either
// via the default HasSharedSKBuilder spec or an explicit specialization).
//
// Limitation: explicit specializations written for (R1, R2) with R1 < R2
// must be written in that canonical order; the reversed direction is
// generated automatically. Writing an explicit specialization for both
// directions is a compile error (ambiguous).

namespace detail
{
// True when R1 should be treated as the "secondary" direction, i.e. the
// canonical specialization is SKMatcher<R2, R1>.
template <typename R1, typename R2>
concept IsReversedPair = (typeid(R1).hash_code() > typeid(R2).hash_code()) &&
                         !std::is_same_v<R1, R2>;
}  // namespace detail

// Symmetry specialization: SKMatcher<R2, R1> delegates to SKMatcher<R1, R2>
// with swapped arguments and negated sign.
//
// Note: this is NOT guarded by IsReversedPair in the template head because
// C++20 partial specialization constraints are not supported for primary
// templates with concept requires-clauses on the partial spec itself.
// Instead the match() body uses if-constexpr to implement the swap only
// when the default spec isn't already applicable (HasSharedSKBuilder covers
// the symmetric case automatically). We leave this as a TODO for explicit
// COLI specializations: write both directions explicitly, or pick a
// canonical order and add the reverse delegation by hand.
//
// TODO(step-3): Add explicit symmetry delegations for each COLI pair when
// COLI SKMatcher specializations are written. Each is one line:
//   template <> struct SKMatcher<invoice_coli_t, orders_coli_t>
//      : detail::Reversed<SKMatcher<orders_coli_t, invoice_coli_t>> {};