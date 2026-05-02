#pragma once

// Scanner utility helpers shared across TPC-H query drivers.
//
// make_filtered_scanner — wraps an existing scanner so next() skips
//   key-value pairs that do not satisfy a predicate.
//
// scan_of (two overloads) — produces a scanner-shaped view over an
//   in-memory std::vector<R>, yielding (R::Key, R) pairs in order.
//   The mutable overload allows the caller to hold a non-const vector;
//   the const overload accepts read-only vectors.
//
// These helpers let query drivers treat in-memory vectors the same way
// they treat on-disk adapters — a single scan loop covers both cases.

#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// make_filtered_scanner
//
// Wraps Scanner `s` so that next() transparently skips key-value pairs where
// `predicate(kv.second)` returns false.  The resulting object has the same
// next() interface as `s` (returns std::optional<std::pair<K,V>>).
//
// Template parameters:
//   Scanner  — any type with a `next()` returning
//              std::optional<std::pair<KeyType, ValueType>>.
//   Predicate — callable: (const ValueType&) -> bool.

template <typename Scanner, typename Predicate>
class FilteredScanner
{
   Scanner    inner_;
   Predicate  pred_;

  public:
   FilteredScanner(Scanner s, Predicate p) : inner_(std::move(s)), pred_(std::move(p)) {}

   // Returns the next key-value pair that satisfies pred_, or nullopt.
   auto next()
   {
      for (;;) {
         auto kv = inner_.next();
         if (!kv) return kv;          // exhausted
         if (pred_(kv->second)) return kv;
         // predicate failed — keep advancing
      }
   }
};

// Factory: deduces Scanner and Predicate from arguments so callers do not
// need to spell out template parameters.
template <typename Scanner, typename Predicate>
auto make_filtered_scanner(Scanner s, Predicate p)
{
   return FilteredScanner<Scanner, Predicate>(std::move(s), std::move(p));
}

// ---------------------------------------------------------------------------
// scan_of — scanner-shaped view over std::vector<R>.
//
// R must provide a nested Key type and a static unfoldKey (or equivalent);
// for in-memory vectors the key is simply reconstructed from the record's
// own primary-key fields via `R::Key` direct construction.  Because
// in-memory vectors already carry typed values, the Key produced here is
// a default-constructed shell; callers that need the actual key should
// hold it alongside the value or derive it from the record fields.
//
// The iterator yields std::optional<std::pair<typename R::Key, R>>.

template <typename R>
class VectorScanner
{
   const std::vector<R>& vec_;
   std::size_t           idx_ = 0;

  public:
   explicit VectorScanner(const std::vector<R>& v) : vec_(v) {}

   std::optional<std::pair<typename R::Key, R>> next()
   {
      if (idx_ >= vec_.size()) return std::nullopt;
      const R& rec = vec_[idx_++];
      // Key is derived from the record; callers can override by specialising.
      typename R::Key k{};
      return std::make_pair(k, rec);
   }
};

// Mutable overload: accepts a non-const vector (values are still read-only
// through the returned pairs).
template <typename R>
auto scan_of(std::vector<R>& v)
{
   return VectorScanner<R>(v);
}

// Const overload.
template <typename R>
auto scan_of(const std::vector<R>& v)
{
   return VectorScanner<R>(v);
}
