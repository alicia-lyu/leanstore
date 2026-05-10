#pragma once

#include "Types.hpp"

// Sentinel value for a key field that does not constrain a join match.
//
// Motivation:
//   Both the TPC-H and geo workloads use multi-field hierarchical keys
//   (e.g. (custkey, orderkey, linenumber) for the COL pipeline,
//   (nationkey, statekey, countykey, citykey, custkey) for geo) where a
//   coarser-prefix "anchor" row leaves the trailing fields unset, and
//   match() / matching_keys() treat those unset fields as wildcards so
//   probe-side rows find their build-side anchors.
//
//   Both schemas allocate IDs starting at 1, so 0 is safe as the sentinel.
//   But that convention is invisible at call sites: a literal `0` in a Key
//   constructor or `field == 0` in match() looks like a real value.  This
//   has misled at least two builders in q5/views.hpp: 105b66a7 worked
//   around the confusion by minting a narrower JK type (q5_co_jk_t),
//   2591c5d8 then re-introduced wildcard machinery in dead form on the
//   wrong key.  With wildcard_match() in place, neither workaround was
//   structurally necessary; q5_co_jk_t was retired in favour of a single
//   3-field q5_sort_key_t with proper per-field wildcard semantics.
//
// Use WILDCARD_KEY whenever a key field is intentionally left unset to
// signal a prefix anchor or wildcard match.  Never write a bare `0` in a
// Key{...} constructor or in a match()/matching_keys() comparison.
//
// Type-safe constexpr (not a #define) so it composes with constexpr
// constructors and respects scope.

inline constexpr Integer WILDCARD_KEY = 0;

// 3-way comparison helper for a single key field with wildcard semantics.
// Returns 0 (match) if either side is the WILDCARD_KEY sentinel; otherwise
// returns -1 / 0 / +1 in the usual a-vs-b ordering.
//
// Use inside match() implementations on hierarchical keys.  The pattern is:
//
//   int match(const Key& other) const {
//       if (int c = wildcard_match(parent_field, other.parent_field); c) return c;
//       if (int c = wildcard_match(child_field,  other.child_field);  c) return c;
//       ...
//       return 0;
//   }
//
// This encapsulates the "if (a == 0 || b == 0) return 0;" branch so a builder
// copying a match() implementation cannot silently drop the wildcard handling.
inline int wildcard_match(Integer a, Integer b)
{
   if (a == WILDCARD_KEY || b == WILDCARD_KEY) return 0;
   return a < b ? -1 : (a > b ? 1 : 0);
}

// Record-type vs query-time responsibility.
//
// Wildcard handling belongs to the *record type's* match() / matching_keys(),
// not to individual queries.  A record-type author who picks
// `matching_keys() = {*this}` because today's queries don't need anchors is
// encoding a query-shape assumption into a layer that should predate
// queries — record types live alongside the schema, queries come and go.
//
// The strict-default shortcut is fine when the consumer set is small and
// well-known; what's not fine is leaving it implicit.  If you choose
// `{*this}` deliberately, document the assumption in a comment alongside a
// pointer to the canonical wildcard-aware reference impls
// (`frontend/tpch/q5/views.hpp::q5_sort_key_t`,
//  `frontend/geo/views.hpp::sort_key_t`) so a future correctness incident
// has a written hint pointing at the right file.
//
// Concrete examples of keys whose strict default is currently load-bearing:
//   - `tpch::q3_family::lineitem_agg_t::Key` — see comment above the struct.
//   - `tpch::q3i::cust_open_due_t::Key`      — single-field, hierarchy not
//                                              extensible without widening.
//   - `tpch::q3::q3_cust_jk_t::Key`          — same as above.
