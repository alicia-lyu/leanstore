#pragma once
#include <type_traits>
#include <variant>
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{

// GeoAction — visitor return value, mirrors tpch::WalkAction but with the
// 4-level skip taxonomy geo's hierarchy needs (TPC-H has only SkipGroup +
// SkipOrder). SkipNation has the largest payoff in a whole-tree scan; SkipCity
// is the smallest (~20 customers under one city). The baseline 3 visitors all
// return Continue — Skip* is dead code for them, but the walker supports it
// so future filter-pushdown visitors don't need a re-design.
enum class GeoAction {
   Continue,
   SkipCity,
   SkipCounty,
   SkipState,
   SkipNation,
};

// Internal skip-level enum used by the walker's state machine. Mirrors GeoAction
// + a sentinel None. Kept distinct from GeoAction so the walker can encode
// "currently skipping at level X" without polluting the visitor return surface.
enum class GeoSkipLevel { None = 0, City = 1, County = 2, State = 3, Nation = 4 };

inline GeoSkipLevel to_skip_level(GeoAction a)
{
   switch (a) {
      case GeoAction::SkipCity:   return GeoSkipLevel::City;
      case GeoAction::SkipCounty: return GeoSkipLevel::County;
      case GeoAction::SkipState:  return GeoSkipLevel::State;
      case GeoAction::SkipNation: return GeoSkipLevel::Nation;
      default:                    return GeoSkipLevel::None;
   }
}

// Build the sort_key_t to seek to after a Skip<Level> action — the next
// sibling at that hierarchy level. E.g. SkipState from (5, 3, ...) seeks to
// {5, 4, 0, 0, 0}, landing the scanner at the first record under nation=5
// state=4 (which is either state=4's record itself or — if no such state — the
// first key past it, which the prefix-match stop condition will catch).
inline sort_key_t next_sibling_key(const sort_key_t& cur, GeoSkipLevel lvl)
{
   sort_key_t s = cur;
   switch (lvl) {
      case GeoSkipLevel::City:   s.citykey   += 1; s.custkey = 0; break;
      case GeoSkipLevel::County: s.countykey += 1; s.citykey  = 0; s.custkey = 0; break;
      case GeoSkipLevel::State:  s.statekey  += 1; s.countykey = 0; s.citykey = 0; s.custkey = 0; break;
      case GeoSkipLevel::Nation: s.nationkey += 1; s.statekey  = 0; s.countykey = 0; s.citykey = 0; s.custkey = 0; break;
      default: break;
   }
   return s;
}

// Returns the (0-indexed) level at which `a` and `b` first differ:
//   0 = nation, 1 = state, 2 = county, 3 = city, 4 = customer, 5 = identical
inline int first_diff_level(const sort_key_t& a, const sort_key_t& b)
{
   if (a.nationkey != b.nationkey) return 0;
   if (a.statekey  != b.statekey ) return 1;
   if (a.countykey != b.countykey) return 2;
   if (a.citykey   != b.citykey  ) return 3;
   if (a.custkey   != b.custkey  ) return 4;
   return 5;
}

// Does `row` lie under the (non-wildcard) prefix of `seek_prefix`?
// WILDCARD_KEY (= 0) fields in seek_prefix mean "match anything"; concrete
// fields must equal-match in `row`. Stop condition for the walker.
inline bool matches_prefix(const sort_key_t& row, const sort_key_t& seek_prefix)
{
   if (seek_prefix.nationkey != WILDCARD_KEY && row.nationkey != seek_prefix.nationkey) return false;
   if (seek_prefix.statekey  != WILDCARD_KEY && row.statekey  != seek_prefix.statekey ) return false;
   if (seek_prefix.countykey != WILDCARD_KEY && row.countykey != seek_prefix.countykey) return false;
   if (seek_prefix.citykey   != WILDCARD_KEY && row.citykey   != seek_prefix.citykey  ) return false;
   return true;
}

// geo_group_walk — single-pass hierarchy-aware walker over the 5-table geo
// merged index. Mirrors col_group_walk's shape (state machine + skip latch
// + Backend-trait seek-skip) but specialized to geo's 1:N:N:N:N hierarchy
// with 4 skip levels.
//
// `seek_prefix`: WILDCARD_KEY (=0) at any level means "scan all values at
// this level"; concrete values bound the scan. E.g. {5, 0, 0, 0, 0} = scan
// all of nation 5; {5, 3, 0, 0, 0} = scan all of nation 5 state 3; etc.
// Custkey field of seek_prefix is ignored (customer is the leaf — visitor
// decides what to do with each).
//
// Visitor must expose on_nation / on_state / on_county / on_city /
// on_customer hooks returning GeoAction. Optional SFINAE-detected hooks:
//   void on_group_end(const sort_key_t& city_sk)
//   void on_group_skipped(GeoSkipLevel lvl, const sort_key_t& boundary_sk)
//   void on_record_visited()
template <template <typename...> class MergedAdapterType,
          template <typename...> class MergedScannerType,
          typename Visitor>
void geo_group_walk(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& mi,
                    const sort_key_t& seek_prefix,
                    Visitor& visitor)
{
   auto scanner = mi.template getScanner<sort_key_t, view_t>();

   // Initial seek. The merged scanner sorts records by sort_key_t lex order
   // (nation < state < county < city < customer within each prefix), so
   // seeking to the city-level key of the first sub-record lands us at the
   // earliest record under the prefix. Same pattern as MergedScannerCounter.
   if (seek_prefix != sort_key_t::max()) {
      scanner->template seek<city_t>(
          city_t::Key{seek_prefix.nationkey, seek_prefix.statekey, seek_prefix.countykey, seek_prefix.citykey});
   }

   sort_key_t cur_sk{};                      // last seen row's sort_key_t (sentinel = all zeros)
   bool       have_prev_city = false;        // gates on_group_end emission
   sort_key_t prev_city_sk{};                // city prefix of last fully-walked group
   GeoSkipLevel active_skip = GeoSkipLevel::None;
   bool       skip_pending = false;
   GeoSkipLevel pending_level = GeoSkipLevel::None;

   // Default to seek-skip for geo. The TPC-H gotcha (RocksDB prefetch buffer
   // invalidation at SF=40 on disk) is less of a concern at geo's working-set
   // sizes; forward-iterate past skipped subtrees would touch O(20-20K)
   // records per skip, which is more expensive than even a flushed prefetch.
   // If profiling later shows this matters, expose via a gflag.
   constexpr bool use_seek_skip_runtime = true;

   auto fire_group_end = [&]() {
      if (have_prev_city) {
         if constexpr (requires { visitor.on_group_end(prev_city_sk); }) {
            visitor.on_group_end(prev_city_sk);
         }
         have_prev_city = false;
      }
   };

   while (auto kv = scanner->next()) {
      if constexpr (requires { visitor.on_record_visited(); }) {
         visitor.on_record_visited();
      }
      // NOTE: use named bindings (not structured) so the lambdas below can
      // capture them — clang in C++20 mode still doesn't allow structured-
      // binding capture for `auto& [k, v] = *kv` in this codebase.
      const auto& k = kv->first;
      const auto& v = kv->second;
      sort_key_t row_sk = SKBuilder<sort_key_t>::create(k, v);

      // Stop condition: row no longer under the seek prefix.
      if (!matches_prefix(row_sk, seek_prefix)) break;

      // Boundary handling: what hierarchy level changed vs the previous row?
      int diff = first_diff_level(cur_sk, row_sk);
      // diff: 0=nation 1=state 2=county 3=city 4=customer 5=same

      // Close active_skip if its level (or a higher one) just transitioned.
      // E.g. active_skip == City: any diff at level ≤ 3 means we've left the
      // skipped city; resume dispatch.
      if (active_skip != GeoSkipLevel::None) {
         int skip_level_idx = static_cast<int>(active_skip);  // City=1, County=2, State=3, Nation=4
         // skip_level_idx maps to: 1→city level (idx 3), 2→county (2), 3→state (1), 4→nation (0)
         int gate_level = 4 - skip_level_idx;
         if (diff <= gate_level) {
            active_skip = GeoSkipLevel::None;
         }
      }

      // City-boundary on_group_end: fire when (n,s,c,ci) tuple changes
      // (i.e. diff ≤ 3 = nation/state/county/city level), provided we had
      // a previous city in flight. The on_group_end argument is the
      // *previous* city's sort_key prefix.
      if (diff <= 3 && have_prev_city) {
         fire_group_end();
      }

      cur_sk = row_sk;

      // Drop record if under an active skip.
      if (active_skip != GeoSkipLevel::None) continue;

      // Dispatch by record value type. Mirror col_pipeline.tpp's std::visit
      // pattern — the value variant tells us the record type; the key variant
      // gives us the typed key via std::get_if.
      GeoAction action = GeoAction::Continue;
      std::visit(
          [&](auto&& val) {
             using V = std::decay_t<decltype(val)>;
             if constexpr (std::is_same_v<V, nation2_t>) {
                if (auto* nk = std::get_if<nation2_t::Key>(&k))
                   action = visitor.on_nation(*nk, val);
             } else if constexpr (std::is_same_v<V, states_t>) {
                if (auto* sk = std::get_if<states_t::Key>(&k))
                   action = visitor.on_state(*sk, val);
             } else if constexpr (std::is_same_v<V, county_t>) {
                if (auto* ck = std::get_if<county_t::Key>(&k))
                   action = visitor.on_county(*ck, val);
             } else if constexpr (std::is_same_v<V, city_t>) {
                if (auto* cik = std::get_if<city_t::Key>(&k)) {
                   // Latch this city as the in-flight group for on_group_end.
                   prev_city_sk = sort_key_t{cik->nationkey, cik->statekey, cik->countykey, cik->citykey, 0};
                   have_prev_city = true;
                   action = visitor.on_city(*cik, val);
                }
             } else if constexpr (std::is_same_v<V, customer2_t>) {
                if (auto* cuk = std::get_if<customer2_t::Key>(&k))
                   action = visitor.on_customer(*cuk, val);
             }
          },
          v);

      // Translate GeoAction to walker-local skip state.
      if (action != GeoAction::Continue) {
         pending_level = to_skip_level(action);
         skip_pending = true;
         if constexpr (requires { visitor.on_group_skipped(pending_level, row_sk); }) {
            visitor.on_group_skipped(pending_level, row_sk);
         }
      }

      if (skip_pending) {
         skip_pending = false;
         active_skip = pending_level;
         if (use_seek_skip_runtime) {
            sort_key_t target = next_sibling_key(row_sk, pending_level);
            // Seek to the city-level key of the next sibling at the requested
            // level — same seek shape as the initial seek.
            scanner->template seek<city_t>(city_t::Key{target.nationkey, target.statekey, target.countykey, target.citykey});
            // After seek, the next record may be past our seek_prefix entirely;
            // the loop's stop condition catches that. cur_sk stays as the
            // pre-seek row so the diff-level math on the next iteration sees
            // the full transition.
         }
         pending_level = GeoSkipLevel::None;
      }
   }

   // Flush the final group.
   fire_group_end();
}

}  // namespace geo_join
