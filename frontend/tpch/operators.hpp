#pragma once

// TPC-H shared operator helpers.
//
// apply_topN — sorts `out` by `cmp` and truncates to the K largest elements.
//   Placed here (frontend/tpch/) rather than frontend/shared/view_templates.hpp
//   because it is TPC-H-specific: it operates on a bounded result vector and
//   encodes the "ORDER BY … LIMIT K" outside-pipeline pattern documented in
//   OPERATORS.md §3 op 8–9.  view_templates.hpp is a general storage-layer
//   utility; mixing query-result operators there would blur the abstraction.

#include <algorithm>
#include <cstddef>
#include <vector>

// Apply a top-K selection in-place to `out`:
//   - If out.size() > K: partial-sort the first K elements by `cmp`, then
//     truncate.  std::partial_sort is O(N log K) — cheaper than a full sort
//     when K << N.
//   - Otherwise: sort the whole vector so the caller always receives results
//     in order.
//
// `cmp(a, b)` returns true when `a` should rank before `b` (i.e. `a` is
// "better").  For ORDER BY revenue DESC, pass
//   [](const auto& a, const auto& b){ return a.revenue > b.revenue; }
template <typename R, typename Cmp>
inline void apply_topN(std::vector<R>& out, std::size_t K, Cmp cmp)
{
   if (out.size() > K) {
      std::partial_sort(out.begin(), out.begin() + static_cast<std::ptrdiff_t>(K),
                        out.end(), cmp);
      out.resize(K);
   } else {
      std::sort(out.begin(), out.end(), cmp);
   }
}
