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
#include <queue>
#include <utility>
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
//
// Note: `apply_topN` requires the caller to first buffer every qualifying
// row in `out` before truncation.  For pipelines whose qualifying-row
// count grows linearly with the walk (Q3 / Q3I), prefer `TopNSink` below,
// which streams rows one at a time into a bounded heap of size K — memory
// is O(K) instead of O(N).  apply_topN is kept for callers whose result
// set is intrinsically small (e.g. a per-shipmode HashAggregate output).
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

// TopNSink — streaming bounded top-K selection.
//
// Plays the same role for Q3 / Q3I that NNameRevenueAggregator plays for
// Q5: a small-buffer post-pipeline aggregator that the visitor / per-emit
// callsite pushes rows into one at a time.  The internal heap holds at
// most K elements, so the per-query memory footprint is O(K) instead of
// O(N) where N = qualifying rows from the walk / scan / join.
//
// Usage pattern (mirrors Q5's NNameRevenueAggregator construction +
// drain shape):
//   TopNSink<AggRow, decltype(cmp)> sink(K, cmp);
//   // ... walk / scan / join, pushing rows: ...
//   sink.offer(std::move(row));
//   // ... after the pipeline drains: ...
//   sink.drain_sorted(out);   // out now holds K rows, sorted best → worst
//
// `cmp(a, b)` returns true when `a` should rank before `b` (a is
// "better"; e.g. higher revenue under ORDER BY revenue DESC).  Same
// semantics as apply_topN's cmp — passing the same comparator yields
// the same final ordering.
//
// Implementation note: the underlying std::priority_queue uses `cmp`
// directly, so heap.top() is the *worst* of the current top-K (the
// candidate to evict on offer).  This is the inverse of how
// std::priority_queue is usually used, but matches our "best at front
// after drain_sorted" invariant.
template <typename R, typename Cmp>
class TopNSink
{
   std::priority_queue<R, std::vector<R>, Cmp> heap;
   std::size_t K;
   Cmp cmp;

 public:
   TopNSink(std::size_t k, Cmp c)
       : heap(c), K(k), cmp(std::move(c))
   {
   }

   // O(log K) per call.
   void offer(R row)
   {
      if (heap.size() < K) {
         heap.push(std::move(row));
      } else if (cmp(row, heap.top())) {
         heap.pop();
         heap.push(std::move(row));
      }
   }

   // Drain the heap into `out` in sorted order (best first).  After this
   // call the sink is empty.  Caller is responsible for `out.clear()`
   // beforehand if it expects an exclusive owner.
   void drain_sorted(std::vector<R>& out)
   {
      const std::size_t start = out.size();
      out.reserve(start + heap.size());
      while (!heap.empty()) {
         out.push_back(heap.top());  // priority_queue::top() is const&
         heap.pop();
      }
      // Heap drain order is worst → best (top() = worst).  Reverse the
      // newly-appended suffix to get best → worst.
      std::reverse(out.begin() + static_cast<std::ptrdiff_t>(start), out.end());
   }

   std::size_t size() const { return heap.size(); }
};
