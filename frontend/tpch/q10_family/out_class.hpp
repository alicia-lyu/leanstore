#pragma once

// Q10QuerySink — post-pipeline sink for Q10's S1/S2/S3/S4 paths.
//
// Wraps the canonical TopNSink<q10_agg_row_t, &q10_agg_row_t::cmp>
// from operators.hpp (CONVENTIONS §Post-pipeline OutClass). The
// visitor / query body builds a fully-assembled q10_agg_row_t (with
// n_name already resolved) and calls emit(row); the sink offers the
// row to the bounded min-heap. finalize(out) drains best→worst.
//
// Tiebreaker — q10_agg_row_t::cmp uses (revenue DESC, c_custkey ASC).
// c_custkey is unique per row (Q10 has one aggregate row per customer),
// satisfying strict weak ordering (anti-pattern #30 avoidance).

#include <cstddef>
#include <utility>
#include <vector>

#include "../operators.hpp"
#include "../q10/views.hpp"
#include "../q10/workload.hpp"  // Q10Stats

namespace tpch::q10
{

// Q10's spec LIMIT is 20. Centralised here so the visitor / executable
// don't drift.
inline constexpr std::size_t Q10_TOPN_K = 20;

class Q10QuerySink
{
   using CmpFn = bool (*)(const q10_agg_row_t&, const q10_agg_row_t&);
   TopNSink<q10_agg_row_t, CmpFn> topn;
   Q10Stats*                      stats;
   // Track evictions by remembering the previous heap.size().
   std::size_t                    last_size = 0;

  public:
   explicit Q10QuerySink(Q10Stats* s = nullptr,
                         std::size_t k = Q10_TOPN_K)
       : topn(k, &q10_agg_row_t::cmp), stats(s)
   {
   }

   void emit(q10_agg_row_t row)
   {
      if (stats) stats->topn_offers++;
      const std::size_t before = topn.size();
      topn.offer(std::move(row));
      const std::size_t after = topn.size();
      // Eviction happens when heap was already at capacity AND the
      // new row beat the current worst (size stays the same but the
      // top element changed). before == after && before == K.
      if (stats && before == Q10_TOPN_K && after == Q10_TOPN_K) {
         stats->topn_evictions++;
      }
      last_size = after;
   }

   // Drain into caller's vector, best → worst (revenue DESC).
   void finalize(std::vector<q10_agg_row_t>& out)
   {
      topn.drain_sorted(out);
   }

   std::size_t size() const { return topn.size(); }
};

}  // namespace tpch::q10
