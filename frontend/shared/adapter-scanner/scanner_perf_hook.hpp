#pragma once

// Thread-local timing hook for LeanStore scanner next() calls. Used by
// `LeanStoreScanner::next()` and `LeanStoreMergedScanner::next()` to
// accumulate per-call CPU time into `iter_next_ns_acc` whenever
// `enabled` is true (set by the per-query perf-capture RAII in
// `frontend/tpch/q3i/perf_context_capture.hpp`).
//
// When `enabled` is false the only cost per next() is a single bool
// load + branch. When `enabled` is true the wrap pays one
// std::chrono::steady_clock::now() pair per next() call (~50 ns on
// Linux), which is acceptable for the diagnostic A1 sweep where the
// goal is to attribute the S3-vs-S1 per-record gap.

#include <chrono>
#include <cstdint>

namespace tpch::scanner_perf
{

inline thread_local bool     enabled          = false;
inline thread_local uint64_t iter_next_ns_acc = 0;
inline thread_local uint64_t iter_next_calls  = 0;

struct ScopedTimer {
   bool active;
   std::chrono::steady_clock::time_point t0;

   ScopedTimer() : active(enabled)
   {
      if (active) t0 = std::chrono::steady_clock::now();
   }

   ~ScopedTimer()
   {
      if (!active) return;
      const auto t1 = std::chrono::steady_clock::now();
      iter_next_ns_acc +=
          std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();
      iter_next_calls += 1;
   }

   ScopedTimer(const ScopedTimer&)            = delete;
   ScopedTimer& operator=(const ScopedTimer&) = delete;
};

}  // namespace tpch::scanner_perf
