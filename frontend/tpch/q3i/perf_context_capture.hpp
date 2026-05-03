#pragma once

// RAII helper that captures RocksDB PerfContext / IOStatsContext counters
// around a single Q3I query invocation and accumulates them into Q3IStats.
//
// Usage (inside a query_by_* body, gated on --micro_perf):
//   PerfContextCapture _pc(FLAGS_micro_perf ? stats : nullptr);
//   ... query body ...
//   // _pc destructor fires on scope exit, accumulating into stats
//
// When stats == nullptr (--micro_perf=false) the constructor and destructor
// are no-ops — zero runtime overhead on the default path.
//
// On non-RocksDB builds (rocksdb/perf_context.h unavailable) the whole
// struct collapses to an always-no-op shell so query.tpp compiles unchanged.

#include "workload.hpp"

#if __has_include(<rocksdb/perf_context.h>)
#include <rocksdb/iostats_context.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>
#define Q3I_HAS_ROCKSDB_PERF 1
#endif

#ifndef ROCKSDB_ONLY
#include "../../shared/adapter-scanner/scanner_perf_hook.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#define Q3I_HAS_LEANSTORE_PERF 1
#endif

#include "../backend.hpp"

namespace tpch::q3i
{

struct PerfContextCapture {
#ifdef Q3I_HAS_ROCKSDB_PERF
   Q3IStats* stats_;

   explicit PerfContextCapture(Q3IStats* stats) : stats_(stats)
   {
      if (!stats_) return;
      rocksdb::get_perf_context()->Reset();
      rocksdb::get_iostats_context()->Reset();
   }

   ~PerfContextCapture()
   {
      if (!stats_) return;
      const rocksdb::PerfContext*    pc  = rocksdb::get_perf_context();
      const rocksdb::IOStatsContext* ioc = rocksdb::get_iostats_context();

      stats_->pc_user_key_comparison_count += pc->user_key_comparison_count;
      stats_->pc_block_cache_hit_count     += pc->block_cache_hit_count;
      stats_->pc_block_read_count          += pc->block_read_count;
      stats_->pc_block_read_byte           += pc->block_read_byte;
      stats_->pc_block_read_time           += pc->block_read_time;
      stats_->pc_block_decompress_time     += pc->block_decompress_time;
      stats_->pc_iter_next_cpu_nanos       += pc->iter_next_cpu_nanos;
      stats_->pc_iter_seek_cpu_nanos       += pc->iter_seek_cpu_nanos;

      stats_->ioc_bytes_read += ioc->bytes_read;
      stats_->ioc_read_nanos += ioc->read_nanos;
      stats_->ioc_open_nanos += ioc->open_nanos;
   }
#else
   // Non-RocksDB build: complete no-op.
   explicit PerfContextCapture(Q3IStats* /*stats*/) {}
   ~PerfContextCapture() = default;
#endif

   // Non-copyable, non-movable — owns the reset/accumulate lifecycle.
   PerfContextCapture(const PerfContextCapture&)            = delete;
   PerfContextCapture& operator=(const PerfContextCapture&) = delete;
};

// LeanStore-side analog of `PerfContextCapture`. Snapshots the per-thread
// `leanstore::WorkerCounters` at ctor and accumulates the per-query diff
// into Q3IStats `ls_*` fields at dtor. Also enables/disables the
// thread-local scanner-next CPU timer
// (`tpch::scanner_perf::iter_next_ns_acc`) so per-query iter_next_ns
// numbers are isolated.
//
// On RocksDB-only builds (macOS) the LeanStore headers are unavailable
// and this collapses to a no-op shell.
struct LeanStorePerfContextCapture {
#ifdef Q3I_HAS_LEANSTORE_PERF
   Q3IStats* stats_;
   uint64_t dt_next_tuple_before_     = 0;
   uint64_t dt_page_reads_before_     = 0;
   uint64_t dt_swip_hot_before_       = 0;
   uint64_t dt_swip_cool_before_      = 0;

   static uint64_t snapshot_dt_array(const std::atomic<uint64_t> arr[])
   {
      uint64_t sum = 0;
      for (uint64_t i = 0; i < leanstore::WorkerCounters::max_dt_id; ++i) {
         sum += arr[i].load();
      }
      return sum;
   }

   explicit LeanStorePerfContextCapture(Q3IStats* stats) : stats_(stats)
   {
      if (!stats_) return;
      auto& wc                = leanstore::WorkerCounters::myCounters();
      dt_next_tuple_before_   = snapshot_dt_array(wc.dt_next_tuple);
      dt_page_reads_before_   = snapshot_dt_array(wc.dt_page_reads);
      dt_swip_hot_before_     = snapshot_dt_array(wc.dt_resolve_swip_hot);
      dt_swip_cool_before_    = snapshot_dt_array(wc.dt_resolve_swip_cool);
      tpch::scanner_perf::iter_next_ns_acc = 0;
      tpch::scanner_perf::iter_next_calls  = 0;
      tpch::scanner_perf::enabled          = true;
   }

   ~LeanStorePerfContextCapture()
   {
      if (!stats_) return;
      tpch::scanner_perf::enabled = false;
      auto& wc                = leanstore::WorkerCounters::myCounters();
      stats_->ls_dt_next_tuple +=
          snapshot_dt_array(wc.dt_next_tuple) - dt_next_tuple_before_;
      stats_->ls_dt_page_reads +=
          snapshot_dt_array(wc.dt_page_reads) - dt_page_reads_before_;
      stats_->ls_hot_hit +=
          snapshot_dt_array(wc.dt_resolve_swip_hot) - dt_swip_hot_before_;
      stats_->ls_cold_hit +=
          snapshot_dt_array(wc.dt_resolve_swip_cool) - dt_swip_cool_before_;
      stats_->ls_iter_next_ns    += tpch::scanner_perf::iter_next_ns_acc;
      stats_->ls_iter_next_calls += tpch::scanner_perf::iter_next_calls;
   }
#else
   explicit LeanStorePerfContextCapture(Q3IStats* /*stats*/) {}
   ~LeanStorePerfContextCapture() = default;
#endif

   LeanStorePerfContextCapture(const LeanStorePerfContextCapture&)            = delete;
   LeanStorePerfContextCapture& operator=(const LeanStorePerfContextCapture&) = delete;
};

// Backend-templated alias — `Q3IPerfCapture<Backend> _pc(stats);` at each
// `query_by_*` site selects the right RAII based on which backend
// instantiated the workload.
template <typename Backend>
struct Q3IPerfCapture;

template <>
struct Q3IPerfCapture<tpch::RocksDBBackend> : PerfContextCapture {
   using PerfContextCapture::PerfContextCapture;
};

#ifndef ROCKSDB_ONLY
template <>
struct Q3IPerfCapture<tpch::LeanStoreBackend> : LeanStorePerfContextCapture {
   using LeanStorePerfContextCapture::LeanStorePerfContextCapture;
};
#endif

}  // namespace tpch::q3i
