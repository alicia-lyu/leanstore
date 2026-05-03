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

}  // namespace tpch::q3i
