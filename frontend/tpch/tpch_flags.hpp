#pragma once

// Shared gflags definitions for the TPC-H Tier 1 executables (Q12 / Q3 / Q9).
//
// Each executable .cpp must `#define TPCH_DEFINE_FLAGS` before including this
// header to act as the single TU that defines the flags; all other includers
// see DECLARE_* declarations only.
//
// `tentative_skip_bytes` is intentionally NOT here — its default differs by
// backend (12288 RocksDB / 4096 LeanStore) so each executable defines it
// directly.

#include <gflags/gflags.h>

#ifdef TPCH_DEFINE_FLAGS
   #define TPCH_FLAG_INT(name, def, doc)    DEFINE_int32(name, def, doc)
   #define TPCH_FLAG_BOOL(name, def, doc)   DEFINE_bool(name, def, doc)
   #define TPCH_FLAG_STRING(name, def, doc) DEFINE_string(name, def, doc)
#else
   #define TPCH_FLAG_INT(name, def, doc)    DECLARE_int32(name)
   #define TPCH_FLAG_BOOL(name, def, doc)   DECLARE_bool(name)
   #define TPCH_FLAG_STRING(name, def, doc) DECLARE_string(name)
#endif

TPCH_FLAG_INT(tpch_scale_factor, 1, "TPC-H scale factor");
TPCH_FLAG_INT(storage_structure, 1,
              "1=base merge-join, 2=pipeline view, 3=MI[0] premerged, 4=base hash-join");
TPCH_FLAG_INT(tx_seconds, 15, "Seconds to run each transaction type");
TPCH_FLAG_INT(warmup_seconds, 0, "Warmup seconds");
TPCH_FLAG_INT(bgw_pct, 0, "Percentage of background write transactions");
TPCH_FLAG_BOOL(log_progress, true, "Log loading/query progress");
TPCH_FLAG_BOOL(micro_perf, false,
               "Capture RocksDB PerfContext / IOStatsContext per query and print totals");
TPCH_FLAG_BOOL(cfstats, false,
               "Snapshot per-CF RocksDB stats before/after helper.run() and print diff");
TPCH_FLAG_INT(load_only_structure, -1,
              "If >=1, populate only the secondary needed for this --storage_structure "
              "at load time. Used by the A5 isolated-DB experiment to remove cross-"
              "structure cache pollution. Default -1 = load all secondaries.");
TPCH_FLAG_STRING(coli_walker_variant, "baseline",
                 "COLI walker dispatch: 'baseline' = std::variant + visitor; "
                 "'fused_emit' = tag-byte switch + direct typed dispatch (A2c).");

#undef TPCH_FLAG_INT
#undef TPCH_FLAG_BOOL
#undef TPCH_FLAG_STRING
