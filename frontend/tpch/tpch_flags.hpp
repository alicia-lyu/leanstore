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
   #define TPCH_FLAG_INT(name, def, doc) DEFINE_int32(name, def, doc)
   #define TPCH_FLAG_BOOL(name, def, doc) DEFINE_bool(name, def, doc)
#else
   #define TPCH_FLAG_INT(name, def, doc) DECLARE_int32(name)
   #define TPCH_FLAG_BOOL(name, def, doc) DECLARE_bool(name)
#endif

TPCH_FLAG_INT(tpch_scale_factor, 1, "TPC-H scale factor");
TPCH_FLAG_INT(storage_structure, 1,
              "1=base merge-join, 2=pipeline view, 3=MI[0] premerged, 4=base hash-join");
TPCH_FLAG_INT(tx_seconds, 15, "Seconds to run each transaction type");
TPCH_FLAG_INT(warmup_seconds, 0, "Warmup seconds");
TPCH_FLAG_INT(bgw_pct, 0, "Percentage of background write transactions");
TPCH_FLAG_BOOL(log_progress, true, "Log loading/query progress");

#undef TPCH_FLAG_INT
#undef TPCH_FLAG_BOOL
