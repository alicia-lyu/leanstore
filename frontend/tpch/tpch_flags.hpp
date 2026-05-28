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
              "1=base merge-join, 2=pipeline view, 3=MI[0] premerged, 4=base hash-join, "
              "5=aCOLI/aCOL pre-aggregated MI (q3i/q10/q10i), "
              "6=shared COL materialised view (q3/q5/q10)");
TPCH_FLAG_INT(tx_seconds, 15, "Seconds to run each transaction type");
TPCH_FLAG_INT(warmup_seconds, 0, "Warmup seconds");
TPCH_FLAG_INT(param_seed, 0,
              "Additive offset into the per-query substitution-parameter "
              "rotation table (PARAM_TABLE). The query loop runs "
              "set_params_for_iter(param_seed + iteration), so the first "
              "measured query uses PARAM_TABLE[param_seed % N] instead of "
              "always PARAM_TABLE[0] (the validation default). Default 0 "
              "preserves the historical single-default-param behaviour. "
              "The paper sweep passes a per-REP value identical across all "
              "four storage structures, so the 3 reps sample 3 distinct "
              "parameters while every structure sees the same one per rep "
              "(fairness: not a head-to-head bias). At the 5L cell only one "
              "query fits the window, so this picks that single query's "
              "parameter; below 5L the rotation also advances across "
              "iterations within a run.");
TPCH_FLAG_INT(bgw_pct, 0, "Percentage of background write transactions");
TPCH_FLAG_BOOL(bg_query_thread, false,
               "Spawn a read-only background thread that runs the same foreground query "
               "on a second worker, back-to-back, for the duration of the foreground "
               "TX loop. Measures how the foreground throughput holds up under "
               "concurrent same-query contention. No writes, no cross-query work.");
TPCH_FLAG_BOOL(bg_point_lookups, false,
               "If true and --bg_query_thread=true, the bg cohort also includes "
               "a point-lookup step that picks a random base table and random PK "
               "and calls adapter.lookup1. Models noisy-neighbor contention from "
               "a workload using different secondaries than the foreground query. "
               "When --bg_query_thread=false, this flag has no effect.");
TPCH_FLAG_BOOL(log_progress, true, "Log loading/query progress");
TPCH_FLAG_BOOL(micro_perf, false,
               "Capture RocksDB PerfContext / IOStatsContext per query and print totals");
TPCH_FLAG_BOOL(cfstats, false,
               "Snapshot per-CF RocksDB stats before/after helper.run() and print diff");
TPCH_FLAG_INT(load_only_structure, -1,
              "If >=1, populate only the secondary needed for this --storage_structure "
              "at load time. Used by the A5 isolated-DB experiment to remove cross-"
              "structure cache pollution. Default -1 = load all secondaries.");
TPCH_FLAG_STRING(coli_walker_variant, "fused_emit",
                 "COLI walker dispatch: 'baseline' = std::variant + visitor; "
                 "'fused_emit' = tag-byte switch + direct typed dispatch. "
                 "Default flipped to 'fused_emit' post-A2c (q3i/PERFORMANCE.md §2 H4); "
                 "pass coli_walker_variant=baseline for A/B regression.");
TPCH_FLAG_INT(use_seek_skip, -1,
              "Override Backend::USE_PHYSICAL_SEEK_SKIP at runtime: "
              "-1 = use trait (default), 0 = force off, 1 = force on. "
              "A3-Linux re-A/B (q3i/PERFORMANCE.md §3 A3 RocksDB re-open).");
TPCH_FLAG_STRING(q10_view_variant, "lineitem",
                 "Q10 S2 view A/B (q10/PERFORMANCE.md): 'lineitem' = per-lineitem "
                 "view + D4 drain-time date filter (baseline); 'preagg' = per-order "
                 "pre-aggregated view (returnflag baked at load, date live at query). "
                 "Both views live in one image; this selects which query_by_view reads. "
                 "Read only by the q10 query path; inert for other queries.");
TPCH_FLAG_STRING(q10i_view_variant, "lineitem",
                 "Q10I S2 view A/B: 'lineitem' = per-lineitem view (baseline); "
                 "'preagg' = per-order pre-aggregated view (returnflag + i_status "
                 "partition baked at load, date live at query). Both views live in one "
                 "image; selects which query_by_view reads. Read only by the q10i path.");
TPCH_FLAG_INT(skip_order_physical, -1,
              "Q10 S3 SkipOrder A/B (q10/PERFORMANCE.md): override the COL "
              "col_group_walk order-level skip strategy. -1 = default "
              "(logical forward-iterate, current behaviour on both backends), "
              "0 = force logical, 1 = force physical seek to the next order. "
              "Physical seek skips a date-failing order's co-located lineitems "
              "instead of decoding them; A/B'd on both backends.");
// acoli_projected flag retired 2026-05-03 alongside customer_acoli_q3i_t /
// orders_acoli_q3i_t (ids 51/52).  The projected aCOLI variant embedded
// pre_revenue (parameterised by l_shipdate) and was never wired into
// production targets.  Project-pushdown may be revisited in a future A/B.

#undef TPCH_FLAG_INT
#undef TPCH_FLAG_BOOL
#undef TPCH_FLAG_STRING
