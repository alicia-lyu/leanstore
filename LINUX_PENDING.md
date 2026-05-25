# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **refresh_sales at 5H (c3, DRAM 0.4) — show LSM strength under memory
  pressure (2026-05-24)** — the 5L refresh (DRAM 1.0,
  `paper-data/2026-05-24-refresh-5L-ssd`) is done; 5H keeps the same SF
  (3850 lsm / 1550 btree, ~5 GiB secondary) but tightens DRAM to 0.4 to
  isolate the memory-pressure effect. **Hypothesis:** LSM
  (write-optimized — sequential SST flush + compaction) holds RF1/RF2
  throughput better than btree (random page eviction) once the buffer
  pool can't hold the working set. **Ready-to-run:**
  `build/scratch/run_refresh_5H_ssd.sh` + `summarize_refresh_5H_ssd.sh`
  (recover from the existing SSD family images on per-structure copies,
  drop caches, S1–S4 both backends, 90s; gitignored — mirror the 5L
  scripts with `DRAM=0.4`). Needs the **real SSD free** (currently the
  paused 5H q-sweep + the q10 agent contend for it). Complement to the
  *abundant*-memory finding (`2026-05-24-refresh-prewarm9` LSM addendum:
  at DRAM 9 LSM showed **no** in-memory speedup and `--prewarm`
  backfired) — the pressured regime is where LSM should instead win.

- **No `latency.csv` from any binary (2026-05-23)** — **DEFERRED to
  next project (2026-05-23)**: btree binaries emit `cpu/bm/cr/dt.csv`
  but no `latency.csv`; LSM emits none of them. Analyzer no longer
  references `latency_p50_ms` / `latency_p99_ms` (columns dropped from
  `DIAG_FIELDS`); the diagnostics-explore plot's P99 panel renders as
  "latency_p99_ms not in CSV" — explicit signal, not silent gap.
  Populating it means threading latency-quantile capture through the
  TPut/`tx_seconds` loop in `frontend/tpch/tpch_workload.hpp` (or
  equivalent) **and re-running the entire experiment matrix** to
  backfill every existing sweep — out of scope for this project;
  revisit next project.

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
