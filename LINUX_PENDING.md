# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **refresh_sales (RF1/RF2): SF≥15 sweep + methodology follow-up
  (2026-05-23)**: first Linux bring-up DONE at SF=1 — (1) `refresh_sales_btree`
  builds + runs on Linux (fixed a SEGV: `recover_last_ids()` was called
  off-Worker in `executable_leanstore.cpp`; now wrapped in `scheduleJobSync`);
  (2) `test_query_q{3,5}_btree` extended with the RF1+RF2 round-trip (green;
  also fixed the q5 leanstore digest to hash `n_name` not the always-0
  `n_nationkey`); (3) S1–S4 sweep both backends ran, results + tidy summary in
  `paper-data/2026-05-23-refresh-sf1/`; (4) `refresh_sales/RUNS.md` entry
  appended. Warm steady-state supports §5.4 (merged RF1 ≈ split, both ≫ view).
  **Remaining for a paper-final number**: (a) re-run at **SF≥15** — the RF2
  reservoir (`ORDERS_SCALE×SF`=1500 at SF=1) exhausts early, so the size-stable
  RF1+RF2 *pair* and RF2-delete steady-state aren't measurable at SF=1; (b) add
  a `--warmup_seconds` pre-loop so whole-run avg isn't warmup-confounded (merged
  warms slower via custkey-scattered access; lsm S4 hit a tail compaction
  stall); (c) optional: wire `refresh_sales_{lsm,btree}` into
  `generate_targets.py` with image-copy logic so it joins the standard
  `make X scale=...` flow (currently driven by `build/scratch/run_refresh_hot.sh`).

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
