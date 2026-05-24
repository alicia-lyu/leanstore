# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **Q10I Phase 5 — Linux perf sweep (5L cell, 2026-05-24)**:
  Phase 4 landed on macOS worktree branch
  `worktree-agent-a86745538133069de` — all four `query_by_*`
  parity-verified at SF=1/5/10 with strict 4-way XOR digest. **On
  Linux**: (a) check out the worktree branch on a fresh node (do
  NOT merge to main — `lineitem_coli_t` widening invalidates
  Q3I/Q5I persisted images on main per E1); (b) build
  `q10i_lsm` + `q10i_btree`; (c) run `test_query_q10i_btree` at
  SF=1 for cross-backend parity (must reproduce the digest
  `0xfe5d346e48bc1e8d`); (d) **5L cell sweep only**: SF=15, S1–S4,
  unlimited DRAM, the paper-headline configuration. Append
  `frontend/tpch/q10i/RUNS.md` after the sweep. Expected story
  (per design doc): **S3 ≥ S2 > S1/S4** mirroring Q3I/Q5I. **S5
  (aCOLI) deferred** per Q10I D3 — the orderdate window makes
  per-customer pre-totals incomposable. Pair-fates with Q10 — run
  Q10's 5L cell in the same session for the paper's joint
  Q10/Q10I comparison.

- **DBToaster baseline (`dbtoaster/`): Phase 0 install + build + sweep
  (2026-05-24)**: scaffold landed on macOS-side dev (SQL, CMake, Makefile,
  Dockerfile, entrypoint, READMEs; `main.cpp` pending). **On Linux**: (a) run
  `LINUX_SETUP.md §Step 6` to install DBToaster 2.3 (`/opt/dbtoaster`) +
  `openjdk-11` + dbgen; (b) `make refresh_sales.hpp` then **inspect the
  generated header** for the real trigger/event API (`on_insert_*` /
  `on_delete_*` signatures, `event_t`/`event_type`, the DBToaster `date`
  representation, `get_relation_id`) — `main.cpp` must be written/finished
  against these actual symbols; (c) correctness verify at small SF (maintained
  map row-counts vs an offline DuckDB join + RF1/RF2 size-stable delta);
  (d) pin SF for a ~5 GB working set, run the RF1/RF2 timed sweep + `ulimit`
  ladder (unlimited, 1.0 GiB/5L, 0.4 GiB/5H), append `refresh_sales/RUNS.md`.
  Compares against LeanStore S2 (btree 22.6k / lsm 4.2k RF1 inserts/s @ SF=1).

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
