# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

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

### From last-20-commit sweep (2026-05-27)

Open follow-ups flagged in commit bodies on `calcite-integration`
(2026-05-25 → 2026-05-26). Each cites its source commit SHA. Items
already closed by a later commit are omitted (notably 63c48999's "run
the 5L sweep / regen q10.pdf" — completed by 60702452).

- **S6 shared-view Linux perf sweep, Q3/Q5/Q10 (2026-05-27)** — commits
  aedf3cc1 (S6 `col_shared_view_t`, id=80), 6e216855 (plotter +
  `space_table.py` wiring). Code and analysis wiring landed and parity-
  verified at SF=1 on both backends, but **no S6 perf data exists yet**.
  Pending: run the S6 sweep so `TPut.s6.csv` lands and the
  "Mat-View (shared)" bar panels / space-table line render. The plotter
  already skips structures with no rows, so the sweep is the only missing
  piece. Q3+Q5 share one physical S6 table in the family image.

- **Q10 S6 view → family-image migration (2026-05-27)** — commit
  aedf3cc1. Q3+Q5 share one S6 table in the family image; Q10 currently
  builds a byte-identical copy in its **standalone** image. Migrating
  Q10's S6 into the shared family image is deferred.

- **Param-rotation A/B run (2026-05-27)** — commits 062afe29
  (`--param_seed` flag + q3 parity test), 94750ba9
  (`experiments/run_param_ab.sh` + `paper-data/scripts/param_ab_report.py`).
  Harness and reporter are built and code-validated only ("not exercised
  for this submission — time"). Pending: run the same-image
  default-vs-rotated A/B on Linux to back the "headline rests on a single
  substitution parameter" claim with evidence (currently a caveat in
  `paper-data/PAPER_EDITS.md` Edit 5).

- **refresh_sales bg=2 `bg_txs` caveat (2026-05-27)** — commit aee6c31a.
  Under the single time-balanced bg worker, S4 (all cells) and btree S1
  (5H/5HH) un-rotate to 2 heavy cold scans, giving a misleadingly low
  `bg_txs`. Noted limitation; candidate follow-up is a multi-worker bg
  cohort for the refresh sweep.

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
