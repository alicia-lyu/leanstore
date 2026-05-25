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

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
