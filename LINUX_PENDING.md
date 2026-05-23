# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **paper-data diagnostics columns are empty across all sweep rows
  (2026-05-23)**: `paper-data/2026-05-18-b/summary/diagnostics.csv`
  has every attribution column blank — `cpu_*`, `bm_*`, `cr_*`,
  `dt_*`, `latency_*`. Headline columns (`tx_per_s`, `ms_per_tx`,
  `size_mib`) are populated correctly, so the analyzer ran and the
  TPut files are intact. The gap is in the per-tx detail join:
  `analyze_paper_sweep.py:238-242` looks for
  `<run_dir>/<tx>/<method>/{cpu,bm,cr,dt,latency}.csv` and falls back
  to `None` if missing.
  *Investigate on Linux*: pick one run dir (e.g.
  `paper-data/2026-05-18-b/raw/q3_lsm/c1-bg2-r1/`), list its
  subdirectories — confirm whether the per-tx detail directories
  exist, and if so whether their CSV headers match the lookup keys
  in `analyze_paper_sweep.py:247-267` (e.g. `"workers LLC-misses /
  TX"`, `"bm_free_pct"`, etc.). Fix may be in the binary's perf-event
  emit code, the analyzer's column-name lookup, or both. Once fixed,
  re-run the analyzer on `-b` to refresh the summary CSV (no need
  to re-sweep).
  *Blocks*: `plot_paper_sweep.py --mode diagnostics-explore` renders
  all six panels as "no data". Plotter is correct, ready for data.

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
