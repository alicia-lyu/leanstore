# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **refresh_sales (RF1/RF2) Linux perf sweep (2026-05-23)**: macOS gate
  green — `refresh_sales_lsm` builds clean; `test_query_q3_lsm` +
  `test_query_q5_lsm` now include an RF1+RF2 round-trip parity check
  (insert N=10, verify 4-way digest agreement post-RF1; delete the same
  keys, verify digest restored to pre-update value). Linux follow-up:
  (1) build `refresh_sales_btree` on a Linux node; (2) extend the
  *leanstore* variants of the q3/q5 query tests (`test_query_q{3,5}_btree`)
  with the same round-trip block (mechanical copy of the rocksdb test
  additions — see the new `apply_rf1_all` / `apply_rf2_all` lambdas);
  (3) run a perf sweep per structure (S1/S2/S3/S4), `--update_size=1`,
  `--refresh_seconds=15`, on a *copy* of the vanilla-family loaded
  image (read images stay clean per `frontend/tpch/refresh_sales/CLAUDE.md`);
  (4) collect `RefreshTPut.s<N>.csv` per structure and append a
  `frontend/tpch/refresh_sales/RUNS.md` entry. Headline check: S3 merged
  RF1+RF2 cost ≈ S1 split, both ≪ S2 view — supports §5.4 "merged
  matches view on reads without paying view's update cost". Optional:
  add `refresh_sales_{lsm,btree}` to `generate_targets.py` (currently
  built via raw `make refresh_sales_lsm`; needs image-copy logic before
  it can join the standard `make X scale=...` flow).

- **LSM binaries don't emit per-tx detail CSVs or rocksdb stderr
  counters (2026-05-23)**: q3/q5/q3i/q5i_lsm and geo_lsm write only
  `TPut.s<N>.csv` + `size.s<N>.csv` + an *empty* `structure<N>_stderr.txt`.
  No `query/<method>/{bm,cpu,cr,dt}.csv` subdir, no rocksdb
  `block.read.count` / `block.cache.{hit,miss}` lines in stderr.
  Analyzer now backfills `cpu_cycles_per_tx` + `cpu_util_pct` +
  `sst_{read,write}_us_per_tx` + `sst_compaction_us` from TPut as
  surrogates, but `cpu_llc_miss_per_tx`, `bm_*`, `cr_*`, `dt_*`, and
  `lsm_block_*` remain empty for LSM rows. *Investigate*: the LSM
  binary's perf-event emit path (likely `frontend/tpch/*/q*_lsm.cpp`
  + the RocksDB adapter) — turn on perf counter capture and dump
  rocksdb statistics to stderr at end-of-structure. Re-running a
  small targeted sweep (one rep × one cell) is enough to validate.

- **No `latency.csv` from any binary (2026-05-23)**: btree binaries
  emit `cpu/bm/cr/dt.csv` but no `latency.csv`; LSM emits none of
  them. Analyzer no longer references `latency_p50_ms` /
  `latency_p99_ms` (columns dropped from `DIAG_FIELDS`). The
  diagnostics-explore plot's P99 panel renders as "latency_p99_ms
  not in CSV" — explicit signal, not silent gap. To populate:
  thread latency-quantile capture through the TPut/`tx_seconds`
  loop in `frontend/tpch/tpch_workload.hpp` (or equivalent).

- **LeanStore `c_hash` is emitted as 7 unquoted comma-separated
  subfields (2026-05-23)**: `<run>/<tx>/<method>/{bm,cpu,cr,dt}.csv`
  rows have a leading `c_hash` value like `7,978,927,218,222,587,432`
  that breaks DictReader column alignment. Analyzer now detects the
  shift per-row (`skip = len(row) - len(header)`), but the cleaner
  fix is to quote the field at emit time. Search for the c_hash
  printer in `backend/profiling/` (likely a `fmt::format` call that
  dumps the hex blob) — wrap in quotes or hex-encode without commas.
  Not urgent (analyzer is robust), but every sweep currently writes
  invalid CSV by strict parsers.

*(All Q5I bring-up items closed 2026-05-17 — see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Q5 first Linux perf sweep
closed 2026-05-14, Q5I LSM and btree first-Linux sweeps closed
2026-05-17. Optional Q5 SF=40 disk-bound replay deferred without
owner.)*
