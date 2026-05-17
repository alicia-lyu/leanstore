# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **Q5I first Linux perf sweep**: macOS correctness for S1–S4
  verified at SF=1 (commit `19104306`). Production `q5i_lsm` /
  `q5i_btree` targets and Makefile rules (`q5i_lsm`, `q5i_lsm_{1..4}`,
  reload variants) already wired. Need on a Linux node:
  1. Build `q5i_lsm` + `q5i_btree`.
  2. Run TX-loop sweep at SF=15 DRAM=0.1 across structures 1–4
     (`make q5i_lsm scale=15 dram=0.1` and the `q5i_btree`
     equivalent).
  3. Append `TPut.csv` paths to `frontend/tpch/q5i/RUNS.md`.
  4. Verify the Q5I paper thesis **S3 ≥ S2 > S1/S4** holds — and
     note whether Q5I inherits the S2 > S3 inversion that Q5 showed
     at the same scale (LSM: S2 205 vs S3 156 TX/s). The inversion
     is infrastructure-level (view materialisation beats MI scan
     for COL-family pipelines on the current walker), not
     query-specific; expect it to recur for Q5I unless the COLI
     walker tuning closes the gap.
  Reload eagerly per `CLAUDE.md` Workflow Rules if any load-path
  file mtime is newer than the persisted iso `.json`.

*(Q5 first Linux perf sweep closed 2026-05-14; see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Optional SF=40 disk-bound
replay deferred without owner.)*
