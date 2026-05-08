# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps).  See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline.

## Q3 perf sweep (post-Phase-4, 2026-05-04)

- **Targets**: `q3_lsm` and `q3_btree` at `--storage_structure 1..4`,
  SF=15 (per Makefile default) — wired in `frontend/CMakeLists.txt`
  and `generate_targets.py` (lines 19, 458–470).
- **Reference shape**: Q3I S3 ≥ S2 > S1/S4 pitch (see
  `frontend/tpch/q3i/PERFORMANCE.md §A1`: +356% at SF=15, +116× at
  SF=40 disk-bound).
- **Expectation**: Q3 is the no-invoice baseline of the same COL
  family; S3 should still match/beat S2 and exceed S1/S4, but with
  smaller absolute gaps because the §3.1.2 sibling-aggregate
  amortisation (Q3I-only) is absent.  Use this comparison to
  isolate the §3.1.3 hierarchical-prefix benefit from the §3.1.2
  sibling-aggregate benefit.
- **Pre-flight**: load-path files
  (`frontend/tpch/tpch_workload.hpp`,
  `frontend/tpch/q3/load.tpp`) gate the persisted JSON image —
  any merge that bumps their mtime forces a full reload on the
  next perf run.  Schedule `make q3_lsm scale=15 dram=0.1`
  (or `--load_only_structure=N` per structure) to refresh
  before clean numbers.
- **Verification harness already green on macOS**:
  `test_query_q3_lsm` strict cross-structure XOR parity at SF=1,
  rows=10, identical digest across S1–S4.

## Linux-only test targets (already wired, run when on Linux)

- `test_query_q3_btree` — Linux-only LeanStore variant of the
  cross-structure parity harness.
- `test_load_col_btree` — Linux-only LeanStore variant of the COL
  load test.
