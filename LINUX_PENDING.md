# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **Q5 first Linux perf sweep**: collect SF=15 (and optionally SF=40
  disk-bound) `TPut.csv` for `q5_lsm` and `q5_btree`. Q5 S1–S4 strict
  XOR parity verified at SF=1 on macOS after Phase 10B (2026-05-11,
  digest=0x2df0d67874759c18); production targets wired via
  `generate_targets.py`. No S5 variant (deliberate — see
  `frontend/tpch/q5/CLAUDE.md §Storage Structure Options`).

  **Phase 10B note**: `frontend/tpch/q5/load.tpp` was modified (NATION
  scan added to the S2 view loader). Any Linux node that has a
  previously-loaded DB image will need a reload pass before the perf
  sweep — the view payload schema changed (added `n_name` field).

  Pre-sweep checklist (per `CLAUDE.md` Workflow Rules):

  1. `git fetch && git merge` on the Linux node. If any of
     `frontend/tpch/tpch_workload.hpp`,
     `frontend/tpch/tpchi_family/tpchi_workload.hpp`, or
     `frontend/tpch/q5/load.tpp` had its mtime bumped, kick off a
     `--load_only_structure=N` reload pass for each N to avoid the
     first-run reload tax on the actual perf invocation.
  2. `make -C build/frontend test_load_col_lsm test_load_col_btree
     test_query_q5_lsm test_query_q5_btree` and confirm `[OK]` across
     S1–S4 on both backends. Perf numbers from an unverified binary
     are noise — only proceed once every parity test is green.
  3. Run the sweep:

     ```bash
     make q5_lsm scale=15 dram=0.1
     make q5_btree scale=15 dram=0.1
     # Optional disk-bound replay:
     make q5_lsm scale=40 dram=0.1
     ```

  4. Append a `frontend/tpch/q5/RUNS.md` entry per the file's
     documented format, including a one-sentence claim-check against
     the predicted **S3 ≥ S2 > S1/S4** §3.1.3 pure-hierarchical shape
     (Q5 mirrors Q3's shape; cross-reference `frontend/tpch/STATUS.md`
     and `frontend/tpch/q3i/PERFORMANCE.md` for tonal precedent).
  5. Update `frontend/tpch/STATUS.md` Q5 row with a one-line perf
     headline mirroring Q3's format.
  6. Once landed, move this entry into `LINUX_HISTORY.md` under a new
     dated subsection with a one-line outcome summary.
