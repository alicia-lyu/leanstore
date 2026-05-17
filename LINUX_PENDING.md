# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **Q5I btree S1/S2/S3 parity failure on Linux (SF≥5)**: discovered
  2026-05-17 during the first-Linux Q5I bring-up. At SF=5 on btree,
  S1 (BMJ chain), S2 (pipeline view scan), and S3 (COLI walker)
  all silently return 0 rows while S4 (hash-join over base tables)
  correctly returns 2 rows at digest `0x8a99cce624ffbeb1`. SF=1
  passes parity vacuously (all four return 0 rows, so the failure
  is latent at the smallest scale and would not have been caught
  by the existing `test_query_q5i_btree` SF=1 gate). LSM passes
  parity at SF=5 (rows=3, digest `0x2f31fe8244b728c1`) so the bug
  is btree-specific. The three failing paths all consume the COLI
  merged index or its split-COLI siblings (S2 reads
  `q5i_pipeline_view_t` which is loaded from the COLI walker per
  Phase 4a commit 1); S4 reads only base TPC-H tables. Suspect
  hypothesis: payload memcpy round-trip mismatch between
  libstdc++ (Linux) and libc++ (macOS), same family of issue as
  the Q5 `q5_pipeline_view_t.n_name` `std::string` bug closed
  2026-05-14 in [`LINUX_HISTORY.md`](LINUX_HISTORY.md). Need to:
  1. Diagnose which payload field round-trips wrong on LeanStore;
     `test_query_q5i_btree` has no cardinality / sentinel-ordering
     prints (the LSM test has rich diagnostics), so consider porting
     the LSM test's check block first to localise the failure
     (splits vs merged vs view vs query stage).
  2. Fix in place and verify `test_query_q5i_btree` SF=5/10 strict
     4-way parity.
  3. Rerun `make q5i_btree scale=15 dram=0.1` and replace the
     placeholder btree entry in `frontend/tpch/q5i/RUNS.md` with
     the validated perf record.

*(Q5 first Linux perf sweep closed 2026-05-14, Q5I LSM first
Linux sweep closed 2026-05-17; see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Optional Q5 SF=40
disk-bound replay deferred without owner.)*
