# Linux / LeanStore Pending Tasks

Items developed on macOS (RocksDB-only, compile-check + unit/load tests)
that need follow-up on a Linux node (full LeanStore B-tree backend,
end-to-end perf sweeps). See [`CLAUDE.md`](CLAUDE.md) Workflow Rules
for the macOS-vs-Linux discipline. Resolved items are rotated out to
[`LINUX_HISTORY.md`](LINUX_HISTORY.md) — keep this file lean and
actionable.

## Active

- **Q5I btree perf sweep rerun**: parity gate restored
  2026-05-17 in `test_query_q5i_leanstore.cpp` — the harness was
  silently skipping `populate_split` / `populate_merged` /
  `populate_q5i_view`, so S1/S2/S3 scanned empty secondaries and
  returned 0 rows on btree at SF≥5 while S4 (base-tables-only)
  returned the correct answer. The original "memcpy round-trip"
  hypothesis was wrong: pure harness omission, not a libstdc++
  parity bug. Fix also ports the RocksDB harness's cardinality +
  sentinel-ordering diagnostic block (adapted to use
  `adapter.size()` inside a `scheduleJobSync` Worker TX in place
  of RocksDB `raw_bytes_in_cf`) so any future drift surfaces as
  `[FAIL]` rows instead of vacuous parity. Validated: SF=1
  non-vacuous parity at `0x9ccdd0fdb2071b38` rows=2; SF=5 parity
  at `0x978b90044797a768` rows=4; SF=10 parity at
  `0x7ebfdbe518bc6d7e` rows=5. LSM SF=5 regression-clean (parity
  holds; digest does not match the prior `0x2f31fe8244b728c1`
  recorded in `q5i/RUNS.md` — within-backend parity is what the
  test verifies, not cross-run digest stability).
  *Remaining work*: rerun `make q5i_btree scale=15 dram=0.1` and
  replace the BLOCKED entry in `frontend/tpch/q5i/RUNS.md`
  (2026-05-17 00:43 MDT) with the validated perf record.

*(Q5 first Linux perf sweep closed 2026-05-14, Q5I LSM first
Linux sweep closed 2026-05-17; see
[`LINUX_HISTORY.md`](LINUX_HISTORY.md). Optional Q5 SF=40
disk-bound replay deferred without owner.)*
