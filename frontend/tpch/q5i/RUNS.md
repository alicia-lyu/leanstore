# Q5I — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  for Q5I (COLI MI § 3.1.2 sibling sub-aggregate over the
  Customer→{Orders, Invoice} fan-out) and how?
```

## Runs

### 2026-05-16 22:22 PDT — Phase 4b macOS correctness verification

- **Commit**: `19104306` (`calcite-integration`)
- **TPut.csv**: not produced — correctness-only macOS run via `test_query_q5i_lsm`
- **Config**: SF=1, DRAM=default, structures=1,2,3,4 (S5 deferred), host=macOS (RocksDB)
- **Claim check**: cannot evaluate the S3 ≥ S2 > S1/S4 perf thesis on
  macOS (per project policy — macOS is for compile-checks and
  correctness only). Confirms only that all four query bodies agree
  on the answer: strict 4-way XOR parity at non-zero digest (e.g.
  `0x2b8dfff15144e310` with 2 rows, `0xc70476c481ebf544` with 1 row
  across reloads — data layout is non-deterministic at SF=1 but
  parity always holds). Linux SF=15 perf sweep tracked in
  `LINUX_PENDING.md`.

### 2026-05-17 00:39 MDT — q5i_lsm SF=15 DRAM=0.1 GiB first-Linux sweep
- **Commit**: `856eaa1e` (`calcite-integration`)
- **TPut.csv**: `build/q5i_lsm/TPut.csv` rows 2–5 (S1–S4; no
  throwaway pre-load — the `q5i_lsm:` Makefile target only chains
  `q5i_lsm_{1..4}`, unlike Q5).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (CloudLab
  `node0.alicial-306048.advosuwmadison-pg0.utah.cloudlab.us`,
  Xeon D-1548 @ 2.00 GHz, 16 cores, NVMe Toshiba 256 GiB ext4
  `/mnt/ssd`).
- **Claim check**: Partially supports the §3.1.2 paper claim —
  measured shape **S2 (161.66) > S3 (76.29) > S1 (24.39) ≈ S4
  (15.30) TX/s** has S2/S3 ≫ S1/S4 as predicted but inverts
  S3 ≥ S2 to S2 > S3, matching Q5 LSM
  ([`q5/RUNS.md`](../q5/RUNS.md) 2026-05-14 entry) and Q3I LSM.
  Same COL-family walker infrastructure gap; not Q5I-specific.
- **Parity gate**: `test_query_q5i_lsm` at SF=5 strict 4-way
  parity at digest `0x2f31fe8244b728c1`, rows=3 (SF=1 was
  vacuous-but-parity-clean — all four 0 rows for the default
  ASIA+1994-01-01 layout; non-vacuous confirmation at SF=5).

### 2026-05-17 00:43 MDT — q5i_btree SF=15 DRAM=0.1 GiB first-Linux sweep — RECLASSIFIED (BLOCKED label was a false alarm)
- **Commit**: `856eaa1e` (`calcite-integration`)
- **TPut.csv**: `build/q5i_btree/TPut.csv` rows 2–5: S2=302.17,
  S3=145.43, S1=49.74, S4=44.73 TX/s. Cache-resident (SF=15
  DRAM=0.1 has secondaries 49–62 MiB ≪ DRAM); see
  [`../RUNS.md §"Why we don't trust SF=15 / DRAM=0.1 alone"`](../RUNS.md).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (same host as
  LSM entry above), `--vi=false --mv=false` per `targets.mk:731`.
- **Reclassification (2026-05-17 10:44 MDT, commit `82927bf8`)**:
  These numbers were originally labeled BLOCKED on the inference
  that the `test_query_q5i_btree` parity failure (S1/S2/S3
  rows=0 digest=`0x0`) implied the production binary was equally
  broken. **That inference was wrong.** The bug was in the test
  harness (`test_query_q5i_leanstore.cpp` skipped
  `populate_split` / `populate_merged` / `populate_q5i_view`),
  not in the production binary — `Q5IWorkload::load()` at
  `q5i/load.tpp:77-81` always calls those populate hooks, so the
  end-to-end sweep ran against properly populated secondaries.
  Confirmed by re-running the same SF=15/DRAM=0.1 sweep after
  the harness fix: S2=235.33, S3=131.05, S1=44.71, S4=41.29 TX/s
  (TPut.csv rows 6–9) — within normal TPC-H load variance of
  the original numbers. The entry stays as the cache-resident
  data point but is no longer treated as "noise".
- **Parity gate**: now [OK] at SF=5 (commit `82927bf8`, digest
  `0x978b90044797a768` rows=4). Test-harness fix and SF=15
  re-run consolidated into the SF=620 sweep below.

### 2026-05-17 10:44 MDT — q5i_btree SF=620 DRAM=0.4 GiB beyond-memory sweep
- **Commit**: `82927bf8` (`calcite-integration`)
- **TPut.csv**: `build/q5i_btree/TPut.csv` rows 10–13 (SF=620,
  DRAM=0.4, S1–S4).
- **Config**: SF=620, DRAM=0.4 GiB, S1–S4, Linux (same host as
  earlier entries), `--vi=false --mv=false`. 2 GiB secondary
  target per `frontend/tpch/RUNS.md §Target-SF table`
  (BTree ≈ 3.3 MiB/SF; SF=620 → ~2 GiB) and 1/5 DRAM rule
  (DRAM = 2 GiB / 5 = 0.4 GiB). Measured secondary sizes:
  S1=2496, S2=2555, S3=2544, S4=2018 MiB; secondary/DRAM ratio
  ≈ 6.4× — well into beyond-memory operating point.
- **Claim check**: Partially supports the §3.1.2 paper claim —
  measured shape **S2 (0.198) > S3 (0.164) > S1 (0.042) > S4
  (0.018) TX/s** has S2/S3 ≫ S1/S4 as predicted but inverts
  S3 ≥ S2 to S2 > S3, the same shape Q3I/Q5 exhibit on both
  backends (paper-acknowledged COL-walker infrastructure gap;
  see [`q3i/PERFORMANCE.md`](../q3i/PERFORMANCE.md)). S3 is
  ~4× S1 and ~9× S4, so the merged-index advantage over the
  split-merge and hash baselines is intact under disk-bound
  load. R MiB / TX 0.017–0.05 (vs ~5e-5 at SF=15) confirms
  real I/O, not cache-resident.
- **Parity gate**: `test_query_q5i_btree` at SF=5 strict 4-way
  parity at digest `0x978b90044797a768` rows=4 (commit
  `82927bf8`); SF=10 also clean.
