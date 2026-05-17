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

### 2026-05-17 00:43 MDT — q5i_btree SF=15 DRAM=0.1 GiB first-Linux sweep — BLOCKED (parity failure)
- **Commit**: `856eaa1e` (`calcite-integration`)
- **TPut.csv**: `build/q5i_btree/TPut.csv` produced (S2=302.17,
  S3=145.43, S1=49.74, S4=44.73 TX/s) **but NUMBERS ARE NOISE** —
  three of four paths silently return empty results (see Parity
  gate below). Withholding these as the perf record; rerun and
  replace this entry after the bug is fixed.
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (same host as
  LSM entry above), `--vi=false --mv=false` per `targets.mk:731`.
- **Parity gate**: **FAILED.** `test_query_q5i_btree` at SF=5
  shows S1/S2/S3 rows=0 digest=`0x0` while S4 rows=2 digest=
  `0x8a99cce624ffbeb1`. S4 is the correct answer (matches the
  LSM SF=5 row-count signal). S1 (BMJ chain), S2 (view scan),
  and S3 (COLI walker) silently drop all data on btree at SF≥5.
  SF=1 passed parity vacuously (all four 0 rows) so the bug was
  latent until SF=5. Bug tracked in `LINUX_PENDING.md`.
