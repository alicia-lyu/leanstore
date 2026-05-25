# Q10 — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date
this file unless their numbers are scattered across other docs and
need consolidating.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  for this query and how?
```

## Runs

### 2026-05-25 01:16 UTC — first Linux 5L perf sweep (both backends)
- **Commit**: `8aac715a` (`calcite-integration`), host `c220g2-011011` (Linux)
- **TPut.csv**: `build/q10_lsm/TPut.csv` (SF=3850) + `build/q10_btree/TPut.csv` (SF=1550)
- **Config**: 5L cell (c0) — DRAM=1.0 GiB, structures=1+2+3+4, SF=3850 (lsm) /
  1550 (btree), isolated (bg=0), `tx_seconds=15` (each Q10 query runs to
  completion past the budget). Fresh `q10.load()` at both scales (the S2
  `q10_view` is not in the shared q3/q5 images). Parity gate green first at
  SF=1: `test_query_q10_lsm` (iter0 `0xe8eb…`, iter1 `0xc6a7…`) and
  `test_query_q10_btree` (iter0 `0xa0c7…`, iter1 `0xe78c…`; the leanstore test
  needs `--wal=true --trunc=true`), strict 4-way XOR parity, 20 rows.
- **ms/query** (lower=better): **LSM** S3 11,326 < S1 16,059 < S2 17,169 ≪ S4
  77,989; **btree** S1 23,535 < S3 35,262 ≪ S4 127,486 < S2 174,260.
- **Claim check**: **supports** the §3.1.3 hierarchical-prefix COL-MI thesis —
  **S3 ≥ S2 and S3 ≫ S4 on both backends** (btree S3 beats the 8.6 GiB view
  ~5×; both ≫ hash). S3 wins outright on LSM; on btree the dense
  custkey-sorted split S1 edges S3 while both crush S2/S4. No q3i-style S2>S3
  anomaly (Q10 is vanilla COL, no invoice sibling). 31 benign
  `turnPage→gotoPage` fallbacks during btree S3 under page pressure; query
  completed clean (exit 0), S3 correctness already pinned by the SF=1 parity gate.

### 2026-05-24 — Phase 5 complete (correctness-only, macOS SF=1)
- **Commit**: Phase 5 commit 1 (`calcite-integration`)
- **TPut.csv**: not produced — macOS correctness check only
- **Config**: SF=1, DRAM=default, structures=1+2+3+4 (all live), host=macOS (RocksDB-only)
- **Claim check**: correctness only — `test_query_q10_lsm` now
  rotates the harness through TWO distinct param sets and asserts
  strict 4-way XOR parity at each. iter=0 (1993-10-01,
  `digest=0xfb1196e6072d5aef`) and iter=1 (1993-02-01,
  `digest=0xe6057c38ca32a1e9`) both report 20 rows / all four paths
  agree. Per-path `Q10Stats` blocks now read clean single-path
  cardinalities (e.g. `customers_scanned == 150` not `600` as
  before). Linux 5L perf sweep tracked in `LINUX_PENDING.md`.

### 2026-05-24 — Phase 4b complete (correctness-only, macOS SF=1)
- **Commit**: Phase 4b commit 3 (`calcite-integration`)
- **TPut.csv**: not produced — macOS correctness check only; no perf run
- **Config**: SF=1, DRAM=default, structures=1+2+3+4 (all live), host=macOS (RocksDB-only)
- **Claim check**: correctness only — `test_query_q10_lsm` reports
  strict 4-way XOR parity at SF=1 (S1 == S2 == S3 == S4, matching
  non-zero digest across two fresh loads). 20 rows per path,
  spec-aligned (LIMIT 20). All four `query_by_*` paths live through
  the shared `Q10PerCustomerAggregator` + `q10_finalize_aggregator`
  pipeline (NATION INL on PK at emit per D6). Paper-claim
  verification deferred to Linux 5L sweep.

### 2026-05-23 — Phase 4a complete (correctness-only, macOS SF=1)
- **Commit**: Phase 4a commit 3 (`calcite-integration`)
- **TPut.csv**: not produced — macOS correctness check only; no perf run
- **Config**: SF=1, DRAM=default, structures=3 (S3 live; S1/S2/S4 stubs), host=macOS (RocksDB-only)
- **Claim check**: correctness only — `test_query_q10_lsm` reports
  `[OK]` on all secondary cardinality + sentinel-ordering + Pattern B
  `pipeline_view rows == |lineitem|` + S3 sanity (non-zero digest, 20
  rows after TopN). Q10Stats funnel: 150 customers → 1500 orders → ~46
  pass date → ~189 lineitems → ~107 returned → ~35 candidate customers
  → top 20 emitted. Paper-claim verification deferred to Linux 5L sweep
  after §7.2 / §7.3 / §7.5 land.
