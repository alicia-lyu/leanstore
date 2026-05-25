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

### 2026-05-25 — S5 aCOL 5L confirmation (both backends)
- **Commit**: `fd771c13` (binary; tree at `5d1fc153`), host `node0` (Linux)
- **Logs**: `build/q10_{btree,lsm}/{1550,3850}-in-1.0/structure5_acol.log`
  (`build/scratch/q10_5L_acol.sh`). Ported to
  `paper-data/2026-05-25-q10/summary/headline.csv` (`mi_acol_preagg` rows).
- **Config**: 5L (c0) DRAM=1.0, SF=1550 btree / 3850 lsm, isolated, tx_seconds=15.
  S5-only (reload built the aCOL into the image; S1-S4/S2-B 5L numbers stand from
  the prior sweep `f92d1868` — Q10 COL data deterministic + unchanged by the q10i
  merge). Ran serially (waited out a concurrent tpchi reload).
- **ms/query** — **btree**: S5 aCOL **160** ≪ S2-preagg 17,435 < S1 18,652 <
  S3 35,024 < S4 126,280 < S2-A 175,506. **lsm**: S5 aCOL **1,367** < S2-preagg
  1,658 < S3-phys 10,110 < S3 11,110 < S1 14,400 < S2-A 17,209 < S4 77,570.
- **Claim check**: the **fair pre-aggregated MI (aCOL, S5) is the fastest
  structure on both backends** — decisively on btree (~109× over the pre-agg
  view, ~219× over raw S3; the lineitem-free aCOL fits the 1 GiB pool while view
  and S3 are page-bound) and still ahead on lsm (1.2× over the view, ~8× over
  S3; block cache softens the page penalty so the co-location margin shrinks).
  Confirms the iteration-cell result. See `PERFORMANCE.md §7`.

### 2026-05-25 — S5 aCOL iteration-cell smoke (btree SF=150 dram=0.1)
- **Commit**: `fd771c13` (`calcite-integration`), host `node0` (Linux)
- **Logs**: `build/q10_btree/150-in-0.1/structureN_clean.log` (clean — re-run
  after a concurrent tpchi reload finished; an earlier contended run was
  discarded). TPut.csv rows scale=150.
- **Config**: iteration cell SF=150 dram=0.1, isolated, tx_seconds=15. Smoke
  test for the new aCOL S5 (correctness already green at SF=1 both backends).
- **ms/query** — **S5 aCOL 9.98** < S2-preagg 21.5 < S1 134 < S3 1,017 < S4
  12,071 < S2-A 17,234. **S5 is the fastest structure.** R MiB/q: S5 0.009,
  S2-preagg 0.072, S3 26.35; records visited/q: S5 118,589 (no lineitems) vs
  S3 1,146,669. aCOL get_size 340 MiB vs S3 424.
- **Claim check**: validates the fair-MI thesis — the aCOL (merged index
  allowed to pre-aggregate, hand-rolled walk) beats both the pre-agg view
  (~2.2×, no per-order customer-col duplication) and the raw S3 MI (~100×, no
  lineitem bulk). No anomaly. 5L confirmation next. See `PERFORMANCE.md §7`.

### 2026-05-25 — 5L A/B confirmation: S2 per-order preagg + S3 physical SkipOrder
- **Commit**: `f92d1868` (`calcite-integration`), host `node0` (Linux)
- **TPut.csv**: `build/q10_btree/TPut.csv` (scale=1550 rows) + LSM structure logs
  under `build/q10_lsm/3850-in-1.0/structureN_{baseA,preagg,physical}.log`
  (LSM RocksDBLogger emits no per-window R MiB).
- **Config**: 5L (c0) DRAM=1.0, SF=1550 (btree) / 3850 (lsm), isolated, `tx_seconds=15`
  (queries run to completion). Reloaded both images (load.tpp added the preagg
  view). A/B on one image each via `--q10_view_variant` (S2) and
  `--skip_order_physical` (S3). Parity green first at SF=1 (test_query_q10_{lsm,btree}:
  4-way XOR + S2-preagg==S3 + S3-physical==S3-logical, both param iters).
- **ms/query** — **btree**: S2-B **17,435** ≈ S1 18,652 < S3-A 35,024 ≈ S3-B 36,116
  ≪ S4 126,280 < S2-A **175,506**. **LSM**: S2-B **1,658** ≪ S3-B **10,110** <
  S3-A 11,110 < S1 14,399 < S2-A 17,209 ≪ S4 77,569.
- **Claim check**: **resolves both 5L anomalies.** (1) The S2 "regression" was a
  strawman per-lineitem view; the fair per-order pre-aggregated view (returnflag
  baked, date live) is **10× faster on btree (0 evictions) and 10.4× on LSM**,
  becoming the fastest structure on both. (2) S3<S1 on btree is the
  filter-hierarchy effect: the physical SkipOrder seek cuts mi_records_visited
  4.1× but leaves **R MiB unchanged (1016)** → no btree wall-time win (page-bound,
  sub-page orders); it **does** help LSM (+9%, no regression — refuted-macOS
  prefetch finding holds). Q10 is the boundary case where co-location is paid for
  but not exploited (prune below the co-location grain). See `PERFORMANCE.md`.

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
