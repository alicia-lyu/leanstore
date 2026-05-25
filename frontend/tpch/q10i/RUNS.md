# Q10I — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  for Q10I (COLI MI § 3.1.2 sibling sub-aggregate combined with §3.1.3
  hierarchical-prefix benefit) and how?
```

## Runs

### 2026-05-25 — S5 aCOLI + fair S2 view: c2 iteration-cell A/B (btree)
- **Commit**: `7d376515` (`calcite-integration`), host `node0` (Linux)
- **Logs**: `build/q10i_btree/150-in-0.1/structureN.log` (+ `structure2_{lineitem,preagg}.log`),
  `build/q10i_btree/TPut.csv` (scale=150). Parity gate green first at SF=1 both
  backends (S1≡S2≡S3≡S4≡S2-preagg≡S5).
- **Config**: iteration cell SF=150 dram=0.1, isolated, `tx_seconds=15`. Standalone
  q10i load (own image dir). A/B: `--q10i_view_variant=lineitem|preagg` (S2);
  S5 = `q10i_btree_5` (aCOLI MI).
- **ms/query** — **S5 aCOLI 10.6** < S2-B preagg 27.2 < S1 550 < S3 3,323 < S4
  13,127 < S2-A 16,751. R MiB/q: S5 0.013, S2-B 0.11, S1 14.8, S3 94.7, S2-A 513.
  S5 worker util 95.7% / 0 evictions; aCOLI get_size 502 MiB.
- **Claim check**: **validates the fair-MI thesis for Q10I, mirroring Q10's aCOL.**
  The aCOLI (merged index allowed to pre-aggregate per-order paid/open/late, drop
  lineitems+invoices, hand-rolled walk) is the **fastest structure** — beats the
  *fair* per-order preagg view (~2.6×; no per-order customer-col duplication) and
  raw S3 (~313×; no lineitem/invoice bulk). The per-lineitem S2-A was a strawman
  (S2-B is ~616× faster). 5L confirmation next. See
  [`../ACOL_ACOLI_PLAYBOOK.md`](../ACOL_ACOLI_PLAYBOOK.md) §7 / [`CLAUDE.md`](CLAUDE.md).

### 2026-05-25 — first Linux smoke test (btree c2, anomaly diagnosis)
- **Commit**: `1077fe8e` (`calcite-integration`, post-merge), host `node0`
- **TPut.csv**: `build/q10i_btree/150-in-0.1/` + `build/q10i_btree/TPut.csv`
- **Config**: btree, SF=150, DRAM=0.1 GiB (c2 — cheap DRAM-overflowing cell,
  ~8x view overflow), structures S1–S4, `tx_seconds=15`. Standalone load
  (own image dir after the generate_targets fix). Parity gate green first
  (test_query_q10i_{lsm,btree} SF=1, 4-way XOR).
- **ms/query**: S1 1,012 · **S3 3,351** · S4 13,254 · **S2 16,938**.
  R MiB/q: S1 16.6 · S3 94.5 · S4 395 · S2 513 (BM-counter fix).
- **Claim check**: **does NOT yet support the S3 ≥ S2 > S1/S4 claim** —
  smoke test reproduces *both* Q10 btree anomalies. (1) S2's per-lineitem
  view (`q10i_pipeline_view_t`) is a page-bound strawman (995 MiB view ≫
  pool, 513 MiB read/q). (2) S3 < S1 (3.3x slower, 5.7x more IO): Q10I has
  no customer-level filter (only the order-date prune), below the COLI
  co-location grain, so the walk drags in co-located invoices+lineitems of
  date-failing orders. The Q10 fixes transfer (per-order pre-aggregated
  view partitioned paid/open/late; S3 filter-hierarchy characterization) —
  queued in LINUX_PENDING before any 5L run. See `../q10/PERFORMANCE.md`.

_(Q10I was design-doc only as of 2026-05-08; Phase 4 + this smoke test
followed.)_

### 2026-05-24 — Phase 4 multi-SF parity (correctness-only)
- **Commit**: (pre-commit; about to land Phase 4 query bodies)
- **Branch**: `worktree-agent-a86745538133069de`
- **TPut.csv**: n/a — correctness-only run (test_query_q10i_lsm)
- **Config**: macOS RocksDB; SF=1, SF=5, SF=10; structures S1–S4
- **Outcome**: strict 4-way XOR parity `[OK]` at all three SFs.
  SF=1 digest `0xfe5d346e48bc1e8d`, SF=5 `0xf88a36288a3eb1a4`,
  SF=10 `0xd39b971cf074297e`; 20 rows each. Q3I/Q5I regressions
  clean. Validates the design's S1/S2/S3/S4 equivalence claim
  for the paper's headline 5L cell.
