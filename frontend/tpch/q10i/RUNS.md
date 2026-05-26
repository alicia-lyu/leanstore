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

### 2026-05-26 12:26 UTC — bg=2, 3-rep 5L rerun (both backends)
- **Commit**: `9483d7c9` (`calcite-integration`), host `c220g2-011011` (Linux)
- **TPut.csv**: per-structure logs under
  `paper-data/q10-q10i-bg2-5L-20260526-040738/raw/q10i_{btree,lsm}/`; medians
  ported to that tag's `summary/headline.csv` (feeds `paper_q10`).
- **Config**: 5L (c0) DRAM=1.0, SF=1550 btree / 3850 lsm, **bg=2** (second
  worker re-runs the query + uniform-random point-lookup stream — the
  no-cohort-fallback fix, commit `9483d7c9`), `param_seed=0`, 3 reps. Replaces
  the 2026-05-25 hand-ported bg=0 single-rep sibling.
- **ms/query (median of 3)** — **btree**: aCOLI S5 **190.7** ≪ S2-B 20,908 < S1
  25,353 < S3 47,674 ≪ S4 142,000 < S2-A **180,394**. **lsm**: S5 1,573 < S2-B
  2,194 < S2-A 16,829 < S3 16,092 < S1 27,162 ≪ S4 98,922.
- **Claim check**: supports the Q10I framing under the *stated* protocol —
  naive Mat-View (S2-A) and Base-Hash are huge; Merged-Idx (S3) trails the
  split Base-Merge (S1) on btree; the partial-agg variants (Mat-View-preagg,
  aCOLI) restore the ordering and are the fastest structures. Consistent with
  the bg=0 supplemental (slightly higher from contention). Feeds `paper_q10`
  with genuine bg=2 (no relabel).

### 2026-05-25 — S5 aCOLI 5L confirmation (both backends)
- **Commit**: `aeb16049` (`calcite-integration`), host `node0` (Linux)
- **Logs**: `build/q10i_{btree,lsm}/{1550,3850}-in-1.0/structureN_{baseA,preagg}.log`;
  `build/scratch/q10i_5L_sweep.sh`. Parity green first at SF=1 both backends.
- **Config**: 5L (c0) DRAM=1.0, SF=1550 (btree) / 3850 (lsm), isolated,
  `tx_seconds=15` (queries run to completion). Fresh standalone q10i load both
  scales (own image dir). A/B: `--q10i_view_variant` (S2), `q10i_*_5` (S5 aCOLI).
- **ms/query** — **btree**: **S5 aCOLI 181** ≪ S2-B 21,349 < S1 26,797 < S3
  47,296 ≪ S4 137,778 < S2-A 173,623. **lsm**: **S5 aCOLI 1,561** < S2-B 2,014
  < S2-A 14,109 < S3 16,570 < S1 26,002 ≪ S4 92,711.
- **Claim check**: **5L confirms the fair-MI thesis for Q10I on BOTH backends**,
  mirroring Q10's aCOL. The aCOLI (per-order paid/open/late baked, lineitems +
  invoices dropped, hand-rolled `acoli_group_walk`) is the **fastest structure**:
  btree beats the fair per-order preagg view **118×** and raw S3 **260×**; lsm
  beats S2-B 1.3× and S3 10.6×. The per-lineitem S2-A is the strawman (btree
  173.6 s). Validates ACOL_ACOLI_PLAYBOOK §6 (S5 beats the *fair* S2, not just
  the strawman). No anomaly. See `CLAUDE.md §Status` + `../ACOL_ACOLI_PLAYBOOK.md`.

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
