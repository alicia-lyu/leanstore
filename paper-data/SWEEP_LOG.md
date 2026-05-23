# Paper-sweep log

Compressed status doc. Updated on each periodic check. Newer entries on top.

---

## Active: tag `2026-05-18-b` — bg=2 cohort, cells c1,c3,c0

**Started**: 2026-05-18 19:21 UTC. PID 205813. Launcher: `experiments/launch_full_sweep.sh 2026-05-18-b c1,c3,c0`.

**Matrix**: 8 TPC-H binaries + 2 geo binaries × {c1,c3,c0} × 4 structures × 1 bg (=2) × 3 reps. Phase 1 (`--families tpch,tpchi`) then phase 2 (`--families geo`); geo last because per-cell cost is largest.

**Cell-skip rationale**: c2 not re-run — `-a` covers c2 for 9/10 binaries (geo_btree c2 has only bg=0 r1; we accept that gap rather than spend the SF=380 reload again).

**Code differences vs `-a`**:
- `bg=2` only (Q3+Q5+random point lookups, time-balanced 1:1:1 cohort dispatch). bg=0/1 dropped from default matrix.
- `--geo_skip_n_queries=true` default — drops join-n / mixed-n / distinct-n (those override `tx_seconds` via `keep_running_condition` and ran ~42 min/phase at SF=19 in `-a`).
- HEAD: `4285645c` (calcite-integration).

**Projected duration** (revised after first cell):
- Phase 1 (TPC-H): c1 took ~15.5 h. c3+c0 share SF so loads are reusable; expect ~30 h more.
- Phase 2 (geo): with `-n` skipped, ~24–48 h.
- **Total estimate: ~60–80 h** from start (originally 18–24 h — q5_btree at SF=1550 s4 hash join is dominant, ~74 min/rep).

**Latest status** (most-recent first):
- **2026-05-23 10:13 UTC** (~111 h in): geo phase ~15% done; per-rep slower than projected.
  - Phase 1: ✓ all 24 TPC-H binary-cells × 3 reps = 72 binary-reps.
  - Phase 2 geo:
    - geo_lsm c1: ✓ 3 reps (~1h each).
    - geo_btree c1: r1 ✓ (took **5h 26m**, much slower than projected); **r2 in progress**.
    - Remaining: geo_btree c1 r2+r3 + geo at c3 + geo at c0.
  - **Revised phase 2 ETA**: geo_btree per-rep ≈ 5.5h dominates (bg=2 cohort × small DRAM × multi-phase per-structure tx_seconds). Total geo: ~55-65h more. Then -c (c2 bg=2) ~3-5h.
  - **-b finish**: ~Tuesday 2026-05-25 evening UTC. -c done by ~Wednesday morning.
- 2026-05-22 22:56 UTC — Phase 1 complete; Phase 2 (geo) started.
- 2026-05-22 14:46 UTC — q5i_lsm c0 ✓; q5i_btree c0 started.
- 2026-05-22 10:38 UTC — q3i_btree c0 ✓; q5i_lsm c0 started.
- 2026-05-22 04:35 UTC — q3i_lsm c0 ✓; q3i_btree c0 started.
- 2026-05-22 01:52 UTC — c0 vanilla COL ✓; tpchi at c0 started.
- 2026-05-21 11:38 UTC — c3 closed; c0 started.
- 2026-05-21 02:01 UTC — q5i_lsm c3 ✓; q5i_btree c3 started.
- 2026-05-20 22:14 UTC — q3i_btree c3 ✓; q5i_lsm c3 started.
- 2026-05-20 16:35 UTC — q3i_lsm c3 ✓; q3i_btree c3 started.
- 2026-05-20 13:30 UTC — tpchi loads done; q3i_lsm c3 starting.
- 2026-05-20 05:39 UTC — c3 vanilla COL family done; tpchi loading.
- 2026-05-18 19:21 UTC — Phase 1 started.

**Headline** (ms/query medians, bg=2, lower=better; 316 rows / 34 inversions; TPC-H complete across c1/c3/c0):

| binary | c1 S1 | c1 S2 | c1 S3 | c1 S4 | c3 S1 | c3 S2 | c3 S3 | c3 S4 | c0 S1 | c0 S2 | c0 S3 | c0 S4 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q3_btree | 366703 | 292398 | **292141** | 652742 | 1018226 | 754148 | **742390** | 1843658 | 967118 | 751315 | **744048** | 1842639 |
| q3_lsm | 5949 | **4163** | 5574 | 56721 | 14863 | 11612 | **10324** | 187935 | 15613 | 13036 | **11183** | 194062 |
| q5_btree | 355492 | 452694 | **303398** | 1433692 | 1006340 | 1206127 | **783085** | 4345937 | 965251 | 1214182 | **784929** | 3889537 |
| q5_lsm | 5974 | 4619 | **4452** | 130141 | 15170 | 12120 | **10166** | 735294 | 15492 | 12456 | **10396** | 738552 |
| q3i_btree | 498753 | **310078** | 333444 | 1340662 | 1479947 | **791139** | 853242 | 3866976 | 1288826 | **791139** | 859107 | 3735525 |
| q3i_lsm | 9615 | **5631** | 5794 | 110803 | 25000 | **14172** | 17419 | 352983 | 24027 | **12786** | 18392 | 313972 |
| q5i_btree | 451060 | 466636 | **347343** | 2084636 | 1578034 | 1292992 | **892061** | 6321113 | 1176886 | 1300052 | **896861** | 5851375 |
| q5i_lsm | 8621 | **4390** | 5774 | 512033 | 28035 | **16226** | 17621 | 1645007 | 26302 | **16753** | 17406 | 1716444 |

(Bold = best of S2/S3 per row.)

**S3/S2 ratio** (lower = S3 wins; **bold = S3 wins**):

| binary | c1 | c3 | c0 | trend |
|---|---:|---:|---:|---|
| q3_btree | 1.00× | **0.98×** | **0.99×** | flat — tied |
| q3_lsm | 1.34× | **0.89×** | **0.86×** | improves with DRAM ✓ |
| q5_btree | **0.67×** | **0.65×** | **0.65×** | strong S3 win, stable |
| q5_lsm | 0.96× | **0.84×** | **0.83×** | improves with DRAM ✓ |
| q3i_btree | 1.08× | 1.08× | 1.09× | S2 wins consistently by ~8% |
| q3i_lsm | 1.03× | 1.23× | **1.44×** | **worsens with DRAM** — anomaly |
| q5i_btree | **0.74×** | **0.69×** | **0.69×** | strong S3 win |
| q5i_lsm | 1.32× | 1.09× | 1.04× | improves with DRAM, close to tied at c0 |

**Reading**:
- **Vanilla COL (q3, q5)**: S3 wins for q3_lsm, q5_btree, q5_lsm at c3/c0. q3_btree essentially tied. Paper claim holds — **S3 ≥ S2 at large SF** for vanilla family.
- **q5_btree S4 (hash)**: 5–10× slower than S3 at large SF — clean S3-vs-hash margin.
- **Invoice family**: q5i_btree wins decisively (0.69×). q5i_lsm closes to 1.04× at c0 (S2 still ahead but marginal).
- **Paper-concern: q3i_lsm and q3i_btree** — both have S2 winning consistently. q3i_lsm is the worst: ratio **worsens** with DRAM (1.03× → 1.44×). This suggests S3's COLI walk doesn't benefit from extra DRAM the way S2's view does (S2 is fully materialised; more DRAM = more of it cached; S3 must still traverse). Worth investigating before paper submission.

34 S3-vs-S2 inversions flagged (was 20) — c0 q3i*/q5i* additions account for the 14-row increase.

---

## Frozen: tag `2026-05-18-a` — bg=[0,1] baseline, c2 only

**Halted at** 19:13 UTC after user call: "If it's only geo workload, let's end it now." Killed mid-`geo_btree` rep 2 of 6 bg=0.

**Coverage** (raw snapshot dirs):

| Binary | c2-bg0 reps | c2-bg1 reps | other cells |
|---|---|---|---|
| q3/q5/q3i/q5i × lsm/btree (8 bins) | 1,2,3 ✓ | 1,2,3 ✓ | — |
| geo_lsm | 1,2,3 ✓ | 1,2,3 ✓ | — |
| geo_btree | 1 only ✗ | — | — |

Summary CSVs: `headline.csv` 472 rows, `stats.csv` 185 rows, `inversions.csv` 51 rows (50 records + header).

**Headline (c2, p50 ms_per_tx, n reps):**

| Binary | S1 bg0 | S2 bg0 | S3 bg0 | S4 bg0 | S2 bg1 | S3 bg1 |
|---|---:|---:|---:|---:|---:|---:|
| q3_lsm | 971 | **518** | 588 | 3273 | 1130 | **952** |
| q5_lsm | 1086 | **546** | 588 | 5528 | 1059 | **943** |
| q3i_lsm | 1469 | **575** | 800 | 8375 | **1114** | 1481 |
| q5i_lsm | 1275 | **485** | 870 | 60060 | **1077** | 1449 |
| q3_btree | 80775 | 65104 | 65574 | 135999 | 66269 | 64851 |
| q5_btree | 79302 | 70225 | **66138** | 318269 | 108707 | — |
| q3i_btree | 106247 | **70175** | 76570 | 254001 | 69930 | 75415 |
| q5i_btree | 117800 | **61463** | 78186 | 354736 | 104734 | 78003 |

(Bold = best of S2/S3 per row. Lower = better.)

**Inversions flagged at c2** (S3 > S2 in ms; 50 rows total):
- LSM family: q3/q5 bg=0 (small ms gap, ~70 ms), q3i/q5i both bg=0 + bg=1
- BTree family: q3i bg=0+1, q5i bg=0, q5_lsm bg=0
- geo_lsm: 24 across both bg

**Reading**: At small SF (c2) the materialized pipeline view (S2) often matches or beats COLI MI (S3) — expected, since c2 fits more of S2 in DRAM. Under contention (bg=1), S3 catches up or wins for q3/q5 (LSM) but not for invoice-extended q3i/q5i (extra invoice walk hurts under page pressure). The bigger-SF cells (c1/c3/c0) in `-b` should resolve this.

**Key files**:
- `paper-data/2026-05-18-a/summary/{headline,stats,inversions,diagnostics}.csv` — git-tracked
- `paper-data/2026-05-18-a/figures/` — gitignored; regenerable via `python3 scripts/plot_paper_sweep.py --tag 2026-05-18-a`
- `paper-data/2026-05-18-a/raw/` — gitignored, large; preserved on disk for re-analysis

---

## Paper coverage — gaps

**No remaining gaps**: matrix is `bg=2 × {c1,c3,c0}`.

**Not gaps — accepted**:
- **c2 entirely (was queued, now dropped 2026-05-23)**: chain watcher killed. `c2` SFs (150/380 TPC-H, 19/52 geo) are too small to be informative — the cells of paper interest are c1/c3/c0 where the secondary structures actually exceed DRAM. The `headline_*_vs_secondary` plot will just span c1→c0; if a reviewer asks for c2, revisit.
- **bg=0 / bg=1 at c1/c3/c0**: dropped by design when we moved to bg=2-only. Paper claim is "S3 holds up under realistic mixed contention", not "S3 wins vs isolated baseline at every SF".
- **geo_btree c2 partial** (only bg=0 r1 from `-a`): abandoned regime.
- **S5 (aCOLI MI)**, **Q10I**, **Q12**: deferred / excluded by design — documented in `frontend/tpch/PLAYBOOK.md §S5`, the q-dir CLAUDE.md files, and `experiments/sweep.yaml §excluded`.
- **q5_btree S3 bg=1 race** (`std::out_of_range` on `nationkey_to_name`, fg/bg map race): doesn't affect `-b`. Log in `frontend/tpch/q5/RUNS.md` if it ever fires.

## Timing investigation (2026-05-19)

**Findings**: per-rep wall time at SF=1550/c3 is dominated by individual query latency, not the `tx_seconds=15` budget.

| Structure (q5_btree c3) | per-query latency | per-rep wall time |
|---|---:|---:|
| S1 (BMJ) | ~6 min | ~18 min |
| S2 (view) | ~10 min | ~22 min |
| S3 (mi_col_walk) | **783 s** (one query) | ~15 min |
| S4 (hash) | ~30 min | **~74 min** |

`tx_seconds` is a budget, not a kill-switch — once a query starts it runs to completion. At 0.4 GiB DRAM vs 4.4 GiB merged index, page-fault thrashing dominates. **This is the correct paper measurement** (the 783 ms/query is the headline number), but it pushed the sweep estimate from 18–24 h to ~60–80 h.

**Not changing the matrix.** Options to shorten (rejected unless user asks): reps 3→2 (-33 % wall time, weaker IQR), skip q5_btree S4 at large SF (-50 % q5_btree time, lose a paper headline cell), preempt mid-query (needs binary changes).

---

## Doc maintenance

This file is the hand-off log between hourly background checks. When updating: replace the "Latest status" bullet under the active tag with current state (snapshots count, current cell/binary, projected ETA). Move the active section under "Frozen" when the tag halts.
