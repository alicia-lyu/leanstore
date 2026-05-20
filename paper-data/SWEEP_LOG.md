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
- **2026-05-20 22:14 UTC** (~51 h in): q3i_btree c3 ✓; q5i_lsm c3 started.
  - c1: ✓ all 8 TPC-H binaries.
  - c3: ✓ q3/q5 vanilla + q3i_lsm + q3i_btree (6/8). **q5i_lsm c3 SF=3850 in rep 1**; q5i_btree pending.
  - c0: pending. Remaining ≈ 2 tpchi at c3 + all 8 at c0 + geo phase.
- 2026-05-20 16:35 UTC — q3i_lsm c3 ✓; q3i_btree c3 started.
- 2026-05-20 13:30 UTC — tpchi loads done; q3i_lsm c3 starting.
- 2026-05-20 05:39 UTC — c3 vanilla COL family done; tpchi loading.
- 2026-05-18 19:21 UTC — Phase 1 started.

**Headline so far** (ms/query medians at bg=2, lower=better):

| binary | c1 S1 | c1 S2 | c1 S3 | c1 S4 | c3 S1 | c3 S2 | c3 S3 | c3 S4 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| q3_btree | 366703 | 292398 | **292141** | 652742 | 1018226 | 754148 | **742390** | 1843658 |
| q3_lsm | 5949 | **4163** | 5574 | 56721 | 14863 | 11612 | **10324** | 187935 |
| q5_btree | 355492 | 452694 | **303398** | 1433692 | 1006340 | 1204239 | **781861** | 4345937 |
| q5_lsm | 5974 | 4619 | **4452** | 130141 | 15170 | 12120 | **10166** | 735294 |
| q3i_btree | 498753 | **310078** | 333444 | 1340662 | — | — | — | — |
| q3i_lsm | 9615 | **5631** | 5794 | 110803 | — | — | — | — |
| q5i_btree | 451060 | 466636 | **347343** | 2084636 | — | — | — | — |
| q5i_lsm | 8621 | **4390** | 5774 | 512033 | — | — | — | — |

(Bold = best of S2/S3 per row.)

**Reading**:
- **q3_btree, q5_btree (vanilla COL)** — S3 wins decisively over S2 at both c1 and c3. Paper claim holds where it matters.
- **q5_btree S4 (hash)** — 5–10× slower than S3 at c1+c3 — clean S3-vs-hash margin.
- **LSM family** — S3 ~10–30% behind S2 at c1, gap narrows at c3 (5574→10324 ms for q3_lsm: S3 closes from 1.34× to 1.11× vs S2). c0 should make S3 win outright.
- **q3i / q5i** at c1 — S3 ahead of S2 only for q5i_btree; the invoice walk taxes S3 at small SF. Need c3+c0 data.

12 S3-vs-S2 inversions flagged, all at c1 (small SF where S2 still fits in DRAM).

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

**Real gap (queued)**:
- **c2 bg=2** (TPC-H + geo_lsm + geo_btree) — queued. `experiments/chain_c2_after_b.sh` (PID 245948 on 2026-05-19) polls -b's PID and auto-launches tag `2026-05-18-c` with `--cells c2 --families tpch,tpchi,geo` when -b exits. c2 images are cached so it's a runs-only sweep (~3–5 h). When it lands, the `headline_*_vs_secondary` plot will have c2→c1→c0 under the same bg=2 regime.

**Not gaps — accepted**:
- **bg=0 / bg=1 at c1/c3/c0**: dropped by design when we moved to bg=2-only. Paper claim is "S3 holds up under realistic mixed contention", not "S3 wins vs isolated baseline at every SF".
- **geo_btree c2 partial** (only bg=0 r1 from `-a`): once `-c` fills c2 bg=2 for geo_btree, the bg=0/1 hole is in the abandoned regime, not the paper one.
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
