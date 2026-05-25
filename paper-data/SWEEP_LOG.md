# Paper-sweep log

Compressed status doc. Updated on each periodic check. Newer entries on top.

---

## ⚠️ ALL pre-2026-05-24 sweeps were measured on an HDD, not the SSD

On 2026-05-24 we discovered `/mnt/ssd` had been the spinning SAS HDD
`/dev/sdb` (`ROTA=1`) the whole time; the real Intel DC S3500 SATA SSD
(`/dev/sdc`, `ROTA=0`) was unmounted. Both engines use `O_DIRECT`, so the
HDD was the true bottleneck — every prior `paper-data/` summary is
HDD-measured and is now stamped `disk=hdd` (see `scripts/mark_disk_media.py`).
Disk fixed (HDD → `/mnt/hdd` label `leanstore-hdd`; SSD → `/mnt/ssd` label
`leanstore-ssd`); see `../LINUX_SETUP.md §3`.

## Done: tag `2026-05-24-refresh-5H-ssd` — refresh_sales memory-pressure A/B (SSD)

**Re-run 2026-05-25**, commit `83240c0a`, host `c220g2-011011`.
`build/scratch/run_refresh_5H_ssd.sh` + `summarize_refresh_5H_ssd.sh`. Both
cells re-run this session at the **same commit** so the A/B is clean:
`2026-05-24-refresh-5L-ssd` (c0, DRAM 1.0 — re-stamped from `c9b5f594` to
`83240c0a`) and `2026-05-24-refresh-5H-ssd` (c3, DRAM 0.4). Same SF
(1550 btree / 3850 lsm, ~5 GiB secondaries) and same per-structure-copy +
drop-caches mechanics; only the buffer pool shrinks 1.0→0.4 GiB, so any
divergence is a pure memory-pressure effect. S1–S4 both backends, 90s,
isolated. RF2 does not exhaust → `pair_tps` (RF1+RF2 iteration rate) is the
metric.

**Findings (pair_tps, 5L→5H).** Confirms the hypothesis cleanly — **LSM is
flat under pressure, btree degrades**. lsm holds within ±2% (S1 1155→1156
1.00×, S2 939→916, S3 1150→1157, S4 1310→1282); btree drops 21–59% (S1
1430→868 0.61×, S2 781→614, S3 1674→1105 0.66×, **S4 5531→2269 0.41×** — the
write-heaviest hot path degrades most). At 5H, LSM overtakes btree on S1/S2/S3
(S1 1156 vs 868, S2 916 vs 614, S3 1157≈1105) and btree's S4 lead collapses
4.2×→1.8×. Write-optimized SST flush/compaction doesn't depend on buffer-pool
residency; btree pays random page eviction once the working set spills the
pool. Complements the abundant-memory `2026-05-24-refresh-prewarm9` finding
(at DRAM 9 LSM showed no in-memory speedup) — the pressured regime is where
LSM instead wins.

## Done: tag `2026-05-25-q10i` — Q10I 5L (c0) aCOLI + fair S2, both backends (SSD)

**Hand-ported 2026-05-25**, commit `aeb16049`, host `node0`. NOT from the paper
harness — q10i isn't in the sweep matrix; rows written from
`build/scratch/q10i_5L_sweep.sh` into `2026-05-25-q10i/summary/headline.csv`
(see its `manifest.yaml`). Single rep, isolated (bg=0), c0 only (DRAM 1.0; SF
1550 btree / 3850 lsm). Q10I = Q10 + invoice payment-status split (paid/open/late
return-revenue buckets). Structure 2 carries two methods (S2 A/B: `pipeline_view`
per-lineitem strawman + `pipeline_view_preagg` per-order fair view); structure 5
= `mi_acoli_preagg` (the aCOLI MI — `<customer_coli_t, orders_acoli_q10i_t>`,
per-order paid/open/late baked, lineitems+invoices dropped, hand-rolled
`acoli_group_walk`). Parity green first at SF=1 both backends.

**Findings (ms/query).** The **aCOLI (S5) is the fastest structure on both
backends**, beating the *fair* per-order preagg view and raw S3 (validates
ACOL_ACOLI_PLAYBOOK §6, mirroring Q10's aCOL): btree **S5 181** ≪ S2-B 21,349 <
S1 26,797 < S3 47,296 ≪ S4 137,778 < S2-A 173,623 (S5 beats S2-B 118×, S3 260×);
lsm **S5 1,561** < S2-B 2,014 < S2-A 14,109 < S3 16,570 < S1 26,002 ≪ S4 92,711.
The per-lineitem S2-A is the strawman (btree 173.6 s). To make this tag
re-analyzable, add q10i to `analyze_paper_sweep.py` maps. Pairs with the
`2026-05-25-q10` tag (vanilla sibling).

## Done: tag `2026-05-25-q10` — Q10 5L (c0) A/B perf investigation, both backends (SSD)

**Hand-ported 2026-05-25**, commit `f92d1868`, host `node0`. NOT from the paper
harness — Q10 isn't in the sweep matrix; rows written directly from the ad-hoc
`build/scratch/q10_5L_sweep.sh` 5L A/B sweep into
`2026-05-25-q10/summary/headline.csv` (see its `manifest.yaml` note + the
parent-repo `frontend/tpch/q10/PERFORMANCE.md`). Single rep, isolated (bg=0),
c0 only (DRAM 1.0; SF 1550 btree / 3850 lsm). Each of S2/S3 carries two methods
(the investigation's A/B variants).

**Findings (ms/query).** The btree S2 "regression" was a strawman per-lineitem
view: the fair **per-order pre-aggregated view** is **10× faster on btree**
(175,506 → 17,435; 0 evictions, now ≈ S1) and **10.4× on LSM** (17,209 → 1,658;
fastest structure). **S3 < S1 on btree** is the filter-hierarchy effect (Q10's
only prune is below the COL co-location grain): a physical SkipOrder seek cuts
records-visited 4.1× but leaves R MiB unchanged → neutral on btree (35.0 →
36.1 s), a clean **+9% on LSM** (11.1 → 10.1 s, no regression). To make this tag
re-analyzable, add q10 to `analyze_paper_sweep.py` maps. Q10 is the
boundary/negative case for the merged-index pitch — pairs with q3i/q5i (where
the customer-level prune makes S3 win).

**Addendum 2026-05-25 — S5 aCOL (the fair pre-aggregated MI).** Added
`mi_acol_preagg` (structure 5) rows for both backends (commit `fd771c13`; the
aCOL MI = `customer_coli_t + orders_acol_t`, per-order returned revenue baked,
no lineitems, hand-rolled `acol_group_walk`). It overturns the "S3<S1 on btree"
negative result by letting the MI pre-aggregate too: **S5 aCOL is the fastest
structure on both backends** — btree **160 ms/q** (~109× over the pre-agg view,
~219× over S3; lineitem-free, fits the pool) and lsm **1,367 ms/q** (1.2× over
the view, ~8× over S3). The grain-soundness + hand-rolled-walk lessons are
written up in `frontend/tpch/ACOL_ACOLI_PLAYBOOK.md`.

## Done: tag `2026-05-24-a-ssd` — q3/q5/q3i/q5i at 5L (c0) on the **real SSD**

**Finished 2026-05-24 ~18:00 UTC**, commit `c9b5f594`, host
`c220g2-011011`. `run_paper_sweep.sh --cells c0 --families tpch,tpchi --reps 3
--skip-load --drop-caches`. 96/96 runs (`disk=ssd`). Images recovered from the
copied c0 snapshots — no reload. One transient `q3i_btree` c0 r2/S1 abort
("turnPage" cascade) was backfilled by an independent re-run.

**SSD vs HDD at c0 (ms/query):** B-tree **~27–46× faster** (every `O_DIRECT`
miss was an HDD seek; queries fell from 12–65 min to ~25–130 s); LSM ~1.3–2×
for S1–S3 (block-cache absorbs most reads), 3–15× for the scan-heavy base/hash
S4. Relative S1–S4 ordering preserved. Companion update sweep:
`2026-05-24-refresh-5L-ssd` (RF1/RF2; S3≥S1>S2 on both backends, clean).

## Frozen: tag `2026-05-18-b` — bg=2 cohort, cells c1,c3,c0 — **HDD (invalid)**

**Stopped 2026-05-24** mid-geo (was loading `geo_btree` SF=194) to free the
disk for the swap. TPC-H phase was complete but **HDD-measured** — superseded
at c0 by `2026-05-24-a-ssd`. c1/c3 and all geo remain HDD-only / unfinished;
rerun on SSD if those cells are needed. Original log below for history.

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
- **2026-05-23 — geo BG semantics redesigned** (SHA `8944030c`):
  - Old `--geo_bg_thread=true` did write-only insert/erase on customer2; asymmetric with TPC-H bg=2 read cohort and not a realistic concurrency model.
  - New: read-only **hierarchical point lookup** loop. At thread start, reservoir-sample 10K valid customer keys (one full scan of customer2 / merged tree). Per TX: pick a key uniformly, issue 5 lookups (customer → city → county → state → nation) in one TX. S1/S2/S4 use 5 separate adapters; S3 uses merged `lookup1<T>`.
  - Pre-redesign c1 captures (`geo_lsm` 3 reps, `geo_btree` 2 reps) quarantined to `raw/geo_{lsm,btree}/legacy-writebg/`; frozen ms/query summary preserved at `summary/legacy/geo_{lsm,btree}_writebg_c1.md`.
  - Smoke-tested SF=52 S1 + S3 with bg=true on Linux: `[bg-sample] {base,merged}: sampled 10000 of 1559999 customer keys`, then `#6783` / `#7902 bg lookup tx in total performed` across the 7-phase sweep. No errors.
  - Geo COL-walker optimization (analogous to TPC-H S3) deferred until first read-bg numbers come in; revisit if S3 still doesn't pull clear of S2 under the new semantics.
  - **Next**: relaunch `--families geo --continue` to backfill c1+c3+c0 under new semantics. ETA depends on first-rep timing under read-bg (lookups likely ≤ write-bg cost per TX).
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
