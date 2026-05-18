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

**Projected duration** (CloudLab node, from `-a` per-cell timings):
- Phase 1 (TPC-H): ~6 binaries-worth of load (LSM/BTree × 3 cells, c3/c0 share SF) ≈ 8–12 h load + ~3.5 h foreground runs = ~12–15 h.
- Phase 2 (geo): with `-n` skipped, ~12 phases instead of 18 → ~4–6 h per cell × 3 cells × 2 binaries / parallelism = ~6–9 h.
- Total ≈ 18–24 h. Halt point if needed: end of phase 1 (TPC-H rows intact).

**Latest status** (paste in updates here):
- 19:21 UTC — Phase 1 started. Loading tpch_lsm SF=1500 for c1.

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

## Open issues / future work

- **geo_btree c2 incomplete**: only bg=0 r1 collected. If we want a complete `-a` matrix later, rerun `q3i_btree` … no wait, `geo_btree` at SF=19. Cost: ~9 h. Defer.
- **S5 (aCOLI MI)**: deferred from paper sweep per `frontend/tpch/PLAYBOOK.md §S5`. Infrastructure work (S5 lacking the hand-tuned COLI walker).
- **bg=0/1 at c1/c3/c0**: not collected in either `-a` or `-b`. If reviewer asks for isolated baseline at large SF, this is a follow-up sweep (`-c`).
- **q5_btree S3 bg=1 race**: occasional `std::out_of_range` on `nationkey_to_name` (race between fg+bg workers). Doesn't affect `-b` directly since bg=2's cohort uses the same path, but watch for missed reps. Listed in `frontend/tpch/q5/RUNS.md` (not yet logged).

---

## Doc maintenance

This file is the hand-off log between hourly background checks. When updating: replace the "Latest status" bullet under the active tag with current state (snapshots count, current cell/binary, projected ETA). Move the active section under "Frozen" when the tag halts.
