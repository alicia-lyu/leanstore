# Q3 — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs and need
consolidating.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  (Q3I §A1 reference shape: S3 ≥ S2 > S1/S4) and how?
```

## Runs

### 2026-05-08 21:36 CDT — q3_lsm SF=15 DRAM=0.1 GiB (default)
- **Commit**: `edd34a1f` (`calcite-integration`)
- **TPut.csv**: not produced — `q3_lsm` runs the query once and prints
  the top-10; `tput_tx` driver is not yet wired (LINUX_PENDING.md
  §"Q3 throughput-shape comparison").
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (CloudLab `node0`).
- **Claim check**: Inconclusive on throughput shape — only confirms
  byte-identical 10-row top-N across S1/S2/S3/S4 (correctness); the
  S3 ≥ S2 > S1/S4 ordering can't be measured until the executable
  gains a TX loop.

### 2026-05-08 21:36 CDT — q3_btree SF=15 DRAM=0.1 GiB (default)
- **Commit**: `edd34a1f` (`calcite-integration`)
- **TPut.csv**: not produced — same wiring gap as `q3_lsm`.
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (CloudLab `node0`).
- **Claim check**: Inconclusive on throughput — confirms the
  post-bring-up `_btree` SEGV fix sticks (cached SF=15 image,
  full S1–S4 sweep completes in 2 s with a top-10 result printed for
  every structure); throughput comparison waits on `tput_tx` wiring.

### 2026-05-08 23:24 CDT — q3_lsm SF=1500 DRAM=0.4 GiB (large LSM beyond-memory, 5× ratio)
- **Commit**: `c500b747` (`calcite-integration`).
- **TPut.csv**: `build/q3_lsm/TPut.csv` rows 10–13 (DRAM=0.4, scale=1500).
- **Config**: SF=1500, DRAM=0.4 GiB, S1–S4, secondaries 1.22–1.50 GiB
  per structure (predicted ~2 GiB; LSM compression brings actual ~25%
  lower), secondary/DRAM ≈ 3.5×, beyond memory regime — see
  [`../RUNS.md`](../RUNS.md).
- **Claim check**: Supports the family claim — shape
  S3 mi_col_walk (0.840) > S2 pipeline_view (0.549) > S1 base_merge_join
  (0.450) > S4 base_hash_join (0.243) TX/s. S3 ≥ S2 > S1/S4 holds at
  large scale. S3 ~3.5× S4 — still pure-§3.1.3 hierarchical-prefix
  benefit dominating; no §3.1.2 sibling confound.

### 2026-05-08 23:01 CDT — q3_btree SF=600 DRAM=0.4 GiB (BTree beyond-memory, 5× ratio)
- **Commit**: `c500b747` (`calcite-integration`).
- **TPut.csv**: `build/q3_btree/TPut.csv` rows 6–9 (DRAM=0.4, scale=600).
- **Config**: SF=600, DRAM=0.4 GiB, S1–S4, secondaries 1.32–1.75 GiB
  per structure (predicted ~2 GiB ✓), secondary/DRAM ≈ 4×, beyond
  memory regime.
- **Claim check**: Mixed — S3 (3.71) leads strongly but **S2 collapses
  to 0.21** (worse than S1 = 1.27). Shape: S3 (3.71) > S1 (1.27) > S2
  (0.21) > S4 (0.04) TX/s. S3 ≥ S1 > S2/S4 — paper claim "S3 dominates"
  holds; "S2 ≥ S1" ordering breaks under BTree disk pressure (Q3 view
  is per-lineitem with FD-attached order columns; the per-lineitem
  fanout cost dominates when scans hit disk, mirroring Q3I §A1's
  H14 hypothesis).

### 2026-05-08 22:55 CDT — q3_lsm SF=300 DRAM=0.08 GiB (small LSM beyond-memory, 5× ratio)
- **Commit**: `c500b747` (`calcite-integration`).
- **TPut.csv**: `build/q3_lsm/TPut.csv` rows 6–9 (DRAM=0.08, scale=300).
- **Config**: SF=300, DRAM=0.08 GiB, S1–S4, secondaries 244–299 MiB
  per structure (predicted ~390 MiB; LSM compression brings actual
  ~30% lower), secondary/DRAM ≈ 3.5×, beyond memory regime.
- **Claim check**: Supports the family claim — shape
  S3 (3.26) > S2 (2.90) > S1 (2.18) > S4 (1.28) TX/s.
  S3 ≥ S2 > S1/S4 holds at small beyond-memory scale.

### 2026-05-08 22:55 CDT — q3_btree SF=15 DRAM=0.1 GiB — post-tput-wiring baseline
- **Commit**: `87f01426` (`calcite-integration`)
- **TPut.csv**: `build/q3_btree/TPut.csv` row 2–5
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, secondary 33–44 MiB / structure
  (BTree). Cache-resident regime — see [`../RUNS.md`](../RUNS.md).
- **Claim check**: Supports the family claim — shape
  S3 (331) > S2 (136) > S1 (101) > S4 (74) TX/s. Same S3 ≥ S2 >
  S1/S4 ordering as LSM at ~1.5× the absolute throughput. BTree
  size 41 MiB / structure validates ~3× the 14 MiB LSM equivalent.

### 2026-05-08 22:54 CDT — q3_lsm SF=15 DRAM=0.1 GiB — post-tput-wiring baseline
- **Commit**: `87f01426` (`calcite-integration`)
- **TPut.csv**: `build/q3_lsm/TPut.csv` row 2–5
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, secondary 12–15 MiB / structure
  (LSM). Cache-resident regime.
- **Claim check**: Supports the family claim S3 ≥ S2 > S1/S4 — shape
  S3 mi_col_walk (209) > S2 pipeline_view (95) > S1 base_merge_join (68)
  > S4 base_hash_join (62) TX/s. S3 is ~2.2× S2 and ~3.2× S1/S4. Same
  Q3I §A1 ordering but with a **steeper S3-vs-S1/S4 gap** than Q3I
  (Q3I S3=156 vs S1=57, ratio 2.7× at the same SF/DRAM) — exactly as
  predicted: Q3 isolates the §3.1.3 hierarchical-prefix benefit
  without the §3.1.2 sibling-aggregate confound.

### 2026-05-11 11:09 CDT — q3_lsm SF=15 DRAM=0.1 GiB — post-fairness-fix
- **Commit**: `51ea87b0` (`calcite-integration`); commits since
  2026-05-08 baseline: `bbc15e68` (S2 custkey seek-skip), `92336200`
  (BMJ final-group flush), `f62a0149` (S1 custkey seek + S4
  inverted-lineitem seek), `d976b047` (merge — TopNSink refactor
  reduces post-pipeline buffer O(N)→O(K)), `51ea87b0`
  (`s4_hashtable_bytes` counter).
- **TPut.csv**: `build/q3_lsm/TPut.csv` rows 6–9 (DRAM=0.1, scale=15,
  post-merge sweep).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, secondary 12–15 MiB /
  structure (LSM). Cache-resident regime. Host: Linux (CloudLab
  `node0`, fresh bring-up — see `LINUX_SETUP.md`).
- **Headline (TX/s)**: S2 pipeline_view **264.26** > S3 mi_col_walk
  211.46 > S4 base_hash_join 113.63 > S1 base_merge_join 80.41.
- **Deltas vs pre-fairness-fix baseline** (`87f01426` on this branch,
  see `q3_lsm SF=15 ... post-tput-wiring baseline` entry above):
  S1 61.03 → 80.41 (+32%), S2 97.09 → 264.26 (2.72×), S3 199.94 →
  211.46 (+6% noise), S4 63.16 → 113.63 (+80%). SSTRead(us)/TX:
  S2 7.54 → 4.34, S4 11.98 → 94.56 (S4 random-seek overhead even
  cache-resident — hash table walk still cheap).
- **Claim check**: **Paper claim S3 ≥ S2 > S1/S4 breaks at this
  config**. With fair access-pattern parity (S1/S2/S4 all use
  physical seek mechanics matching S3's `WalkAction::SkipGroup`),
  S2 (the per-lineitem view with FD-attached order columns) overtakes
  S3 at cache-resident scale — view sequential scan with custkey-skip
  beats MI walk when DRAM dominates and the MI's per-record dispatch
  overhead doesn't amortize. The merged-index advantage is reserved
  for the beyond-memory regime (see SF=300 / SF=600 / SF=1500
  entries below).

### 2026-05-11 11:09 CDT — q3_btree SF=15 DRAM=0.1 GiB — post-fairness-fix
- **Commit**: `51ea87b0` (`calcite-integration`); same fixes as the
  LSM sibling above.
- **TPut.csv**: `build/q3_btree/TPut.csv` rows 6–9 (DRAM=0.1,
  scale=15, post-merge sweep).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, secondary 33–44 MiB /
  structure (BTree). Cache-resident regime. Host: Linux (CloudLab
  `node0`).
- **Headline (TX/s)**: S2 pipeline_view **408.55** > S3 mi_col_walk
  314.09 > S4 base_hash_join 168.73 > S1 base_merge_join 101.30.
- **Deltas vs pre-fairness-fix baseline** (same branch,
  `q3_btree SF=15 ... post-tput-wiring baseline` entry above):
  S1 95.78 → 101.30 (+6%), S2 128.50 → 408.55 (3.18×), S3 334.24 →
  314.09 (−6% noise), S4 70.71 → 168.73 (2.39×). R MiB/TX:
  S2 2.075e-05 → 6.526e-06 (3.18× less, mirror of S2 TPut gain).
- **Claim check**: **Same paper-claim break as LSM** — S2 overtakes
  S3 at cache-resident BTree once S2 has physical custkey skip. BTree
  per-record dispatch overhead is higher than LSM (no inline bloom
  filter shortcut, B-tree descents are O(log n) each), which widens
  the absolute S2 lead.

### 2026-05-11 11:13 CDT — q3_lsm SF=300 DRAM=0.08 GiB — post-fairness-fix
- **Commit**: `51ea87b0` (`calcite-integration`); same fixes as
  SF=15 above.
- **TPut.csv**: `build/q3_lsm/TPut.csv` rows 14–17 (DRAM=0.08,
  scale=300, post-merge sweep).
- **Config**: SF=300, DRAM=0.08 GiB, S1–S4, secondaries 244–305 MiB
  per structure, secondary/DRAM ≈ 3.5×, beyond-memory regime. Host:
  Linux (CloudLab `node0`).
- **Headline (TX/s)**: S2 pipeline_view **4.99** > S3 mi_col_walk
  4.20 > S1 base_merge_join 2.32 >> S4 base_hash_join 0.62.
- **Deltas vs pre-fairness-fix baseline** (`c500b747` SF=300 entry
  above): S1 2.06 → 2.32 (+13%, custkey seek-skip), S2 4.91 →
  4.99 (+2%, stable), S3 3.13 → 4.20 (+34%, BMJ-flush + TopNSink
  buffer relief), **S4 1.22 → 0.62 (−49% regression!)**.
- **S4 regression diagnosis**: SSTRead(us)/TX jumped 92,795 →
  1,203,646 (13×). The inverted-lineitem seek pattern (`f62a0149`)
  replaces a sequential lineitem scan with one physical `Seek` per
  qualifying orderkey. Cache-resident this is a +80% win (fewer
  rows processed); beyond-memory it is a catastrophe — each seek
  triggers bloom-filter + random SST reads, losing LSM's
  sequential-prefetch friendliness. Hash-table working-set itself
  is small here (~2.7 MiB at SF=300, only 3.3% of the 80 MiB DRAM
  budget per `s4_hashtable_bytes` extrapolation — `51ea87b0`); the
  regression is access-pattern, not memory pressure.
- **Claim check**: **Paper claim S3 ≥ S2 > S1/S4 still breaks at the
  S3-vs-S2 boundary**: S2 leads S3 by 19% even beyond-memory at this
  size. S3 > S1 (+81%) and S3 >> S4 (6.8×) hold strongly. The S3-vs-S2
  gap may close at larger scales — the per-lineitem view's row-count
  scales linearly with lineitem volume while the MI's working set
  scales the same; the differentiator is per-record dispatch cost vs
  scan locality, which S2 wins on at this DRAM-pressure ratio. **TODO**:
  diagnose with deeper memory pressure (SF=1500 / DRAM=0.4 GiB) to
  see if the crossover finally lands.

### 2026-05-11 12:11 CDT — q3_btree SF=600 DRAM=0.4 GiB — post-fairness-fix
- **Commit**: `51ea87b0` (`calcite-integration`); same fixes as
  SF=15/SF=300 above.
- **TPut.csv**: `build/q3_btree/TPut.csv` rows 10–13 (DRAM=0.4,
  scale=600, post-merge sweep).
- **Config**: SF=600, DRAM=0.4 GiB, S1–S4, secondaries 1.32–1.75 GiB
  per structure, secondary/DRAM ≈ 4×, deeply beyond-memory regime.
  Fresh load (data not previously on /mnt/ssd). Host: Linux (CloudLab
  `node0`).
- **Headline (TX/s)**: S3 mi_col_walk **1.30** > S2 pipeline_view 0.75
  > S1 base_merge_join 0.39 >> S4 base_hash_join 0.087.
- **Deltas vs pre-fairness-fix baseline** (`c500b747` SF=600 entry
  above): S1 1.27 → 0.39 (**−69% regression**), S2 0.21 → 0.75
  (**3.6× win** — S2 finally breathes once the per-customer skip-seek
  cuts the read-fanout footprint), S3 3.71 → 1.30 (**−65% regression**),
  S4 0.04 → 0.087 (+118%). Both S1 and S3 regressed — large enough
  to suggest a real cause, not noise, but **fresh load means RNG-seeded
  TPC-H data differs from the `c500b747` snapshot**, so part of the
  delta may be data-shape variation. **Investigation needed** —
  candidates: (a) BMJ final-group flush adds ε work to S1 hot path
  even when not firing (drainers now check `final_flushed`); (b) the
  S1 custkey pre-scan + std::lower_bound per fetched order = ~1.5M
  orders × O(log 30K) ≈ 22M comparisons at this SF, potentially
  cache-thrashing; (c) S3 regression has no obvious cause from this
  branch — the col_group_walk path didn't change, TopNSink emit is
  a heap-of-10 push, neither should cost 65%. **Action**: A/B the
  S1 pre-scan path vs a streaming variant; rerun the `c500b747`
  binary on the same fresh data to isolate data-shape vs code regression.
- **Claim check**: **At this beyond-memory scale the paper claim
  S3 ≥ S2 > S1/S4 partially holds** — S3 still leads S2 (1.30 vs
  0.75, +73%), S2 > S1 (now! previously inverted), S4 catastrophic.
  The cache-resident inversion (S2 > S3) does not extend to
  deep-disk-bound on BTree; sequential MI walk reasserts its
  locality advantage at the right DRAM-pressure ratio. But the
  absolute numbers are suspicious given the regressions — treat
  this as a data point pending the S1/S3 regression diagnosis.
