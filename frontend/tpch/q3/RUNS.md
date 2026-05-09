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
- **Commit**: `c500b747` (`calcite-integration`); images at `v0/`.
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
- **Commit**: `c500b747` (`calcite-integration`); images at `v0/`.
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
- **Commit**: `c500b747` (`calcite-integration`); images at `v0/`.
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
