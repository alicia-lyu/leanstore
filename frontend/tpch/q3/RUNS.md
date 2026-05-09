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
