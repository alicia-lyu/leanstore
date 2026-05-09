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

### 2026-05-08 22:13 CDT — q3_lsm SF=15 DRAM=0.1 GiB — first throughput sweep
- **Commit**: `claude/q3-tput-wiring` HEAD (off `calcite-integration`)
- **TPut.csv**: `build/q3_lsm/TPut.csv` (4 rows: S1–S4)
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (CloudLab `node0`).
- **Claim check**: **Supports the family claim S3 > S2 > S1 ≈ S4.**
  Numbers (TX/s): S1 base_merge_join=66.97, S2 pipeline_view=97.54,
  S3 mi_col_walk=210.52, S4 base_hash_join=65.02. S3 is ~2.2× S2 and
  ~3.2× S1/S4 — same shape as Q3I §A1 (S3 ≥ S2 > S1/S4) but with the
  pure §3.1.3 hierarchical-prefix benefit (no §3.1.2 sibling-aggregate),
  so the absolute S3 gap over S1/S4 is steeper here than in Q3I.
  Q3 binary now uses `TpchExecutableHelper::run` (mirrors `q3i_lsm` /
  `q12_lsm` wiring), so `make q3_lsm scale=N dram=M` produces TPut.csv
  going forward.
