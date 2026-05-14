# Q5 — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date
this file unless their numbers are scattered across other docs and
need consolidating.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  for this query and how?
```

## Runs

### 2026-05-14 16:13 CDT — q5_lsm SF=15 DRAM=0.1 GiB first-Linux sweep
- **Commit**: `a1fbdc15` (`calcite-integration`)
- **TPut.csv**: `build/q5_lsm/TPut.csv` rows 6–9 (DRAM=0.1, scale=15;
  rows 2–5 are the throwaway pre-load to populate
  `/mnt/ssd/q5_lsm/build/15.json` and are not part of the perf record).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (CloudLab node, Xeon
  E5-2660 v3, 40 cores, 157 GiB RAM, SATA SSD `/mnt/ssd` `/dev/sdb`).
- **Claim check**: Partially supports the §3.1.3 pure-hierarchical
  paper claim — measured shape **S2 (205.0) > S3 (155.6) > S1 (42.8)
  ≈ S4 (32.3) TX/s** confirms the MI/view family beats the
  split-merge and hash baselines (S2/S3 ≫ S1/S4 as predicted) but
  inverts S3 ≥ S2: the materialised view outperforms the COL group
  walk at this SF, the same shape Q3I exhibits (see
  [`q3i/PERFORMANCE.md`](../q3i/PERFORMANCE.md)). The inversion is
  attributed to the same infrastructure gap — the COL walker is
  hand-tuned for Q3I but not Q5; nothing Q5-specific.

### 2026-05-14 16:13 CDT — q5_btree SF=15 DRAM=0.1 GiB first-Linux sweep
- **Commit**: `a1fbdc15` (`calcite-integration`)
- **TPut.csv**: `build/q5_btree/TPut.csv` rows 6–9 (DRAM=0.1, scale=15;
  rows 2–5 are the throwaway pre-load).
- **Config**: SF=15, DRAM=0.1 GiB, S1–S4, Linux (same host as above,
  `--wal=true --vi=false`).
- **Claim check**: Same shape as LSM — **S2 (331.0) > S3 (249.6) > S1
  (66.9) ≫ S4 (9.5) TX/s**. Same partial support for the §3.1.3
  paper claim with the same S2/S3 inversion. The S4 hash-join
  baseline is ~30× slower than LSM at the same SF — LeanStore's
  hash-join scan path under DRAM=0.1 GiB is the suspected bottleneck;
  see q3i/PERFORMANCE.md for the analogous Q3I S4 finding.
