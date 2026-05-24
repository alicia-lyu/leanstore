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

_(Q10I is design-doc only as of 2026-05-08 — no executable, no runs.)_

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
