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

### 2026-05-08 21:32 CDT — Q3 SF=15 sweep, throughput numbers blocked
- **Commit**: `708daa84` (`calcite-integration`)
- **TPut.csv**: not produced — `q3_{lsm,btree}` executables run the query
  once and exit; `tput_tx` driver is not yet wired (see LINUX_PENDING.md
  §"Q3 throughput-shape comparison").
- **Config**: SF=15, DRAM=0.1GiB, S1–S4, Linux (CloudLab `node0`).
- **Claim check**: Inconclusive on throughput shape — only confirms
  cross-structure correctness (byte-identical 10-row top-N across
  S1/S2/S3/S4 on both backends); the S3 ≥ S2 > S1/S4 ordering can't be
  measured until the executable gains a TX loop.
