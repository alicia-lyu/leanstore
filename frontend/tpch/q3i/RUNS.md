# Q3I — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file. Pre-existing run-grade numbers from before this file's creation are
captured in [`PERFORMANCE.md`](./PERFORMANCE.md) (§A1 anomaly, §3 worklist
G-series, §A6 memory-pressure sweep). New runs go here.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  (S3 ≥ S2 > S1/S4 with §3.1.2 sibling sub-aggregate amortisation) and
  how?
```

## Runs

_(no new runs since this file was created on 2026-05-08; Q3I `_btree`
perf sweep is blocked on the COLI tagged-key variant-dispatch failure
tracked in `LINUX_PENDING.md`.)_
