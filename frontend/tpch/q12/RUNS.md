# Q12 — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs and need
consolidating.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the Q12 paper
  claim (S3/S4 merged-index variants beat S1 traditional+merge-join and
  match S2 view at lower storage cost) and how?
```

## Runs

_(no perf runs logged since this file was created on 2026-05-08.)_
