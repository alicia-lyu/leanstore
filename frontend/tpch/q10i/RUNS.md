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
