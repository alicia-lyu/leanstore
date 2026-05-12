# Q5I — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the paper claim
  for Q5I (COLI MI § 3.1.2 sibling sub-aggregate over the
  Customer→{Orders, Invoice} fan-out) and how?
```

## Runs

_(Q5I is design-doc only as of 2026-05-08 — no executable, no runs.)_
