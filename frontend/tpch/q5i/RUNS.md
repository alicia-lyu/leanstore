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

### 2026-05-16 22:22 PDT — Phase 4b macOS correctness verification

- **Commit**: `19104306` (`calcite-integration`)
- **TPut.csv**: not produced — correctness-only macOS run via `test_query_q5i_lsm`
- **Config**: SF=1, DRAM=default, structures=1,2,3,4 (S5 deferred), host=macOS (RocksDB)
- **Claim check**: cannot evaluate the S3 ≥ S2 > S1/S4 perf thesis on
  macOS (per project policy — macOS is for compile-checks and
  correctness only). Confirms only that all four query bodies agree
  on the answer: strict 4-way XOR parity at non-zero digest (e.g.
  `0x2b8dfff15144e310` with 2 rows, `0xc70476c481ebf544` with 1 row
  across reloads — data layout is non-deterministic at SF=1 but
  parity always holds). Linux SF=15 perf sweep tracked in
  `LINUX_PENDING.md`.
