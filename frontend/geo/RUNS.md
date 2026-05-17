# Geo — Run Log

Append one entry per perf run. **Don't backfill** runs that pre-date this
file unless their numbers are scattered across other docs.

## Entry format

```
### YYYY-MM-DD HH:MM TZ — <one-line headline>
- **Commit**: `<short-sha>` (`<branch>`)
- **TPut.csv**: `<path>` *(or "not produced — <reason>")*
- **Config**: SF=<n>, DRAM=<n>GiB, structures=<list>, host=<linux/macos>
- **Claim check**: <1 sentence — does this run support the geo paper
  claim (merged indexes beat traditional + materialized view on the
  Nation→…→Customer hierarchical join) and how?
```

## Runs

### 2026-05-17 09:28 PDT — geo_lsm SF=15 sweep on macOS; MI matches view, beats base/hash on join-nsc

- **Commit**: `010e1bac` (`calcite-integration`)
- **TPut.csv**: not produced — geo binary emits inline per-iter rows
  in `build/geo_lsm/15-in-0.1/structure{1..4}.log` (no TPut.csv
  aggregator step in this branch). Headline join-n / join-nsc TPut
  (TX/s): S1 (base)=115/4887, S2 (mat_view)=219/10830, S3
  (merged_idx)=119/11931, S4 (hash)=81/4830.
- **Config**: SF=15, DRAM=0.1 GiB, structures=1–4, host=macos
  (workflow-rule override per user request; smoke-grade only —
  paper-grade Linux replay still pending).
- **Claim check**: Supports the paper claim at the deep
  (join-nsc) level — the single MI (S3, 11.9k TX/s) edges out the
  materialized view (S2, 10.8k) and comfortably beats traditional
  indexes (S1, 4.9k) and hash (S4, 4.8k). At the root level
  (join-n) the view is fastest (S2 219 vs S3 119), consistent
  with a pre-aggregated single-scan being view-friendly.

### Fix applied during this run

`generate_targets.py` was emitting four TPC-H–only diagnostic flags
(`--micro_perf`, `--cfstats`, `--coli_walker_variant`,
`--use_seek_skip`) into every recipe, including geo, which doesn't
declare them. Guarded the diag-flags block on `exec_fname.startswith
("geo_")` and regenerated `targets.mk`.
