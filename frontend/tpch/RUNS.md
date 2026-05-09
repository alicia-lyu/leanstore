# TPC-H Run-Configuration Rationale

Shared rationale for picking `scale=` and `dram=` on `q*_lsm` /
`q*_btree` perf sweeps. Per-query `q*/RUNS.md` ledgers reference this
doc instead of restating it.

## Why we don't trust SF=15 / DRAM=0.1 alone

At SF=15, per-structure secondary sizes are tiny: ~17–20 MiB on RocksDB
LSM, ~50 MiB on LeanStore B-tree (post-G8 projections). With
`--dram_gib=0.1` (100 MiB buffer pool) the working set sits in cache on
both backends. Cache-resident numbers tell us about CPU / dispatch
overhead, not about the merged-index advantage we're trying to show:
**the paper claim that S3 amortises sibling sub-aggregate work into the
merged scan, beating S1/S4, hardens when scans hit disk**, not when they
hit DRAM.

The focus is therefore **beyond-memory**, with `dram ≈ secondary / 5` as
the operating-point rule. (1/5 forces 80% miss-rate steady-state on a
uniform workload — well past the "secondary fits in cache" boundary
without being so harsh that load-time dominates.)

## Size scaling — empirically verified

Per-structure secondary size, S5 column from `q3i/PERFORMANCE.md` and
today's `build/q3i_lsm/TPut.csv`:

| Backend  | SF=1     | SF=15      | SF=40       | MiB / SF |
|----------|---------:|-----------:|------------:|---------:|
| LSM      | 1.3 MiB  | 17–20 MiB  | ~47 MiB     | ~1.3     |
| BTree    | —        | 49 MiB     | 130–140 MiB | ~3.3     |

**BTree ≈ 3 × LSM** (49/17 ≈ 2.9 at SF=15; 130/47 ≈ 2.8 at SF=40).
The factor-of-3 reflects LSM's compression + dense leaves vs B-tree's
fill-factor headroom and lack of block compression in this tree.

## Target-SF table

To land secondary sizes of 2 / 5 / 20 GiB:

| Target secondary | LSM SF (~1.3 MiB/SF) | BTree SF (~3.3 MiB/SF) | DRAM (1/5 rule) |
|------------------|---------------------:|-----------------------:|-----------------|
| 2 GiB            | ~1500                | ~620                   | 0.4 GiB         |
| 5 GiB            | ~3850                | ~1550                  | 1 GiB           |
| 20 GiB           | ~15000               | ~6200                  | 4 GiB           |

## Machine-capacity check (CloudLab `node0`)

| Resource | Capacity | Implication |
|----------|----------|-------------|
| `/mnt/ssd` (NVMe) | 880 GiB free | TPC-H raw is ~N GiB / SF; engine footprint smaller (projection + LSM compression). SF=620 BTree fits comfortably. SF=1500 LSM is on the edge (~150–250 GiB engine footprint). SF≥6000 does **not** fit. |
| RAM | 251 GiB | `--dram_gib` is the buffer-manager budget, not RAM. 0.4–4 GiB DRAM settings are trivial. |
| Cores | 64 | Plenty for the worker-threads / pp-threads defaults; not a bottleneck. |
| `perf_event_paranoid` | 0 | Required for the perf-counter columns in `TPut.csv`. |

**Load-time scaling** is linear in SF: q3i_lsm SF=15 takes ~113 s, so
SF=600 → ~75 min, SF=1500 → ~3 h. **Practical session ceiling**:
~SF=1000 LSM, ~SF=400 BTree per fresh load. DRAM variants on the same
SF reuse the persisted `.json` and pay only the 15 s TX-loop cost.

**Conclusion**: 2 GiB target is the realistic ceiling for a single
session on this node; 5 GiB BTree (SF=1550) is reachable but sloppy on
load time; 20 GiB targets are out of reach without TB-class SSDs.

## Operating-point recipe

1. Pick a target secondary size between 500 MiB and 2 GiB.
2. From the table above, derive `scale=` per backend (round to 50).
3. Set `dram=secondary_GiB / 5` (round to one decimal).
4. Run `make q*_{lsm,btree} scale=… dram=…`. The umbrella target
   expands to per-structure `_N` sub-targets, each in its own process —
   a SEGV / assertion in one structure won't poison the others' rows.
5. Reuse the cached `.json` to A/B different DRAM values cheaply.
6. Append an entry to the relevant `q*/RUNS.md` recording
   `secondary / DRAM` ratio and observed shape.

## When the rule doesn't apply

- **Smoke / parity runs**: cache-resident SF=1 (or SF=15 dram=4) is fine
  — purpose is correctness, not perf shape.
- **Cycle / dispatch microbenchmarks**: cache-resident is what you want
  — secondaries-fit-in-cache is the controlled environment for
  per-record CPU work.
- **Load-cost amortisation studies** (PERFORMANCE.md §H12): different
  metric (load-time / TX-volume break-even), different SF policy.
