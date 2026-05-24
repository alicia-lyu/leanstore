# results/ — DBToaster refresh_sales baseline output

## What this baseline measures

Generic incremental-view-maintenance (IVM) cost for the **Q3 and Q5 pipeline
views** under a TPC-H RF1/RF2 update stream, as a baseline for LeanStore's
materialized-pipeline-view structure (**S2**). DBToaster compiles the two
view `SELECT`s into delta-maintenance code; the harness fires the update
events and measures per-update latency, throughput, and resident memory.

## Output CSV — `RefreshTPut.dbtoaster.csv`

Extends the LeanStore format (`frontend/tpch/refresh_sales/executable_rocksdb.cpp:169`)
with a memory column:

```
elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s,vmrss_kb
```

Throughput reflects maintaining **both** views per event, directly comparable
to the single LeanStore S2 number (S2 maintains both q3+q5 views per order).
RF1 is timed during the streamed insert tail; RF2 during the delete loop
(separate phases, not interleaved pairs — at this scale even the LeanStore run
could not measure pair steady-state, see `frontend/tpch/refresh_sales/RUNS.md`),
so `pair_orders_per_s` is an aggregate, not a size-stable pair rate.

**LeanStore S2 comparison targets** (SF=1, `refresh_sales/RUNS.md`):
btree S2 ≈ 22.6k, lsm S2 ≈ 4.2k RF1 inserts/s.

## Working-set anchor and memory footprint (unlimited memory)

The paper memory-pressure cells fix a **5 GiB secondary** and sweep DRAM
**5L = 1.0 GiB** / **5H = 0.4 GiB** (`paper-data/scripts/plot_paper_sweep.py:66-69`,
`paper-data/REVISION_SNIPPETS.md:82-88`). We pin the DBToaster maintained
working set to the same ~5 GB by choosing SF empirically: load at SF
0.01 / 0.1, read post-warmup VmRSS, extrapolate to ~5 GB, regenerate at that SF
(measured: ~140 MB at SF=0.01, ~1.41 GiB at SF=0.1 → **SF≈0.36** for ~5 GB;
much smaller than LeanStore's SF≈3–4 because DBToaster keeps the full join
multiset *plus* hash-indexed delta maps, not a packed B-tree).

**We do not sweep `ulimit -v`.** DBToaster is pure in-memory: the views + delta
maps must all stay resident (no eviction, no SSD spill), so a cap below the
working set just OOMs rather than degrades — there is no useful pressure curve.
Instead we run on **unlimited memory** (`make experiment` / `entrypoint.sh`)
and report the footprint as a lower bound on the RAM requirement:
- **VmRSS after warmup** ≈ the maintained-view working set (the *loose lower
  bound* on RAM).
- **peak RSS** (`time -v` "Maximum resident set size") ≈ the run high-water
  mark, which is a **multiple** of the working set (observed ≈1.7× at SF=0.36:
  ~4.9 GiB working set vs ~8.25 GiB peak; the precise multiple under sustained
  RF1/RF2 is left to a later characterization).

## The headline contrast

LeanStore's `--dram_gib` is a **buffer-pool** budget over an SSD-backed
secondary: at 0.4 GiB it serves a 5 GiB secondary by caching ~8% and spilling
the rest to SSD, degrading gracefully (the 5L → 5H cells). DBToaster must hold
the entire ~5 GiB working set (and several GiB more at peak) resident. So the
finding is the **memory requirement gap**: generic IVM needs RAM ≥ its full
in-memory footprint to maintain the same views that LeanStore S2 serves from a
sub-GiB buffer pool — reported as DBToaster's throughput + working-set/peak RSS
at the ~5 GB anchor, beside LeanStore's S2 numbers and 5L/5H curve.

## Caveats

- **DECIMAL → double**: DBToaster has no fixed-point; revenue columns are
  doubles vs LeanStore's `Numeric`. Fine for throughput/memory comparison.
- **Key streams differ**: dbgen `-U` keys ≠ LeanStore's sparse grid, but the
  per-update cost is comparable (same disjoint-keyspace, size-stable pair).
