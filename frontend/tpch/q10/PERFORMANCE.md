# Q10 Performance Investigation — btree S2 regression & S3 < S1

Investigation into two anomalies in the first-Linux 5L Q10 sweep
(`RUNS.md` 2026-05-25 entry):

| ms/query (5L) | S1 split | S2 view | S3 merged | S4 hash |
|---|--:|--:|--:|--:|
| **btree** | 23,535 | **174,260** | 35,262 | 127,486 |
| (LSM) | 16,059 | 17,169 | **11,326** | 77,989 |

1. **S2 (view) regressed to 174 s on btree** — ~5–7× S1/S3. Was the paper's
   "S3 ≥ S2" claim built on a strawman baseline?
2. **S3 (merged) is slower than S1 (split) on btree** (35 s vs 23.5 s), the
   reverse of LSM where S3 wins.

Method: each fix is a **runtime-toggleable A/B** measured on one image
(`--q10_view_variant`, `--skip_order_physical`), iterated at a cheap
DRAM-overflowing cell (SF=150 dram=0.1, ~8× view overflow — same regime as
5L) and confirmed at 5L. Instrumentation: `--q10_stats` (per-query Q10Stats
cardinalities) + per-window buffer-manager IO counters (R/Evicted MiB,
LeanStore logger fix).

---

## §1 — Where we are

Both anomalies are explained and one is fully fixed:

- **S2 was a strawman.** The per-lineitem view (`q10_pipeline_view_t`) scans
  *every* lineitem and applies the orderdate window only at drain time (the D4
  anomaly), so it churns the whole 8.6 GiB view to keep ~4% of orders. The
  **fair S2 — a per-order pre-aggregated view** (`q10_pipeline_view_preagg_t`,
  returnflag baked, date live) — is **855× faster** at the iteration cell
  (17,317 → **20 ms/query**, btree) and becomes the *fastest* structure, not a
  slow baseline. **The "S2 regression" is closed: it was the view design, not
  the engine.**
- **S3 < S1 on btree is real and architectural.** Q10 has no customer
  predicate, so the COL walker's only prune is order-level (`SkipOrder`), which
  forward-iterates each date-failing order's co-located lineitems. The
  **physical SkipOrder seek** (variant B) cuts records-visited ~4× (CPU) but
  **does not** close the wall-time gap on btree: the regime is page-bound and
  the ~4-lineitem orders are sub-page-sized, so the seek skips *decoding* the
  lineitems but not *reading* the leaf pages they share with the next order.
  **This is a citable lesson, not a bug: COL-MI co-location is a btree
  liability when the only prune is below the co-location grain.**

### Iteration cell (btree, SF=150, dram=0.1) — measured

| structure | ms/query | R MiB/q | Evicted MiB/q | size MiB | pp_0 / worker util |
|---|--:|--:|--:|--:|--:|
| S1 base | 128 | 2.47 | 1.72 | 415 | 99.96% / 41% |
| **S2-A** per-lineitem | **17,317** | 510.5 | 425.0 | 834 | 99.95% / 10% |
| **S2-B** per-order preagg | **20** | 0.07 | **0** | ~small | 99.96% / **89%** |
| S3-A logical SkipOrder | 1,011 | 26.07 | 20.29 | 424 | 99.96% / 21% |
| S3-B physical SkipOrder | 986 | 25.77 | 20.33 | 424 | 99.96% / 17% |
| S4 hash | 12,008 | 361.6 | 318.9 | 329 | 99.95% / 10% |

- **S2-B vs S2-A**: 855× faster; IO 7,500× less; 0 evictions (the preagg view
  fits the pool); bottleneck flips from paging (pp_0) to query work (worker
  89%). At this cell S2-B (20 ms) beats S1 (128 ms) and S3 (1,011 ms).
- **S3-B vs S3-A**: `mi_records_visited` 1,147,041 → 280,993 (**4.1× fewer**,
  the skip works), worker cycles −18%, **but R MiB 26.07 → 25.77 (−1%)** and
  wall-time 1,011 → 986 ms (−2.5%). Page-bound; seek can't skip sub-page-sized
  orders' pages.

### 5L confirmation (both backends) — *pending sweep*

> Filled from `build/scratch/q10_5L_sweep.sh` output once it lands; logged in
> `RUNS.md`. Expected: S2-B collapses the 174 s btree S2 toward S1/S3 levels;
> S3-B trims btree S3 CPU but leaves the page-bound gap; LSM S3 stays the
> winner (S3-B neutral-or-better — the macOS "Seek kills prefetch" finding was
> refuted on Linux, `backend.hpp`).

---

## §2 — Hypothesis ledger

| ID | Hypothesis | Status | Takeaway |
|----|-----------|--------|----------|
| Q10-H1 | S2 slow because the per-lineitem view is huge (scan-bound) | **CONFIRMED + REMEDIATED** | 899K view rows, 510 MiB read/q, drain-time date filter keeps 4%; per-order preagg view → 855× faster, 0.07 MiB/q |
| Q10-H2 | S2 slow because it aggregates ~28× the rows (work-bound) | **SECONDARY** | pp_0 at 99.9% vs worker 10% says page-bound dominates; the preagg fix removes both (fewer rows *and* smaller scan) |
| Q10-H3 | S3 < S1 on btree because the order-level logical SkipOrder reads date-failing orders' co-located lineitems | **CONFIRMED** | S3 visits the full 1.147M-record MI / 26 MiB; S1 reads 2.81 MiB (never seeks lineitems of a date-failing order) |
| Q10-H4 | A physical SkipOrder seek closes the S3<S1 btree gap | **REFUTED (the finding)** | Seek cuts records-visited 4× (CPU) but not page IO; ~4-lineitem orders share leaf pages with the next order — page-bound gap persists |
| Q10-H5 | Physical SkipOrder regresses LSM S3 (Seek invalidates RocksDB prefetch) | **(5L pending)** | Premise refuted on Linux for SkipGroup (`backend.hpp`); A/B'd not assumed |

---

## §3 — S2: the per-order pre-aggregated view (variant B)

**Diagnosis.** `q10_pipeline_view_t` is keyed `(custkey, orderkey, linenumber)`
— one row per lineitem — carrying unaggregated `l_extendedprice/l_discount`, a
live `l_returnflag`, and a 217-byte FD customer payload duplicated per
lineitem. `query_by_view` scans the whole view, filters returnflag per row,
rolls every returned lineitem into a per-orderkey map, and applies the
orderdate window only at drain (Decision D4). Result: it touches all ~|LINEITEM|
rows to keep the ~4% of orders in the date window (page-bound — pp_0 saturated,
510 MiB read/query at the iteration cell).

**Key realisation.** `l_returnflag='R'` is a spec *constant*, not a
substitution parameter (only `:d` is parameterised), so it is **bakeable**. The
view should pre-aggregate at the finest *sound* grain — per order (the date
filter is order-granular + parameterised, so per-customer pre-agg would be
unsound, but per-order is fine).

**Fix.** `q10_pipeline_view_preagg_t` (id=72), one row per `(custkey, orderkey)`
carrying `returned_revenue = SUM(l_extendedprice·(1−l_discount))` over
`l_returnflag='R'` (baked at load), `o_orderdate` (live), + FD customer cols.
Orders with zero returned revenue are not emitted. Loader
`populate_q10_view_preagg` reuses the S3 walker in a new
`Q10FilterMode::ViewLoadPreagg` (returnflag bake + per-order flush at order
boundary / group end — Pattern B, no view/walker drift). Query collapses to
scan → date filter → per-customer SUM → NATION INL → TopN. The D4 anomaly is
gone.

**Result** (iteration cell, same image, A/B): 17,317 → **20 ms/query** (855×);
view 834 → tiny (SF=1: 5949 rows/0.29 MiB → 655 rows/0.039 MiB, ~7.5× smaller);
fits the pool (0 evictions); regime flips page-bound → compute-bound.

**Parity.** `test_query_q10_{lsm,btree}` re-run S2 with `--q10_view_variant=
preagg`; digest matches S3 at both param iters on both backends.

---

## §4 — S3: physical SkipOrder seek (variant B) and the filter-hierarchy lesson

**Diagnosis.** Q10's `on_customer` always returns `Continue` (no per-customer
predicate), so the customer-level physical `SkipGroup` — the trait that wins
for Q3I/Q5I on btree — *never fires*. Q10's only prune is the orderdate window
(`SkipOrder`), which `col_group_walk` handles by forward-iterating every
date-failing order's co-located lineitems. So S3 reads ~the whole MI (1.147M
records / 26 MiB at the iteration cell) where S1 reads 2.81 MiB (S1 `continue`s
on a date-failing order and never seeks its lineitem secondary).

**Fix attempt.** A physical SkipOrder path in `col_group_walk`
(`--skip_order_physical=1`): on `SkipOrder`, `seek<orders_coli_t>({custkey,
skip_orderkey+1})`. The COL key sorts orderkey before the type tag, so the
rejected order's lineitems sort strictly before the target and are skipped.
This is an explicit A/B flag, **not** gated by the backend trait: order-level
skip was historically kept logical on both backends ("~4-lineitem orders don't
pay back a tree descent"); Q10's high-order-rejection regime tests exactly that
assumption.

**Result** (iteration cell, A/B): `mi_records_visited` 1,147,041 → 280,993
(4.1× fewer, −18% worker cycles) but R MiB 26.07 → 25.77 (−1%) and wall-time
−2.5%. **The seek skips decoding the date-failing lineitems but not reading the
leaf pages they share with the next order** (orders are sub-page-sized). The
regime is page-bound, so wall-time barely moves.

**Why S1's per-order lineitem seek skips pages but S3's SkipOrder seek does
not** (both scan all 225K orders to apply the date filter, both admit the same
~34K surviving lineitems; the difference is whether the ~860K date-failing
lineitems' *pages* are read):

- **S1's lineitem secondary is a separate tree, seeked only for surviving
  orders.** A date-failing order `continue`s without touching the lineitem
  tree, so consecutive rejects contribute zero lineitem reads. Survivors are
  sparse (~1 in 26), so each lineitem seek hops to the next survivor over a
  ~25-order run (~100 lineitems ≈ a whole leaf page) of co-sorted date-failing
  lineitems — the seek *batches the rejected run into one page-spanning jump*.
- **S3 cannot batch.** It must read every order record to apply the date
  filter, and in the MI those order records are interleaved on the *same leaf
  pages* as the lineitems — so reading orders forces reading their lineitem
  pages. Its SkipOrder seek hops one order at a time; a rejected order's ~4
  lineitems share a page with the next order's record, so the hop is sub-page
  (skips *decoding*, the 4× `mi_records_visited` drop, but not *reading*). It
  cannot seek straight to the next survivor because it only learns an order is
  rejected by reading its `o_orderdate` — which is on that page. The seek would
  skip pages only if one order's lineitems spanned ≥1 full leaf page; TPC-H's
  ~4-lineitem orders never do.

**The finding.** The COL-MI advantage is **filter-hierarchy-dependent**. It
pays off when the dominant prune is *at or above* the co-location grain
(customer-level `SkipGroup`, as in Q3I/Q5I — skip whole groups, skip whole
pages). It is a btree liability when the only prune is *below* the co-location
grain (Q10's order-level window) and the sub-units (orders) are smaller than a
leaf page — you pay to read the co-located records you can't use. On LSM the
forward-iterate is a cheap block-cache scan, so S3 still wins; on btree it is
page traversal, so S1 ties/edges it. **S3's Q10 pitch is therefore "wins on LSM
query time; on btree ties/loses to the dense split S1 for an order-only prune,
while still crushing S2-strawman/S4 — the MI's value for Q10 is
storage/maintenance + LSM speed, not btree query speed."**

**Parity.** `test_query_q10_{lsm,btree}` re-run S3 with
`--skip_order_physical=1`; digest matches logical S3 / S1 / S4 at both param
iters on both backends; `mi_records_visited` drops ~4× confirming the skip.

---

## §5 — Paper framing

- **S2**: report the *fair* (per-order pre-aggregated) S2, not the per-lineitem
  strawman. With it, S3's pitch is **"S3 matches the pre-aggregated view's
  query locality on raw co-located data — without the view's storage,
  maintenance, and recompute-on-schema-change cost"** — the standard
  materialised-view-vs-index tradeoff, stated honestly. (At the iteration cell
  the small preagg view actually *beats* S3 on query time; that is expected and
  fine — it is a query-specialised pre-aggregation that bakes the returnflag
  constant and must be re-derived if the query family changes.)
- **S3 on btree**: do not overclaim. S3 wins on LSM; on btree, for Q10's
  order-only prune, the dense custkey-sorted split S1 ties/edges it while both
  crush S2-strawman/S4. The general lesson (co-location helps iff the dominant
  prune is at/above the co-location grain) is more valuable than a forced win.

---

## §6 — Reproduce

```bash
# Parity (SF=1) — both A/B variants checked:
./build/frontend/test_query_q10_lsm   --ssd_path=… --csv_path=… --tpch_scale_factor=1
./build/frontend/test_query_q10_btree --ssd_path=…file… --csv_path=… \
    --tpch_scale_factor=1 --dram_gib=8 --wal=true --trunc=true

# Iteration-cell A/B (btree, SF=150 dram=0.1), per-structure logs under
# build/q10_btree/150-in-0.1/structureN.log:
make q10_btree            scale=150 dram=0.1 q10_stats=true                       # baseline (S2-A, S3-A)
make q10_btree_2          scale=150 dram=0.1 q10_stats=true q10_view_variant=preagg   # S2-B
make q10_btree_3          scale=150 dram=0.1 q10_stats=true skip_order_physical=1     # S3-B

# 5L confirmation, both backends: build/scratch/q10_5L_sweep.sh
```

Flags (`tpch_flags.hpp`): `--q10_stats` (Q10Stats block),
`--q10_view_variant=lineitem|preagg` (S2 A/B), `--skip_order_physical=-1|0|1`
(S3 A/B; -1/0 logical, 1 physical seek). Defaults preserve original behaviour
(lineitem / logical).
