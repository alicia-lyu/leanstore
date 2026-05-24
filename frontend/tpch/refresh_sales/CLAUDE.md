# refresh_sales/ — TPC-H RF1/RF2 update experiment (vanilla Q3/Q5)

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

Implementation guide for the **update-throughput experiment**, framed as
TPC-H's own **refresh functions RF1/RF2** (spec §5.1.2). Sourced from the
approved plan at `.claude/plans/refactored-juggling-robin.md`. This directory
holds the dedicated update binary; the per-structure maintenance it drives
lives in the shared COL pipeline + per-query workloads (see Critical files).
(Directory is named `refresh_sales/` after the TPC-H refresh functions it
implements — RF1 "New Sales" inserts, RF2 "Old Sales" deletes.)

> **macOS execution scope** (per top-level `CLAUDE.md`): on macOS, build the
> targets to confirm they compile and run `test_load_*` / `test_query_*` for
> correctness only. Do **not** run the end-to-end update binary or collect perf
> numbers here — that belongs on Linux. Gate: build → `test_load_*` →
> `test_query_*` (incl. the new post-RF1 parity check) all green *before* any
> Linux perf run.

## Why RF1/RF2, not "NewOrder adapted from TPC-C"

The write workload uses TPC-H's **standard** update operations, so there is no
schema adaptation to defend before reviewers:

- **RF1 ("New Sales")** — insert new orders, each with 1–7 lineitems (spec:
  loop SF×1500). This is exactly the COL-schema write that exercises the
  merged index's maintenance cost.
- **RF2 ("Old Sales")** — delete the corresponding orders + lineitems
  (insert/delete pair; maps onto the geo `insert1`/`erase1` pattern). **Run
  together with RF1** for three reasons: (1) it bounds the DB size during a long
  run (insert a batch, delete an equal batch),
  so insert timing and merged-index size don't drift; (2) delete-maintenance is
  its own signal — tombstone/compaction cost on LSM, in-place delete on B-tree,
  per structure — a comparison RF1 alone can't show; (3) RF1+RF2 is the
  standard TPC-H refresh pair, so reporting both is the complete, expected
  story rather than insert-only.

RF1/RF2 touch **only ORDERS and LINEITEM** — the COL tables. We do *not* adapt
TPC-C NewOrder (which would mean inventing warehouse/district/new-order
bookkeeping absent from the TPC-H schema, and inviting "why not CH-benCHmark?"
/ "that's not a real NewOrder" objections). Cite CH-benCHmark in related work
for the HTAP angle; implement RF1/RF2 for the actual experiment.

**PARTSUPP stock decrement is NOT part of the core experiment.** RF1 does not
touch PARTSUPP, and PARTSUPP is in no COL secondary, so a stock update would
cost the same under S1–S4 and add nothing to the merged-index comparison. It
is kept only as a clearly-labelled optional CH-benCHmark-style extension (see
§Optional extension), reported separately if at all.

## Context

The paper revision (due ~2026-05-30) needs a unified experiment narrative.
The current split — geo microbenchmark (geographic schema) for updates +
read-only TPC-H/TPCHI queries — reads as two disjoint stories. Instead of a
separate COL microbenchmark, we add the **RF1/RF2 update workload** to the
vanilla TPC-H queries (Q3, Q5), which already run on the shared **COL**
(customer–orders–lineitem) schema, and report **update throughput per storage
structure**. Existing read-query numbers are **not** re-measured.

The headline contrast this enables: the merged index matches the materialised
view on read latency *without* paying the view's update cost — RF1 throughput
per structure shows S3 (merged) maintaining at traditional-index levels while
S2 (view) pays more.

**Scope:** vanilla Q3 + Q5, both backends (lsm, btree). Q3I/Q5I deferred
(additive later via the COLI pipeline). No new query shapes — read queries
already cover join/count.

## Update size / freshness parameter (`--update_size`)

`--update_size` = number of RF1 orders applied per refresh batch before the
secondary structures are brought current — the **freshness knob**. Small batch
= secondaries lag the base by few orders (fresh, frequent maintenance); large
batch = more staleness tolerated (amortised maintenance).

**Current scope: `--update_size=1`** — eager maintenance, every insert/delete
updates the active structure's secondary immediately, so the DB is always
fresh. The flag still exists for a later deferred-freshness sweep, whose
natural larger setting is the TPC-H RF1 standard SF×1500 orders (0.1% of base)
per refresh.

Why it matters for the merged-index claim: views are typically
**batch-refreshed** to amortise maintenance, trading freshness for cost; the
merged index maintains **incrementally** (always fresh) at low cost. Sweeping
`--update_size` exposes this — at high freshness (small batch) the S2 view's
per-refresh maintenance cost rises while S3 (merged) stays flat — directly
supporting "matches the view on reads without the view's update cost."

**Design fork (affects effort).** Only a genuine freshness knob under
**deferred (batched)** maintenance: buffer `update_size` RF1 orders in the
base, then apply to each structure's secondary in one batch and measure.
Under purely **eager** maintenance (every insert maintains all secondaries
immediately, `update_size`≈1) the DB is always fresh and the parameter only
scales total volume. The merged index and split adapters batch trivially
(defer the per-record inserts); the *compelling* result needs the S2 view to
exploit an efficient batch/bulk refresh (its amortisation advantage) — that
view-side refresh kernel is the work beyond the eager half-day core (~+1–2
days). **Decided: eager (`update_size=1`) for now** — the half-day core. The
deferred-freshness sweep is a documented later stretch, not in the current
scope.

## Tables RF1/RF2 touch (verified in `tpch_tables.hpp`)

- **RF1 insert:** `orders_t` (refs an existing custkey), K∈[1..7] `lineitem_t`
  (ref valid partkey/suppkey). Plus, per active structure: `split_orders` +
  `split_lineitem` (S1), `merged_col` (S3), `q3_pipeline_view_t` /
  `q5_pipeline_view_t` (S2).
- **RF2 delete:** the same orders + lineitems, removed from base + the active
  structure's secondary in one transaction (TPC-H requires ORDERS/LINEITEM
  delete consistency).
- **Read-only references:** customer, part, supplier, partsupp, nation, region
  — never written by the core experiment. Q5's supplier→nation→region join
  still resolves because new lineitems carry valid suppkeys.
- **Lineitem (partkey, suppkey) validity:** must reference an existing PARTSUPP
  row so Q5's join resolves. Pick the part first, then one of *its* suppliers
  (see generator) — item-driven, valid by construction.

## Key facts established during exploration

- No TPC-C / NewOrder / CH-benCHmark code exists; RF1/RF2 are net-new, modeled
  on geo's `maintain_*` / `erase_*` pattern.
- The COL pipeline already exists: `CustomerOrdersLineitemPipeline<Backend>`
  (`tpch_family/col_pipeline.{hpp,tpp}`) owns `merged_col` (S3) +
  `split_orders`/`split_lineitem` (S1) and shows the tagged-key construction
  of `orders_coli_t` / `lineitem_col_t` we reuse for inserts/deletes. Q3 and Q5
  share one instance (`q3/executable_rocksdb.cpp` constructs both).
- S2 view maintenance is trivial: `q3_pipeline_view_t` (q3/views.hpp, id=35)
  bakes no filters — per-lineitem rows + FD-attached cols — so RF1 just inserts
  K view rows. **Both q3_view and q5_view are maintained per order** to keep
  the shared family image consistent under S2.
- Record layouts don't change → read-sweep images stay valid; copy the loaded
  image and run RF1/RF2 on the copy (also protects the canonical read images).

## Approach

### 1. RF1/RF2 generator (`tpch_family/refresh.hpp`, new)

RF1 inserts one new order + its lineitems above the loaded keyspace; RF2 deletes
**pre-existing** orders + their lineitems from the bottom of the loaded keyspace.
The two operations touch **disjoint keyspaces** — RF2 does not delete what RF1
inserts (per TPC-H §5.1.2; geo's "reservoir of own insertions" pattern does NOT
transfer). Paired insert-N / delete-N keeps DB size stable steady-state.
Lineitem (partkey, suppkey) must be a valid PARTSUPP pair (so Q5's supplier join
resolves), chosen **item-driven**: pick the part, then one of its suppliers —
not the loader's random-pair-snap (`load_lineitems_1order`, `tpch_workload.hpp:382`),
which draws part and supplier independently and isn't item-driven.

**RF1** (one new order):

- Pick an existing custkey (urand `[1, last_customer_id]`).
- Mint a fresh orderkey via `orderkey_from_index(rf1_next_index++)` starting at
  `rf1_next_index = ORDERS_SCALE * tpch_scale_factor + 1` post-recover. This
  preserves the TPC-H §4.2.3 sparse-orderkey grid (8 of every 32 integers
  populated) — same allocator as `loadOrders`. Set `o_orderdate` ≈ run date;
  register it in `order_dates` BEFORE generating lineitems
  (`load_lineitems_1order` requires this at `tpch_workload.hpp:388`).
- For each of K∈[1..7] lines: choose `partkey` (urand over `[1, last_part_id]`;
  NURand for hot-part skew is optional and not required by RF1). Then pick one
  of that part's suppliers by scanning PARTSUPP over the `ps_partkey = p`
  prefix (≤4 rows). Generate the line via `lineitem_t::generateRandomRecord(p,
  s, o_orderdate, computeRetailPrice(p))`; fold into `accumulate_for_order` to
  derive `o_totalprice`/`o_orderstatus`.
- Return `{orders_t, std::vector<lineitem_t>}`.

**RF2** (one pre-existing order to delete):

- Walk an `rf2_next_index` cursor starting at 1; the next orderkey is
  `orderkey_from_index(rf2_next_index++)` — same sparse-grid traversal as the
  loader. Cursor lives outside the database (two `Integer`s in `RefreshState`),
  faithful to the TPC-H spec's "exhaustible external stream" model.
- Look up `orders[K]` to get `o_custkey` (needed to construct tagged secondary
  keys for S1/S3 erase); scan `lineitem[{K, 1..}]` and break when orderkey
  changes to enumerate the linenumbers to delete.
- Return `{orderkey, custkey, linenumbers, exhausted}`. Cursor exhaustion when
  `rf2_next_index > loaded_index_max` signals the harness to wrap or stop.

### 2. Per-structure maintain (RF1) + erase (RF2) methods
Single-record insert/erase helpers next to `populate_merged`/`populate_split`
in `col_pipeline.tpp`, plus per-query bodies (Q3 owns them; Q5 reuses S1/S3/S4
verbatim and adds only its S2 view path):
- **S4 base:** insert/delete `orders_t` + `lineitem_t`.
- **S1 split:** insert/delete `orders_coli_t` + `lineitem_col_t` (reuse
  `populate_split` tagged-key build).
- **S3 merged:** insert/delete the same tagged records in `merged_col`.
- **S2 view:** insert/delete K `q{3,5}_pipeline_view_t` rows (FD cols from the
  new order + a customer lookup for `c_mktsegment`).

### 3. Dedicated update binary (this directory: `tpch/refresh_sales/`)
A standalone executable that constructs the vanilla-family workload (same
adapter set as `q3/executable_rocksdb.cpp`), recovers, then runs **RF1 and RF2
together** for a fixed duration at the selected `--storage_structure`: each
refresh inserts an `--update_size` batch of new orders (RF1) and deletes an
equal-size batch of previously-inserted orders (RF2), keeping the DB size
stable across the run. Emits **both insert and delete TX/s** per structure to
CSV. Does **not** touch `TpchExecutableHelper` (the read-query harness). lsm +
btree variants: `refresh_sales/executable_rocksdb.cpp`,
`refresh_sales/executable_leanstore.cpp`. Wire into `frontend/CMakeLists.txt` +
`generate_targets.py`.

### 4. Correctness gate (CLAUDE.md: build → test_load → test_query → perf)
Extend `tests/q3/test_query_q3_*.cpp` (and q5) to run N RF1 inserts then
re-verify cross-structure XOR parity still holds, and that RF2 restores the
pre-insert state. Green before any perf run.

## Write-performance measurement

The measured **unit of work is the RF1/RF2 pair** (size-stable, matches TPC-H
refresh semantics): one pair = insert `--update_size` new orders (RF1) + delete
`--update_size` previously-inserted orders (RF2). Run a fixed-duration loop of
pairs per `--storage_structure` (same harness shape as the read-query TX loop).
Insert N + delete N holds the DB size constant, so the number reflects
*steady-state* maintenance cost — not a growing index.

Report, per structure/backend:

- **Headline — pair throughput:** pairs/sec, normalised to **orders/sec** or
  **ms per `update_size` batch** so it is comparable across `update_size`
  values. This is the figure that backs "the merged index maintains at
  traditional-index levels without the view's update cost."
- **Attribution — RF1 vs RF2 split:** time RF1 and RF2 *separately within* each
  pair and also report insert-throughput and delete-throughput. The cost lands
  differently by backend (LSM: insert = cheap append, delete = tombstone +
  later compaction; B-tree: both in-place, delete may merge nodes), which is
  why a structure wins or loses.

Mirrors TPC-H: the power test times RF1 and RF2 separately, but they run as a
size-stable pair.

## Optional extension — CH-benCHmark-style stock update

Not part of the RF1 result. If a mixed-OLTP angle is wanted, additionally
decrement `partsupp_t::ps_availqty` per lineitem (`update1` on `Key{l_partkey,
l_suppkey}` — always a real row since `s` came from the part's supplier set).
PARTSUPP is in no COL secondary, so this is structure-independent and does not
affect the S1–S4 comparison; report it as a separate, clearly-labelled
extension, never folded into the RF1 numbers.

## Critical files

- `frontend/geo/maintain.tpp` — insert/erase pattern to mirror (read-only).
- `frontend/tpch/tpch_family/col_pipeline.{hpp,tpp}` — add insert/erase helpers.
- `frontend/tpch/tpch_family/refresh.hpp` — **new** RF1/RF2 generator.
- `frontend/tpch/q3/{workload.hpp,load.tpp,query.tpp}` — Q3 maintain/erase;
  `q3/views.hpp` `q3_pipeline_view_t` for the S2 insert/delete. Same for q5.
- `frontend/tpch/refresh_sales/executable_{rocksdb,leanstore}.cpp` — **new** binary.
- `frontend/CMakeLists.txt`, `generate_targets.py` — wire the new targets.
- `tests/q3/test_query_q3_*.cpp`, `tests/q5/test_query_q5_*.cpp` — RF1 parity +
  RF2 restore check.

## Reuse (do not re-implement)

- `orders_t` / `lineitem_t` `generateRandomRecord` static methods.
- `col_pipeline.tpp` tagged-key construction.
- `Adapter::insert` / `Adapter::erase` / `MergedAdapter::insert` / `erase`
  (`frontend/shared/adapter-scanner/Adapter.hpp`).
- existing XOR-parity test harness.

## Run / DB isolation (Linux only)

- Read-query numbers are **not** re-measured.
- Copy the vanilla-family loaded image into an isolated path (lsm: dir copy;
  btree: file copy); run the update binary on the copy — no reload, and the
  canonical read images stay clean. Q3/Q5 share the image.
- `<update_bin> --recover --storage_structure=N --ssd_path=<copy> ...`;
  collect TX/s from CSV. Append a `RUNS.md` entry afterward.

## Verification

- **macOS (compile + correctness):** build the lsm update binary + run
  `test_query_q3_lsm`/`test_query_q5_lsm` with the post-RF1 parity check and
  RF2-restore check — all `[OK]`, identical digest across S1–S4.
- **Linux (perf):** build btree + lsm; `test_load_*` → `test_query_*` green;
  copy image; run the update binary per structure; confirm RF1 TX/s; `RUNS.md`.

## Estimate

**Code: ~half a day** — generators and COL insert paths already exist; the
new surface is the RF1/RF2 generator, the per-structure insert/erase bodies,
and a small standalone binary. Plus the correctness checks and a short run
(image copy + timed loops per structure, minutes each — separate from code
time, backgroundable). Comfortably within the 1-week deadline.

## References

- TPC-H Standard Specification v3.0.1, §5.1.2 (refresh functions RF1/RF2):
  https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf
- CH-benCHmark (TU München), HTAP related work / stock-update extension:
  https://db.in.tum.de/research/projects/CHbenCHmark/
