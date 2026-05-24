# new_order/ — NewOrder write transaction for vanilla TPC-H (Q3/Q5)

This file provides guidance to Claude Code (claude.ai/code) when working with
code in this repository.

Implementation guide for the **NewOrder update experiment**. Sourced from the
approved plan at `.claude/plans/refactored-juggling-robin.md`. This directory
holds the dedicated update binary; the per-structure maintenance it drives
lives in the shared COL pipeline + per-query workloads (see Critical files).

> **macOS execution scope** (per top-level `CLAUDE.md`): on macOS, build the
> targets to confirm they compile and run `test_load_*` / `test_query_*` for
> correctness only. Do **not** run the end-to-end update binary or collect perf
> numbers here — that belongs on Linux. Gate: build → `test_load_*` →
> `test_query_*` (incl. the new post-insert parity check) all green *before*
> any Linux perf run.

## Context

The paper revision (due ~2026-05-30) needs a unified experiment narrative.
The current split — geo microbenchmark (geographic schema) for updates +
read-only TPC-H/TPCHI queries — reads as two disjoint stories. Instead of a
separate COL microbenchmark, we add a **NewOrder write transaction** to the
vanilla TPC-H queries (Q3, Q5), which already run on the shared **COL**
(customer–orders–lineitem) schema, and report **update throughput per storage
structure**. Existing read-query numbers are **not** re-measured.

CH-benCHmark keeps the *full* TPC-C schema + the unmodified TPC-C transaction
mix and *adds* SUPPLIER/NATION/REGION (overlap: ITEM≈PART, STOCK≈PARTSUPP,
ORDER≈ORDERS, ORDER-LINE≈LINEITEM, CUSTOMER≈CUSTOMER). It omits nothing from
TPC-C. Our schema is TPC-H-base, so we map NewOrder onto what exists: insert
ORDERS + K LINEITEM, decrement PARTSUPP stock (≈ TPC-C STOCK). The
warehouse/district/new-order bookkeeping is **dropped because those tables do
not exist in the TPC-H schema** (a schema-forced deviation, not a
simplification of convenience — state it in one sentence in the paper).

**Scope:** vanilla Q3 + Q5, both backends (lsm, btree). Q3I/Q5I deferred
(additive later via the COLI pipeline). No new query shapes — read queries
already cover join/count.

## Tables a NewOrder touches (verified in `tpch_tables.hpp`)

- **Insert:** `orders_t` (refs an existing custkey), K `lineitem_t` (ref
  existing partkey/suppkey). Plus, per active structure: `split_orders` +
  `split_lineitem` (S1), `merged_col` (S3), `q3_pipeline_view_t` /
  `q5_pipeline_view_t` (S2).
- **Update (stock, ≈ CH-benCHmark):** `partsupp_t::ps_availqty` via `update1`
  on `Key{l_partkey, l_suppkey}` per lineitem. PARTSUPP is in no COL secondary,
  so this costs the same under S1–S4 — it adds CH-benCHmark fidelity *without*
  distorting the merged-index comparison. Include it.
- **Read-only references:** customer, part, supplier, nation, region — never
  written. Q5's supplier→nation→region join still resolves because new
  lineitems carry existing suppkeys.
- **Forced omission (schema-driven):** WAREHOUSE / DISTRICT / NEW-ORDER updates
  — those tables don't exist in the TPC-H schema. Documented as a deviation,
  not claimed as a faithful TPC-C NewOrder.
- **Reuse:** `orders_t::generateRandomRecord(...)` and
  `lineitem_t::generateRandomRecord(partkey, suppkey, o_orderdate,
  p_retailprice)` already exist — the generator just calls them K times.

## Key facts established during exploration

- No TPC-C / NewOrder / CH-benCHmark code exists; NewOrder is net-new,
  modeled on geo's `maintain_*` pattern.
- The COL pipeline already exists: `CustomerOrdersLineitemPipeline<Backend>`
  (`tpch_family/col_pipeline.{hpp,tpp}`) owns `merged_col` (S3) +
  `split_orders`/`split_lineitem` (S1) and shows the tagged-key construction
  of `orders_coli_t` / `lineitem_col_t` we reuse for inserts. Q3 and Q5 share
  one instance (`q3/executable_rocksdb.cpp` constructs both).
- S2 view maintenance is trivial: `q3_pipeline_view_t` (q3/views.hpp, id=35)
  bakes no filters — per-lineitem rows + FD-attached cols — so NewOrder just
  inserts K view rows. **Both q3_view and q5_view are maintained per order**
  to keep the shared family image consistent under S2.
- Record layouts don't change → read-sweep images stay valid; copy the loaded
  image and run writes on the copy (also protects the canonical read images).

## Approach

### 1. NewOrder generator (`tpch_family/new_order.hpp`, new)

**Part/supplier selection must be item-driven** (TPC-C/CH-benCHmark intent),
not the loader's random-pair-snap. TPC-C NewOrder picks each line's *item* by
`NURand(8191, 1, 100000)` and the supplier is *derived* from the stock entry
(CH-benCHmark: `su_suppkey = mod((s_w_id * s_i_id), 10000)`); every
(item, warehouse) is stocked, so validity is automatic. The repo loader
(`load_lineitems_1order`, `tpch_workload.hpp:382`) instead draws part **and**
supplier independently and snaps to the nearest existing PARTSUPP pair — a
data-gen convenience, not an order model (it isn't item-driven and the snap can
shift the chosen part). TPC-H PARTSUPP stocks only 4 suppliers per part, so we
stay item-driven by picking the part first, then one of *its* suppliers:

- Pick an existing custkey (urand `[1, last_customer_id]`).
- Mint a fresh orderkey beyond the loaded max (`recover_last_ids()` + counter);
  set `o_orderdate` ≈ run date; register it in `order_dates`.
- For each of K∈[1..7] lines: choose `partkey` by **NURand** over
  `[1, last_part_id]` (hot-part skew → realistic stock-update contention;
  uniform is an acceptable fallback). Then pick one of that part's suppliers by
  scanning PARTSUPP over the `ps_partkey = p` prefix (≤4 rows). Generate the
  line via `lineitem_t::generateRandomRecord(p, s, o_orderdate,
  computeRetailPrice(p))`; fold into `accumulate_for_order` to derive
  `o_totalprice`/`o_orderstatus`.
- Decrement that PARTSUPP row's `ps_availqty` by `l_quantity` (always a real
  row, since `s` came from the part's own supplier set).
- Return `{orders_t, std::vector<lineitem_t>}`.

### 2. Per-structure maintain methods
Single-record insert helpers next to `populate_merged`/`populate_split` in
`col_pipeline.tpp`, plus per-query `maintain_*` bodies (Q3 owns them; Q5
reuses S1/S3/S4 verbatim and adds only its S2 view path):
- **S4 base:** insert `orders_t` + `lineitem_t`.
- **S1 split:** insert `orders_coli_t` + `lineitem_col_t` (reuse
  `populate_split` tagged-key build).
- **S3 merged:** insert the same tagged records into `merged_col`.
- **S2 view:** insert K `q{3,5}_pipeline_view_t` rows (FD cols from the new
  order + a customer lookup for `c_mktsegment`).
- **All structures:** decrement `partsupp_t::ps_availqty` per lineitem
  (`update1` on `Key{l_partkey, l_suppkey}`) — structure-independent, runs once
  per NewOrder regardless of the selected `--storage_structure`.

### 3. Dedicated update binary (this directory: `tpch/new_order/`)
A standalone executable that constructs the vanilla-family workload (same
adapter set as `q3/executable_rocksdb.cpp`), recovers, then loops NewOrder for
a fixed duration at the selected `--storage_structure`, emitting TX/s to CSV.
Does **not** touch `TpchExecutableHelper` (the read-query harness). lsm + btree
variants: `new_order/executable_rocksdb.cpp`, `new_order/executable_leanstore.cpp`.
Wire into `frontend/CMakeLists.txt` + `generate_targets.py`.

### 4. Correctness gate (CLAUDE.md: build → test_load → test_query → perf)
Extend `tests/q3/test_query_q3_*.cpp` (and q5) to run N NewOrders then
re-verify cross-structure XOR parity still holds. Green before any perf run.

## Critical files

- `frontend/geo/maintain.tpp` — pattern to mirror (read-only).
- `frontend/tpch/tpch_family/col_pipeline.{hpp,tpp}` — add insert helpers.
- `frontend/tpch/tpch_family/new_order.hpp` — **new** generator.
- `frontend/tpch/q3/{workload.hpp,load.tpp,query.tpp}` — Q3 `maintain_*`;
  `q3/views.hpp` `q3_pipeline_view_t` for the S2 insert. Same for q5.
- `frontend/tpch/new_order/executable_{rocksdb,leanstore}.cpp` — **new** binary.
- `frontend/CMakeLists.txt`, `generate_targets.py` — wire the new targets.
- `tests/q3/test_query_q3_*.cpp`, `tests/q5/test_query_q5_*.cpp` — parity
  check after inserts.

## Reuse (do not re-implement)

- `orders_t` / `lineitem_t` `generateRandomRecord` static methods.
- `col_pipeline.tpp` tagged-key construction.
- `Adapter::insert` / `MergedAdapter::insert`
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
  `test_query_q3_lsm`/`test_query_q5_lsm` with the post-insert parity check —
  all `[OK]`, identical digest across S1–S4.
- **Linux (perf):** build btree + lsm; `test_load_*` → `test_query_*` green;
  copy image; run the update binary per structure; confirm TX/s; `RUNS.md`.

## Estimate

**Code: ~half a day** — generators and COL insert paths already exist; the
new surface is the NewOrder generator, four short `maintain_*` bodies, and a
small standalone binary. Plus the correctness parity check and a short run
(image copy + timed loops per structure, minutes each — separate from code
time, backgroundable). Comfortably within the 1-week deadline.

## References

- CH-benCHmark (TU München): https://db.in.tum.de/research/projects/CHbenCHmark/
  — source of the `su_suppkey = mod((s_w_id * s_i_id), 10000)` stock→supplier
  derivation; confirms TPC-C transaction set is kept unchanged.
