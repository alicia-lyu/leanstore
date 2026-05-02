# Q3I Phase 2C — Unified harness + finalisation

Lands the cross-structure parity check, retires the temporary
Phase 1 test binary, and propagates the Phase 2 design upward to
shared TPC-H docs.

## Prerequisite

`phase_2a_refactor.md` and `phase_2b_baselines.md` complete. All
four `query_by_*` bodies emit deterministic top-10 ordered by
revenue DESC.

## Step 1 — Switch to single-load harness, then make parity green

The harness landed in Phase 2B (`tests/q3i/test_query_q3i_rocksdb.cpp`)
loads each storage structure into a fresh DB inside the same process.
**This pattern is broken**: TPC-H data generation is not deterministic
across reloads in one process (the global RNG state advances during
each `load()` call), so the four runs see different datasets and the
XOR digest cannot match by construction. Phase 2B verification at
SF=1 confirmed this: S2 reported `rewrote 6025 lineitems` while S4
reported `rewrote 5942 lineitems` in the same harness invocation.

The Q12 harness (`tests/q12/test_query_q12_rocksdb.cpp`) sidesteps the
issue by **loading once** and populating every secondary up front, then
dispatching all four `query_by_*` against the same in-memory state.
Q3I must do the same. **No further phase work proceeds until parity is
green**.

### 1a — Restructure harness to load-once + run-all-four

```
File: frontend/tpch/tests/q3i/test_query_q3i_rocksdb.cpp
Target: test_query_q3i_lsm
```

Mirror the Q12 main() shape:

```
main():
  declare all 8 base TPC-H adapters + invoice adapter
  declare pipeline_view adapter (q3i_pipeline_view_t)
  declare merged COLI adapter
  declare three split COLI adapters (orders/lineitem/invoice)

  rocks_db.open()
  TPCHWorkload tpch(...); tpch.load()

  Q3IWorkload q3i(tpch, customer, orders, lineitem, invoice,
                  pipeline_view, coli_pipeline);

  // Build EVERY secondary up front, regardless of FLAGS_storage_structure.
  populate_q3i_view(pipeline_view, ...)          // for S2
  coli.populate_split()                          // for S1
  coli.populate_merged()                         // for S3
  // S4 needs no extras

  // Run all four paths against the same loaded DB.
  std::vector<q3i_agg_row_t> r_base, r_view, r_merged, r_hash;
  q3i.query_by_base  (r_base);
  q3i.query_by_view  (r_view);
  q3i.query_by_merged(r_merged);
  q3i.query_by_hash  (r_hash);

  // Sort each result deterministically (orderkey ASC) for digest stability.
  // top-10 epilogue inside each query_by_* already returns ≤10 rows.
  for v in {r_base, r_view, r_merged, r_hash}:
      sort v by o_orderkey

  digest_b = xor_digest(r_base)   ...etc
  print "[S{n}] rows={size} digest={digest:#x}"
  print top-10 of r_merged in revenue-DESC order

  assert all four sizes equal, all four digests equal
  exit non-zero on any parity failure
```

Notes / gotchas:

- The Phase 2B harness uses `FLAGS_storage_structure` to gate which
  secondary is populated inside `Q3IWorkload::load()`. The new harness
  must NOT route through that switch — call `populate_q3i_view`,
  `coli.populate_split()`, and `coli.populate_merged()` directly so all
  three are built. (`load()` is fine for non-test executables; tests
  bypass it.)
- The `Q3IWorkload` ctor must already accept all the adapters it needs;
  if Phase 2B narrowed the ctor signature, widen it back to take the
  pipeline_view + full COLI pipeline (which exposes `merged_adapter()`,
  `split_orders()`, `split_lineitem()`, `split_invoice()`).
- Each `query_by_*` is responsible for its own top-10 + ordering by
  `revenue DESC`; the harness re-sorts by `o_orderkey` ASC purely so
  the XOR digest is stable across rows that tie on revenue.
- Drop the per-structure `wipe ssd_path` / `tpch.load()` loop entirely.

### 1b — Diagnose and fix any real query-body bugs that surface

Once data is shared, the digests should converge. If they don't, the
divergence reflects real query-body bugs — not data drift. Phase 2B
already showed at least one suspicious symptom that survives the
single-load fix: S4 returned 9 rows at SF=1 (vs S3's 10) with revenue
values an order of magnitude smaller than S3, suggesting either a
join-projection bug, a missed lineitem, or the post-aggregate hashmap
key being wrong.

For each path that fails parity:

1. Re-run with the same load and dump per-row diffs against the S3
   oracle (S3 is the trusted reference — its body is unchanged from
   Phase 1).
2. Trace divergence to a single operator: pre-aggregate cardinality,
   join cardinality, post-aggregate row, threshold/mktsegment gating,
   or top-10 selection.
3. Fix in `query.tpp` (or `populate_q3i_view` for S2). Do NOT modify
   S3 — it is the oracle.
4. Repeat until all four digests match.

Likely suspects to check first:

- **S4**: post-join hash-aggregate keying. Q3I's GROUP BY is
  `(o_orderkey, o_orderdate, o_shippriority, cust_open_due)`; if the
  current code keys on just `o_orderkey` it may be dropping the row
  for an orderkey that re-appears with a different cust_open_due (it
  shouldn't — orderkey determines custkey — but verify).
- **S4**: lineitem probe path may be filtering out rows that the
  oracle keeps (e.g. shipdate condition applied at the wrong layer
  of the chain).
- **S2**: `populate_q3i_view` two-pointer merge over OL. Q12 hit
  exactly this trap and switched away from `BinaryMergeJoin` (see
  Q12 §"manual two-pointer merge" note in `frontend/tpch/CLAUDE.md`).
  Verify the view emits one row per `(custkey, orderkey)` post-aggregate
  and not per `(order, lineitem)` pair.
- **S1**: 3-BMJ chain join order vs filter pushdown — confirm the
  threshold filter on `cust_open_due` fires before the OL join, not
  after, so excluded custkeys never enter the chain.

### 1c — Retire `test_query_q3i_phase1_lsm`

Only after parity is green:

- Remove the CMake target.
- Move `frontend/tpch/tests/q3i/test_query_q3i_phase1_rocksdb.cpp`
  to `TRASH/` per project rules; log the move in `TRASH-FILES.md`.

**Commits** (split for reviewability):

- "Q3I Phase 2C step 1a: load-once harness for test_query_q3i_lsm"
- One commit per real query-body fix from 1b ("Q3I Phase 2C: fix
  S{n} {symptom}").
- "Q3I Phase 2C step 1c: retire test_query_q3i_phase1_lsm"

**Verification (gate)**:

```
cd build && make test_query_q3i_lsm -j$(sysctl -n hw.ncpu)
mkdir -p ../test_data_q3i ../test_csv_q3i
./frontend/test_query_q3i_lsm \
    --ssd_path=../test_data_q3i \
    --csv_path=../test_csv_q3i \
    --tpch_scale_factor=1
```
Expected: 4 identical digests, 4 identical row counts, exit 0,
top-10 rows printed.

**Step 2 (doc updates) MUST NOT start until this gate passes.**

## Step 2 — Doc updates

### `frontend/tpch/q3i/CLAUDE.md`

- Flip Phase 2 status to "complete" with a brief landed-summary
  (mirrors Phase 1's "Status (2026-05-01): complete" block).
- Add a §"Implementation Notes" subsection documenting:
  - The widened `q3i_pipeline_view_t` schema and its sort key
  - The two scanner-wrapper aggregators
    (`CustomerOpenDueAggregator`, `LineitemRevenueAggregator`)
  - The S1 3-BMJ chain and S4 3-HJ chain isomorphism
  - The `secondary` → `split` rename
  - The `apply_topN` epilogue uniform across paths
- Update the §"Storage Structure Options" table if the row 1 / 4
  descriptions need refreshing (S1 = "merge family via custkey-sorted
  split COLI indexes", S4 = "no-MI baseline via 3-HJ chain").

### `frontend/tpch/OPERATORS.md`

Two additions:

1. **New top-level §Filter Pushdown section** (placed between
   §Per-Operator Strategy and §Worked Examples). Codifies the
   principle that already governs Q12 §4 predicate hoisting and
   the three Q3I plans:

   > Every parameterised filter pushes as far down the operator
   > graph as possible, stopping ONLY at secondary structures (so
   > MIs / split indexes / pipeline views stay reusable across
   > param sets). Concretely:
   >
   > - Single-table filters fuse with TableScan at query time.
   > - Aggregate-output filters (e.g. Q3I's threshold on
   >   `cust_open_due`) fuse with the SortedAggregate; never a
   >   post-aggregate Filter node.
   > - For merged-index physical execution (single-scanner
   >   walkers), every filter applies inside the Visitor's `on_*`
   >   hook for that record type — there is no post-walk Filter
   >   node.
   > - Secondary structures (MIs, split indexes, pipeline views)
   >   are loaded UNFILTERED on parameterised predicates.

   Cross-link from `q3i/CLAUDE.md §Plan Descriptions` and (when
   written) `q12/CLAUDE.md`.

2. **Q3I addendum**: invoice-extended queries (Q3I, Q5I, Q10I)
   shift the family logical plan to **custkey-sorted**, replacing
   the orderkey-sorted Q3 pattern. The threshold filter on
   `cust_open_due` fuses with the per-customer aggregate (not
   "outside-pipeline" as §3 op 7 implies for vanilla Q3). Reference
   `q3i/plans/family_logical.dot` and the scanner-wrapper +
   3-BMJ-chain pattern as the operator-level expression of the
   §3.1.2 sibling + §3.1.3 hierarchical hybrid.

### `frontend/tpch/CLAUDE.md`

- §Tests: add row for `test_query_q3i_lsm` (replace Phase 1 row).
- §Layout: confirm q3i/ entries reflect Phase 2 state.
- Confirm §"Known Design Limitations" still accurately describes
  the project-pushdown deferral.

**Commit**: "Q3I Phase 2C step 2: doc updates (Q3I CLAUDE.md
status, OPERATORS.md filter-pushdown + Q3I addendum, tpch tests
index)".

**Verification**: `make q12_lsm test_query_q12_lsm test_load_coli_lsm
test_query_q3i_lsm` all green.

## DRY follow-ups (out of scope here, deferred)

Identified during planning but deferred until vanilla Q3's bodies
land:

- Lift `LineitemRevenueAccumulator` (the per-orderkey SUM) to a
  shared TPC-H header so vanilla Q3 reuses it.
- Lift textually-identical predicates (`l_shipdate > DATE`,
  `o_orderdate < DATE`, `c_mktsegment = SEGMENT`) when both
  consumers exist.

Don't pre-share to a hypothetical Q3 — design after both consumers
are real.

## Out of scope

- Production `q3i_lsm` / `q3i_btree` targets (Phase 3).
- Q5I / Q10I bodies (separate phase plans, will reuse this template).
- Memory-pressure / scale-factor sweeps.
