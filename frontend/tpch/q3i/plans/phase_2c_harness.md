# Q3I Phase 2C — Unified harness + finalisation

Lands the cross-structure parity check, retires the temporary
Phase 1 test binary, and propagates the Phase 2 design upward to
shared TPC-H docs.

## Prerequisite

`phase_2a_refactor.md` and `phase_2b_baselines.md` complete. All
four `query_by_*` bodies emit deterministic top-10 ordered by
revenue DESC.

## Step 1 — Unified `test_query_q3i_lsm` harness

```
File: frontend/tpch/tests/q3i/test_query_q3i_rocksdb.cpp
Target: test_query_q3i_lsm  (CMake)
```

Behaviour (mirrors `test_query_q12_lsm`):

```
for s in {1, 2, 3, 4}:
    rm -rf <ssd_path>            # always wipe — defeats stale-load
                                 # trap that bit Phase 1 (see commit
                                 # eb357510 / d8980426)
    FLAGS_storage_structure = s
    q3i.load()
    out_s.clear()
    n_s = q3i.query_by_<...>(out_s)
    sort out_s deterministically (orderkey ASC) for digest stability
    digest_s = xor_digest(out_s)
    print "[{s}] rows={n_s} digest={digest_s:#x}"

assert n_1 == n_2 == n_3 == n_4
assert digest_1 == digest_2 == digest_3 == digest_4
print top-10 of out_3 in revenue-DESC order
exit non-zero on any parity failure
```

Retire `test_query_q3i_phase1_lsm`:

- Remove the CMake target.
- Move `frontend/tpch/tests/q3i/test_query_q3i_phase1_rocksdb.cpp`
  to `TRASH/` per project rules; log the move in `TRASH-FILES.md`.

**Commit**: "Q3I Phase 2C step 1: unified test_query_q3i_lsm
harness; retire Phase 1 binary".

**Verification**:
```
cd build && make test_query_q3i_lsm -j$(sysctl -n hw.ncpu)
rm -rf ../test_data_q3i ../test_csv_q3i
./frontend/test_query_q3i_lsm \
    --ssd_path=../test_data_q3i \
    --csv_path=../test_csv_q3i \
    --tpch_scale_factor=1
```
Expected: 4 identical digests, exit 0, top-10 rows printed.

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
