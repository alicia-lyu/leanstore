# Q3I: Shipping Priority × Customer Outstanding Balance (Invoice-Extended Q3)

## TPC-H Definition (Extended)

```sql
SELECT  o_orderkey,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        o_orderdate,
        o_shippriority,
        cust_open_due
FROM    customer, orders, lineitem,
        (SELECT i_custkey, SUM(i_totaldue) AS cust_open_due
           FROM invoice
          WHERE i_status = 'O'
          GROUP BY i_custkey) AS oi
WHERE   c_mktsegment = ':1'
  AND   c_custkey    = o_custkey
  AND   c_custkey    = oi.i_custkey
  AND   l_orderkey   = o_orderkey
  AND   o_orderdate  < ':d'
  AND   l_shipdate   > ':d'
  AND   oi.cust_open_due > :threshold
GROUP BY o_orderkey, o_orderdate, o_shippriority, cust_open_due
ORDER BY revenue DESC
LIMIT 10;
```

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | Market segment names (e.g. BUILDING, AUTOMOBILE, MACHINERY) | Customer market segment filter |
| `:d` | March 15 of a year in \[1993, 1997\] | Order date upper bound / ship date lower bound |
| `:threshold` | Non-negative numeric | Minimum per-customer open invoice total to include |

**Validation values**: SEGMENT = BUILDING, DATE = 1995-03-15, THRESHOLD = 0.

---

## Motivation

Q3I is the **COLI MI showcase** for combining §3.1.3 hierarchical join with a
§3.1.2 sibling sub-aggregate:

- **Why not plain Q3?** Q3 joins CUSTOMER × ORDERS × LINEITEM and returns the
  top-10 unshipped orders by revenue. Adding `cust_open_due` requires grouping
  INVOICE by `i_custkey` before the main join — a correlated sub-aggregate that
  is expensive to compute separately for each custkey in S1/S4.

- **Why the COLI MI?** The 4-table merged index `MI(customer, orders, lineitem,
  invoice)` keyed by `custkey` co-locates all four record types per customer.
  One `PremergedJoin` pass computes `cust_open_due` while scanning invoices
  (§3.1.2 sibling within the custkey group) and accumulates revenue from the
  orders × lineitem sub-hierarchy (§3.1.3). The threshold filter can be applied
  early to skip custkey groups entirely.

- **Comparison story**: S3 (MI\[COLI\]) amortises the per-customer invoice scan
  across the hierarchical join, whereas S1/S4 must build a separate invoice
  hashmap before the main join. At high scale factors the COLI scan's locality
  should dominate.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + merge join | None | Pre-build cust\_open\_due map from INVOICE; BinaryMergeJoin(OL) + CUSTOMER hash lookup |
| 2 | Intermediate pipeline view | `q3i_pipeline_view_t` (joined\_ol\_t rows) | Pre-build cust\_open\_due map; view scan + CUSTOMER hash filter |
| 3 | MI\[COLI\] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | PremergedJoin over 4-table tagged-key MI; cust\_open\_due computed in same pass |
| 4 | Traditional indexes + hash join | None | Pre-build cust\_open\_due map; HashJoin(OL) + CUSTOMER hash lookup |

---

## Calcite Column Index Mapping

TODO: fill in once Calcite plan files for Q3I exist. The concatenated
CUSTOMER ∥ ORDERS ∥ LINEITEM ∥ INVOICE schema will be indexed and the mapping
from Calcite's `$N` references to C++ struct fields will be documented here.

---

## Required Record Types

- `q3i_pipeline_view_t` — currently aliased to `joined_ol_t`; widen to include
  `cust_open_due` aggregate field once the COLI MI driver is implemented.
- `q3i_agg_row_t` — final output row: `o_orderkey`, `revenue`, `o_orderdate`,
  `o_shippriority`, `cust_open_due`.

---

## File Structure

| File | Status |
|------|--------|
| `views.hpp` | Skeleton — `q3i_pipeline_view_t` alias and `q3i_agg_row_t` declared |
| `workload.hpp` | Skeleton — `Q3IWorkload<Backend>` declared, COLI pipeline member |
| `per_structure_workload.hpp` | Skeleton — alias-only |
| `load.tpp` | Skeleton — ctor wires refs; `load()` / `get_size()` bodies are TODO stubs |
| `query.tpp` | Skeleton — all `query_by_*`, predicates, `print()` are TODO |
| `executable_rocksdb.cpp` | Skeleton — `main()` returns 0 |
| `executable_leanstore.cpp` | Skeleton — `main()` returns 0 (`#ifndef ROCKSDB_ONLY`) |
| `CLAUDE.md` | This file |

---

## CMake Targets

TODO: add to `frontend/CMakeLists.txt` when body implementations land.

```cmake
# macOS (ROCKSDB_ONLY) section:
add_executable(q3i_lsm tpch/q3i/executable_rocksdb.cpp)
# ...

# Linux section:
add_executable(q3i_btree tpch/q3i/executable_leanstore.cpp)
add_executable(q3i_lsm   tpch/q3i/executable_rocksdb.cpp)
```

---

## Implementation Phases

### Phase 1 — Minimal end-to-end: merged path only (S3)

**Goal**: one runnable path that exercises the COLI MI showcase end-to-end.

**Deliverables**:

- `load.tpp` — `load()` for structures 1/3: `tpch.load()` + `coli.populate_merged()` for S3; `get_size()` dispatch on `FLAGS_storage_structure`.
- `query.tpp` — `query_by_merged` only: `PremergedJoin` over the 4-table COLI MI; `cust_open_due` accumulated in the custkey group while scanning `invoice_coli_t` siblings; threshold filter to skip entire custkey groups early.
- `query.tpp` — `Params::defaults()` with real values: `mktsegment = "BUILDING"`, `date = 1995-03-15` (encoded as days since epoch), `threshold = 0`.
- `query.tpp` — `q3i_agg_row_t::print()` — formatted output of `(o_orderkey, revenue, o_orderdate, o_shippriority, cust_open_due)`.
- `query.tpp` — predicates sufficient for `query_by_merged`: `q3i_predicate_orders` (orderdate < date), `q3i_predicate_lineitem` (shipdate > date), `q3i_predicate_customer` (mktsegment match).

**Not yet in Phase 1**: top-10 sort; baseline S1/S2/S4 paths.

**Exit criterion**: `test_query_q3i_lsm` builds, loads S3, calls `query_by_merged`, and prints qualifying order rows without crashing. Row count non-zero at SF=1.

---

### Phase 2 — Baseline structures (S1, S2, S4)

**Goal**: all four `query_by_*` paths produce identical aggregate results (no top-10 yet), verified by XOR parity.

**Deliverables**:

- `load.tpp` — `load()` and `get_size()` extended for S1 (base only), S2 (`populate_q3i_view`), S4 (base only).
- `views.hpp` — `q3i_pipeline_view_t` widened beyond the `joined_ol_t` alias to include a `cust_open_due` aggregate field, enabling S2 to pre-materialise the per-customer invoice sum.
- `query.tpp` — `query_by_base` (S1): pre-build `cust_open_due` hashmap from INVOICE; `BinaryMergeJoin(OL)` + CUSTOMER hash lookup; threshold filter on hashmap entry.
- `query.tpp` — `query_by_view` (S2): pre-build `cust_open_due` hashmap; view scan + CUSTOMER hash filter.
- `query.tpp` — `query_by_hash` (S4): pre-build `cust_open_due` hashmap; `HashJoin(OL)` + CUSTOMER hash lookup.
- `populate_q3i_view` body: two-pointer merge over OL scanners, emit one `q3i_pipeline_view_t` row per lineitem with `cust_open_due` field populated from the pre-built hashmap.

**Exit criterion**: XOR parity check across all four paths passes at SF=1. Identical digest for S1/S2/S3/S4.

---

### Phase 3 — Top-10 selection + experiment harness

**Goal**: spec-compliant top-10 and runnable `q3i_lsm` / `q3i_btree` executables.

**Deliverables**:

- `query.tpp` — top-10 `ORDER BY revenue DESC` across all four paths via priority queue or `partial_sort`.
- `frontend/CMakeLists.txt` — add `q3i_lsm` (macOS + Linux) and `q3i_btree` (Linux only) targets following the `geo_lsm` / `geo_btree` pattern.
- `tests/` — `test_load_q3i_lsm` (load S3 and report COLI distribution stats) and `test_query_q3i_lsm` (all four paths + parity + shape check) binaries.
- `frontend/tpch/CLAUDE.md §Tests` index updated with new rows.
- `generate_targets.py` entries for `q3i_lsm` / `q3i_btree` Makefile targets.

**Exit criterion**: `test_query_q3i_lsm` at SF=1 reports top-10 rows, identical parity across all four paths, and non-degenerate digest.

---

Q5I and Q10I will follow the same 3-phase pattern once Q3I Phase 3 lands.
