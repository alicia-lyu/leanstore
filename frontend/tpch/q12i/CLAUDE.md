# Q12I: Shipping Modes × Payment Status (Invoice-Extended Q12)

## TPC-H Definition (Extended)

```sql
SELECT l_shipmode,
       SUM(CASE WHEN o_orderpriority IN ('1-URGENT','2-HIGH')
                THEN 1 ELSE 0 END) AS hi,
       SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT','2-HIGH')
                THEN 1 ELSE 0 END) AS lo,
       SUM(CASE WHEN i_status = 'L'
                THEN 1 ELSE 0 END) AS late_invoiced
FROM   customer, orders, lineitem, invoice
WHERE  c_custkey = o_custkey
  AND  l_orderkey = o_orderkey
  AND  l_invoicekey = i_invoicekey
  AND  l_shipmode IN (':1', ':2')
  AND  l_commitdate < l_receiptdate
  AND  l_shipdate   < l_commitdate
  AND  l_receiptdate >= ':d'
  AND  l_receiptdate <  ':d' + 1 year
GROUP BY l_shipmode;
```

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | REG AIR, AIR, RAIL, SHIP, TRUCK, MAIL, FOB | First ship mode filter |
| `:2` | Same list, must differ from `:1` | Second ship mode filter |
| `:d` | January 1 of a year in \[1993, 1997\] | Start of 1-year receipt date window |

**Validation values**: SHIPMODE1 = MAIL, SHIPMODE2 = SHIP, DATE = 1994-01-01.

---

## Motivation

Q12I is the **COLI MI showcase** for the §3.1.2 sibling + §3.1.3 hierarchical
hybrid pattern:

- **Why not plain Q12?** Q12 joins only ORDERS × LINEITEM and therefore needs
  only a 2-table OL merged index. Adding `late_invoiced` requires a join with
  INVOICE — a table that is neither a parent nor a child of LINEITEM in the OL
  hierarchy. INVOICE is a §3.1.2 *sibling* of ORDERS (both are keyed under
  `custkey`) while LINEITEM is a §3.1.3 *child* of ORDERS.

- **Why the COLI MI?** The 4-table merged index
  `MI(customer, orders, lineitem, invoice)` keyed by custkey interleaves all
  four record types in a single B-tree scan. One `PremergedJoin` pass can
  simultaneously compute `hi`, `lo` (from the orders × lineitem join) and
  `late_invoiced` (from the invoice sibling). No separate invoice lookup is
  needed.

- **Comparison story**: the four storage structures let us measure the overhead
  of the sibling join against a hash-join baseline (S4) and against the
  materialized view shortcut (S2). S3 (MI\[COLI\]) is expected to have the
  strongest I/O advantage under memory pressure because the index is compact and
  co-located.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + merge join | None | BinaryMergeJoin(OL) + invoice point-lookup |
| 2 | Intermediate pipeline view | `q12i_pipeline_view_t` (joined\_ol\_t rows) | View scan + invoice point-lookup |
| 3 | MI\[COLI\] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | PremergedJoin over 4-table tagged-key MI |
| 4 | Traditional indexes + hash join | None | HashJoin(OL) + invoice point-lookup |

---

## Calcite Column Index Mapping

TODO: fill in once Calcite plan files for Q12I exist. The concatenated
CUSTOMER ∥ ORDERS ∥ LINEITEM ∥ INVOICE schema will be indexed and the mapping
from Calcite's `$N` references to C++ struct fields will be documented here.

---

## Required Record Types

- `q12i_pipeline_view_t` — currently aliased to `joined_ol_t`; widen to include
  invoice fields once the COLI MI driver is implemented.
- `q12i_agg_row_t` — final aggregate output row: `l_shipmode`, `hi`, `lo`,
  `late_invoiced`.

---

## File Structure

| File | Status |
|------|--------|
| `views.hpp` | Skeleton — `q12i_pipeline_view_t` alias and `q12i_agg_row_t` declared |
| `workload.hpp` | Skeleton — `Q12IWorkload<Backend>` declared, COLI pipeline member |
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
add_executable(q12i_lsm tpch/q12i/executable_rocksdb.cpp)
# ...

# Linux section:
add_executable(q12i_btree tpch/q12i/executable_leanstore.cpp)
add_executable(q12i_lsm   tpch/q12i/executable_rocksdb.cpp)
```

---

## Implementation Status (skeleton)

Stubbed (present, syntax-clean, but no logic):

- `Q12IWorkload` ctor initialises member references; `params = Params::defaults()`
- `load()` calls `tpch.load()` only — secondary structure population is TODO
- `get_size()` returns `0.0` — dispatch on `FLAGS_storage_structure` is TODO
- `populate_q12i_view` — declared, body empty
- `Params::defaults()` returns `Params{}` — real values are TODO
- `q12i_agg_row_t::print()` writes a TODO marker
- All predicate bodies return `false`
- All `query_by_*` bodies call `out.clear(); return 0`

Not yet present (out of scope for skeleton):

- View row type wider than `joined_ol_t`
- `query_by_*` operator logic
- Predicate implementations
- XOR parity test
- CMake targets

---

## Out of Scope (Skeleton)

- `query_by_*` body implementations
- `populate_q12i_view` body
- Substitution parameter values in `Params::defaults()`
- Calcite plan generation / column-index mapping
- Performance measurements
- CMake target additions
- Tests (`test_load_q12i_lsm`, `test_query_q12i_lsm`)
