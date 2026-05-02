# Q9I: Profit by Nation/Year × Invoice Currency Mix (Invoice-Extended Q9)

## TPC-H Definition (Extended)

```sql
SELECT  nation, o_year,
        SUM(amount)                                          AS profit,
        SUM(CASE WHEN i_status = 'P' THEN amount ELSE 0 END) AS paid_profit
FROM (
  SELECT n_name                                             AS nation,
         EXTRACT(YEAR FROM o_orderdate)                    AS o_year,
         l_extendedprice * (1 - l_discount)
           - ps_supplycost * l_quantity                    AS amount,
         i_status
  FROM   part, supplier, lineitem, partsupp, orders, nation, invoice
  WHERE  s_suppkey    = l_suppkey
    AND  ps_suppkey   = l_suppkey
    AND  ps_partkey   = l_partkey
    AND  p_partkey    = l_partkey
    AND  o_orderkey   = l_orderkey
    AND  s_nationkey  = n_nationkey
    AND  p_name LIKE '%:1%'
    AND  l_invoicekey = i_invoicekey
) AS profit_with_status
GROUP BY nation, o_year
ORDER BY nation ASC, o_year DESC;
```

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | Any colour substring present in `p_name` (e.g. green, red, blue) | LIKE filter on part name |

**Validation value**: COLOR = green.

---

## Motivation

Q9I is the **COLI MI showcase** for the §3.1.2 sibling pattern layered on top
of Q9's deep 6-table join:

- **Why not plain Q9?** Q9 aggregates profit by (nation, year) but does not
  distinguish paid vs. outstanding revenue. Adding `paid_profit` requires a join
  with INVOICE — a §3.1.2 sibling of ORDERS within the COLI custkey hierarchy.

- **Why the COLI MI?** For S3, the COLI scan over `MI(customer, orders,
  lineitem, invoice)` delivers orders, lineitems, and invoices co-located by
  custkey. The `i_status='P'` aggregate accumulates in the same scan without a
  separate invoice pass. Nation/supplier/part/partsupp are small dimension tables
  that fit in memory as hashmaps, so they do not need to be in the MI.

- **Comparison story**: S3 eliminates the separate invoice scan needed in
  S1/S4. At high scale factors the co-location benefit is expected to widen
  because INVOICE rows are adjacent to their parent LINEITEM rows in the MI.

---

## Storage Structure Options

| # | Strategy | Secondary structure | Join strategy |
|---|----------|--------------------|-|
| 1 | Traditional indexes + merge join | None | Pre-build nation/supplier/part/partsupp/invoice hashmaps; BinaryMergeJoin(OL) |
| 2 | Intermediate pipeline view | `q9i_pipeline_view_t` (joined\_ol\_t rows) | Pre-build hashmaps; view scan + per-row lookups |
| 3 | MI\[COLI\] only | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | PremergedJoin over 4-table tagged-key MI; invoice in same pass |
| 4 | Traditional indexes + hash join | None | Pre-build nation/supplier/part/partsupp/invoice hashmaps; HashJoin(OL) |

---

## Calcite Column Index Mapping

TODO: fill in once Calcite plan files for Q9I exist. The concatenated
PART ∥ SUPPLIER ∥ LINEITEM ∥ PARTSUPP ∥ ORDERS ∥ NATION ∥ INVOICE schema will
be indexed and the mapping from Calcite's `$N` references to C++ struct fields
will be documented here.

---

## Required Record Types

- `q9i_pipeline_view_t` — currently aliased to `joined_ol_t`; widen to include
  nation name, `ps_supplycost`, and `i_status` fields once the COLI MI driver
  is implemented.
- `q9i_agg_row_t` — final output row: `nation`, `o_year`, `profit`,
  `paid_profit`.

---

## File Structure

| File | Status |
|------|--------|
| `views.hpp` | Skeleton — `q9i_pipeline_view_t` alias and `q9i_agg_row_t` declared |
| `workload.hpp` | Skeleton — `Q9IWorkload<Backend>` declared, COLI pipeline member |
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
add_executable(q9i_lsm tpch/q9i/executable_rocksdb.cpp)
# ...

# Linux section:
add_executable(q9i_btree tpch/q9i/executable_leanstore.cpp)
add_executable(q9i_lsm   tpch/q9i/executable_rocksdb.cpp)
```

---

## Implementation Status (skeleton)

Stubbed (present, syntax-clean, but no logic):

- `Q9IWorkload` ctor initialises member references; `params = Params::defaults()`
- `load()` calls `tpch.load()` only — secondary structure population is TODO
- `get_size()` returns `0.0` — dispatch on `FLAGS_storage_structure` is TODO
- `populate_q9i_view` — declared, body empty
- `Params::defaults()` returns `Params{}` — real values are TODO
- `q9i_agg_row_t::print()` writes a TODO marker
- All predicate bodies return `false`
- All `query_by_*` bodies call `out.clear(); return 0`

Not yet present (out of scope for skeleton):

- View row type wider than `joined_ol_t` (with nation, ps\_supplycost, i\_status)
- `query_by_*` operator logic
- Predicate implementations (LIKE for `q9i_predicate_part`)
- In-memory hashmaps for nation/supplier/part/partsupp
- XOR parity test
- CMake targets

---

## Out of Scope (Skeleton)

- `query_by_*` body implementations
- `populate_q9i_view` body
- Substitution parameter values in `Params::defaults()`
- Calcite plan generation / column-index mapping
- Performance measurements
- CMake target additions
- Tests (`test_load_q9i_lsm`, `test_query_q9i_lsm`)
