# Q3: Shipping Priority Query

## TPC-H Definition (Section 2.4.3)

The Shipping Priority Query retrieves the 10 unshipped orders with the highest revenue, for orders placed by customers in a given market segment before a given date. Revenue is `SUM(l_extendedprice * (1 - l_discount))`.

## SQL

```sql
SELECT l_orderkey,
       SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate,
       o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = '[SEGMENT]'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '[DATE]'
  AND l_shipdate > DATE '[DATE]'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

## Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| SEGMENT | AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY | Market segment filter on CUSTOMER |
| DATE | Day in [1995-03-01, 1995-03-31] | Cutoff for both o_orderdate and l_shipdate |

**Validation values**: SEGMENT = BUILDING, DATE = 1995-03-15.

**Approved query variants**: None in Appendix B.

**Selectivity notes**: Each segment covers ~20% of customers. The DATE parameter controls two filters simultaneously: `o_orderdate < DATE` (orders placed before) and `l_shipdate > DATE` (items not yet shipped). For the validation date (1995-03-15), roughly half of orders and lineitems pass their respective filter.

## Tables and Join Graph

```
CUSTOMER --[c_custkey = o_custkey]--> ORDERS --[o_orderkey = l_orderkey]--> LINEITEM
```

3 tables, 2 joins. The ORDERS-LINEITEM join is the heavy one (LINEITEM is the largest TPC-H table).

## Merged Index

### Structure 3: MI(ORDERS, LINEITEM) by orderkey

- ORDERS key: `(o_orderkey)` — 1 record per order
- LINEITEM key: `(l_orderkey, l_linenumber)` — N records per order
- Natural prefix hierarchy: LINEITEM extends the ORDERS key

The MI covers the ORDERS-LINEITEM join. CUSTOMER is joined at query time via merge join on `o_custkey = c_custkey`.

### Alternative: MI(CUSTOMER, ORDERS, LINEITEM) by custkey

An alternative merged index interleaves all three tables by `custkey`:

- CUSTOMER key: `(c_custkey)` — 1 record per customer
- ORDERS key: `(o_custkey, o_orderkey)` — extend ORDERS with custkey (functionally dependent on orderkey)
- LINEITEM key: `(l_custkey, l_orderkey, l_linenumber)` — extend LINEITEM with custkey (same FD)

Since `o_custkey` is functionally determined by `o_orderkey`, each ORDERS and LINEITEM record can be augmented with the custkey at load time. This creates a 3-level prefix hierarchy:

```
custkey -> CUSTOMER record
custkey, orderkey -> ORDERS record
custkey, orderkey, linenumber -> LINEITEM record
```

A single PremergedJoin scan over this MI produces the full 3-way join result without a separate CUSTOMER merge join. The trade-off is a larger MI (3 tables vs. 2) and wider keys (custkey prepended to every ORDERS and LINEITEM record).

This variant is documented for consideration but has no DOT plan files. If implemented, it would replace both the PremergedJoin and the CUSTOMER merge join in structure 3's plan.

## Column Index Mappings

### Standalone Table Scans (for structure 1 filters)

**LINEITEM** (16 cols): l_orderkey=$0, l_linenumber=$1, l_partkey=$2, l_suppkey=$3, l_quantity=$4, l_extendedprice=$5, l_discount=$6, l_tax=$7, l_returnflag=$8, l_linestatus=$9, l_shipdate=$10, l_commitdate=$11, l_receiptdate=$12, l_shipinstruct=$13, l_shipmode=$14, l_comment=$15

**ORDERS** (9 cols): o_orderkey=$0, o_custkey=$1, o_orderstatus=$2, o_totalprice=$3, o_orderdate=$4, o_orderpriority=$5, o_clerk=$6, o_shippriority=$7, o_comment=$8

**CUSTOMER** (8 cols): c_custkey=$0, c_name=$1, c_address=$2, c_nationkey=$3, c_phone=$4, c_acctbal=$5, c_mktsegment=$6, c_comment=$7

### Concatenated ORDERS||LINEITEM (for structure 3 PremergedJoin output, 25 cols)

ORDERS $0-$8, then LINEITEM $9-$24. Key columns:

- o_orderkey=$0, o_custkey=$1, o_orderdate=$4, o_shippriority=$7
- l_extendedprice=$14, l_discount=$15, l_shipdate=$19

### Concatenated CUSTOMER||ORDERS||LINEITEM (for structure 2_4 post-hash-join, 33 cols)

CUSTOMER $0-$7, ORDERS $8-$16, LINEITEM $17-$32. Key columns:

- c_mktsegment=$6, o_orderkey=$8, o_orderdate=$12, o_shippriority=$15
- l_orderkey=$17, l_extendedprice=$22, l_discount=$23, l_shipdate=$27

## Plan Descriptions

> **Implementation note**: While the logical plans below show MergeJoin for the
> CUSTOMER join, the implementation uses **HashJoin** for all outside-pipeline
> joins. See `OPERATORS.md §7` for justification. The plans are preserved as-is
> to match Calcite's output; the physical deviation is documented there.

### Structure 1: Traditional Indexes + Merge Join

Filters pushed down to immediately after table scans (matching the paper's plan shape):

```
LINEITEM scan -> Filter(l_shipdate > DATE) -> Project(l_orderkey, revenue)
  -> Sort(l_orderkey) -> SortedAggregate(group by l_orderkey, SUM revenue)
  -> MergeJoin(l_orderkey = o_orderkey) with:
ORDERS scan -> Filter(o_orderdate < DATE) -> Sort(o_orderkey)
  -> Sort(o_custkey) -> MergeJoin(o_custkey = c_custkey) with:
CUSTOMER scan -> Filter(c_mktsegment = SEGMENT) -> Sort(c_custkey)
  -> Project(l_orderkey, revenue, o_orderdate, o_shippriority)
  -> Sort(revenue DESC, o_orderdate ASC) -> Limit 10
```

The LINEITEM aggregate before the ORDERS join is valid because `o_orderdate` and `o_shippriority` are functionally dependent on `l_orderkey` (each order has one orderdate and one shippriority). Aggregating first reduces the number of rows entering the merge join.

### Structure 2 & 4: Hash Join (Default Calcite Plan)

All three tables hash-joined, then filtered, projected, aggregated:

```
CUSTOMER HashJoin ORDERS on c_custkey=o_custkey
  -> HashJoin LINEITEM on o_orderkey=l_orderkey
  -> Filter(c_mktsegment=SEGMENT AND o_orderdate < DATE AND l_shipdate > DATE)
  -> Project(l_orderkey, revenue, o_orderdate, o_shippriority)
  -> Aggregate(group by l_orderkey, o_orderdate, o_shippriority; SUM revenue)
  -> Sort(revenue DESC, o_orderdate ASC) -> Limit 10
```

Structure 2 materializes the full output; structure 4 executes at query time with clustered indexes.

### Structure 3: Merged Index + PremergedJoin

PremergedJoin replaces ORDERS+LINEITEM scans and their merge join. Filters applied after PremergedJoin using concatenated ORDERS||LINEITEM indices:

```
PremergedJoin(MI_ORDERS_LINEITEM)
  -> Filter(o_orderdate < DATE [$4] AND l_shipdate > DATE [$19])
  -> Project(l_orderkey=$0, revenue=*($14,-(1,$15)), o_custkey=$1, o_orderdate=$4, o_shippriority=$7)
  -> SortedAggregate(group by l_orderkey, o_custkey, o_orderdate, o_shippriority; SUM revenue)
  -> Sort(o_custkey) -> MergeJoin(o_custkey = c_custkey) with:
CUSTOMER scan -> Filter(c_mktsegment = SEGMENT) -> Sort(c_custkey)
  -> Project(l_orderkey, revenue, o_orderdate, o_shippriority)
  -> Sort(revenue DESC, o_orderdate ASC) -> Limit 10
```

The aggregate preserves o_custkey, o_orderdate, o_shippriority as group-by columns (functionally dependent on l_orderkey) so they survive for the CUSTOMER join and final projection.

## Execution Style: Monolithic vs Cascade

### Record Types Required

The merge-join infrastructure (`frontend/shared/merge-join/`) requires typed join results. `JoinState`, `BinaryMergeJoin`, `HashJoin`, and `PremergedJoin` are all templated on a `JR` (join result) type that must provide `JR::Key(Rs::Key...)` and `JR(Rs...)` constructors. The generic `joined_t<TID, JK, fold_pks, Ts...>` template from `frontend/shared/view_templates.hpp` satisfies these requirements without per-query type definitions.

**Required types for Q3:**

- **Base tables**: `orders_t`, `lineitem_t`, `customerh_t` — in `tpch_tables.hpp`
- **Join result**: `joined_t<TID, Integer, false, orders_t, lineitem_t>` — used by PremergedJoin (structure 3) and BinaryMergeJoin/HashJoin (structures 1, 4)
- **MI records**: Same `orders_t`, `lineitem_t` in `MI_ORDERS_LINEITEM` (structure 3)
- **Intermediate pipeline view** (structure 2): Stores the ORDERS ⋈ LINEITEM join output (same pipeline the MI interleaves) — can reuse `joined_t` or define a `q3_pipeline_view_t` with a key suited to the downstream access pattern

### What "Monolithic" Means in Practice

The join result type (`joined_t`) is unavoidable. But everything *after* the join — filter, project, aggregate, CUSTOMER HashJoin, ORDER BY, LIMIT — can be fused into a single function using local variables rather than additional typed iterators:

```
// Structure 3 sketch (PremergedJoin + CUSTOMER HashJoin)
void q3_query_structure3(MergedAdapter& mi, Adapter<customerh_t>& cust) {
    // Phase 1: PremergedJoin produces joined_t<orders_t, lineitem_t>
    // Immediately filter, project, aggregate into a local map
    std::map<Integer, std::tuple<Numeric, Integer, Integer, Integer>> agg;
    //  key=l_orderkey, value=(revenue, o_custkey, o_orderdate, o_shippriority)
    premerged_join(mi, [&](const joined_t& row) {
        auto& o = std::get<orders_t>(row.payloads);
        auto& l = std::get<lineitem_t>(row.payloads);
        if (o.o_orderdate >= DATE) return;
        if (l.l_shipdate <= DATE) return;
        Numeric rev = l.l_extendedprice * (1 - l.l_discount);
        auto& [sum, custkey, odate, shippr] = agg[row.key.jk];
        sum += rev; custkey = o.o_custkey; odate = o.o_orderdate; shippr = o.o_shippriority;
    });

    // Phase 2: HashJoin with filtered CUSTOMER (build hash table, probe with aggregated rows)
    // Phase 3: Sort by (revenue DESC, o_orderdate ASC), take top 10
}
```

### Feasibility Assessment

**Works well for Q3** because:

1. The PremergedJoin callback fuses filter + project + aggregate — no separate operator stages needed
2. The CUSTOMER HashJoin happens after aggregation (far fewer rows), so the hash table is small
3. Only 3 tables, 2 joins — the function stays readable

**Trade-offs**:

- **Pro**: No per-query intermediate types beyond the generic `joined_t` instantiation
- **Pro**: Column access is field-based (`o.o_orderdate`) rather than positional (`$4`)
- **Con**: The CUSTOMER HashJoin uses shared `HashJoin` from `frontend/shared/merge-join/hash_join.hpp`
- **Con**: Harder to reuse across queries — each query gets its own bespoke function

---

## Implementation Status

**Shared infrastructure completed** (2026-04-29):

- `views_ol.hpp`: fully implemented — `ol_sort_key_t`, `joined_ol_t`,
  `SKBuilder<ol_sort_key_t>`. Unit-tested (12 tests in `test_views_ol.cpp`).
- Shared merge-join infra: `PremergedJoin` decoupled from record types via
  `jk_from_variants` and `SKBuilder::to_key<R>`.
- `OrdersLineitemPipeline` trimmed (2026-04-29): only `populate_merged` and
  `get_merged_size` remain. All operator drivers and view loading are per-query
  and belong in `query.tpp` / `load.tpp`.

**Q3-specific method bodies** remain TODO stubs (`load.tpp` + `query.tpp`).
`populate_merged` and `get_merged_size` are ready in the pipeline; per-query
files own everything else.

`q3/per_structure_workload.hpp` is now **alias-only** (2026-04-30): all
forwarder bodies live in `frontend/tpch/per_structure_workload.hpp`.

Stubbed methods:

- `Q3Workload<Backend>::Q3Workload(...)` — wire gflags into `params`.
- `Q3Workload<Backend>::load()` — dispatches to per-query `populate_view` /
  `ol.populate_merged`. **TODO debt**: `load.tpp` references `ol.populate_view`
  and `ol.get_view_size`, which were removed from the pipeline; fix before
  implementing load bodies.
- `Q3Workload<Backend>::get_size() const` — dispatches to per-query view size /
  `ol.get_merged_size`.
- `Q3Workload<Backend>::query_by_base(out)` — see §Plan Descriptions, Structure 1;
  §Execution Style: Monolithic vs Cascade.
- `Q3Workload<Backend>::query_by_view(out)` — see §Plan Descriptions, Structure 2 & 4.
- `Q3Workload<Backend>::query_by_merged(out)` — see §Plan Descriptions, Structure 3;
  §Execution Style: Monolithic vs Cascade (monolithic post-join sketch).
- `Q3Workload<Backend>::query_by_hash(out)` — see §Plan Descriptions, Structure 2 & 4
  (hash join variant).
- `BaseQ3<Backend>::query / get_size` and the View/Merged/Hash siblings —
  forwarders to the right `Q3Workload` method.
- `q3_predicate_orders`, `q3_predicate_lineitem`, `q3_predicate_joined` —
  see §Plan Descriptions for filter conditions.

Cross-cutting per-query refactor (operator drivers, view loading, build wiring):
see `frontend/tpch/CLAUDE.md`.
