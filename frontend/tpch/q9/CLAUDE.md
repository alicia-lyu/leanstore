# Q9: Product Type Profit Measure Query

## TPC-H Definition (Section 2.4.9)

The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts ordered in that year that contain a specified substring in their names and were filled by a supplier in that nation. Profit is `l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`.

## SQL

```sql
SELECT nation, o_year, SUM(amount) AS sum_profit
FROM (
    SELECT n_name AS nation,
           EXTRACT(YEAR FROM o_orderdate) AS o_year,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM part, supplier, lineitem, partsupp, orders, nation
    WHERE s_suppkey = l_suppkey
      AND ps_suppkey = l_suppkey
      AND ps_partkey = l_partkey
      AND p_partkey = l_partkey
      AND o_orderkey = l_orderkey
      AND s_nationkey = n_nationkey
      AND p_name LIKE '%[COLOR]%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC;
```

## Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| COLOR | Color names from TPC-H P_NAME generation (e.g., green, red, blue) | Substring filter on p_name |

**Validation values**: COLOR = green.

**Approved query variants**: None in Appendix B.

**Selectivity notes**: The LIKE filter on p_name is the only selection predicate. Different color substrings have varying frequencies in the generated part names. This filter applies to the PART table only (~200K parts at SF=1). Since Q9 has no date filters, selectivity variation is limited to the COLOR parameter.

## Tables and Join Graph

6 tables, 5 joins:

```
NATION <-[s_nationkey=n_nationkey]- SUPPLIER <-[s_suppkey=l_suppkey]- LINEITEM
                                                                        |
                                              [l_orderkey=o_orderkey]   |  [l_partkey=p_partkey]
                                                                        v         |
                                                    ORDERS  <-----------+         v
                                                                               PART
                                              PARTSUPP -[ps_partkey=l_partkey, ps_suppkey=l_suppkey]-> LINEITEM
```

Join conditions:
1. ORDERS.o_orderkey = LINEITEM.l_orderkey
2. LINEITEM.l_partkey = PART.p_partkey
3. LINEITEM.l_partkey = PARTSUPP.ps_partkey AND LINEITEM.l_suppkey = PARTSUPP.ps_suppkey
4. LINEITEM.l_suppkey = SUPPLIER.s_suppkey
5. SUPPLIER.s_nationkey = NATION.n_nationkey

## Merged Index

**MI(ORDERS, LINEITEM)** interleaved by `orderkey`.

- Same MI as Q3 and Q12
- The ORDERS-LINEITEM join dominates I/O (LINEITEM is the largest TPC-H table)
- Remaining 4 joins (PART, PARTSUPP, SUPPLIER, NATION) done at query time

## Column Index Mapping

### Concatenated Join Order: ORDERS||LINEITEM||PART||PARTSUPP||SUPPLIER||NATION (48 cols)

The plan joins tables in this order, producing a 48-column concatenated tuple:

**ORDERS** (9 cols, $0-$8): o_orderkey=$0, o_custkey=$1, o_orderstatus=$2, o_totalprice=$3, o_orderdate=$4, o_orderpriority=$5, o_clerk=$6, o_shippriority=$7, o_comment=$8

**LINEITEM** (16 cols, $9-$24): l_orderkey=$9, l_linenumber=$10, l_partkey=$11, l_suppkey=$12, l_quantity=$13, l_extendedprice=$14, l_discount=$15, l_tax=$16, l_returnflag=$17, l_linestatus=$18, l_shipdate=$19, l_commitdate=$20, l_receiptdate=$21, l_shipinstruct=$22, l_shipmode=$23, l_comment=$24

**PART** (9 cols, $25-$33): p_partkey=$25, p_name=$26, p_mfgr=$27, p_brand=$28, p_type=$29, p_size=$30, p_container=$31, p_retailprice=$32, p_comment=$33

**PARTSUPP** (5 cols, $34-$38): ps_partkey=$34, ps_suppkey=$35, ps_availqty=$36, ps_supplycost=$37, ps_comment=$38

**SUPPLIER** (7 cols, $39-$45): s_suppkey=$39, s_name=$40, s_address=$41, s_nationkey=$42, s_phone=$43, s_acctbal=$44, s_comment=$45

**NATION** (4 cols, $46-$49): n_nationkey=$46, n_name=$47, n_regionkey=$48, n_comment=$49

### Key Column References in Plans

- Profit calculation: `l_extendedprice($14) * (1 - l_discount($15)) - ps_supplycost($37) * l_quantity($13)`
- Project output: NATION=$47, O_YEAR=EXTRACT(YEAR, $4), profit formula, p_name=$26
- Filter: LIKE($26, '%green%') in baseline_s4, or LIKE($3, '%green%') in family logical plan (post-projection, where $3=p_name)

## Plan Descriptions

> **Implementation note**: While the logical plans below show MergeJoin for all
> 5 joins, the implementation uses **HashJoin** for all 4 outside-pipeline joins
> (PART, PARTSUPP, SUPPLIER, NATION). Only the innermost ORDERS×LINEITEM join
> varies by storage structure. See `OPERATORS.md §7` for justification.

### Structure 1: Traditional Indexes + Merge Join

5 merge-joins with explicit sorts. Filter (LIKE on p_name) applied after projection:

```
ORDERS scan -> Sort(o_orderkey)
  MergeJoin(o_orderkey = l_orderkey) with:
LINEITEM scan -> Sort(l_orderkey)
  -> Sort(l_partkey)
  -> MergeJoin(l_partkey = p_partkey) with:
PART scan -> Sort(p_partkey)
  -> Sort(l_partkey, l_suppkey)
  -> MergeJoin(l_partkey=ps_partkey AND l_suppkey=ps_suppkey) with:
PARTSUPP scan -> Sort(ps_partkey, ps_suppkey)
  -> Sort(l_suppkey)
  -> MergeJoin(l_suppkey = s_suppkey) with:
SUPPLIER scan -> Sort(s_suppkey)
  -> Sort(s_nationkey)
  -> MergeJoin(s_nationkey = n_nationkey) with:
NATION scan -> Sort(n_nationkey)
  -> Project(nation=$47, o_year=EXTRACT(YEAR,$4), profit, p_name=$26)
  -> Sort(nation ASC, o_year DESC)
  -> Filter(LIKE($3, '%green%'))  [post-projection: $3 = p_name]
  -> Aggregate(group by nation, o_year; SUM profit)
```

### Structure 2 & 4: Hash Join (Default Calcite Plan)

Same 5 joins but all hash-joins. Filter before projection:

```
ORDERS HashJoin LINEITEM on o_orderkey=l_orderkey
  -> HashJoin PART on l_partkey=p_partkey
  -> HashJoin PARTSUPP on l_partkey=ps_partkey AND l_suppkey=ps_suppkey
  -> HashJoin SUPPLIER on l_suppkey=s_suppkey
  -> HashJoin NATION on s_nationkey=n_nationkey
  -> Filter(LIKE($26, '%green%'))  [pre-projection: $26 = p_name]
  -> Project(nation=$47, o_year=EXTRACT(YEAR,$4), profit)
  -> Sort(nation ASC, o_year DESC)
  -> Aggregate(group by nation, o_year; SUM profit)
```

### Structure 3: Merged Index + PremergedJoin

Only the innermost ORDERS-LINEITEM merge-join is replaced by PremergedJoin. The remaining 4 merge-joins are unchanged:

```
PremergedJoin(MI_ORDERS_LINEITEM) [replaces ORDERS scan + LINEITEM scan + MergeJoin]
  -> Sort(l_partkey)
  -> MergeJoin(l_partkey = p_partkey) with PART
  -> Sort(l_partkey, l_suppkey)
  -> MergeJoin(l_partkey=ps_partkey AND l_suppkey=ps_suppkey) with PARTSUPP
  -> Sort(l_suppkey)
  -> MergeJoin(l_suppkey = s_suppkey) with SUPPLIER
  -> Sort(s_nationkey)
  -> MergeJoin(s_nationkey = n_nationkey) with NATION
  -> Project(nation, o_year, profit, p_name)
  -> Sort(nation ASC, o_year DESC)
  -> Filter(LIKE($3, '%green%'))
  -> Aggregate(group by nation, o_year; SUM profit)
```

The PremergedJoin output has the same 25-column layout as the ORDERS||LINEITEM MergeJoin output, so all downstream join conditions and column indices remain valid.

## Execution Style: Monolithic vs Cascade

### Record Types Required

The merge-join infrastructure requires typed join results (see Q3 CLAUDE.md for details). The generic `joined_t<TID, JK, fold_pks, Ts...>` from `frontend/shared/view_templates.hpp` avoids per-query type definitions.

**Required types for Q9:**

- **Base tables**: `orders_t`, `lineitem_t`, `part_t`, `partsupp_t`, `supplier_t`, `nation_t` — all in `tpch_tables.hpp`
- **Join result**: `joined_t<TID, Integer, false, orders_t, lineitem_t>` — for the ORDERS ⋈ LINEITEM pipeline (structures 1, 3, 4)
- **MI records**: Same `orders_t`, `lineitem_t` in `MI_ORDERS_LINEITEM` (structure 3)
- **Intermediate pipeline view** (structure 2): Stores the ORDERS ⋈ LINEITEM join output; remaining 4 joins + filter + aggregate at query time

Additional `joined_t` instantiations are needed for each subsequent merge join if using `BinaryMergeJoin` (e.g., joining the OL result with PART, then with PARTSUPP, etc.). These are generic template instantiations, not per-query class definitions.

### What "Monolithic" Means in Practice

After the ORDERS ⋈ LINEITEM join (which must produce a typed `joined_t`), the remaining 4 joins and final aggregate can be written as a single function. Two viable approaches:

1. **HashJoin for all 4 remaining joins** — build PART (filtered by LIKE), PARTSUPP, SUPPLIER, NATION hash tables once before the OL scan; probe each per-row inside the callback. See OPERATORS.md §3 op 7.
2. **Hash-aggregate outside the pipeline** — group by `(n_name, o_year)` is unrelated to pipeline sort key, so a hash-aggregate is legitimate here (past the comparison axis). See OPERATORS.md §3 op 6.

```
// Structure 3 sketch (PremergedJoin + hash lookups for small tables)
void q9_query_structure3(MergedAdapter& mi, ...) {
    // Build all 4 dimension hash tables once before scan
    auto part_ht = build_hash_table<part_t>(part, like_filter);  // filtered by LIKE
    auto partsupp_ht = build_hash_table<partsupp_t>(partsupp);
    auto supplier_ht = build_hash_table<supplier_t>(supp);       // ~10K rows
    auto nation_ht = build_hash_table<nation_t>(nat);            // 25 rows

    // PremergedJoin produces joined_t<orders_t, lineitem_t>
    // Inside callback: probe PART -> PARTSUPP -> SUPPLIER -> NATION
    // Compute profit, hash-aggregate by (n_name, EXTRACT(YEAR, o_orderdate))
}
```

### Feasibility Assessment

**Feasible but verbose for Q9** because:

1. 5 joins mean multiple sort-and-merge phases — long but straightforward
2. The PremergedJoin replaces the heaviest join; remaining 4 joins use HashJoin (build once, probe per-row)
3. All dimension tables fit in memory — hash table build is a one-time cost

**Trade-offs**:

- **Pro**: No per-query intermediate types — only generic `joined_t` instantiations
- **Pro**: The LIKE filter on p_name can be applied immediately after PART join, reducing data for subsequent joins
- **Con**: The function is ~100-150 lines — approaches the limit of single-function readability
- **Con**: All 4 dimension hash tables must fit in memory simultaneously (feasible: PART ~200K, PARTSUPP ~800K, SUPPLIER ~10K, NATION 25 at SF=1)

## Status

All three DOT plan files are semantically equivalent and verified. No changes needed to Q9 DOT files.

---

## Implementation Status (skeleton)

As of 2026-04-28, `frontend/tpch/q9/` contains compile-ready skeletons.
Method bodies are TODO comments citing the relevant section of this file.

Loading lives in `OrdersLineitemPipeline` per the Pipeline Convention
(`frontend/tpch/CLAUDE.md §Pipeline Convention`). `load()` and `get_size()`
are one-line dispatchers. NATION, SUPPLIER, PART, PARTSUPP are base-only
and are not part of any pipeline secondary structure.

Stubbed methods (`load.tpp` + `query.tpp`):

- `Q9Workload<Backend>::Q9Workload(...)` — wire gflags into `params`.
- `Q9Workload<Backend>::load()` — dispatches to `ol.populate_view` /
  `ol.populate_merged`; base-table size case is a TODO pending
  `TPCHWorkload::get_size()`.
- `Q9Workload<Backend>::get_size() const` — dispatches to
  `ol.get_view_size` / `ol.get_merged_size`.
- `Q9Workload<Backend>::query_by_base(out)` — see §Plan Descriptions, Structure 1
  (5 merge joins); §Execution Style.
- `Q9Workload<Backend>::query_by_view(out)` — see §Plan Descriptions, Structure 2 & 4;
  OL pipeline view scan + 4 remaining joins.
- `Q9Workload<Backend>::query_by_merged(out)` — see §Plan Descriptions, Structure 3;
  §Execution Style: hash-lookup sketch for NATION + SUPPLIER, merge join for PART +
  PARTSUPP inside PremergedJoin callback.
- `Q9Workload<Backend>::query_by_hash(out)` — see §Plan Descriptions, Structure 2 & 4
  (hash join variant — all 5 joins hash-joined).
- `BaseQ9<Backend>::query / get_size` and the View/Merged/Hash siblings —
  forwarders to the right `Q9Workload` method.
- `q9_predicate_joined` — placeholder (always true); LIKE filter on `p_name`
  applied post-PART-join inside `query_by_*` bodies.

Cross-cutting TODOs (`OrdersLineitemPipeline` method bodies, build wiring):
see `frontend/tpch/CLAUDE.md`.
