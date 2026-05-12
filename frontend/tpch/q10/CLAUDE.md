# Q10: Returned Item Reporting

## Status

Design-doc only; skeleton and bodies pending. The invoice-extended
sibling lives in [`../q10i/`](../q10i/CLAUDE.md) — Q10 here is the
no-extension baseline.

---

## TPC-H Definition (§2.4.10 — "Returned Item Reporting")

```sql
SELECT  c_custkey, c_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
        c_acctbal, n_name, c_address, c_phone, c_comment
FROM    customer, orders, lineitem, nation
WHERE   c_custkey   = o_custkey
  AND   l_orderkey  = o_orderkey
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '3' MONTH
  AND   l_returnflag = 'R'
  AND   c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC;
```

The official query is run with `LIMIT 20` for top-20 customers.

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:d` | First day of a month between 1993-02-01 and 1995-01-01 | Start of the 3-month order window |

**Validation values**: DATE = 1993-10-01.

### Real-world meaning

Q10 finds the customers who have returned the most goods (by
returned-lineitem revenue) over a 3-month window, along with their
contact info. The intent is operational customer service: if a
customer is returning a lot of merchandise, the account manager
should be flagged so they can reach out and understand what's going
wrong (product quality, mis-shipments, fraud, dissatisfaction with
fit, etc.). The output drives a "follow up with these customers"
action list, not a strategic decision.

---

## Plan & execution sketch

Q10 is a **4-table join** (Customer, Orders, Lineitem, Nation) with
a per-customer aggregate, top-20 ORDER BY, and a wide GROUP BY (8
columns) carrying the customer contact row.

- **NATION** (25 rows) is loaded into an in-memory hashmap keyed on
  `n_nationkey → n_name`.
- **OL pipeline** drives the main scan; the orderdate window is
  pushed into the orders side of the join, and `l_returnflag = 'R'`
  is pushed into the lineitem scan. Only ~25% of lineitems carry
  `R` so this prunes aggressively.
- **CUSTOMER** is consulted per qualifying orderkey — at S3 the
  COLI-style hierarchical scan would already co-locate it; at S1/S4
  it's a hashmap or merge-join side carrying the 6 fat output
  columns plus `c_nationkey`.
- The accumulator is keyed by `c_custkey` and rolls up
  `l_extendedprice * (1 - l_discount)` over qualifying lineitems.
  The 7 non-aggregate output columns are FD-attached (each
  customer has exactly one row in CUSTOMER), so they ride along on
  the per-custkey state.
- Post-aggregate: sort by `revenue DESC`, take top 20.

## Storage-structure variant axis

Same S1–S4 convention as Q3 / Q12 — see the top-level
[`../CLAUDE.md §Storage-structure → Wrapper Mapping`](../CLAUDE.md#storage-structure--wrapper-mapping).
Q10's per-customer roll-up makes the COL pipeline (the
custkey-hierarchical 3-table MI used by Q3) a natural S3 candidate
once the implementation lands; Q3's `CustomerOrdersLineitemPipeline`
can likely be reused verbatim. NATION attaches as a small hashmap
on the side, like Q5. S5 (aCOLI) is not applicable — Q10 has no
invoice extension; that variant lives in `../q10i/`.

## Implementation status

| Phase | Status |
|-------|--------|
| 0 — design doc (this file) | complete |
| 0.5 — skeleton (`workload.hpp`, `views.hpp`, `load.tpp`, `query.tpp`, executables) | pending |
| 1 — `Params::defaults()` + predicate bodies | pending |
| 2 — `query_by_*` bodies for S1/S2/S3/S4 | pending |
| 3 — `test_query_q10_{lsm,btree}` parity test | pending |
| 4 — CMake + `generate_targets.py` wiring | pending |
| 5 — Linux perf sweep (`RUNS.md`) | pending |
