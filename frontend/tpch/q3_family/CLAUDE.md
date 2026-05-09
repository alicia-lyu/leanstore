# q3_family/ — Shared Building Blocks for Q3 and Q3I

This directory contains headers that are shared between Q3 and Q3I (and any
future Q3-flavoured query). Every symbol here must be usable by both queries
without modification; query-specific logic stays in `q3/` or `q3i/`.

**What lives in `tpch_family/` instead**: three utilities were lifted out of
this directory when Q5 became a second consumer (2026-05-09 refactor):

- **Revenue arithmetic** — `tpch::lineitem_revenue<L>()` lives in
  `tpch_family/revenue.hpp`. `LineitemRevenueAccumulator` here calls it;
  the shipdate gate stays in this file (Q3-family-specific).
- **Base stats counters** — `tpch::TPCHFamilyStats` (scan/filter/join/MI-walk/
  stage timing) lives in `tpch_family/family_stats.hpp`. `Q3FamilyStats`
  derives from it and adds `topN_candidates` / `stage_us_topN`.
- **COL merge kernel** — `tpch::col_two_pointer_merge` lives in
  `tpch_family/col_two_pointer_merge.hpp`. `view_loaders.hpp` is now a
  thin re-export shim; existing callers (`q3/load.tpp`, `q3i/load.tpp`,
  `q5/load.tpp`) compile unchanged via `populate_q3_view_core`.

Everything else in this directory (predicates, params, agg_row, visitor,
lineitem aggregator, view-loaders shim) is Q3-pair-specific and stays here.

## Shared Symbols

### `accumulators.hpp` — `LineitemRevenueAccumulator<P>`

Accumulates `SUM(l_extendedprice * (1 - l_discount))` for a single order,
gated by the `l_shipdate > params.shipdate` predicate.  Three `consume()`
overloads accept `lineitem_t`, `lineitem_coli_t`, and `lineitem_acoli_t` via
duck-typing — no common base class required.  A `reset()` method clears the
running total between order boundaries.

**Consumers**: Q3I S1/S2/S3/S4/S5 (`query.tpp`); Q3 will wire at Phase 0.5.

**Rationale**: identical per-record arithmetic across all storage structures
and both queries; a single definition prevents divergence and satisfies
OPERATORS.md §6.1 comparison-integrity.

---

### `predicates.hpp` — `q3_predicate_customer/orders/lineitem<P>`

The three spec-shared single-table filters templated on a `Params` struct
that carries `mktsegment`, `orderdate`, and `shipdate`:

- `q3_predicate_customer<P>(c, params)` — `c_mktsegment == params.mktsegment`
- `q3_predicate_orders<P>(o, params)` — `o_orderdate < params.orderdate`
- `q3_predicate_lineitem<P>(l, params)` — `l_shipdate > params.shipdate`

**Consumers**: Q3I delegates its predicate bodies here; Q3 will do the same
at Phase 0.5.

**Rationale**: the three filters are identical in the SQL spec for Q3 and Q3I;
centralising them means a spec-correction fix applies to both queries.

---

### `params.hpp` — `PARAM_TABLE`, `set_params_for_iter`

A 10-entry substitution-parameter rotation table covering all 5 TPC-H market
segments × the 5 March-15 order dates in \[1993, 1997\].  `set_params_for_iter`
maps a loop-iteration index to a `Params` value by cycling through the table.

**Consumer pattern**: each per-query `Params::set_params_for_iter(long iter)`
delegates to this free function, passing its own `Params&`.  Q3I uses it;
Q3 will wire at Phase 0.5.

**Rationale**: both Q3 and Q3I use the same TPC-H substitution-parameter
domain; a single table guarantees they exercise the same parameter coverage
in benchmarks.

---

### `agg_row.hpp` — `q3_agg_row_base_t`

Base aggregate-output row carrying the four fields common to all Q3-family
queries: `o_orderkey`, `revenue`, `o_orderdate`, `o_shippriority`.  Provides:

- `print_base(os)` — emits the four columns tab-separated (no newline).
- `print(os)` — calls `print_base` then emits a newline; Q3 uses this
  directly; Q3I overrides it to append `cust_open_due`.
- `cmp(a, b)` — comparator for `apply_topN`: revenue DESC, orderdate ASC,
  orderkey ASC.  Deterministic across all structures.

**Consumers**: Q3 aliases `q3_agg_row_t = q3_family::q3_agg_row_base_t`
directly.  Q3I derives `q3i_agg_row_t` from it (adds `cust_open_due`).

**Rationale**: the four base output columns and their sort order are spec-
identical for Q3 and Q3I; hoisting them eliminates a copy-paste target.
Previously internal to `q3i/views.hpp`; hoisted Phase A2.

---

### `stats.hpp` — `Q3FamilyStats`

Cardinality counters shared by Q3 and Q3I:

- Scan counts: `customers_scanned`, `orders_scanned`, `lineitems_scanned`
- Filter-pass counts: `customers_passing_filter`, `orders_passing_filter`,
  `lineitems_passing_filter`
- Join/aggregate counts: `join_callbacks`, `aggregator_rows_out`,
  `topN_candidates`
- Per-stage timing: `stage_us_scan_filter`, `stage_us_aggregator`,
  `stage_us_join`, `stage_us_topN`
- MI walk counters: `mi_records_visited`, `mi_groups_skipped`

**Consumers**: Q3 uses `Q3FamilyStats` directly (`using Stats =
q3_family::Q3FamilyStats`).  Q3I's `Q3IStats` derives from it and adds
invoice/BMJ-chain/aCOLI/backend-specific counters.

**Rationale**: common counters let benchmarks compare S1–S5 on the same axes;
hoisting prevents per-query stats drift.  Previously internal to
`q3i/workload.hpp`; hoisted Phase A3.

---

### `coli_visitors.hpp` — `Q3FamilyVisitor<Derived, Params, LineitemType, AggRow, Stats>`

CRTP base implementing the C×O×L core of the group-walk visitor over COL and
COLI merged indexes:

- `on_customer` — gates by `c_mktsegment`; sets `mktsegment_ok`.
- `on_order` — gates by `o_orderdate`; flushes the previous order register at
  boundaries; invokes the CRTP `per_order_admit_check` hook on the first order
  per custkey.
- `on_lineitem` — accumulates revenue via `LineitemRevenueAccumulator`.
- `on_group_end` — flushes the last open order; resets per-group state.

Three CRTP customisation points (all have base no-op defaults):

| Hook | Q3I override | Q3 usage |
|------|-------------|----------|
| `per_order_admit_check(order)` | gates on `cust_open_due > threshold` | base (always admit) |
| `extra_emit_fields(row)` | fills `cust_open_due` | base (no-op) |
| `on_record_visited_hook()` | bumps invoice-specific counters | base (no-op) |

**Consumers**: Q3I's `COLIGroupWalkVisitor` (`q3i/query.tpp`) subclasses this,
overriding all three hooks.  Q3 will use `Q3FamilyVisitor` directly with the
base no-op overlay (no invoice arm to wire).

**Rationale**: Q3 and Q3I share the identical C×O×L walk logic; the invoice-
specific surface is isolated to three narrow hooks.  Previously the entire
visitor lived in `q3i/query.tpp`; hoisted Phase A4.

---

### `lineitem_agg.hpp` — `lineitem_agg_t`

Per-(custkey, orderkey) lineitem revenue aggregate row: the output type of
`LineitemRevenueAggregator`.  Carries `Numeric revenue`, keyed by
`(custkey, orderkey)`.  `Key::match` orders custkey-major, orderkey-minor.
Includes `std::hash` specialisation for HashJoin.

**Consumers**: Q3I S1 BMJ #3 (right side); Q3 S1 BMJ #2 (right side).

**Rationale**: both queries' S1 chain feeds the same scanner-wrapper output
shape into the final BMJ — hoisted Phase 4 §7.2.  Previously inline in
`q3i/views.hpp` (where `tpch::q3i::lineitem_agg_t` is now an alias).

---

### `lineitem_revenue_aggregator.hpp` — `LineitemRevenueAggregator<Backend, LineitemType, Params>`

Scanner-wrapper aggregator (OPERATORS.md §3 op 6) that drives a
custkey-sorted split-lineitem adapter and emits one
`(lineitem_agg_t::Key, lineitem_agg_t)` per `(custkey, orderkey)` group.
The `l_shipdate` filter is fused at consume time via
`LineitemRevenueAccumulator`.

**Consumers**: Q3I S1 with `LineitemType = lineitem_coli_t`; Q3 S1 with
`LineitemType = lineitem_col_t`.

**Rationale**: identical group-flush logic for both queries, only the row
type differs; the type parameter `LineitemType` lets each query supply its
own COLI / COL projection.  Previously a Q3I-only class in
`q3i/query.tpp`; hoisted Phase 4 §7.2.

---

### `view_loaders.hpp` — `populate_q3_view_core<OrdersAdapter, LineitemAdapter, EmitFn>`

Two-pointer merge over orders (sorted by orderkey) and lineitem (sorted by
`(orderkey, linenumber)`).  For each lineitem belonging to an order, calls:

```
emit(custkey, orderkey, linenumber, order, lineitem)
```

No filters are applied — predicate hoisting keeps the view reusable across
all param sets.  The caller is responsible for any per-custkey pre-maps (e.g.
Q3I's `open_due_map` and `mktseg_map`) before invoking this function.

**Consumers**: Q3I's `populate_q3i_view` (`q3i/load.tpp`) wraps this with an
emit callback that attaches `cust_open_due` per custkey.  Q3's
`populate_q3_view` will use a simpler callback that omits that field.

**Rationale**: the two-pointer merge kernel is mechanically identical for both
queries; only the per-row emit differs.  Previously inlined in
`q3i/load.tpp`; hoisted Phase A5.
