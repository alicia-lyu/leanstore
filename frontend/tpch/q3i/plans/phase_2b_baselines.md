# Q3I Phase 2B — Baseline query paths (S1, S2, S4)

Lands the three baseline `query_by_*` bodies after Phase 2A's
substrate is in place (rename, shared helpers, widened view types,
S3 retrofit). Reads as a single concept per step; each step is one
commit.

## Prerequisite

`phase_2a_refactor.md` complete. Specifically:

- `coli.split_orders()` / `split_lineitem()` / `split_invoice()`
  accessors exist
- `apply_topN`, `make_filtered_scanner`, `scan_of` are available
- `q3i_pipeline_view_t` is widened; `cust_open_due_t` and
  `lineitem_agg_t` row types exist
- `query_by_merged` (S3) emits top-10 via `apply_topN`

## Design recap

Per `family_logical.dot` and `q3i/CLAUDE.md §Plan Descriptions`:

- **S1 (merge family)**: chain of 3 `BinaryMergeJoin` over
  custkey-sorted streams + 2 SortedAggregate scanner-wrappers
- **S2 (merge family)**: scan the materialised `q3i_pipeline_view_t`
  + outside-pipeline filters + `apply_topN`
- **S4 (no-MI baseline)**: chain of 3 `HashJoin` mirroring S1's
  shape + post-join hash-aggregate by orderkey

S1 and S4 are structurally isomorphic — same chain length, same
join order, same per-record arithmetic via shared accumulators.
They differ only in join algorithm + S4's post-join hash-aggregate
(forced by HashJoin's unsorted output).

Filter pushdown is uniform across all three: every parameterised
filter fuses with TableScan or with the per-customer aggregate's
emit gate. None applies post-join.

## Step 1 — S1 query path: scanner-wrappers + 3 BMJs

### New scanner-wrapper aggregators (in `q3i/query.tpp`)

```cpp
// Wraps coli.split_invoice() (custkey-sorted invoice_coli_t scanner).
// Filter i_status='O' fused at consume time; threshold filter fused
// at emit (drops groups failing > THRESHOLD).
struct CustomerOpenDueAggregator {
   // scanner-shaped: next() yields (custkey, cust_open_due_t).
   // Internally drives split_invoice's getScanner(), accumulates
   // via CustomerOpenDueAccumulator, emits at custkey transitions.
};

// Wraps coli.split_lineitem() (custkey, orderkey, linenumber-sorted).
// Filter l_shipdate > DATE fused at consume time. Emits one row
// per (custkey, orderkey) carrying revenue.
struct LineitemRevenueAggregator {
   // scanner-shaped: next() yields ((custkey,orderkey), lineitem_agg_t).
   // Internally drives split_lineitem's getScanner(), accumulates
   // via LineitemRevenueAccumulator, emits at (custkey, orderkey)
   // transitions.
};
```

Both follow the `MergedScannerCounter` pattern in
`frontend/geo/mixed_query.tpp`. Both embed the **shared accumulator
structs from Phase 1** unchanged — that's what preserves §6.1
comparison-integrity with S3.

### `query_by_base` body

```cpp
template <typename Backend>
long Q3IWorkload<Backend>::query_by_base(std::vector<q3i_agg_row_t>& out)
{
   // Filter-fused inputs.
   auto cust_scan = make_filtered_scanner(customer.getScanner(),
       [&](const customerh_t& c) { return c.c_mktsegment == params.mktsegment; });
   CustomerOpenDueAggregator agg_inv(coli.split_invoice(), params);
   auto ord_scan = make_filtered_scanner(coli.split_orders().getScanner(),
       [&](const orders_coli_t& o) { return o.o_orderdate < params.orderdate; });
   LineitemRevenueAggregator agg_lin(coli.split_lineitem(), params);

   // BMJ #1: customer ⋈ agg_inv on custkey
   using JR1 = joined_t<..., customerh_t, cust_open_due_t>;
   BinaryMergeJoin<custkey_sk_t, JR1, customerh_t, cust_open_due_t>
       bmj1(cust_scan, agg_inv);

   // BMJ #2: bmj1 ⋈ split_orders on custkey  (1:N preserves custkey-order)
   using JR2 = joined_t<..., JR1, orders_coli_t>;
   BinaryMergeJoin<custkey_sk_t, JR2, JR1, orders_coli_t>
       bmj2(bmj1.as_scanner(), ord_scan);

   // BMJ #3: bmj2 ⋈ agg_lin on (custkey, orderkey)
   using JR3 = joined_t<..., JR2, lineitem_agg_t>;
   BinaryMergeJoin<cust_order_sk_t, JR3, JR2, lineitem_agg_t>
       bmj3(bmj2.as_scanner(), agg_lin);

   bmj3.run([&](const JR3& row) {
      out.push_back({/* extract orderkey, revenue, orderdate,
                       shippriority, cust_open_due */});
   });
   apply_topN(out, 10, by_revenue_desc);
   return static_cast<long>(out.size());
}
```

The exact `joined_t` instantiations and sort-key types
(`custkey_sk_t`, `cust_order_sk_t`) work out at implementation time
— they likely already exist as `sk_for_t<R>`-derived types in
`view_templates.hpp`.

**Commit**: "Q3I Phase 2B step 1: scanner-wrapper aggregators + S1
3-BMJ chain".

**Verification**: with S1 set, `query_by_base` returns the same
top-10 row set as `query_by_merged` at SF=1.

## Step 2 — S2: populate view + view scan

### `populate_q3i_view` (in `q3i/load.tpp`)

Per OPERATORS.md §3 op 4, view loading may use simplifications.
Drive a small Visitor over base tables + per-custkey lookup tables
(memory-bounded at SF=1; acceptable simplification).

```cpp
template <typename Backend>
static void populate_q3i_view(...)
{
   // Build per-custkey lookups once.
   std::unordered_multimap<Integer, orders_t>   by_cust;
   std::unordered_multimap<Integer, lineitem_t> by_order;
   std::unordered_multimap<Integer, invoice_t>  inv_by_cust;
   /* … populate from each base scanner … */

   auto cs = customer.getScanner();
   while (auto ckv = cs->next()) {
      // Per-custkey: drive a population Visitor that delegates to
      // CustomerOpenDueAccumulator + LineitemRevenueAccumulator,
      // emitting one q3i_pipeline_view_t row per (custkey, orderkey).
      // Per-table filters (i_status, o_orderdate, l_shipdate) fuse
      // at consume time; mktsegment + threshold are NOT applied
      // (predicate hoisting — view stays reusable across params).
   }
}
```

Same accumulator structs as Phase 1 / S1 — comparison-integrity
preserved.

### `query_by_view` body

```cpp
template <typename Backend>
long Q3IWorkload<Backend>::query_by_view(std::vector<q3i_agg_row_t>& out)
{
   auto scanner = pipeline_view.getScanner();
   while (auto kv = scanner->next()) {
      const auto& row = kv->second;
      if (row.c_mktsegment != params.mktsegment) continue;
      if (row.cust_open_due <= params.threshold) continue;
      out.push_back({kv->first.orderkey, row.revenue, row.o_orderdate,
                     row.o_shippriority, row.cust_open_due});
   }
   apply_topN(out, 10, by_revenue_desc);
   return static_cast<long>(out.size());
}
```

`load()` switch in `q3i/load.tpp` extends to call
`populate_q3i_view(...)` for case 2.

**Commit**: "Q3I Phase 2B step 2: populate_q3i_view + query_by_view".

**Verification**: with S2 set, parity with S1/S3 at SF=1.

## Step 3 — S4: 3-HJ chain + post-aggregate; refresh DOT

### `query_by_hash` body

```cpp
template <typename Backend>
long Q3IWorkload<Backend>::query_by_hash(std::vector<q3i_agg_row_t>& out)
{
   // Pre-aggregate INVOICE → small (custkey, due) relation.
   // Base INVOICE is invoicekey-sorted, NOT custkey-sorted, so we
   // must hash-aggregate (cannot stream by custkey transitions —
   // that's S1's job over the custkey-sorted split_invoice).
   std::vector<cust_open_due_t> agg_invoice;
   {
      std::unordered_map<Integer, Numeric> bucket;
      auto inv = invoice.getScanner();
      while (auto kv = inv->next()) {
         if (!q3i_predicate_invoice(kv->second)) continue;
         bucket[kv->second.i_custkey] += kv->second.i_totaldue;
      }
      for (auto& [ck, due] : bucket)
         if (due > params.threshold) agg_invoice.push_back({ck, due});
   }

   // Filter-fused base scanners.
   auto cust_scan = make_filtered_scanner(customer.getScanner(),
       [&](const customerh_t& c) { return c.c_mktsegment == params.mktsegment; });
   auto ord_scan  = make_filtered_scanner(orders.getScanner(),
       [&](const orders_t& o) { return o.o_orderdate < params.orderdate; });
   auto lin_scan  = make_filtered_scanner(lineitem.getScanner(),
       [&](const lineitem_t& l) { return l.l_shipdate > params.shipdate; });

   // HJ #1: agg_invoice ⋈ customer on custkey
   using JR1 = joined_t<..., cust_open_due_t, customerh_t>;
   HashJoin<custkey_sk_t, JR1, cust_open_due_t, customerh_t>
       hj1(scan_of(agg_invoice), cust_scan);

   // HJ #2: hj1 ⋈ orders on custkey
   using JR2 = joined_t<..., JR1, orders_t>;
   HashJoin<custkey_sk_t, JR2, JR1, orders_t>
       hj2(hj1.as_scanner(), ord_scan);

   // HJ #3: hj2 ⋈ lineitem on orderkey
   using JR3 = joined_t<..., JR2, lineitem_t>;
   HashJoin<ol_sort_key_t, JR3, JR2, lineitem_t>
       hj3(hj2.as_scanner(), lin_scan);

   // Post-join hash aggregate by orderkey (HashJoin output unsorted).
   std::unordered_map<Integer, q3i_agg_row_t> per_order;
   hj3.run([&](const JR3& jr) {
      const auto& l = /* extract */;
      const auto& o = /* extract */;
      const auto& d = /* extract cust_open_due */;
      auto& slot = per_order[o.o_orderkey];
      slot.o_orderkey     = o.o_orderkey;
      slot.o_orderdate    = o.o_orderdate;
      slot.o_shippriority = o.o_shippriority;
      slot.cust_open_due  = d.cust_open_due;
      LineitemRevenueAccumulator::consume_into(slot.revenue, l, params);
   });
   for (auto& [_, row] : per_order) out.push_back(row);
   apply_topN(out, 10, by_revenue_desc);
   return static_cast<long>(out.size());
}
```

S4 mirrors S1's chain length and join order. The only structural
differences from S1 are (a) algorithm (HJ vs BMJ) and (b) the
final hash-aggregate (because HashJoin output isn't sorted).
`LineitemRevenueAccumulator::consume_into` static helper
(introduced as part of Phase 2A's accumulator code, or added here)
keeps the per-record arithmetic identical to S1's wrapper.

### Refresh `q3i/plans/baseline_s4.dot`

Current `baseline_s4.dot` shows an asymmetric design with two
hashmap-lookup pre-builds; replace with the 3-HJ chain matching the
above body and S1's structure.

**Commit**: "Q3I Phase 2B step 3: query_by_hash 3-HJ chain + refresh
baseline_s4.dot".

**Verification**: with S4 set, parity with S1/S2/S3 at SF=1.

## Critical files

- `frontend/tpch/q3i/query.tpp` — three new bodies + two
  scanner-wrappers
- `frontend/tpch/q3i/load.tpp` — `populate_q3i_view`; load()
  case 2 wiring
- `frontend/tpch/q3i/plans/baseline_s4.dot` — refresh

## Out of scope

- Unified harness + Phase 1 binary retirement — see
  `phase_2c_harness.md`
- All doc-level updates beyond the DOT refresh — see
  `phase_2c_harness.md`
