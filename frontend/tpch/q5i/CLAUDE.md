# Q5I: Local Supplier Volume × Invoice Payment Status

## Status

Design-doc only; skeleton and bodies pending Q3I Phase 3 completion.

---

## Original TPC-H Q5 (§2.4.5 — "Local Supplier Volume")

```sql
SELECT  n_name,
        SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM    customer, orders, lineitem, supplier, nation, region
WHERE   c_custkey   = o_custkey
  AND   l_orderkey  = o_orderkey
  AND   l_suppkey   = s_suppkey
  AND   c_nationkey = s_nationkey
  AND   s_nationkey = n_nationkey
  AND   n_regionkey = r_regionkey
  AND   r_name      = ':1'
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY revenue DESC;
```

### Substitution Parameters

| Parameter | Domain | Description | Validation value |
|-----------|--------|-------------|-----------------|
| `:1` | One of `{AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST}` | Region filter | ASIA |
| `:d` | January 1 of a year in [1993, 1997] | Start of the 1-year order-date window | 1994-01-01 |

### Real-world meaning of the original

Q5 measures, for each nation in a chosen region, the revenue generated
by lineitems where the customer and the supplier are both located in
that nation — i.e. *local* commerce. The constraint `c_nationkey =
s_nationkey` is the linchpin: it filters out cross-border trade and
keeps only intra-nation sales, so the result reads as "how much
business does each nation in this region do with itself?". The
business question is whether to open a local distribution centre in a
given country, where high local-revenue nations are the strongest
candidates.

## Q5I — Invoice-Extended Variant

```sql
SELECT  n_name,
        SUM(l_extendedprice * (1 - l_discount)) AS nominal_revenue,
        SUM(CASE WHEN i_status = 'P'
                 THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS realised_revenue,
        SUM(CASE WHEN i_status = 'O'
                 THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS open_revenue,
        SUM(CASE WHEN i_status = 'L'
                 THEN l_extendedprice * (1 - l_discount) ELSE 0 END) AS late_revenue
FROM    customer, orders, lineitem, invoice, supplier, nation, region
WHERE   c_custkey      = o_custkey
  AND   l_orderkey     = o_orderkey
  AND   l_invoicekey   = i_invoicekey
  AND   l_suppkey      = s_suppkey
  AND   c_nationkey    = s_nationkey
  AND   s_nationkey    = n_nationkey
  AND   n_regionkey    = r_regionkey
  AND   r_name         = ':1'
  AND   o_orderdate   >= ':d'
  AND   o_orderdate   <  ':d' + INTERVAL '1' YEAR
GROUP BY n_name
ORDER BY nominal_revenue DESC;
```

### What the extension adds

A single new join (`l_invoicekey = i_invoicekey`) and three additional
CASE-aggregates that split the existing `revenue` sum by invoice
payment status. The original `nominal_revenue` is preserved as
`SUM(l_extendedprice * (1 - l_discount))` — Q5's exact answer. The
new columns slice that total into:

- `realised_revenue` — invoiced and paid (`i_status = 'P'`)
- `open_revenue` — invoiced, not yet due/paid (`i_status = 'O'`)
- `late_revenue` — invoiced, past due (`i_status = 'L'`)

By design `realised + open + late = nominal`, so the extension is a
strict refinement of Q5; running it with all three statuses summed
back together must reproduce the original Q5 result row-for-row.

### Real-world meaning of the extension

Q5 tells a country manager *how much business* their nation does
locally; Q5I tells them *how much of that business is actually money
in the bank*. A nation with high `nominal_revenue` but a high
`open + late` fraction is doing a lot of selling on credit and
carrying significant counterparty risk — the local-distribution-
centre decision shifts. Conversely, a nation with modest
`nominal_revenue` but near-100% `realised_revenue` is a stable
revenue source even if the headline number isn't dramatic.

The split also exposes one specific risk: a region where most
revenue is `late_revenue` is a region where the business is
effectively financing its customers, which is a different
operational posture than just "selling locally". Q5I makes that
visible without needing to run a follow-up query.

## Why Q5I is a natural COLI showcase

- **C+O+L footprint**: Q5 already joins Customer, Orders, Lineitem
  (and Supplier+Nation+Region on top). The COLI MI absorbs the
  C+O+L portion into a single tagged-key scan; only Supplier/Nation/
  Region remain as a separate small-table hash side.
- **§3.1.2 sibling pattern**: Invoice attaches at custkey, sibling
  to Orders. The extra join `l_invoicekey = i_invoicekey` is
  resolved inside the COLI scan — invoices for a given customer
  are co-located with that customer's lineitems, so the join
  becomes a range probe within the custkey group, not a separate
  index lookup.
- **Aggregation is per-nation, not per-customer**: distinguishes
  Q5I from Q3I (top-N orders) and Q10I (top customers). Q5I's
  result cardinality is bounded by `|nation in region| ≈ 5`, so
  the aggregation is tiny even at SF=100; the dominant cost is
  the join + scan, which is exactly what the COLI MI optimises.

## Open questions before bodies land

1. **Cardinality calibration**: with ASIA and DATE=1994-01-01, the
   Q5 selectivity is ~5–10% of lineitems. Adding the invoice probe
   doesn't change the row count (every lineitem has exactly one
   invoice via `l_invoicekey`), but it doubles the per-row payload
   touched. Verify SF=1 row counts match Q5 exactly when all three
   status sums are added back together.
2. **`i_status` distribution**: the data generator emits 'P' 70% /
   'O' 25% / 'L' 5%. The `late_revenue` column will be small — this
   is realistic, but worth checking that all three columns are
   non-zero at SF=1 so the cross-structure parity test has signal.
3. **Join order within S4**: S1/S2/S3 implement the same logical pipeline at
   different precomputation levels (no precomputation → pipeline view → merged
   index). S4 uses hash join and may differ physically, but should follow the
   same logical post-pipeline operator chain (supplier semi-join → aggregate by
   c_nationkey → nation equi-join → sort) unless the hash-join order makes this
   genuinely awkward, in which case document the deviation.

---

## § Cardinality Structure

Q5I uses the three-framing taxonomy from PLAYBOOK.md:

- **Pure hierarchical (C→O→L)**: Q3/Q5 without Invoice — not this.
- **Hierarchical + sibling sub-aggregate (§3.1.2)**: Q3I — invoices aggregate
  to a scalar `cust_open_due` per customer before the O×L stream.
- **Hierarchical with semi-join filter**: Q5I — the primary chain is Customer →
  Invoice → Lineitem (1:N at each step). Orders appear only as a semi-join
  filter on Lineitem: a lineitem survives only if its order's `o_orderdate`
  falls in the query window. Orders contribute no fields to any output row.

Key cardinality facts (SF=1 reference):

- C → Invoice: ~20 invoices/customer (2 invoices/order × 10 orders/customer)
- Invoice → Lineitem: 1:N, ~2 lineitems/invoice (40 lineitems/customer ÷ 20 invoices)
- Orders (semi-join only): date filter admits ~10–20% → ~2–4 invoices/customer survive
- Result cardinality: ≤5 rows (one per nation in the chosen region)

Why the hierarchy is C→I→L, not C→O→L: Q5's revenue sum is grouped by nation,
not by order. Orders provide only the date window (semi-join). Invoice provides
`i_status`, partitioning each lineitem's revenue into `{nominal, realised, open,
late}`. The COLI MI co-locates invoices and lineitems under the same custkey
prefix, enabling a hierarchical C→I→L scan with a per-lineitem orderdate
survival check.

Lineitem key structure: `lineitem_coli_t`'s key encodes all four domain tags
explicitly: `[cust][custkey][ord][orderkey][inv][invoicekey][li][linenumber]`.
Within a given order, lineitems sort by `invoicekey`, then by `linenumber`.
Invoice records at the custkey level also sort by `invoicekey`. All invoice
records for a custkey group are fully scanned before any lineitem (invoice
tag=2 < orders tag=3 < lineitem tag=4), so the invoice buffer is guaranteed
complete at `on_lineitem` time (CONVENTIONS.md Rule 10).

---

## § Storage Structure Options

| S | Strategy | Secondary structure | Join strategy | Params baked in |
|---|----------|---------------------|---------------|-----------------|
| S1 | Split indexes + BinaryMergeJoin | Customer primary index (by custkey) + custkey-sorted secondaries: `orders_coli_t`, `lineitem_coli_t`, `invoice_coli_t` | BMJ C⋈O on `custkey` then ⋈L on `(custkey, orderkey)`; orders semi-join for date filter; per-lineitem seek on `invoice_coli_t[(custkey, invoicekey)]` | none |
| S2 | Pipeline view + sequential scan | `q5i_pipeline_view_t` keyed by `(custkey, orderkey, linenumber)`; `i_status` resolved at view-load time | Sequential scan + side-table probes at query time | none (predicate-hoisted) |
| S3 | COLI MI (4-table merged index) | `COLIPipeline` — `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | Sequential group-walk: invoice prefix buffered (Rule 10 Pattern B); per-order date semi-join; per-lineitem `i_status` lookup → 3 partial aggregates | none |
| S4 | Hash join baseline | Base tables only | Customer hash-build on `custkey` → lineitem scan; orders semi-join via hash probe on `orderkey`; per-lineitem seek on `invoice_t[l_invoicekey]` | none |

S5 deferred (same rationale as Q3I S5).

---

## § Plan Descriptions (Query Shapes)

Plan files live in `q5i/plans/`. Create at Phase 0.5 alongside the skeleton.

Side-tables build (shared across S1–S4):

```text
region_t [semi-join: r_name=':1' → qualifying regionkey]
  ⋈ nation_t [equi-join: build nation_set{n_nationkey}; n_name via PK lookup]
  ⋉ supplier_t [semi-join: build supplier_nation_set{(s_nationkey, s_suppkey)}]
[supplier_nation_set is derived from qualifying nations, so a record passing
 the supplier semi-join is guaranteed to have a qualifying c_nationkey]
```

Post-pipeline operator chain (shared — monolithic in `query_by_*`, Rule 11):

```text
1. ⋉ supplier_nation_set[(c_nationkey, l_suppkey)]
   [primary admission; supplier_nation_set subsumes the nation_set check]
2. aggregate by c_nationkey, routing revenue by i_status:
     nominal  += revenue       (every qualifying lineitem)
     realised += revenue if P
     open     += revenue if O
     late     += revenue if L
   [hash aggregation: ≤5 qualifying nationkeys per region]
3. n_name = nation.lookup(c_nationkey)  [PK lookup in post-pipeline step 3]
4. sort by nominal_revenue DESC
```

S3 — COLI MI group-walk (canonical):

```text
COLIPipeline::group_walk per custkey:
  on_customer: c_nationkey ∈ nation_set? → SkipGroup
  on_invoice:  buffer full invoice_coli_t into per-group invoice_buf[]
               [CONVENTIONS.md Rule 10 Pattern B]
  on_orders:   o_orderdate ∉ [date, date+1y) → SkipOrder
  on_lineitem: look up invoice_buf[key.invoicekey] → i_status
               emit {c_nationkey, l_suppkey, l_extendedprice, l_discount,
                     i_status} to post-pipeline chain

→ post-pipeline operator chain (above)
```

S2 — Pipeline view scan:

```text
q5i_pipeline_view_t sequential scan:
  per-row: c_nationkey ∈ nation_set? → skip
  emit {c_nationkey, l_suppkey, l_extendedprice, l_discount,
        i_status, o_orderdate} to post-pipeline chain

post-pipeline operator chain:
  filter: o_orderdate ∈ [date, date+1y)
  ⋉ supplier_nation_set[(c_nationkey, l_suppkey)]
  → aggregate by c_nationkey by i_status
  → n_name = nation.lookup(c_nationkey)  [PK lookup in post-pipeline step 3]
  → sort by nominal_revenue DESC
```

Note: `c_nationkey ∈ nation_set` early-exit applied consistently across S1–S4.

S1 — BinaryMergeJoin chain:

```text
customerh_t primary index [sorted by custkey]
  c_nationkey ∈ nation_set? → skip
  ⋈[BMJ on custkey]
orders_coli_t [sorted by custkey]  ← semi-join: date filter
  ⋈[BMJ on (custkey, orderkey)]
lineitem_coli_t [sorted by (custkey, orderkey, invoicekey, linenumber)]
  seek: invoice_coli_t[(custkey, key.invoicekey)] → i_status
  emit {c_nationkey, l_suppkey, l_extendedprice, l_discount, i_status}

→ post-pipeline operator chain (above)
```

S4 — Hash join baseline:

```text
side_tables_build: (same as S3)

invoice_t scan  → hash-build on invoicekey: invoice_map{invoicekey → i_status}
customer_t scan → c_nationkey ∈ nation_set? → skip
                → hash-build on custkey (PK only)
orders_t scan   → hash-probe on custkey → date filter
                → hash-build on orderkey (PK only)
lineitem_t scan → hash-probe on orderkey → get custkey
                → customer.lookup(custkey) → c_nationkey  [primary-index PK lookup]
                → hash-probe invoice_map[l_invoicekey] → i_status
                → emit {c_nationkey, l_suppkey, l_extendedprice, l_discount, i_status}

→ post-pipeline operator chain (above)
```

---

## § Filter Pushdown Principle

1. Parameterised filters (`r_name = ':1'`, `o_orderdate ∈ [':d', ':d'+1y)`) —
   never baked into any secondary. The S2 view must be predicate-hoisted so one
   load serves all `(region, date)` param combos.
2. `i_status` routing — all three statuses are always computed per query
   (`nominal`, `realised`, `open`, `late`). No filter pushdown on `i_status`;
   every lineitem's revenue is routed into one of 3 partial aggregates.
3. Side-table join taxonomy (CONVENTIONS.md Rules 1, 4, 7, 10, 11):
   Region is a semi-join (qualifying regionkey only). Nation is an **equi-join** —
   `n_name` flows downstream as the GROUP BY key (Rule 1). Build payload is
   PK-only per Rule 4: `nation_set{n_nationkey}`. `n_name` is resolved in the
   post-pipeline nation equi-join step (step 3 of the post-pipeline chain) via
   nation primary-index PK lookup; ≤5 lookups per query since there are ≤5
   qualifying nations per region. No new type minted (Rule 7). Supplier is a
   semi-join: `supplier_nation_set{(s_nationkey, s_suppkey)}` encodes the
   cross-equality `c_nationkey = s_nationkey` as a composite PK (Rule 5).
4. Orders as semi-join: `o_orderdate` is a survival predicate on Lineitem.
   Applied at query time in all structures; never baked into any secondary.

---

## § Required Record Types

New types (shape + key only; implementation in Phase 1):

```
q5i_pipeline_view_t (id ≈ 62)
  Key:     (custkey: Integer, orderkey: Integer, linenumber: Integer)
  Payload: { l_extendedprice: Numeric, l_discount: Numeric,
             l_suppkey: Integer, c_nationkey: Integer,
             o_orderdate: Timestamp, i_status: Varchar<1> }
  Note: order-sharing pipeline output (C×O×L×I join), predicate-hoisted.
  i_status resolved from invoice join at view-load time.
  n_name NOT stored; resolved in post-pipeline nation equi-join step.

q5i_agg_row_t  (in-memory)
  Fields: { n_name: Varchar<25>, nominal_revenue: Numeric,
            realised_revenue: Numeric, open_revenue: Numeric,
            late_revenue: Numeric }
  Sort: descending by nominal_revenue; ≤5 rows per query

q5i_jr1_t  (id ≈ 63) — BMJ output: customer ⋈ orders (date semi-join)
  Key:     (custkey: Integer, orderkey: Integer)
  Payload: { c_nationkey: Integer, o_orderdate: Timestamp }
  Note: c_nationkey carried for supplier probe at lineitem step; no n_name.

q5i_jr2_t  (id ≈ 64) — BMJ output: jr1 ⋈ lineitem + invoice seek result
  Key:     (custkey: Integer, orderkey: Integer, linenumber: Integer)
  Payload: { c_nationkey: Integer, l_extendedprice: Numeric,
             l_discount: Numeric, l_suppkey: Integer, i_status: Varchar<1> }
```

Modified in place (Phase 1 only):

```
lineitem_coli_t (id=32, tpch_family/views_coli.hpp)
  → Add: l_suppkey: Integer
  Reason: Q5I's supplier cross-equality needs l_suppkey in the COLI
  group-walk. Q3I never reads it — additive, harmless.
```

Reused verbatim:

- `customer_coli_t` (id=30) — full customerh_t; carries `c_nationkey`
- `orders_coli_t` (id=31) — `{o_orderdate, o_shippriority}`
- `invoice_coli_t` (id=33) — `{i_totaldue, i_status}`

---

## Implementation Status (Phase 1 in progress — 2026-05-15)

> **Phase model note (2026-05-15)**: the PLAYBOOK has collapsed Phase 0.5
> (skeleton) into Phase 1. The skeleton landed as "Phase 1 commit 1";
> this section tracks the full Phase 1 milestone. See
> [`PLAYBOOK.md §3.6`](../PLAYBOOK.md) for the new unified Phase 1 definition.

Phase 0.5 skeleton landed. All 8 per-query files exist; `q5i_lsm` links
and runs to exit 0; `test_query_q5i_lsm` reports `[OK]` parity at
digest 0x0 (all four `query_by_*` return empty). No parity claim yet.

Stub files: `query.tpp` (all `query_by_*`), `views.hpp` (`print()`).
Real bodies: `load.tpp` (ctor, `load()`, `get_size()`), `query.tpp`
(`Params::defaults()`, `set_params_for_iter`, `q5i_predicate_orders`).

Design decisions locked during plan review (carry into Phase 1):

1. Logical join order is C→O→L→I (not C→I→L)
2. `n_name` resolved at CUSTOMER ⋈ RN join time, cached in
   `nationkey_to_name`, used as aggregation key directly
3. Invoice hash-build is PK-only (`invoice_set{invoicekey}`);
   `i_status` via `invoice.lookup(l_invoicekey)` at probe time
4. `supplier_nation_set` is post-pipeline only in S3 (walker uses
   only `nation_set`)
