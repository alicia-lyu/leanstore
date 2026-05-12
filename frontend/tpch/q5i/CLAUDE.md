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

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:1` | One of REGION.r_name (AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST) | Region filter |
| `:d` | January 1 of a year in [1993, 1997] | Start of the 1-year order window |

**Validation values**: REGION = ASIA, DATE = 1994-01-01.

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
3. **Plan-side join order**: Calcite is free to put Invoice on
   either side of the Lineitem×Supplier join. The COLI MI prefers
   Invoice early (sibling to Orders, before the Supplier hash
   probe); the baseline structures may pick a different order.
   Comparison fairness requires both branches to use their natural
   plan, not a forced one.
