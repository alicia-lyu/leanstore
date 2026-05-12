# Q10I: Returned Item Reporting × Customer Payment Behaviour

## Status

Design-doc only; skeleton and bodies pending Q3I Phase 3 completion.

---

## Original TPC-H Q10 (§2.4.10 — "Returned Item Reporting")

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

(The official query is run with `LIMIT 20` for top-20 customers.)

### Substitution Parameters

| Parameter | Domain | Description |
|-----------|--------|-------------|
| `:d` | First day of a month between 1993-02-01 and 1995-01-01 | Start of the 3-month order window |

**Validation values**: DATE = 1993-10-01.

### Real-world meaning of the original

Q10 finds the customers who have returned the most goods (by
returned-lineitem revenue) over a 3-month window, along with their
contact info. The intent is operational customer service: if a
customer is returning a lot of merchandise, the account manager
should be flagged so they can reach out and understand what's going
wrong (product quality, mis-shipments, fraud, dissatisfaction with
fit, etc.). The output drives a "follow up with these customers"
action list, not a strategic decision.

## Q10I — Invoice-Extended Variant

```sql
SELECT  c_custkey, c_name,
        SUM(l_extendedprice * (1 - l_discount)) AS return_revenue,
        SUM(CASE WHEN i_status = 'O' THEN i_totaldue ELSE 0 END) AS open_balance,
        SUM(CASE WHEN i_status = 'L' THEN i_totaldue ELSE 0 END) AS late_balance,
        c_acctbal, n_name, c_address, c_phone, c_comment
FROM    customer, orders, lineitem, invoice, nation
WHERE   c_custkey    = o_custkey
  AND   l_orderkey   = o_orderkey
  AND   i_custkey    = c_custkey
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '3' MONTH
  AND   l_returnflag = 'R'
  AND   c_nationkey  = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY return_revenue DESC;
```

(`LIMIT 20` retained.)

### What the extension adds

The new join is `i_custkey = c_custkey` — Invoice attaches as a
sibling of Orders under Customer, **not** as a child of Lineitem.
This is intentional: Q10's per-customer aggregate is the natural
shape, and we want to roll up that customer's *entire* invoice
exposure (not just invoices tied to the returned lineitems in the
window) alongside their return revenue.

Two new aggregates per customer:

- `open_balance` — total `i_totaldue` across the customer's
  outstanding invoices (`i_status = 'O'`).
- `late_balance` — total across past-due invoices (`i_status = 'L'`).

The original `return_revenue` is preserved unchanged.

### Real-world meaning of the extension

Q10 surfaces customers who return a lot. Q10I surfaces customers
who return a lot **and** are slow to pay — a strict superset of
warning signs for the account manager.

Three patterns in the result are interesting:

- **High returns, low open/late balance**: probably a legitimately
  unhappy customer; the operational response is product quality
  follow-up, not credit action.
- **High returns, high late balance**: classic "buy, complain,
  withhold payment" pattern — possible fraud or chronic disputed-
  invoice behaviour. The account manager should escalate to
  collections, not customer success.
- **High returns, high open (not late) balance**: customer is
  buying actively and returning actively; payments are still
  flowing. Lower priority than the previous pattern.

The extension turns Q10 from a "who needs a phone call" report
into a triaged "who needs which kind of phone call" report, by
combining returns telemetry with payment-status telemetry — two
independent signals that the operational team would otherwise have
to cross-reference manually.

## Why Q10I is a natural COLI showcase

- **C+O+L footprint**: Q10 already joins Customer, Orders, Lineitem
  (Nation joins as a small hash side, like Q5).
- **§3.1.2 sibling pattern, in its purest form**: Q10I is the
  cleanest demonstration of the sibling property in the TPC-H
  suite. The per-customer roll-up is the dominant cost, and both
  Customer and Invoice are keyed under custkey — so the COLI MI
  reads each customer's row, all their invoices, all their orders,
  and all their lineitems in a single contiguous scan with no
  cross-table seeking.
- **Different aggregation shape than Q3I/Q5I**: Q3I aggregates per
  order (top-N orderkeys), Q5I aggregates per nation. Q10I
  aggregates per customer, which means the result cardinality
  (~150K customers at SF=1 before the LIMIT 20) is the largest of
  the three — the post-aggregate sort+top-20 cost is non-trivial
  and the comparison across S1–S4 must include sort time honestly.
- **Two independent payment columns**: `open_balance` and
  `late_balance` are aggregated separately from `return_revenue`,
  exercising the COLI scan's ability to feed multiple accumulators
  per record-type variant in one pass.

## Open questions before bodies land

1. **Customer set vs. invoice set**: the SQL above invoices ALL of
   a customer's invoices (no date filter on `i_invoicedate`).
   Should there be one? Capping at the same 3-month window would
   make the metric "open balance accrued during the return window"
   instead of "total open exposure to this customer". The latter
   is more operationally useful (the manager wants the full
   picture before calling); confirm with the user.
2. **LIMIT 20 cutoff and tie-breaking**: same TPC-H spec rule as
   Q10 (deterministic by sort columns).
3. **`open + late` zero customers**: customers with no outstanding
   or late invoices will have both new columns at 0. The data
   generator emits 25%+5% O/L per invoice, so almost every
   customer will have *some* exposure at SF=1; verify, since a
   degenerate all-zeros result would defeat the parity test
   (similar to the Q12 degenerate-shape concern documented in
   `q12/CLAUDE.md`).
4. **Group-by column count**: Q10's GROUP BY is fat (8 columns).
   Adding the two new aggregates doesn't change the GROUP BY,
   which is fortunate — but it means the per-row aggregator state
   is also fat, and S2 (materialised view) would be paying for it
   redundantly per qualifying lineitem.
