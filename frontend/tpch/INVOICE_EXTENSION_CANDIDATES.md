# TPC-H Invoice-Extension Candidate Evaluation

## Purpose

The COLI 4-table merged index — `MI(customer × orders × lineitem ×
invoice)`, keyed by `custkey` with Calcite-style tagged keys — is the
showcase substrate for the §3.1.2-sibling + §3.1.3-hierarchical hybrid
pattern. It pays off most when a query already joins **Customer +
Orders + Lineitem (C+O+L)**, because INVOICE then folds in as a second
§3.1.2 sibling under custkey at no extra access cost. Q3 has this
shape; Q9 and Q12 don't.

This document inventories all 22 standard TPC-H queries, identifies
those with the natural C+O+L footprint, and ranks them as candidates
for invoice-extension queries beyond Q3.

## Method

For each Q1–Q22 we record the table set (per TPC-H spec v2.17.3
§2.4.1–§2.4.22, local copy at `tpch-doc.pdf`) and flag whether all
three of CUSTOMER, ORDERS, LINEITEM participate. Candidates with
C+O+L=Y get a per-query analysis covering the invoice-extension
hypothesis, join shape, predicate-hoisting plan, expected COLI
advantage, and S1–S4 comparison story. Non-candidates are listed with
a one-line rejection reason.

## 1. Inventory of TPC-H Q1–Q22

| Q# | Purpose | Tables | Cust | Ord | Line | C+O+L |
|----|---------|--------|------|-----|------|-------|
| 1 | Pricing Summary Report | L | N | N | Y | N |
| 2 | Minimum Cost Supplier | P, S, PS, N, R | N | N | N | N |
| 3 | Shipping Priority | C, O, L | Y | Y | Y | **Y** |
| 4 | Order Priority Checking | O, L | N | Y | Y | N |
| 5 | Local Supplier Volume | C, O, L, S, N, R | Y | Y | Y | **Y** |
| 6 | Forecasting Revenue Change | L | N | N | Y | N |
| 7 | Volume Shipping | C, O, L, S, N (×2) | Y | Y | Y | **Y** |
| 8 | National Market Share | C, O, L, S, P, N (×2), R | Y | Y | Y | **Y** |
| 9 | Product Type Profit Measure | P, PS, S, L, O, N | N | Y | Y | N |
| 10 | Returned Item Reporting | C, O, L, N | Y | Y | Y | **Y** |
| 11 | Important Stock Identification | PS, S, N | N | N | N | N |
| 12 | Shipping Modes & Order Priority | O, L | N | Y | Y | N |
| 13 | Customer Distribution | C, O | Y | Y | N | N |
| 14 | Promotion Effect | L, P | N | N | Y | N |
| 15 | Top Supplier | L, S | N | N | Y | N |
| 16 | Parts/Supplier Relationship | PS, P, S | N | N | N | N |
| 17 | Small-Quantity-Order Revenue | L, P | N | N | Y | N |
| 18 | Large Volume Customer | C, O, L | Y | Y | Y | **Y** |
| 19 | Discounted Revenue | L, P | N | N | Y | N |
| 20 | Potential Part Promotion | S, N, PS, P, L | N | N | Y | N |
| 21 | Suppliers Who Kept Orders Waiting | S, L (×3), O, N | N | Y | Y | N |
| 22 | Global Sales Opportunity | C, O | Y | Y | N | N |

Legend: C=CUSTOMER, O=ORDERS, L=LINEITEM, S=SUPPLIER, P=PART,
PS=PARTSUPP, N=NATION, R=REGION.

**Six queries pass C+O+L=Y**: **Q3, Q5, Q7, Q8, Q10, Q18**.

> Note: an earlier draft of the scouting pass marked Q9 as C+O+L=Y by
> mistake. The canonical Q9 SQL joins PART, PARTSUPP, SUPPLIER,
> LINEITEM, ORDERS, NATION — no CUSTOMER. Q9 is correctly N in the
> table above. The earlier `q9i/` and `q12i/` skeletons (both
> contrived extensions of queries lacking the C+O+L footprint) have
> been deleted; this document is now the sole record of those
> rejections.

## 2. Natural-Fit Shortlist

### 2.1 Q3 — Shipping Priority *(reference, already covered by `q3i/`)*

- **Hypothesis**: rank pending orders for top-N customers, weighted by
  outstanding invoice balance.
- **Join shape**: INVOICE attaches as a §3.1.2 sibling at custkey
  (joins to Customer+Orders before lineitem fan-out).
- **Predicate hoisting**: `c_mktsegment=:1` → customer-variant filter;
  `o_orderdate<:d` → orders-variant filter; `l_shipdate>:d` →
  lineitem-variant filter; `i_status='O'` → invoice-variant filter
  inside the COLI scan; threshold on `cust_open_due` is post-aggregate.
- **COLI advantage**: pure hybrid — single custkey scan covers
  Customer, Orders, Lineitem, AND the invoice subquery aggregate.
- **S1–S4**: documented in `q3i/CLAUDE.md` (placeholder; bodies TBD).

### 2.2 Q5 — Local Supplier Volume *(strong candidate)*

- **Hypothesis (Q5I)**: regional revenue *net of unpaid invoices* —
  "what's the realised vs. nominal revenue in each nation, given
  invoice payment status?"
- **Join shape**: INVOICE attaches as a §3.1.2 sibling under custkey
  (Customer is already on the join path); Q5 also folds in
  Supplier×Nation×Region but those are reduce-side.
- **Predicate hoisting**:
  - `r_name=':1'` → region scan filter (outside COLI; pushes into the
    nation hash join build side)
  - `o_orderdate >= :d AND < :d + 1 year` → orders-variant filter
    inside COLI scan
  - `c_nationkey = s_nationkey` → post-join (cross-record predicate;
    cannot push down)
  - `i_status='P'` (proposed) → invoice-variant filter inside COLI
    scan; CASE inside accumulator for the paid/unpaid split
- **COLI advantage**: same hybrid as Q3I, but with a richer
  per-nation aggregate. Showcases that the COLI MI's payoff scales
  beyond the simple top-10 of Q3.
- **S1–S4 comparison**: S1/S4 pay one invoice secondary-index lookup
  per qualifying lineitem; S2 must store an even fatter view (now
  including Customer+Invoice fields); S3 is the natural single-scan.

### 2.3 Q10 — Returned Item Reporting *(strong candidate)*

- **Hypothesis (Q10I)**: top-20 customers by returned-item revenue,
  with their invoice-payment behaviour as a tie-breaker context
  field. "Who returns a lot AND has a high outstanding balance?"
- **Join shape**: INVOICE attaches as §3.1.2 sibling at custkey.
  Per-customer aggregate is already the natural shape of Q10, so
  invoice paid/open totals slot in cleanly.
- **Predicate hoisting**:
  - `o_orderdate >= :d AND < :d + 3 months` → orders-variant filter
  - `l_returnflag='R'` → lineitem-variant filter
  - `i_status IN ('O','L')` (proposed) → invoice-variant filter
  - Nation join is a separate hash side, post-aggregate
- **COLI advantage**: heavy emphasis on §3.1.2 sibling property —
  Customer+Invoice both keyed under custkey, both aggregated per
  customer. Q10I is the cleanest demonstration of the sibling
  pattern *without* the §3.1.3 hierarchy adding complexity (the
  lineitem fan-out is just feeding the revenue aggregator).
- **S1–S4 comparison**: S3 wins decisively here because the
  per-customer roll-up is the dominant cost; sibling co-location
  saves an entire invoice-side scan.

### 2.4 Q18 — Large Volume Customer *(secondary candidate)*

- **Hypothesis (Q18I)**: customers placing large orders, gated by
  invoice payment behaviour — "which whales are slow to pay?"
- **Join shape**: §3.1.2 sibling at custkey. Q18 already has a
  HAVING-style aggregate over lineitem (`SUM(l_quantity) > :n`); the
  invoice extension adds a per-customer paid-fraction filter.
- **Predicate hoisting**:
  - HAVING `SUM(l_quantity) > :n` → post-join aggregate filter
  - `i_status='O' AND i_totaldue > :m` (proposed) → invoice-variant
    filter for outstanding-balance gate
- **COLI advantage**: similar hybrid to Q3I but with HAVING
  semantics; useful as a SECOND query if we want to vary the
  aggregation shape across the showcase suite. Less compelling than
  Q5I/Q10I because Q18 is already complex and the invoice extension
  feels narrative-light.

### 2.5 Q7 / Q8 — Multi-nation Volume Shipping / Market Share *(weak candidates)*

- **Why weaker**: both have heavy NATION/REGION involvement and
  self-joins on NATION. The C+O+L portion is one piece of a larger
  query whose dominant cost is the supplier/nation-side joins, not
  the customer-orders-lineitem hierarchy. Invoice extension would be
  contrived (e.g. "market share weighted by invoice currency mix") —
  business meaning is thin.
- **Verdict**: skip unless we run out of stronger candidates.

## 3. Reject List

| Query | Reason invoice extension is poor fit |
|-------|--------------------------------------|
| Q1 | Lineitem-only; no custkey context |
| Q2 | Supplier-side; no orders/lineitem |
| Q4 | O+L only, no Customer |
| Q6 | Lineitem-only |
| Q9 | No Customer; invoice extension is contrived (earlier `q9i/` skeleton deleted) |
| Q11 | Stock/supplier; no orders/lineitem |
| Q12 | O+L only, no Customer (earlier `q12i/` skeleton deleted) |
| Q13 | C+O only, no Lineitem — but a different MI (CO+I) might be interesting; out of scope |
| Q14, Q17, Q19 | Lineitem×Part; no custkey path |
| Q15 | Lineitem×Supplier; no custkey path |
| Q16, Q20 | Part/supplier-side |
| Q21 | O+L only, multi-self-joined lineitem; custkey not on join path |
| Q22 | C+O only, no Lineitem |

## 4. Recommendation

Implement **Q5I** as the primary post-Q3 invoice extension:

1. Strong, realistic business question (paid vs. nominal regional
   revenue).
2. Adds a different aggregation shape than Q3 (nation × revenue
   sums vs. Q3's top-10 by orderkey).
3. Exercises the same §3.1.2 sibling property as Q3I but with a
   bigger fan-out, which should make the COLI advantage easier to
   measure across S1–S4.

Implement **Q10I** as the secondary candidate:

1. Cleanest standalone showcase of the §3.1.2 sibling pattern (per-
   customer roll-up, dominant cost on the customer side).
2. Complements Q3I/Q5I by emphasising the customer-side aggregation
   over the order-side one.
3. Shape is different enough from Q3I/Q5I to widen the experiment
   suite without redundancy.

The earlier `q9i/` and `q12i/` skeletons have been **deleted**: the
contrast point ("queries without C+O+L are bad invoice-extension
candidates") is fully captured by §3 of this document, and keeping
unused skeletons in the tree is dead weight. If those contrast cases
become paper-worthy later, they can be re-introduced from this
document's specification.

**Defer** Q7I, Q8I, Q18I unless the experimental suite needs more
queries. Q18I is the strongest of these three if a fourth is needed.

## 5. Open Questions

1. **Per-customer aggregate for Q5I**: should `i_status='P'` filter
   be a row predicate or a CASE inside the accumulator (so that
   total revenue and paid revenue are reported side-by-side)? Q3I
   chose CASE; Q5I should match for consistency.
2. **Q10I time window**: TPC-H Q10 uses `o_orderdate IN [d, d+3
   months)`. Should the invoice predicate be on `i_invoicedate` in
   the same window, or independent? The data generator emits invoice
   dates in `[max(orderdate of bundled orders) + uniform(7,60)]`, so
   coupling them is simpler.
3. **MI(C, O, I) without Lineitem?** Q13 and Q22 are C+O only. A
   3-table CO+I MI might be worth a sentence in the related-work
   discussion, but is out of scope for the current paper.
4. **Cardinality calibration**: Q5I and Q10I selectivity should be
   tuned at SF=1 once bodies land — Q5I is sensitive to the region
   filter, Q10I to the date window.

## 6. Next Steps

1. User review of Q5I/Q10I as the recommended candidates.
2. On approval: add `q5i/` and `q10i/` skeleton directories using the
   same 8-file shape as the existing `q3i/`.
3. Implement query bodies in dependency order: Q3I first (it's the
   pedagogical anchor), then Q5I, then Q10I.
4. Defer CMake targets and tests until at least one query body is
   production-ready.
