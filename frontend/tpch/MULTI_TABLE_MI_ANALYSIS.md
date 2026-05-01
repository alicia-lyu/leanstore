# Multi-Table Merged Index Pipeline Selection

## 1. Problem Statement

The current ORDERS × LINEITEM pipeline joins only 2 tables and shows a
modest ~7% throughput advantage for the merged index at SF=40. This is
insufficient to demonstrate the "combinatorial advantage" the paper
argues for — where a single index scan replaces what would otherwise be
a series of joins.

The geo benchmark already evaluates multi-table merged indexes (up to
5 tables: Nation → State → County → City → Customer), but only tests
**strict hierarchies** with very small parent tables:

| Level | Count at SF=1 | Ratio to child |
|-------|---------------|----------------|
| Nation | 25 | 1:60 |
| State | 1,500 | 1:20 |
| County | 30,000 | 1:4 |
| City | 120,000 | 1:2 |
| Customer | 240,000 | — |

Each parent level is tiny relative to its children. The join pattern is
a strict 1-to-many tree, with every record belonging to exactly one
parent at each level.

A new 3-table pipeline must demonstrate something the geo benchmark does
not: **balanced cardinality across tables** and/or **non-hierarchical
(sibling) join patterns**.

## 2. Evaluation Criteria

1. **Cardinality balance.** Tables in the MI should have comparable
   magnitude. An MI dominated by one table's records is effectively a
   single-table secondary index with sparse markers from the other
   tables — the interleaving advantage is weak.

2. **Join pattern distinctiveness from geo.** The geo benchmark
   thoroughly tests strict hierarchy (1-to-many tree with small
   parents). The new pipeline should test a different pattern —
   ideally many-to-many joins where sibling tables share a join key
   without a parent-child relationship between them.

3. **Paper alignment.** The paper uses Customer-Order-Invoice as its
   primary running example (Section 3.1.2) and Nation-State-County-City
   as its hierarchical example (Section 3.1.3). Validating the primary
   example strengthens the paper.

4. **Pipeline reuse.** A merged index that serves at least 2 queries
   (using the full table set or a subset) demonstrates the
   combinatorial advantage — one physical structure supports multiple
   query patterns.

5. **Secondary index demonstration.** Reviewers want to see foreign
   keys in MI sort keys, where the MI acts as a secondary index for
   tables whose primary key differs from the sort key.

## 3. Candidate Analysis

### TPC-H table cardinalities at SF=1

| Table | Count | Notes |
|-------|-------|-------|
| CUSTOMER | 150,000 | |
| ORDERS | 1,500,000 | ~10 per customer |
| LINEITEM | 6,000,000 | ~4 per order, ~40 per customer |
| PART | 200,000 | |
| PARTSUPP | 800,000 | exactly 4 per part |
| SUPPLIER | 10,000 | |
| NATION | 25 | fixed |
| REGION | 5 | fixed |

---

### Candidate A: MI(CUSTOMER, ORDERS, LINEITEM) on custkey

**Sort key**: `(custkey, orderkey, linenumber)`

| Table | Index type | Key in MI |
|-------|-----------|-----------|
| CUSTOMER | primary | `(c_custkey, 0, 0)` |
| ORDERS | secondary | `(o_custkey, o_orderkey, 0)` |
| LINEITEM | secondary | `(custkey_from_order, l_orderkey, l_linenumber)` |

**Cardinality at SF=1**: 150K : 1.5M : 6M (ratio 1 : 10 : 40)

**TPC-H queries**: Q3 (stubbed), Q5, Q7, Q10, Q18 — 5 full 3-table
queries, plus Q13 and Q22 using 2-of-3 subsets.

**Why ruled out**: `custkey` is functionally dependent on `orderkey` (each
order belongs to exactly one customer). Prepending custkey to the
ORDERS and LINEITEM keys creates a hierarchical prefix chain:

```
custkey → (custkey, orderkey) → (custkey, orderkey, linenumber)
```

This is structurally the same pattern as the geo benchmark — a strict
tree where CUSTOMER is the root, ORDERS are children, and LINEITEM are
grandchildren. The scan follows a single path down the tree for each
customer. There is no many-to-many relationship; every order belongs to
exactly one customer, and every lineitem belongs to exactly one order.

The cardinality ratio (1:10:40) is also imbalanced — LINEITEM records
make up ~78% of the MI, similar to the geo benchmark where the
leaf-level Customer table dominates.

This candidate validates the paper's hierarchical example
(Section 3.1.3), but the geo benchmark already covers that pattern
thoroughly. It does not validate the paper's primary Customer-Order-
Invoice example (Section 3.1.2), which is explicitly a many-to-many
sibling pattern.

---

### Candidate B: MI(PART, PARTSUPP, LINEITEM) on partkey

**Sort key**: `(partkey, suppkey, orderkey, linenumber)`

| Table | Index type | Key in MI |
|-------|-----------|-----------|
| PART | primary | `(p_partkey, 0, 0, 0)` |
| PARTSUPP | primary | `(ps_partkey, ps_suppkey, 0, 0)` |
| LINEITEM | secondary | `(l_partkey, l_suppkey, l_orderkey, l_linenumber)` |

**Cardinality at SF=1**: 200K : 800K : 6M (ratio 1 : 4 : 30)

**TPC-H queries**: Q9 (stubbed), Q20 (full 3-table); Q2
(PART+PARTSUPP), Q14, Q17, Q19 (PART+LINEITEM) — 6 queries total.

**Why ruled out**: Severe cardinality imbalance. LINEITEM (6M) accounts
for **~86%** of all records in the MI (6M / 7M total). PART (200K) and
PARTSUPP (800K) together are only 14%. The merged index is effectively
a LINEITEM secondary index sorted by `(l_partkey, l_suppkey)` with
sparse PART and PARTSUPP markers.

This imbalance weakens the interleaving advantage:

- A scan over any partkey range is dominated by LINEITEM pages.
  Encountering a PART or PARTSUPP record is rare.
- The PremergedJoin spends most of its time in the LINEITEM portion
  of each partkey group, with only ~1 PART record and ~4 PARTSUPP
  records per group vs. ~30 LINEITEM records.
- The MI's storage overhead (wider keys on every LINEITEM record for
  the secondary index) is paid on 86% of the records.

Additionally, the join pattern is still hierarchical — a prefix chain
on `partkey → (partkey, suppkey)` — not a sibling pattern. PARTSUPP
and LINEITEM are in a parent-child relationship via the
`(partkey, suppkey)` composite key, not independent siblings.

The strongest argument for this candidate is its use of standard TPC-H
tables and the secondary index on LINEITEM. However, the cardinality
imbalance undermines the multi-table interleaving story.

---

### Candidate C: MI(CUSTOMER, ORDERS, INVOICE) on custkey — Recommended

**Sort key**: `(custkey, type_discriminator, record_pk)` — all three
tables join on the **same** key `custkey`.

| Table | Index type | Key in MI |
|-------|-----------|-----------|
| CUSTOMER | primary | `(c_custkey, ...)` |
| ORDERS | secondary | `(o_custkey, o_orderkey)` |
| INVOICE | secondary | `(i_custkey, i_invoicekey)` |

**Cardinality at SF=1**: 150K : 1.5M : TBD (target: comparable to
ORDERS, see §4)

**Why recommended**: This candidate scores highest on every evaluation
criterion.

**Balanced cardinality.** Unlike Candidates A and B where one table
dominates (LINEITEM at 78–86%), the Invoice table's cardinality can be
set to a comparable magnitude to Orders. With Invoice at 1.5M–3M, the
three tables contribute roughly equally to the MI's total size:

| Invoice count | C : O : I ratio | Largest table share |
|---------------|-----------------|---------------------|
| 1,500,000 | 1 : 10 : 10 | 47% (tie O and I) |
| 3,000,000 | 1 : 10 : 20 | 64% (I) |

Even at 3M, Invoice's share (64%) is far less dominant than LINEITEM's
86% in Candidate B.

**Many-to-many sibling pattern.** Orders and Invoice are **independent
siblings** under Customer. They share the join key `custkey` but have
no relationship to each other — an order does not determine an invoice,
and vice versa. This produces a true Cartesian product within each
custkey group: a customer with 10 orders and 20 invoices produces
10 × 20 = 200 joined triples.

This is fundamentally different from both the geo benchmark (strict
1-to-many tree) and Candidates A/B (hierarchical prefix chains).
The paper explicitly describes this pattern in Section 3.1.2:

> "buffers B for Customer, Orders, and Invoice collect rows per
> custkey; multiple orders and invoices produce many-to-many Cartesian
> products for that key."

**Paper alignment.** Customer-Order-Invoice is the paper's primary
running example, used in:

- Section 3.1.2: Sort order for multi-table joins with identical join keys
- Section 2 (Introduction): Motivating example for complex application objects
- Section 3.2.2: Support for various join multiplicities
- Section 5.1: Trade-offs discussion

Validating this example with real benchmarks directly strengthens the
paper's narrative.

**Secondary index demonstration.** Both ORDERS and INVOICE use
secondary indexes in the MI (sorted by `custkey` rather than their
primary keys `o_orderkey` / `i_invoicekey`). This directly addresses
reviewer interest in FK-based MI sort keys.

**Pipeline reuse.** Custom queries over Customer-Orders-Invoice can
exercise different access patterns:

- Full 3-table join with aggregation (e.g., per-customer spending)
- 2-of-3 subset queries (e.g., Customer-Orders only, Customer-Invoice only)
- Filtered joins (e.g., invoices for customers in a specific segment)

## 4. Invoice Table Design — Open Questions

### Cardinality

The Invoice table's cardinality should be set to achieve balanced
interleaving. Two natural choices:

| Option | Invoices per customer | Total at SF=1 | C:O:I ratio | Notes |
|--------|----------------------|---------------|-------------|-------|
| Match Orders | ~10 | 1,500,000 | 1:10:10 | Most balanced; O and I are symmetric siblings |
| 2× Orders | ~20 | 3,000,000 | 1:10:20 | Still reasonable; I is the larger sibling |

**Considerations**:

- In real-world ERP systems, invoice count typically correlates with
  order count (1–3 invoices per order for partial payments or
  installments). This suggests 1.5M–4.5M at SF=1.
- For the many-to-many demonstration, **symmetry between Orders and
  Invoice is more compelling** — it maximizes the "sibling" nature of
  the join. 1.5M invoices (matching Orders) makes the strongest case.
- However, if invoices are intended to represent billing events
  (monthly statements, partial payments), a 2:1 or 3:1 ratio to orders
  is also plausible.

### Schema

The paper's ER diagram shows a minimal schema: `invoicekey` (PK),
`custkey` (FK), `totaldue`. For a realistic benchmark, additional
fields may include:

- `i_invoicedate` — date field enabling date-range predicates (parallel
  to `o_orderdate`)
- `i_status` — categorical field enabling filtered joins (parallel to
  `o_orderstatus` or `c_mktsegment`)
- `i_comment` — variable-length padding for realistic record sizes

### Data Generation

- Deterministic, seeded by `(custkey, invoice_index)` for reproducibility
- `i_totaldue` could be independent random or correlated with
  `o_totalprice` for the same customer
- `i_invoicedate` range should overlap with `o_orderdate` range
  (1992-01-01 to 1998-12-31) to enable meaningful date-filtered joins

### Query Design

At least 2 queries exercising different patterns:

1. **Aggregation query**: Per-customer total (SUM of order totals +
   SUM of invoice dues), filtered by customer segment — exercises full
   3-way join with grouped aggregation
2. **Filtered join query**: Customers whose total invoiced amount
   exceeds their total order amount — exercises the many-to-many
   Cartesian product and comparative aggregation

## 5. Conclusion

| Criterion | A: C-O-L | B: P-PS-L | C: C-O-I |
|-----------|----------|-----------|----------|
| Cardinality balance | Poor (1:10:40) | Poor (1:4:30) | Good (1:10:10–20) |
| Distinct from geo | No (hierarchical) | No (hierarchical) | **Yes** (many-to-many siblings) |
| Paper alignment | Partial (§3.1.3) | None | **Primary example** (§3.1.2) |
| Pipeline reuse | 5+ TPC-H queries | 6 TPC-H queries | Custom queries (≥2) |
| Secondary indexes | 2 tables rekeyed | 1 table rekeyed | 2 tables rekeyed |

**MI(CUSTOMER, ORDERS, INVOICE) on custkey** is the recommended pipeline.
It is the only candidate that tests a genuinely non-hierarchical join
pattern with balanced cardinality, directly validates the paper's primary
example, and fills a gap that the geo benchmark does not cover.

The trade-off is that Invoice is a synthetic table requiring custom data
generation and queries (no standard TPC-H queries use it). This is
acceptable because the goal is to validate the paper's theoretical claims,
not to benchmark standard query workloads.
