# Q10I: Returned Item Reporting × Return-Payment Status

**Reading guide**: For SQL and plan descriptions, read §TPC-H Definition
and §Plan Descriptions. For implementation status, read §Implementation
Phases. For OutClass / wildcard / latent-assumption details, see
`../CONVENTIONS.md`. Skip the rest unless reconstructing a design
decision or adding a new storage structure.

## Status

**Phase 4 complete (2026-05-24)**: all four `query_by_*` bodies
implemented and parity-verified at SF=1, SF=5, SF=10 with strict
4-way XOR digest (`S1 ≡ S2 ≡ S3 ≡ S4`). Q3I/Q5I regressions clean.
Paper scope: **5L cell only** (headline cell). Pair-fates with Q10.

Phase 1 (2026-05-23): 8-file skeleton + test harness + strict
cardinality / sentinel-ordering assertions all `[OK]` at SF=1.
`lineitem_coli_t` widened with `l_returnflag` (D14).

**Merged to calcite-integration (2026-05-25)**: the E1 "worktree-only"
hold was lifted by explicit user decision; the `lineitem_coli_t`
widening (D14) invalidated the Q3I/Q5I COLI images, reloaded with the
merge.

**S5 aCOLI + fair S2 view added (2026-05-25)**: the smoke test
reproduced Q10's two btree anomalies (per-lineitem S2 strawman; S3 < S1,
prune below the COLI co-location grain), so the Q10 fixes were ported.
S5 (D3 overturned — per-order grain is soundly bakeable) is a 2-type
aCOLI MI `<customer_coli_t, orders_acoli_q10i_t>` with per-order
paid/open/late baked, lineitems+invoices dropped, walked by the
hand-rolled `acoli_group_walk`; the fair S2-B is the per-order preagg
view (`--q10i_view_variant=preagg`). Both parity-verified at SF=1 both
backends (S1≡S2≡S3≡S4≡S2-preagg≡S5). **The aCOLI is the fastest
structure on both backends** (mirrors Q10's aCOL), beating the fair
per-order preagg view *and* raw S3:
- **c2 (SF=150 dram=0.1) btree ms/q**: S5 10.6 < S2-B 27.2 < S1 550 <
  S3 3,323 < S4 13,127 < S2-A 16,751.
- **5L (c0, dram=1.0) btree (SF=1550) ms/q**: S5 **181** ≪ S2-B 21,349 <
  S1 26,797 < S3 47,296 ≪ S4 137,778 < S2-A 173,623 (S5 beats S2-B 118×,
  S3 260×).
- **5L lsm (SF=3850) ms/q**: S5 **1,561** < S2-B 2,014 < S2-A 14,109 <
  S3 16,570 < S1 26,002 ≪ S4 92,711.

See `RUNS.md` and [`../ACOL_ACOLI_PLAYBOOK.md`](../ACOL_ACOLI_PLAYBOOK.md).

## Sibling Docs

Every non-`CLAUDE.md` Markdown in `q10i/` and `q10i/plans/` (the latter
has no `CLAUDE.md`; indexed here as the nearest ancestor). Read each on
the trigger described:

- [`plans/family_logical.dot`](plans/family_logical.dot) — **shared
  logical plan for S1, S2, S3**: COL chain × INVOICE per-lineitem
  probe; partitioned aggregate by linked invoice's `i_status`.
- [`plans/family_s3_physical.dot`](plans/family_s3_physical.dot) —
  **S3 physical specialisation** over the COLI MI (CONVENTIONS Rule 10
  Pattern B for the invoice prefix).
- [`plans/family_s4_baseline.dot`](plans/baseline_s4.dot) — **S4
  baseline** (HashJoin chain with INVOICE as its own HashJoin
  relation; PK-only builds per Rule 4, chained INL recovery per
  Rule 13).
- [`RUNS.md`](RUNS.md) — perf-run ledger; appended after every Linux
  sweep.

Read [`../q10/CLAUDE.md`](../q10/CLAUDE.md) for the vanilla Track-1
sibling that Q10I extends. Read [`../q5i/CLAUDE.md`](../q5i/CLAUDE.md)
for the closest Track-2 cousin — Q5I established the per-lineitem
invoice-status partition pattern (Pattern B view loader, COLI walker
with `invoice_buf`); Q10I reuses that walker shape with Q10's
per-customer top-20 post-pipeline.

---

## TPC-H Definition

### Original Q10 (§2.4.10 — "Returned Item Reporting")

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
  AND   c_nationkey  = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name,
         c_address, c_comment
ORDER BY revenue DESC
LIMIT 20;
```

### Q10I — Invoice-Extended Variant

```sql
SELECT  c_custkey, c_name,
        SUM(CASE WHEN i_status = 'P' THEN l_extendedprice * (1 - l_discount)
                 ELSE 0 END) AS paid_returns,
        SUM(CASE WHEN i_status = 'O' THEN l_extendedprice * (1 - l_discount)
                 ELSE 0 END) AS open_returns,
        SUM(CASE WHEN i_status = 'L' THEN l_extendedprice * (1 - l_discount)
                 ELSE 0 END) AS late_returns,
        SUM(l_extendedprice * (1 - l_discount)) AS return_revenue,
        c_acctbal, n_name, c_address, c_phone, c_comment
FROM    customer, orders, lineitem, invoice, nation
WHERE   c_custkey    = o_custkey
  AND   l_orderkey   = o_orderkey
  AND   l_invoicekey = i_invoicekey    -- the new join (D1)
  AND   o_orderdate >= ':d'
  AND   o_orderdate <  ':d' + INTERVAL '3' MONTH
  AND   l_returnflag = 'R'
  AND   c_nationkey  = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name,
         c_address, c_comment
ORDER BY return_revenue DESC
LIMIT 20;
```

The total `return_revenue` is preserved as a sanity column: the three
partial sums must add to it (parity invariant — see Phase-3 check).

### Substitution Parameters

| Parameter | Domain | Description | Validation |
|-----------|--------|-------------|------------|
| `:d` | First day of a month in [1993-02-01, 1995-01-01] | Start of the 3-month order window | `1993-10-01` |

Same as Q10. 24 valid months total (Feb 1993 → Jan 1995 inclusive).

**Selectivity notes**: returnflag='R' selects ~25% of lineitems
(generator-determined). A 3-month order window covers ~1/24 of orders
in the [1992, 1998] generated range. `i_status` distribution is
generator-set (~25% paid, ~70% open, ~5% late at SF=1 per the data
generator). At SF=15 the top-20 result set is comfortably stable.

---

## Motivation

### What the extension adds

Q10 surfaces customers returning the most goods over a 3-month window.
Q10I overlays the **payment status of the linked invoice for each
returned lineitem** — partitioning the per-customer return revenue
into three buckets:

- `paid_returns` (`i_status = 'P'`) — return revenue on items already
  paid for; the company **owes a refund**.
- `open_returns` (`i_status = 'O'`) — return revenue on items with
  outstanding invoices; **suppress the upcoming bill**.
- `late_returns` (`i_status = 'L'`) — return revenue on items the
  customer was past-due on; **escalation-worthy** (disputing AND
  not paying).

`return_revenue` (the total) is preserved as the sort key for the
top-20 plus a parity-check column.

### Real-world meaning

Q10 says *who needs a phone call*. Q10I says *who needs which kind of
phone call and what the financial action item is*. Three operationally
distinct cases the manager would otherwise cross-reference manually:

- **High `paid_returns`**: refund flow. Process refunds, follow up on
  product-quality issues.
- **High `open_returns`**: billing flow. Hold or reverse the pending
  invoice before it issues.
- **High `late_returns`**: collections flow. Customer is both
  withholding payment AND returning goods — possible fraud, chronic
  dispute, or churn risk; escalate before further extending credit.

### Why Q10I is a natural COLI showcase

- **C+O+L+I footprint**: Q10 already joins Customer × Orders ×
  Lineitem; adding INVOICE via the existing `l_invoicekey` link fills
  out the COLI MI's full 4-table set without changing the operator
  shape.
- **§3.1.2-ish per-lineitem dimension probe**: invoice contributes
  exactly one row per surviving lineitem via `l_invoicekey =
  i_invoicekey`. Inside the COLI MI's custkey group, all invoices for
  the customer scan **before** the first lineitem (sentinel ordering
  `customer=1 < invoice=2 < orders=3 < lineitem=4`), so the walker
  builds `invoice_buf[invoicekey → i_status]` during `on_invoice` and
  looks up `i_status` per surviving lineitem at `on_lineitem` —
  CONVENTIONS Rule 10 Pattern B, the same shape Q5I uses.
- **Per-customer top-20 cardinality**: ~150K customers at SF=1 before
  the LIMIT 20, so the post-aggregate sort+top-N step is non-trivial
  and the comparison across S1–S4 must include sort time honestly.
  Distinguishes Q10I from Q3I (per-order top-N) and Q5I (per-nation
  aggregate).
- **Four independent partial aggregates**: routed by `i_status` on the
  linked invoice — exercises the walker's ability to feed multiple
  accumulators per record-type variant in one pass without inflating
  the per-row work.

---

## Cardinality Structure

Q10I uses framing #1 (pure-hierarchical along the COL chain) per
PLAYBOOK §3.5 step 4, with INVOICE attached as a **per-lineitem
dimension probe** (one INVOICE row per surviving lineitem via
`l_invoicekey`). It is *not* §3.1.2 in the strict sense (no sibling
sub-aggregate reduces to a per-customer scalar) and *not* §3.1.3 in
the genuine-tree sense (INVOICE does not co-locate as a sub-hierarchy
contributing M:N rows). The MI benefit is hierarchical-prefix scan
locality on the COL chain *plus* invoice co-location at the custkey
level so the per-lineitem invoice lookup is a buffered map probe (no
B-tree seek per lineitem in S3).

Anti-pattern #27 disclaimer: do not import "no sibling shortcut"
wording — there is no sibling sub-aggregate here. The invoice arm
emits one row per surviving lineitem, not one row per customer.

C→O→L cardinality (SF=1 reference):

- C → O: 1:N, ~10 orders per customer.
- O → L: 1:N, ~4 lineitems per order.
- Total chain before filters: ~6M lineitems.
- After orderdate filter (1/24 ≈ 4%): ~250K.
- After returnflag='R' (~25%): ~60K surviving return lineitems.

Invoice arm (per lineitem):

- Each lineitem carries `l_invoicekey` → exactly one INVOICE row.
- Each customer has ~20 invoices total (2 invoices/order × 10
  orders/customer).
- Per surviving lineitem: one `invoice_buf` map lookup → `i_status`
  → route revenue.

NATION: 25 rows, attaches as a per-customer INL on PK at emit time —
no in-memory map, no `nation_set` (Q10I has no region filter). See D9.

---

## Storage Structure Options

| S | Strategy | Secondary structure | Join strategy | Params baked in |
|---|----------|---------------------|---------------|-----------------|
| S1 | Split indexes + BinaryMergeJoin | Custkey-sorted secondaries: `orders_coli_t`, `lineitem_coli_t` (widened per D14); per-lineitem seek on `invoice_coli_t[(custkey, l_invoicekey)]` | 2-BMJ chain C⋈O⋈L on custkey-extended prefix; per-emit invoice seek; NATION INL at emit | none |
| S2 | Pipeline view + sequential scan | `q10i_pipeline_view_t` keyed by `(custkey, orderkey, linenumber)`; `i_status` FD-attached at view-load time; wide customer cols FD-attached | Sequential view scan → per-order SUM (D10) → orderdate filter → per-customer SUM (4-way partition by i_status) → NATION INL → TopN(20) | none (predicate-hoisted) |
| S3 | COLI MI (4-table merged index) | `COLIPipeline` — `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` | Sequential group-walk: invoice prefix → `invoice_buf` (Rule 10 Pattern B); per-order date filter; per-lineitem `i_status` lookup → 4 partial aggregates; incremental top-20 emit at on_group_end | none |
| S4 | Hash join baseline | Base tables only | C HashBuild → date-filtered O probe → C⋈O; HashBuild on C⋈O.o_orderkey → returnflag-filtered L probe; HashJoin C⋈O⋈L ⋈ INVOICE on `l_invoicekey = i_invoicekey`; per-customer HashAggregate of 4 partial sums; NATION INL at emit; TopN(20) | none |
| S5 | aCOLI MI (pre-aggregated, drop-children) | `MergedAdapter<customer_coli_t, orders_acoli_q10i_t>` — full customer + one `orders_acoli_q10i_t` per order with returned revenue > 0 (no lineitems/invoices) | Hand-rolled `acoli_group_walk` (`next_raw` + tag switch, no `std::variant`); per-order baked `{paid,open,late}` read directly, orderdate filter live, per-customer SUM, NATION INL, TopN(20) | `l_returnflag='R'` + the `i_status` P/O/L partition (spec constants) |

**S5 (D3, revised 2026-05-25 — overturns the original "omitted").** The
original D3 only considered the **per-customer** grain, where baking is indeed
unsound (the parameterised `o_orderdate` window sits *between* the customer and
order grain — you can't subtract out-of-window orders from a baked customer
total). But the **per-order** grain *is* sound (ACOL_ACOLI_PLAYBOOK §2 grain
corollary): `l_returnflag='R'` and the `i_status` P/O/L partition are TPC-H spec
constants, and `o_orderdate` applies at order grain, *above* the per-order
aggregate. So S5 bakes per-order `{paid,open,late}` returned revenue and applies
the date window at query time. Following Q10's aCOL (the playbook's "S5 done
right"): **drop the lineitems *and* invoices** (both consumed into the buckets)
and **hand-roll the walker** — Q3I's generic-scan aCOLI is the cautionary tale.
The fair S2 view (`q10i_pipeline_view_preagg_t`, per-order, `--q10i_view_variant=
preagg`) shares the same load-time bucketing and is the comparison baseline for
S5 (playbook §6). See [`../ACOL_ACOLI_PLAYBOOK.md`](../ACOL_ACOLI_PLAYBOOK.md)
and [`../q10/PERFORMANCE.md`](../q10/PERFORMANCE.md).

---

## Plan Descriptions

**Logical joins (Rule 12)** — shared across all four storage
structures:

- **#1** `CUSTOMER ⋈ ORDERS on c_custkey = o_custkey`
- **#2** `ORDERS ⋈ LINEITEM on (custkey, o_orderkey) = (custkey,
  l_orderkey)` — custkey extended via FD (LINEITEM inherits
  `c_custkey` through its parent order; matches the COLI MI's
  custkey-prefixed lineitem key structure)
- **#3** `LINEITEM ⋈ INVOICE on (custkey, l_invoicekey) = (custkey,
  i_invoicekey)` — custkey extended via FD (both LINEITEM and
  INVOICE inherit `c_custkey` through their parent records; matches
  the COLI MI's custkey-prefixed invoice key structure).
  Per-lineitem dimension probe; INVOICE contributes only
  `i_status` downstream
- **#4** `CUSTOMER ⋈ NATION on c_nationkey = n_nationkey`
  — lowered to per-customer INL on NATION PK (Rule 13); no
  in-memory map

S3 fuses #1, #2, #3 into the single `coli_group_walk` visitor; #4
runs at emit time. S1 lowers #1+#2 as a 2-BMJ chain over custkey-
sorted secondaries plus a per-emit invoice seek for #3; #4 at emit.
S2 reads C×O×L pre-materialised from the view (with #3 already
resolved at view-load); #4 at emit. S4 realises #1+#2 as PK-only set
builds (Rule 4) + INL recovery (Rule 13), #3 as its own HashJoin
relation with PK-only invoice build + INL `i_status` recovery, #4 at
emit.

### How the four approaches differ

**S3 (COLI MI + COLIGroupWalk)** is the tightest expression. Within
each custkey group, the walker sees `customer → invoice* → (order →
lineitem*)+` in byte-lex order. `on_invoice` builds
`invoice_buf[invoicekey → i_status]` (Pattern B Rule 10) so by the
time `on_lineitem` fires, every surviving lineitem's `i_status` is a
local hashmap lookup. `on_order` applies the orderdate filter;
`on_lineitem` applies returnflag, looks up `i_status`, routes revenue
into the correct partition, and always adds to `return_revenue`. At
`on_group_end`, finalise the four sums for the customer, resolve
`n_name` via NATION INL on PK, and offer to a bounded top-20 sink.

**S1 (split secondaries + 2-BMJ chain + per-emit invoice seek)** runs
the same logical plan as S3 over three custkey-sorted streams plus a
per-emit invoice seek. BMJ #1 joins `customerh_t ⋈ orders_coli_t` on
custkey; BMJ #2 joins the BMJ-#1 output ⋈ `lineitem_coli_t` on
`(custkey, orderkey, linenumber)`. Each emitted lineitem seeks
`invoice_coli_t[(custkey, l_invoicekey)]` via its custkey-prefixed
key (custkey already in hand from BMJ #1) and reads `i_status`. The
post-pipeline aggregator chain is identical to S3.

**S2 (materialised pipeline view + Pattern-B loader)** caches one
`q10i_pipeline_view_t` row per lineitem with the wide customer
payload + `i_status` FD-attached at load time. The Pattern-B loader
**reuses** the S3 group-walk path with parameterised filters dropped
(orderdate and returnflag), emitting one view row per surviving
lineitem. At query time, sequential view scan → returnflag filter →
per-order SUM (the intermediate per-order step is required because
orderdate is hoisted out of the view and applies at order
granularity — D10) → orderdate filter → per-customer 4-way SUM →
NATION INL → TopN(20). The view is reusable across all DATE param
sets.

**S4 (HashJoin chain baseline)** uses base tables only. Build on
CUSTOMER (PK-only `cust_set<c_custkey>` per Rule 4) → probe
orderdate-filtered ORDERS → C⋈O. **On the first join with customer
(at #1), recover the customer wide payload via INL on customer PK
and attach (`c_name`, `c_acctbal`, `c_address`, `c_phone`,
`c_comment`, `c_nationkey`) to the joined record** so the wide
cols flow downstream with the per-custkey state — not deferred to
emit. Build hash on C⋈O.o_orderkey (PK-only) → probe
returnflag-filtered LINEITEM → C⋈O⋈L stream. Then a proper HashJoin
(inner) C⋈O⋈L ⋈ INVOICE on `l_invoicekey = i_invoicekey`: build a
**PK-only** `invoice_set<i_invoicekey>` (Rule 4); probe with the
C⋈O⋈L stream; recover `i_status` per surviving lineitem via INL on
invoice PK (Rule 13 chained-INL pattern, mirrors Q5 S4's
`c_nationkey` recovery). HashAggregate per c_custkey with 4 partial
sums + already-attached customer cols. NATION INL at emit (only
`n_name` needs lookup; cached in `nationkey_to_name`). TopN(20).

### Filter Pushdown Principle

See the canonical rule in [Filter Pushdown](../OPERATORS.md#filter-pushdown).
Q10I-specific:

Every parameterised filter is pushed as far down the operator graph as
possible, **stopping only at secondary structures** so they remain
reusable across param sets (predicate hoisting per PLAYBOOK §3.5 step 7).

- The COLI MI, COLI custkey-sorted secondaries, and
  `q10i_pipeline_view_t` are all loaded **without** applying the
  orderdate window. A new DATE triggers a new query, not a new load.
  **No parameterised filter may be baked into any secondary.**
- `l_returnflag = 'R'` is spec-hardcoded but **not** baked at view-
  load time either — keeping it live preserves the secondary's
  reusability for any future Q10-shaped query that might want a
  different returnflag. (S2 view stores `l_returnflag`; the query
  applies the filter at scan time.)
- `i_status ∈ {'P','O','L'}` is the **partition key**, not a filter.
  Every surviving lineitem routes to *some* bucket; no `i_status`
  predicate is pushed.
- `o_orderdate ∈ [d, d+3mo)` fuses with `TableScan(ORDERS)` /
  walker `on_order`; pushed below ORDERS HashBuild in S4.
- `l_returnflag = 'R'` fuses with `TableScan(LINEITEM)` / walker
  `on_lineitem`; pushed below LINEITEM probe in S4.
- D9: NATION join is always an **INL on PK** — no in-memory map,
  no `nation_set` (Q10I has no region filter, so the set would be
  degenerate over all 25 rows).
- D10 callout: S2's hoisted orderdate forces the intermediate
  per-order aggregator step before the per-customer roll-up — the
  filter applies at order granularity, so lineitems must regroup
  to their order before the orderdate prune.
- D1: the invoice arm is unfiltered by the orderdate window via
  any direct predicate. Lineitem orderkey filter (via ORDERS scan
  in the chain) is what implicitly determines which invoices are
  consulted (only invoices linked to surviving return lineitems
  are read), so the per-customer invoice rollup naturally aligns
  with the chain's surviving customers without a separate filter.

---

## Required Record Types

Composition with sibling queries: **share record types with Q10 and
Q5I wherever the shape is identical.** Per PLAYBOOK §3.5 step 8, the
relationship is composition (DRY), not inheritance.

Reused verbatim from `views_coli.hpp`:

- `customer_coli_t` (id=30) — carries the full Q10I customer output
  payload (`c_name`, `c_address`, `c_nationkey`, `c_phone`,
  `c_acctbal`, `c_comment`). No change required.
- `orders_coli_t` (id=1) — carries `o_orderdate`. No change required.
- `invoice_coli_t` (id=33) — carries `i_totaldue`, `i_status`.
  Adequate as-is; only `i_status` is consumed by Q10I (and an
  in-memory hashmap key on `invoicekey`).

Modified in place (Phase 1, flagged below as D14):

- `lineitem_coli_t` (id=32) — currently
  `{l_extendedprice, l_discount, l_shipdate}`. Q10I needs
  `l_returnflag` (filter) and `l_invoicekey` (invoice-probe key)
  added in place per project-pushdown trigger #1. Re-run
  `test_load_coli_lsm` + Q3I/Q5I regression after the widening.

For S1 (split secondaries) — same custkey-sorted types Q5I uses
(`customer_coli_t` primary + secondaries on `orders_coli_t` /
`lineitem_coli_t` / `invoice_coli_t`).

New for Q10I (S2 view + final aggregate):

- `q10i_pipeline_view_t` — Key `(custkey, orderkey, linenumber)`,
  one row per lineitem. Payload: `l_extendedprice`, `l_discount`,
  `l_returnflag`, `o_orderdate`, `i_status` (resolved at view-load
  via the invoice-buf walk; FD-attached per lineitem), plus the wide
  customer columns (`c_name`, `c_address`, `c_nationkey`, `c_phone`,
  `c_acctbal`, `c_comment`). `n_name` **NOT** stored — resolved via
  NATION INL at emit (D9). All `Varchar<N>` (POD), never
  `std::string` (libstdc++ `std::string` is non-standard-layout;
  would corrupt across insert/getScanner via memcpy-based
  record_traits).
- `q10i_agg_row_t` — derives from `q10_agg_row_t` (Q10 sibling) by
  adding `paid_returns`, `open_returns`, `late_returns` (D13).
  In-memory only; no SKBuilder / ADD_RECORD_TRAITS.

### Side-table runtime structures

- `invoice_buf` — `std::unordered_map<Integer, Varchar<1>>` from
  `invoicekey` to `i_status`, populated per custkey group in S3's
  `on_invoice` and cleared at `on_group_end`. Pattern B (Rule 10).
  Per-customer size ≈ 20 entries at SF=1; the buffer is freed
  before the next customer's group begins.
- NO `nation_set`, NO `nation_name_map`. NATION is exclusively an
  INL probe at emit time (D9).

---

## Locked-in Design Decisions (Phase 0)

Numbered for cross-referencing from §Plan Descriptions and the
DOTs. D6–D10 mirror Q10's identically; the rest are Q10I-specific.

- **D1.** New join key: `l_invoicekey = i_invoicekey` (per-lineitem
  dimension probe). Invoice contributes only `i_status` downstream.
- **D2.** Three partial aggregates per customer (`paid_returns`,
  `open_returns`, `late_returns`) routed by `i_status`.
  `return_revenue` (the total) is the sort key + parity check.
- **D3.** S5 omitted (orderdate window is parameterised; per-customer
  pre-totals don't compose).
- **D4.** S3 substrate: COLIPipeline (4-table COLI MI). Pattern B
  Rule 10 — `invoice_buf` populated during `on_invoice`, consumed
  during `on_lineitem`.
- **D5.** S3 grouping: per-customer accumulator with incremental
  top-20 emit at `on_group_end`. Bounded sink avoids ~150K-customer
  intermediate vector.
- **D6.** S3 visitor is **bespoke**, built on `coli_group_walk`. Do
  NOT subclass `q3_family::Q3FamilyVisitor` (memory:
  `feedback-col-walk-is-shared-util`).
- **D7.** S2 view loader: Pattern B. Reuses S3 group-walk with
  parameterised filters dropped (memory:
  `feedback-view-loader-reuses-query`).
- **D8.** S2 view payload: per-lineitem with `i_status` FD-attached
  + wide customer columns FD-attached; no `n_name`.
- **D9.** NATION uniformly INL on PK at emit time. No `nation_set`,
  no `nation_name_map` (memory: `feedback-nation-lookup-pattern`).
- **D10.** S2 post-view aggregation has an **intermediate per-order
  step** (orderdate hoisted, applies at order granularity).
- **D11.** S4 invoice arm: **own HashJoin relation** with PK-only
  `invoice_set<i_invoicekey>` build + INL `i_status` recovery (Rule
  4 + Rule 13). NOT a side-map probe inline in the lineitem scan.
  Customer wide payload is recovered via INL at the **first join
  with customer** (#1, C⋈O) — attached to the joined record and
  carried forward, not deferred to emit — mirrors Q5 S4's chained-
  INL pattern.
- **D12.** S1 BMJ chain: 3-way (C⋈O⋈L) over custkey-sorted COLI
  secondaries + per-emit invoice seek (Q5I-isomorphic).
- **D13.** Composition with Q10: `q10i_agg_row_t` extends
  `q10_agg_row_t` (PLAYBOOK §3.5 step 8 — composition, not
  inheritance).
- **D14.** `lineitem_coli_t` widening (Phase 1): add `l_returnflag`
  and `l_invoicekey` to the secondary payload per project-pushdown
  trigger #1. Re-run `test_load_coli_lsm` + Q3I/Q5I regression.

---

## Open Questions

- **Per-status invoice-distribution at SF=1 / SF=15**: confirm
  enough non-zero `paid/open/late` mix exists for the parity test to
  be meaningful (i.e., that `paid_returns + open_returns +
  late_returns == return_revenue` is a real constraint, not all-
  zeros-pass). Verify at Phase 3 with a content-walk dump.
- **Composition of Q10 / Q10I visitor**: where the `on_invoice`
  hook bolts onto a Q10 base — defer to Phase 1. Q10's visitor
  doesn't have an `on_invoice` hook; either lift it via CRTP or
  fork. Same shape question as Q10's own Phase-1 visitor-reuse
  open question.
- **`lineitem_coli_t` widening regression scope** (D14): when the
  field add lands, re-run `test_load_coli_lsm` (cardinality / FK
  / sentinel-ordering) and the full Q3I / Q5I parity tests. The
  payload widening shifts byte offsets; any test that hard-codes
  expected row-size bytes needs review.
- **`q10i_pipeline_view_t` payload bloat** (carried forward from
  the original sketch): per-lineitem FD-duplication of the wide
  customer payload (~250 bytes of customer cols × ~30 lineitems
  per customer) inflates S2 storage. Acceptable at 5L scale per
  D8; revisit if S2 becomes the binding cost in the cell.

---

## Implementation Phases

- **Phase 0** (this commit) — design doc + 3 DOT plans. Per the
  Q10I Phase 0 plan. Zero `.hpp` / `.cpp` / `.tpp` changes.
- **Phase 1** — skeleton: 8 per-query files (`views.hpp`,
  `workload.hpp`, `load.tpp`, `query.tpp`, `per_structure_workload.hpp`,
  `executable_{rocksdb,leanstore}.cpp`, CMake +
  `generate_targets.py`). `lineitem_coli_t` widening (D14) lands
  here. Compiles cleanly; query stubs return 0; `test_query_q10i_lsm`
  reports digest-0x0 parity vacuous `[OK]`.
- **Phase 4 §7.1** ✅ — S3 `query_by_merged` via `coli_group_walk` +
  bespoke Q10I visitor (D4–D7) + bounded top-20 sink (D5).
- **Phase 4 §7.3** ✅ — S2 `query_by_view` over Pattern-B-loaded
  view (D7); per-customer aggregator → top-20 sink.
- **Phase 4 §7.5** ✅ — S4 `query_by_hash` — PK-only `ord_set` +
  INL recovery (Rule 13) for customer payload + i_status.
- **Phase 4 §7.2** ✅ — S1 `query_by_base` — chain-scan over
  custkey-sorted COLI splits + per-emit invoice point-seek.
- **Phase 5–8** ✅ — `per_structure_workload.hpp` alias-only,
  executables, `test_query_q10i_{lsm,btree}` strict parity, CMake
  + `generate_targets.py` wiring (all landed in Phase 1).
- **S5** — omitted by design (D3).
- **Linux perf sweep** — 5L cell only; tracked in
  `LINUX_PENDING.md` once Phase 4 lands.

---

## Contingency

Pair-fates with Q10 (paper story needs both). If Phase 1+ stalls or
the 5L cell doesn't return clean numbers before the paper deadline:

- §Status framed from the start as "Phase 0 complete; scope = 5L
  cell only; results pending".
- No predictive perf claim in §Motivation. Preserve the neutral
  tone of this doc.
- Cross-reference Q10 (paired fate; the pair stand or fall
  together).
- D3 (S5 omitted) and D9 (NATION = INL only) read as deliberate
  scope choices, not omissions.

If results do land, this doc becomes the frozen target for code
review. If they don't, it reads as a self-contained future-work
appendix.
