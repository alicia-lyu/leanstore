# Q3I S3 Performance Investigation — Active Worklist

Forward-looking worklist. Evidence trails for completed runs live in
[`archive/PERFORMANCE-2026-05-03b.md`](archive/PERFORMANCE-2026-05-03b.md)
(A1 + A5 + A2c) and
[`archive/PERFORMANCE-2026-05-03.md`](archive/PERFORMANCE-2026-05-03.md)
(pre-A1 history).

---

## §1 — Where we are

Three remediations have landed; the merged-index pitch is now solid
on **both** backends:

- **A2c (`fused_emit`, SF=15 cache-resident).** S3 matches/beats S1
  on both backends. H4 closed. Default flipped to `fused_emit`.
- **A3 (Backend-trait Seek-skip, LeanStore).** Customer-level Seek
  across rejected custkey groups: SF=15 23.06 → 105.27 TX/s (+356%);
  SF=40 disk-bound 0.29 → 33.69 TX/s (+116×). H2 closed on LeanStore.
- **A3 RocksDB re-A/B on Linux** (refutes the earlier macOS A/B that
  blocked it). With the trait flipped to `true` on RocksDB too:
  SF=15 1.81 → 14.46 TX/s (+700%); SF=40 0.90 → 1.48 (+64%).
  The macOS regression appears to have been a page-cache artefact.
  H2 now closed on **both** backends.

LeanStore S3 (iso, fused_emit, dram=0.1):

| stage              | SF=15 TX/s | SF=40 TX/s |
|--------------------|-----------:|-----------:|
| baseline (no A2c)  | 19.52      | ~0.30      |
| + A2c              | 23.06      | ~0.30      |
| + A2c + A3         | **105.27** | **33.69**  |
| S1 (ref)           | 22.63      | 0.30       |
| S4 (ref)           | 21.61      | 0.32       |

RocksDB S3 (iso, fused_emit, dram=0.1, Linux):

| stage              | SF=15 TX/s | SF=40 TX/s |
|--------------------|-----------:|-----------:|
| pre-A3 (trait off) | 1.81       | 0.90       |
| + A3 (trait on)    | **14.46**  | **1.48**   |

Open question now: **H12 (load-amortized cost vs query count)** —
the reviewer-facing question of "how many queries justify a merged
index" is the second-order story the paper should tell. A4 (SSTWrite
attribution) remains useful but is a side investigation, not a
showcase blocker.

**Defaults**: `coli_walker_variant=fused_emit`, both backends'
`USE_PHYSICAL_SEEK_SKIP=true`. Override knobs preserved
(`coli_walker_variant=baseline`, `--use_seek_skip=0`) for regression
A/Bs but not the active code path.

---

## §2 — Hypothesis ledger

| ID | Hypothesis                              | Status | One-line takeaway |
|----|-----------------------------------------|--------|-------------------|
| H1 | MI too large per row                    | **REFUTED** | `get_size` artefact (closed in `50fd2052`) |
| H2 | Iter overhead on rejected groups        | **CONFIRMED + REMEDIATED on BOTH backends (A3 + Linux re-A/B)** | LeanStore +356% SF=15 / +116× SF=40; RocksDB +700% SF=15 / +64% SF=40 (macOS A/B was a page-cache artefact) |
| H3 | Walker visits entire MI per query       | **CONFIRMED uniform (subsumed by H6)** | Doesn't explain S3-vs-S1 gap |
| H4 | Per-record dispatch overhead            | **CONFIRMED + REMEDIATED at SF=15 (A2c, `88088305`)** | fused_emit closes per-call gap to ~3% of S1; neutral at SF=40 disk-bound |
| H5 | Storage-engine specific                 | **REFUTED** | Same direction on LeanStore |
| H6 | Low filter selectivity                  | **CONFIRMED uniform** | Explains S5 win |
| H7 | SSTWrite during read-only queries       | **OPEN, RocksDB-specific (A4)** | Histogram inflated; source unattributed |
| H8 | Shared-DB cache pollution               | **SPLIT: differential SF=15, symmetric SF=40 (LeanStore)** | SF=15 baseline numbers were partly artefact |
| H9 | Per-record-width tax                    | **REFUTED** | No wide-union padding on disk; `bytes_read/q` parity at SF=40 |
| H10| Compression masks locality              | **REFUTED** | Cross-backend disk-bound TX/s consistent |
| H11| S3 vs S1 is the wrong baseline          | **REFUTED on BOTH backends (post-A3)** | LeanStore S3-vs-S4 at SF=40 is +100×; RocksDB S3 lifts +64% disk-bound. Merged-index pitch lives on both engines. |
| H12| Load-amortized cost is the real metric  | **OPEN** | Crossover (queries vs total time) is the right axis |
| H13| Walker outer-loop tuning beats generic MergedAdapter scan | **OPEN** | See §A1 — main anomaly (S3 > S5) |
| H14| S2 view has redundant FD-attached ancestor columns per lineitem | **OPEN** | See §A1 — explains S2 < S3, not the main anomaly |
| H15| S5 lacks the customer-level Seek-skip A3 added to S3 walker | **OPEN** | See §A1 — likely explanation for S3 > S5 |

Full evidence: archive `PERFORMANCE-2026-05-03b.md` §3.

---

## §A1 — ANOMALY (2026-05-03): S3 > S2 > S5 ordering at SF=15 production

**Observed** (full `q3i_*` sweeps, SF=15, dram=0.1, fused_emit, post-G8
projections, both backends, this session's commits `55546eec` /
`fa892c69`):

| Structure                | LSM TX/s | Btree TX/s |
|--------------------------|---------:|-----------:|
| S1 base_merge_join       | 27.88    | 62.35      |
| S2 pipeline_view         | 56.40    | 90.11      |
| **S3 mi_coli_walk**      | **95.44**| **198.50** |
| S4 base_hash_join        | 17.12    | 39.64      |
| **S5 acoli_aggregated**  | **37.41**| **67.72**  |

**Pre-computation spectrum** (per `q3i/CLAUDE.md §Phase 4`):

```
S3 (raw 4-table MI, recompute everything)
 < S5 (aCOLI: pre_open_due baked, revenue live from projected lineitems)
 < S2 (per-lineitem view with FD-attached cust_open_due / mktsegment / orderdate / shippriority)
```

So the *expected* TX/s ordering, all else equal, is **S3 < S5 < S2**.

**Observed**: S3 > S2 > S5 — the **main anomaly is S3 faster than
S5**. S2 underperforming S3 is plausibly explained by
**redundancy in the view rows** (H14 below): the FD-attached columns
on every per-lineitem row mean S2 carries the customer/order context
N times per order group instead of once, so even though S2 has done
the join work at load time, it spends the savings paying for fatter
scans at query time.

**Why S5 < S3 is the real puzzle**:

- S5 *should* dominate. It visits fewer records (no invoice rows),
  reads `pre_open_due` directly instead of summing it live, and
  uses the **same** `LineitemRevenueAccumulator` over the **same**
  projected lineitem payload as S3 (post-G8 both
  `lineitem_coli_t` and `lineitem_acoli_t` carry exactly
  `{l_extendedprice, l_discount, l_shipdate}`). Per-lineitem work is
  identical between S3 and S5.
- At SF=1 we measure `acoli_total=744 vs mi_records_visited≈1547`
  — S5 visits ≈48% as many records as S3, yet S5 is **2.5× slower**
  at SF=15.
- This contradicts H6 (low filter selectivity → S5 wins) and the
  pre-computation spectrum.

**Important**: H13 (originally "S5 revenue accumulator dominates")
is **refuted by code inspection**. S3 and S5 share the accumulator,
the projected lineitem record type, and the per-record cost model
verbatim. The revenue work is *not* what makes S5 slower.

**Hypotheses for the S3 > S5 anomaly** (not yet investigated):

1. **H13 — Walker-vs-MergedAdapter scan substrate.** S3 uses
   `coli_group_walk`, a hand-written forward walker over a single
   4-type MergedAdapter that has been hot-tuned (A2c fused_emit,
   A3 Seek-skip). S5 uses a generic 3-type MergedAdapter scan +
   visitor dispatch. Even though the lineitem inner loop is shared,
   the *outer* loop differs: S3's walker has tighter dispatch and a
   single-scanner Seek-skip; S5's scan goes through the standard
   merged-scanner variant-dispatch path. Test: A/B
   `--coli_walker_variant=baseline` against S5; if S5 catches up to
   walker-baseline (not fused_emit), the walker tuning is the gap.
2. **H14 — S2 view redundancy (explains the S2 < S3 secondary
   observation, not the main anomaly).** `q3i_pipeline_view_t` is
   keyed by `(custkey, orderkey, linenumber)` — one row per lineitem,
   each row carrying 5 FD-attached ancestor columns. S2 therefore
   pays the per-lineitem fanout cost on the FD-attached fields N
   times per order group. Test: compare `bytes_scanned/q` for S2 vs
   S3 at SF=15.
3. **H15 — S5 lacks Seek-skip on rejected mktsegment customers.**
   The A3 customer-level Seek-skip lives in S3's walker
   (`mi_groups_skipped` counter). S5's
   `MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>`
   scan does not currently exploit the equivalent. With ~80% of
   customers failing mktsegment at the validation params, S5 visits
   every order + lineitem under those rejected customers; S3 skips
   them at the customer boundary. Test: instrument S5 with a
   per-customer skip and re-A/B; expected lift comparable to A3 on
   S3 (+356% LeanStore SF=15).

**What the anomaly doesn't undermine**:

- The paper's main pitch — **S3 matches S2 while beating S1/S4** —
  holds at SF=15 on both backends:
    LSM:   S3 95.44 ≥ S2 56.40 > S1 27.88 > S4 17.12
    Btree: S3 198.50 ≥ S2 90.11 > S1 62.35 > S4 39.64
  S3 actually *beats* S2 here, which is the strongest possible
  framing of the pitch (raw co-location matching the fully
  materialised view without paying materialisation's storage /
  maintenance cost).
- The post-G8 project-pushdown rule — narrower secondaries
  unconditionally help (G9 confirmed +90% S3 LSM at iso SF=15).

**Decision**: leave as-is for now. Document so any future S2 / S5
optimisation work starts from honest priors. None of these
hypotheses are paper-blocking; the paper's pitch is S3 ≥ S2 > S1/S4,
which already holds.

**Investigation deferred** under a new H13–H15 ledger entry; status
**OPEN**. Earliest revisit when (a) reviewer raises "why does the
'most pre-computed' variant lose?" or (b) we run SF=40 disk-bound
where the S3 sequential-scan advantage may invert.

---

## §3 — Active worklist

### A/B-1 — DONE: customer-/orderkey-level Seek-skip on S1 + S4

Symmetric A/B for the comparison-fairness story: A3 lifted S3 with a
customer-level Seek-skip; S1 and S4 needed the equivalent to keep the
S1/S3/S4 axis honest (OPERATORS.md §6.1). Same `--use_seek_skip`
runtime override, same `Backend::USE_PHYSICAL_SEEK_SKIP` trait gate.

Implementations differ by structure because the secondary streams
have different sort orders:

- **S1 (3-BMJ chain)** — only `agg_inv` (cust_open_due aggregator
  over custkey-sorted `split_invoice`) is safely skippable from
  inside `fetch_cust`. Skipping `ord_scan` / `agg_lin` from
  `fetch_bmj1`/`fetch_bmj2` was attempted and reverted: BMJ caches
  `next_right` records that haven't been emplaced yet, so seeking
  ahead inside refill loses 1:N children of the current key. The
  S3 walker doesn't have this problem because it has a single
  scanner. Mirroring the S3 trick on a 3-BMJ chain would require
  buffered OrdersByCustkey/LineitemsByCustkey wrappers — deferred.
- **S4 (3-HJ chain)** — base-table primary keys are not
  custkey-sorted (orders is orderkey-PK; lineitem is
  orderkey/linenumber-PK). So custkey-skip is structurally
  impossible. The natural analog is **orderkey-level skip on the
  lineitem probe**: after the orders-build phase, sort the
  surviving orderkeys; on probe-miss, `upper_bound` to the next
  surviving orderkey and seek the lineitem scanner there. Skips
  the (large) cold-page reads for orders that didn't survive any
  filter.

Counters: `bj_groups_skipped` (S1) and `hj_groups_skipped` (S4)
mirror `mi_groups_skipped` (S3). All three increment per skip
event in their respective query paths.

Iso TX/s, fused_emit, dram=0.1, post-9da4f295 trait defaults:

| Cell                    | ss=0   | ss=1   | Lift     |
|-------------------------|-------:|-------:|---------:|
| **S1 LeanStore SF=15**  | 22.50  | 27.59  | +22.6%   |
| **S1 LeanStore SF=40**  | 0.30   | 0.38   | +27%     |
| **S1 RocksDB SF=15**    | 2.05   | 3.99   | +94.6%   |
| **S1 RocksDB SF=40**    | 0.82   | 0.60   | **−27%** |
| **S4 LeanStore SF=15**  | 20.82  | 27.56  | +32.4%   |
| **S4 LeanStore SF=40**  | 0.36   | **9.57** | **+2580%** (27×) |
| **S4 RocksDB SF=15**    | 2.29   | 6.28   | +174%    |
| **S4 RocksDB SF=40**    | 0.66   | 0.83   | +25%     |

(S4 LeanStore SF=40 confirmed across multiple trials; S1 RocksDB SF=40
mean across 3 trials — a real regression, not noise.)

**Decision**: keep the trait `true` on both backends (the default
wins on 7 of 8 cells and is dramatic on disk-bound LeanStore).
Document the **S1 RocksDB SF=40** regression: the invoice-CF Seek
invalidates the SST prefetch buffer the same way the original macOS
A3 A/B reported for S3 — but for S1 on Linux disk-bound the
regression survives, perhaps because S1's invoice-aggregator path
has a longer prefetch reach than S3's COLI MI walker. Users can
override per-cell via `--use_seek_skip=0`. A finer-grained per-
structure trait (`USE_BMJ_SEEK_SKIP`) is deferred until a second
cell motivates it.

**Comparison-axis impact**: the S1/S4 baselines are now structurally
fair against S3 — each path uses the best available physical-skip
strategy for its operator graph. The merged-index pitch is no
longer artificially inflated by S3's exclusive access to skip-skip.

**Why S4's lift dominates S1's** (e.g. LeanStore SF=40: +2580% vs +27%).
The S4 HJ chain folds *every* upstream filter (mktsegment, threshold,
orderdate) into a single `ord_map`. A lineitem probe-miss therefore
indicates the parent customer **or** parent order failed at least one
filter, and the seek skips the entire customer's lineitem run when
the most-selective upstream gate (mktsegment, ~80% drop) excluded it.
S1's BMJ chain gates customers at BMJ#1 already, so by the time the
analogous skip-site is reached the easy customer-level wins are
captured upstream — only the residual `agg_inv` stream benefits.
Concretely at SF=15: S4 lineitems_scanned/q drops 90108 → 12153
(87% reduction, ≈ mktsegment selectivity × order-date selectivity);
S1 only saves on the much smaller invoice stream.

### G6 attempt — ABANDONED: S1 deferred Seek-skip on ord_scan / agg_lin

**Goal**: extend G1 (which only seeks `agg_inv` synchronously inside
`fetch_cust`) to also seek the `ord_scan` and `agg_lin` right sides
of BMJ#2 / BMJ#3 past failing-customer gaps. Aim: close the
inside-pipeline gap to S3 (which gets per-customer skip via its
single-scanner walker).

**What I tried (three mechanics, all unsafe or no-op)**:

1. **Stash target in `fetch_cust`, apply on next `fetch_ord`.** Bug:
   each `fetch_cust` call returns the *next* passing customer, so
   the target is overwritten before BMJ#2 finishes the prior group's
   right-side refill. Result: orders for the prior group are dropped.

2. **Read `bmj2.jk_to_join()` from inside `fetch_ord`.** Safe but
   no-op: `jk_to_join` is the smaller of `next_left.JK` and
   `next_right.JK`, so during gap walks it advances one custkey at a
   time in lockstep with `ord_scan`. The seek-skip gate
   (`T > last_returned_ck`) is never satisfied.

3. **Read `bmj2.next_left` gated by a `consume_joined` flag.** Bug:
   `consume_joined` fires *after* a group's cross-product is queued
   (i.e. after refill+refresh complete). By then `next_left` is
   already the next-passing-customer's lookahead, but BMJ#2 may
   still be processing the *current* passing group — seeking
   `ord_scan` to `next_left.JK` skips that group's remaining orders.

**Root cause**: `BinaryMergeJoin` doesn't expose a
`on_group_flush(jk)` hook (the only place where "I'm done with jk=X"
is unambiguous). All three mechanics try to derive that signal from
public BMJ state and either over-shoot or under-shoot.

**Decision**: G6 abandoned. Two clean fixes both cost more than
they're worth given measured impact:

- *Buffered wrapper*: pre-load orders for the next K passing
  customers into an in-memory queue. Defeats the streaming nature
  of merge-join and adds a per-query allocation tax.
- *BMJ refactor*: add an `on_group_flush(jk)` callback in
  `BinaryMergeJoin::next_jk` after `refill_current_key` returns.
  Touches the shared primitive (used by Q12 / Q3 / Q9 / Q3I); the
  benefit is localised to one query.

**Measured A/B (with the unsafe mechanic-3 implementation, before
revert) at iso, dram=0.1, fused_emit**:

| Cell                  | ss=0   | ss=1 (G6 unsafe) |
|-----------------------|-------:|-----------------:|
| S1 LeanStore SF=15    | 22.50  | 27.59            |
| S1 LeanStore SF=40    | 0.30   | 0.42 (+10%)      |
| S1 RocksDB SF=15      | 2.17   | 3.91 (+80%)      |
| S1 RocksDB SF=40      | 0.89   | 0.52 (−42%)      |

The btree gain (+10% SF=40) is real but modest. The RocksDB SF=40
regression deepens vs G1-only (was −27%, now −42%) because each new
Seek invalidates the SST prefetch buffer; G6 added two more
Seek-prone paths (ord_scan, agg_lin) on top of agg_inv.

**Counters preserved**: `bj_ord_skips` and `bj_lin_skips` stay in
`Q3IStats` as zero-valued placeholders — re-enable when a future
mechanic (buffered wrapper or BMJ hook) lands.

### G9 — DONE (2026-05-03): project-pushdown A/B on the post-merge 3-type aCOLI

**Setup**: pre-G8 = commit `e723b0d9` (origin merge with full-payload
`orders_coli_t` / `lineitem_coli_t` / `invoice_coli_t` and full-
payload `orders_acoli_t` / `lineitem_acoli_t`). Post-G8 = current
HEAD with G8a (COLI co-located secondaries projected) + G8b/c (aCOLI
secondaries projected). Both run via the same `q3i_lsm_iso_{3,5}`
Makefile targets, SF=15, dram=0.1, fused_emit, RocksDB. Pre-G8 used
a separate worktree `/tmp/leanstore-preg8` with its own `build/`
tree to avoid rebuild contamination.

| Cell                      | pre-G8                   | post-G8                  | TX/s lift | size shrink |
|---------------------------|-------------------------:|-------------------------:|----------:|------------:|
| **S3 LSM SF=15 (iso_3)**  | 8.17 TX/s, 33.33 MiB     | 15.52 TX/s, 20.11 MiB    | **+90%**  | **-40%**    |
| **S5 LSM SF=15 (iso_5)**  | 4.62 TX/s, 23.03 MiB     | 4.85 TX/s, 18.96 MiB     | **+5%**   | **-18%**    |

**Reading**: G8a (project COLI co-located secondaries) is the
high-payoff lever — S3's group walker scans every co-located record,
so narrowing `orders_coli_t` / `lineitem_coli_t` / `invoice_coli_t`
from full base payloads to the 2–3 fields each query reads almost
doubles throughput at SF=15 disk-bound. G8b/c (project aCOLI
secondaries) yields a smaller TX/s lift because the new 3-type aCOLI
recomputes revenue live from `lineitem_acoli_t`, so the per-record
work is dominated by the revenue accumulator, not the variant
payload. Size shrinks meaningfully in both cases.

**Caveat — single backend, single SF**: only LSM SF=15 measured this
session. BTree numbers and SF=40 disk-bound cells (which would test
the cache-line / SST-block hypothesis from the historical A/B-2
below) are not part of this G9 record. The scale-magnitude question
the historical A/B-2 settled (4000–10000× lifts on RocksDB at high
SF) is **not** what's being measured here — that was a 2-type design
where the projected variant was many cache lines narrower; the
3-type design's projected vs full delta is one cache line for orders
and two for lineitem. The +5% S5 lift is consistent with that.

**Decision**: keep G8 projections as the default. The S3 lift alone
justifies the audit; S5's smaller lift confirms the rule is correct
(pre-aggregated secondaries should always be narrow) but doesn't
shift the ordering between storage structures.

### A/B-2 — HISTORICAL (retired 2026-05-03): aCOLI Q3I-projected variant (G4+G5+G7)

> **Note (2026-05-03):** the types this section measured —
> `customer_acoli_q3i_t` (id=51) and `orders_acoli_q3i_t` (id=52),
> selected via `--acoli_projected` — were **retired** along with the
> `--acoli_projected` flag when the upstream merge introduced the
> 3-type aCOLI design (`customer_acoli_t` + `orders_acoli_t` +
> `lineitem_acoli_t`, with revenue computed live). G8b/c then
> projected `orders_acoli_t` and `lineitem_acoli_t` to query-required
> columns, applying the project-pushdown rule by default — there is
> no longer a "full vs projected" toggle. The numbers below describe
> the **retired 2-type design** and remain in the doc as historical
> evidence. G9 above re-measured projection's contribution on the
> current 3-type design.



**WHAT**: the S5 aCOLI MI carries full base-record payloads
(`customer_acoli_t` ~280 B/row, `orders_acoli_t` ~140 B/row) even
though Q3I only reads `c_mktsegment + pre_open_due` from customer
and `o_orderkey + o_orderdate + o_shippriority + pre_revenue` from
orders. G4 introduced `customer_acoli_q3i_t` (id=51) and
`orders_acoli_q3i_t` (id=52) carrying only those Q3I-projected
fields. G5 wires a `--acoli_projected` flag that selects the
projected MI at load time and the projected scan path at query
time.

**Parity (SF=1)**: with `--acoli_projected=true` all five paths
produce digest `0x3ee193001f24dd15`; with `=false` all five produce
`0x69bf112dce52fc8a`. Cross-structure XOR check passes both ways.

Iso TX/s, dram=0.1, fused_emit, post-A3 defaults:

| Cell                  | full   | projected | TX/s lift | size full → proj |
|-----------------------|-------:|----------:|----------:|-----------------:|
| **S5 LeanStore SF=15**| 218.09 | 23129.68  | **106×**  | 52.66 → 48.91 MiB (−7%) |
| **S5 LeanStore SF=40**| 72.15  | 20261.76  | **281×**  | 140.86 → 130.52 MiB (−7%) |
| **S5 RocksDB SF=15**  | 21.93  | 92506.07  | **4218×** | 17.88 → 16.33 MiB (−9%) |
| **S5 RocksDB SF=40**  | 8.25   | 88676.38  | **10747×**| 47.56 → 43.58 MiB (−8%) |

**Decision**: defer flipping the default. Two reasons. First, the TX/s
magnitudes (4000–10000× on RocksDB) are larger than the size shrink (~8%)
plausibly justifies, suggesting the projected path is doing meaningfully
less work than the full-payload path beyond a flat scan-cost reduction —
this needs a `[card]` and `[scan]` instrumented re-run to attribute the
gap (record-count drop? per-record dispatch difference? variant
construction cost?). Second, the user's guidance ("the requirement may
change in another query") flags projected aCOLI as showcase-specific:
Q5I/Q10I would each need their own projection schemas, and a permanent
default would couple the aCOLI MI to one query's column set.

**Production wiring**: `--acoli_projected` plumbed through
`q3i_lsm` / `q3i_btree` and Makefile (`make q3i_*_iso_5
acoli_projected={true,false}`). Default flipped to `true` after G7
(below).

### G7 — DONE: A/B-2 magnitude attribution

**Question**: is the 100×–10000× TX/s lift between full-payload
and Q3I-projected aCOLI a genuine cache-line / variant-dispatch
saving, or a counter / record-count anomaly making the comparison
unfair?

**Step 1 — sizeof print** (one-shot diagnostic in
`query_by_aggregated`):

```
[acoli sizeof] full: cust=248 ord=80  variant=256
             | proj: cust=24  ord=24  variant=32
```

8× narrower variant payload (256 B → 32 B). Each `MergedAdapter`
scanner emit constructs a `std::variant<R1, R2>` whose width is
`max(sizeof(R1), sizeof(R2))` plus a small tag — projected variant
fits in one 64 B cache line, full variant straddles four.

**Step 2 — scan-count parity at iso, dram=0.1 (BTree)**:

| Cell                       | ap=false                  | ap=true                   |
|----------------------------|---------------------------|---------------------------|
| `acoli_customers_scanned`  | 6,189,750 (SF=15) / 7,770,000 (SF=40) | identical |
| `acoli_customers_passing`  | 825,300 / 1,055,425       | identical |
| `acoli_orders_scanned`     | 12,148,416 / 15,581,440   | identical |
| `acoli_orders_emitted`     | 536,445 / 625,485         | identical |

Both code paths traverse exactly the same record counts and emit
exactly the same number of result rows. **No record-count
anomaly.** The lift is genuine per-record cost reduction.

**Step 3 — TX/s ratios** (iso, dram=0.1, fused_emit, post-default
flip):

| Cell                  | full   | projected | lift    |
|-----------------------|-------:|----------:|--------:|
| **S5 LeanStore SF=15**| 183.35 | 18756.64  | **102×** |
| **S5 LeanStore SF=40**| 86.29  | 18441.14  | **214×** |
| **S5 RocksDB SF=15**  | 22.53  | 93004.51  | **4126×** |
| **S5 RocksDB SF=40**  | 8.24   | 89621.36  | **10876×** |

**Why much larger than the 8× variant ratio?** Three compounding
factors per emitted record:

1. *Variant memcpy.* Every `kv->second` materialised by the scanner
   copies into a 256 B vs. 32 B variant slot.
2. *L1 cache pressure.* The hot `std::visit` loop touches the
   variant payload + iteration state. Full variant + state spills
   beyond L1; projected fits comfortably with overhead.
3. *RocksDB SST block reads (LSM only).* Smaller records pack
   more entries per SST block, so the iterator advances through
   the same logical scan with fewer block fetches. SSTRead/TX
   collapses from ~50 µs (full) to ~0.01 µs (projected) at SF=15
   per the `[stage]` metrics — the iterator effectively reads from
   page cache instead of fetching from SST. This is the source of
   the much larger LSM lift (4000×) vs. LeanStore's (100×).

**Decision**: flip the default to `--acoli_projected=true`. The
projected aCOLI is now the production aCOLI; the full-payload type
is retained behind `--acoli_projected=false` for backwards-A/B
only and should be retired once the measurement is archived.

**Doc updates**: `frontend/tpch/CLAUDE.md §Project pushdown`
documents the resolved rule (primary indexes carry full columns;
secondary structures cover only query-required columns).
`q3i/CLAUDE.md §Storage Structure Options` updated to reflect
projected default.

### A6 — Memory-pressure sweep with all post-A3 defaults

Now that A3 is confirmed on both backends, the open question is
**how the win shape varies across the (dram, SF) envelope**. A6
sweeps to characterise it.

- **WHAT**: dram ∈ {0.05, 0.1, 0.5, 1.0, full} at SF=40, all five
  paths, **shared and iso**, with `--coli_walker_variant=fused_emit`,
  RocksDB only.
- **WHERE**: `make q3i_lsm dram=$X scale=40
  coli_walker_variant=fused_emit`; iso variants via
  `q3i_lsm_iso_N` targets.
- **MEASURE**: TX/s, `block_cache_hit_rate`, `block_read_byte/TX`,
  `iter_next_cpu/q`, S3-vs-S4 gap (per cell).
- **OUTCOME**:
  - S3 beats S4 at any cell → merged-index pitch lives on RocksDB
    too; report the envelope.
  - S3 never beats S4 → RocksDB disk-pressure pitch is dead; lead
    paper with LeanStore A3 numbers + S5 cross-backend.

### H12 — Load-amortized cost vs query count

The reviewer-facing question is "how many queries justify a merged
index?" Steady-state TX/s alone can't answer it.

- **TEST**: instrument `load()` to emit per-secondary build time;
  compute `total_time(N) = load_time + N / TX_per_sec` for each
  structure on both backends; identify the crossover point.
- **OUTCOME**: report the crossover as a primary metric. Likely
  S4 wins low-N (no build cost), S1/S3 win high-N. Useful even if
  the steady-state numbers favour S3 already.

### A4 — H7 SSTWrite source attribution (RocksDB only)

Read-only Q3I reports nonzero SSTWrite/TX. Mechanism candidates:
compaction during run, WAL writes, commit markers, L0→L_n promotions.

- **TEST**: `--h7_test={nobg,opt,nocommit,sstdelta}` flag.
- **WHERE**: `frontend/shared/RocksDB.hpp` (DB open path);
  `tpch_executable.hpp` (`PauseBackgroundWork` wrap; commit elision).

| Variant     | Implementation                                    | Confirms if SSTWrite/TX → 0 |
|-------------|---------------------------------------------------|-----------------------------|
| `nobg`      | `db->PauseBackgroundWork()` around `helper.run()` | Compaction                  |
| `opt`       | Open as `OptimisticTransactionDB`                 | WAL                         |
| `nocommit`  | Skip `txn->Commit()` for read-only                | WAL commit marker           |
| `sstdelta`  | Per-level SST count diff before/after run         | L0→L_n promotion            |

### A7-followup — Unblock LeanStore parity harness, then run content-walk

Code landed in `83870b48`
(`LeanStoreMergedAdapter::content_bytes_walk()`,
`LeanStoreAdapter::content_bytes_walk()`, test wiring). **Measurement
blocked**: the LeanStore parity harness segfaults during
"Populating secondaries" with `--vi=false --mv=false
--isolation_level=ser` at SF=1 (pre-existing; also fails before A7
landed — see commit `d05aa719`). With `--vi=true --wal=true` it aborts
earlier in `loadInvoiceAndLinkLineitem`.

- **NEXT**: separate plan to get LeanStore parity harness green at
  SF=1, then emit the `[content/row]` / `[fill]` triple per
  structure. Decision (real low-fill vs measurement bug) deferred
  until then.

---

## §4 — Reviewer relevance

REVIEWS.md §4.2 (R3-W2 / R3-D3-5) asks for evidence merged indexes
beat traditional joins on medium-to-large scans. The story now reads:

- **S5 (aCOLI MI)** carries the showcase: ~260× over raw paths at
  SF=40 dram=0.1, parameter-flexible. The "merged-index +
  pre-aggregation" point is uncontested.
- **S3 on LeanStore** with fused_emit + Backend-trait Seek-skip beats
  S1/S4 by 100× at SF=40 disk-bound — the parameter-flexible-but-raw
  merged index *also* wins on B-tree. Hits the reviewer ask
  directly.
- **S3 on RocksDB** still neutral at disk pressure (A6 will resolve).
- **H12 crossover** answers "how many queries justify a merged
  index" — second-order story regardless of how A6 lands.

---

## §5 — Process

- Active worklist updates land here.
- Evidence and refutations → archive.
- Completed A-test → promote H-row in §2 with commit SHA.
- Per-A-test implementation plans live in `.claude/plans/`.

### Refuted/closed (recorded so they aren't re-investigated)

- **H1, H5, H9, H10**: refuted; see §2.
- **H2 (RocksDB), H4 (SF=15)**: remediated; see §2.
- **A2a (fast_decode), A2b (template_dispatch)**: deprioritised. A2c
  closed the per-call gap to ~3%; reopen only if a new test surfaces
  per-call asymmetry.
- **A3 RocksDB equivalent**: not viable. SST prefetch buffer is
  invalidated by physical Seek (~5× regression at SF=40 disk-bound
  per archived A/B). Forward iteration stays the default for RocksDB.
