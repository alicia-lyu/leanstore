# Q3I — Run Analysis

Synthesis of the runs logged in [`RUNS.md`](RUNS.md). RUNS.md is a
chronological log — one entry per perf sweep. **This doc** is the
analytical companion: cross-config comparisons, hypotheses, open
questions, recommendations. Update when a new run shifts the
conclusions; don't backfill to mirror RUNS.md.

When pulling numbers in, **cite the source run** by date + commit SHA.

Sibling doc: [`q3/RUNS_ANALYSIS.md`](../q3/RUNS_ANALYSIS.md) — the Q3
version. Q3I extends Q3 with the invoice sibling sub-aggregate
(`cust_open_due`), so many findings cross-pollinate. Where Q3 and Q3I
diverge structurally, this doc flags it.

---

## 1. Storage structure comparison (S1–S5)

Five structures, same logical plan, same XOR parity within each
backend at SF=1. They differ in pre-materialization and physical
operators:

| # | Strategy | Pre-materialized | Per-query work |
|---|----------|------------------|----------------|
| S1 | BMJ chain over custkey-sorted COLI split secondaries | Customer + invoice agg + orders + lineitem secondaries (custkey-sorted) | 3 BMJs; custkey pre-scan + physical seek-skip on orders side after the Q3I fairness fix (commit `426a79cd`) |
| S2 | Sequential view scan | Per-lineitem `q3i_pipeline_view_t` with FD-attached `c_mktsegment`, `cust_open_due`, `o_orderdate`, `o_shippriority` | View scan; physical custkey seek-skip on (mktseg-miss OR open_due ≤ threshold) (commit `426a79cd`) |
| S3 | COLIGroupWalk over MI[COLI] | 4-table merged adapter (customer + invoice + orders + lineitem co-located by custkey) | Single MI walk with `WalkAction::SkipGroup` physical seek + logical `SkipOrder` |
| S4 | HashJoin chain + inverted-lineitem seek | None | Customer mktseg set → invoice open_due map → orders map → physical seek per qualifying orderkey (commit `426a79cd`) |
| S5 | aCOLI MI: 3-table MergedAdapter with `pre_open_due` baked into `customer_acoli_t` | aCOLI MI (`customer_acoli_t + orders_coli_t + lineitem_acoli_t`); invoice rows eliminated at scan | Single MI walk over fewer record types, same `WalkAction::SkipGroup` seek as S3 |

**Comparison-integrity invariant**: after the 2026-05-11 Q3I fairness
port (`426a79cd`), all five structures use the same physical-seek
mechanic where it applies (custkey for S1/S2/S3/S5, orderkey for S4).
The S1/S2/S4 paths previously used logical filter pushdown only,
which gave S3/S5 an unfair I/O advantage.

### Headline numbers (post-2026-05-11 Q3I fairness fix)

Run sources: SF=15 LSM/BTree (`c482945d`), SF=300 LSM (`c482945d`,
altered paths against pre-fix S3/S5 baselines), SF=600 BTree
(`c482945d`), SF=1500 LSM (`c482945d`).

| Config | S1 | S2 | S3 | S4 | S5 | Winner |
|--------|----|----|----|----|----|--------|
| SF=15 LSM (cache-resident) | 56.4 | 269.7 | 187.7 | 57.0 | 72.8 | **S2** |
| SF=15 BTree (cache-resident) | 73.5 | 450.8 | 304.1 | 82.9 | 103.7 | **S2** |
| SF=300 LSM (4× beyond-mem) | 1.57 | 4.60 | 3.52 | 0.45 | 2.01 | **S2** |
| SF=1500 LSM (4.5× beyond-mem) | 0.29 | 0.86 | 0.65 | 0.08 | 0.48 | **S2** |
| SF=600 BTree (6× beyond-mem) | 0.17 | 0.49 | 0.57 | 0.04 | **1.46** | **S5** |

Numbers are TX/s; bold marks the winning structure for that config.

---

## 2. Memory regime: cache-resident vs beyond-memory

Same as Q3, the regime determines whether per-record CPU work or
per-byte I/O dominates.

### Cache-resident

S2 wins by a wide margin (1.4–1.5× over S3). The per-lineitem view
is the most "pre-cooked" structure; FD-attached `c_mktsegment`,
`cust_open_due`, `o_orderdate`, `o_shippriority` payload means the
post-pipeline filter is one pointer dereference and no cross-record
lookup. S3 pays variant-dispatch tax on every record.

**Compared to Q3**: Q3I cache-resident lead for S2 is comparable
(Q3 1.25–1.30×). The extra `cust_open_due` payload doesn't visibly
penalize S2 at cache-resident — the row is small enough to fit in
cache regardless.

### Beyond-memory

LSM keeps S2 ahead through SF=1500. BTree inverts.

**LSM**: at SF=300 (4× beyond) S2 still leads S3 by 30%; at SF=1500
(4.5× beyond) the gap holds at 32%. **The S2 advantage on LSM does
NOT close with scale.** Same finding as Q3.

**BTree**: at SF=600 (6× beyond) S3 beats S2 by 17%, AND **S5
overtakes S3 by 2.6×**. This is the most surprising result of the
sweep — see §4 below.

### The S3 ≈ S2 envelope (Q3I, S5 ignored)

| Config | S3/S2 ratio | Trend vs Q3 |
|--------|-------------|-------------|
| SF=15 LSM | 0.70 | Tighter than Q3 (0.80) |
| SF=15 BTree | 0.67 | Tighter than Q3 (0.77) |
| SF=300 LSM | 0.77 | Same as Q3 (0.84) |
| SF=1500 LSM | 0.76 | Wider gap than Q3 (0.63) |
| SF=600 BTree | 1.17 | Smaller MI lead than Q3 (1.73) |

The Q3I S3-vs-S2 gap is consistently *tighter* than Q3 at cache-
resident, because the COLI MI scans more bytes (invoice rows) per
qualifying customer than the COL MI does. The Q3I S2 view has the
same row count as Q3 S2 but a wider payload (extra `cust_open_due`
column) — both effects cancel partially, but the COLI invoice scan
costs more.

At deep-disk-bound BTree SF=600 the MI lead is smaller for Q3I (1.17×)
than Q3 (1.73×) — the invoice rows the COLI MI must traverse are pure
overhead at I/O-bound, and the wider S2 view doesn't pay the same
penalty because it's just a sequential scan over a denser layout.

---

## 3. LSM vs BTree

### Cache-resident: BTree uniformly faster (1.1–1.7×)

| Path | LSM TX/s | BTree TX/s | BTree/LSM |
|------|---------:|-----------:|----------:|
| S1 | 56.4 | 73.5 | 1.30× |
| S2 | 269.7 | 450.8 | 1.67× |
| S3 | 187.7 | 304.1 | 1.62× |
| S4 | 57.0 | 82.9 | 1.45× |
| S5 | 72.8 | 103.7 | 1.42× |

(SF=15, DRAM=0.1 GiB.)

Same three drivers as Q3 (no compaction tax, no bloom-filter
overhead, no level merge). Q3I shows slightly larger BTree wins
than Q3 (S2 1.67× vs 1.55× for Q3) — the additional COLI invoice
scan benefits more from BTree's contiguous-page layout than from
LSM's level-merged scan.

### Beyond-memory: structural backend split (same as Q3)

| Backend (beyond-mem) | Ordering | S3 vs S2 |
|----------------------|----------|----------|
| LSM (SF=300/0.08) | S2 > S3 > S5 > S1 >> S4 | S2 ahead (S3 = 77%) |
| LSM (SF=1500/0.4) | S2 > S3 > S5 > S1 >> S4 | S2 ahead (S3 = 76%) |
| BTree (SF=600/0.4) | **S5 > S3 > S2 > S1 >> S4** | S3 ahead (S2 = 86%) |

The same backend-structural pattern as Q3 holds for Q3I (LSM keeps
S2 ahead; BTree inverts to MI lead at deep-disk-bound). Additional
Q3I-specific finding: **on BTree deep-disk-bound, S5 (paper-deferred
aCOLI MI) beats everything else by 2.6×.** Discussion in §4.

### Storage cost (SF=15)

| Path | LSM (MiB) | BTree (MiB) | BTree/LSM |
|------|----------:|------------:|----------:|
| S1 | 19.53 | 59.06 | 3.02× |
| S2 | 18.68 | 61.03 | 3.27× |
| S3 | 19.97 | 60.16 | 3.01× |
| S4 | 16.20 | 48.77 | 3.01× |
| S5 | 18.97 | 57.07 | 3.01× |

BTree is ~3× larger on disk than LSM (Snappy compression off vs on).
Slightly larger ratio than Q3 (Q3 was 2.78×) because the extra
invoice payload doesn't compress as well as repeated integer keys.

### Practical recommendation

- **DRAM plentiful → BTree** with **S2**: 1.5–1.7× faster than LSM,
  4.4× faster than S3. The view's pre-aggregated layout dominates
  cache-resident.
- **DRAM tight, LSM backend → S2**: never lose to S3 on LSM, even
  at SF=1500.
- **DRAM tight, BTree backend → S5 if paper deferral can be revisited,
  otherwise S3**: S5 wins at SF=600 deep-disk-bound by 2.6× over S3,
  but the project-wide §S5 deferral framing (`PLAYBOOK.md`) needs
  revisiting (see §4).

---

## 4. S5 (aCOLI MI) is paper-deferred but wins on BTree deep-disk-bound

**The most surprising finding of this sweep.** At Q3I BTree
SF=600/DRAM=0.4 (6× beyond-mem), S5 (1.46 TX/s) outperforms S3
(0.57 TX/s) by 2.6×. Across other configs:

| Config | S5/S3 ratio |
|--------|-------------|
| SF=15 LSM | 0.39 |
| SF=15 BTree | 0.34 |
| SF=300 LSM | 0.57 |
| SF=1500 LSM | 0.73 |
| **SF=600 BTree** | **2.56** |

The S5 advantage scales with two factors:

1. **Memory pressure ratio** (higher → S5 wins more). At cache-
   resident S5 is 0.34–0.39× S3 (per-record dispatch overhead
   penalizes the smaller scan benefit). At 4× beyond-mem LSM S5
   climbs to 0.57. At 4.5× LSM, 0.73. At 6× BTree, 2.56.
2. **Backend** (BTree → S5 wins more). LSM's sequential SST scan
   handles the COLI MI's invoice rows cheaply (~1 µs per skipped
   row); BTree pays a full descent for every page. Eliminating
   invoice rows entirely from the MI (which is what S5's aCOLI
   does — invoice is collapsed to a `pre_open_due` scalar in the
   customer record) is more valuable on BTree.

**What this means for §S5 deferral**: the project-wide deferral
rationale in `../PLAYBOOK.md §S5` is "S3 > S5 anomaly traced to S5
lacking the hand-tuned `coli_group_walk` walker." That framing
appears to be **regime-specific** — true at cache-resident, but
inverted at deep-disk-bound BTree. **Open question** flagged:
does the S5 win require deep memory pressure, or does it generalize
to moderate pressure on BTree too? An intermediate config
(SF=300 BTree, DRAM=0.1 GiB) would settle this. Not yet run.

If the BTree-disk-bound S5 win generalizes, S5 becomes a **legitimate
paper-axis candidate** — not deferred, but a 5th storage variant
worth reporting. The aCOLI design (collapse parameter-independent
sibling aggregates at load time) is a clean §3.1.2 instance.

---

## 5. Cross-query comparison: Q3 vs Q3I

Both queries' S1–S4 paths follow the same pattern after fairness
fixes:

| | Q3 cache-resident | Q3I cache-resident | Q3 disk-bound BTree | Q3I disk-bound BTree |
|--|-------------------|---------------------|-----------------------|------------------------|
| Winner | S2 | S2 | S3 | **S5** |
| S3/S2 ratio | 0.77–0.80 | 0.67–0.70 | 1.73 | 1.17 |
| S4 vs S1 | +41–67% | wash | catastrophic | catastrophic |

**Q3I has S5**, Q3 doesn't. Q3 deliberately omits S5 (per
`q3/CLAUDE.md §S5`) because aCOLI for Q3 would carry no information
beyond COL — no invoice means no pre-aggregated sibling. Q3I's S5
demonstrates the §3.1.2 sibling sub-aggregate showcase, and its
BTree-disk-bound dominance is *the* result that justifies the
aCOLI design.

**Q3I S3 vs Q3 S3**: at the same SF/DRAM Q3I S3 is consistently
slower than Q3 S3 (e.g. SF=15 LSM: Q3I 188 vs Q3 211, −11%). This
is the COLI invoice-scan tax. The trade-off works out at deep-disk-
bound BTree because Q3I S3 still beats Q3I S2/S1/S4, but it confirms
that the COLI MI is not strictly dominant over the COL MI for the
hierarchical part.

---

## 6. Open questions / pending diagnoses

### Q1. S5 generalization to BTree moderate-pressure

(§4 above.) Does S5 still win at SF=300 BTree / DRAM=0.1 GiB? If
yes, the §S5 deferral framing needs revisiting. **Not yet measured**
— SF=300 BTree was not in the original RUNS.md sweep list because
the pre-fairness data on BTree-disk-bound was already covered at
SF=600. Worth a one-off.

### Q2. S1 regression pattern

Q3I BTree SF=15 shows S1 −24% vs pre-fix baseline. Q3I LSM SF=300
shows S1 −8% vs pre-fix. Q3 SF=600 BTree shows S1 −65%. Common cause
candidate: the custkey pre-scan + `std::lower_bound`-per-fetched-order
overhead. At Q3I the pre-scan additionally builds `cust_open_due_map`,
amplifying the cost.

**Hypothesis**: replace `std::lower_bound` with a `std::unordered_set`
membership check (O(1) instead of O(log n)) inside `fetch_ord`. Should
narrow the S1 regression without sacrificing the seek logic.

**Action**: A/B in a followup commit. Not done.

### Q3. Q3I S4 inverted-seek wash

Q3I S4 doesn't show Q3's dramatic deltas at cache-resident
(Q3 LSM SF=15: +80%; Q3I LSM SF=15: −17%). Q3I S4's hash-build
cost (3 maps: customer set + open_due + orders) dominates the
lineitem-pass cost; inverting the lineitem pass doesn't move the
needle. **Implication**: the Q3I S4 fairness port could be reverted
without losing performance — the access-pattern parity argument
holds in principle but doesn't bite empirically. Flagging but not
reverting (cleaner to keep all 4 baselines symmetric across Q3/Q3I).

### Q4. Q3 SF=600 BTree S1/S3 regression diagnosis

Inherited from Q3 RUNS_ANALYSIS Q1 — still open. May correlate with
the S1 pre-scan overhead Q2 above.

### Q5. LINUX_PENDING audit

Inherited from Q3 RUNS_ANALYSIS Q4 — the 2026-05-08 "5/5 consecutive
runs" closure on Q3I btree parity was a lucky-sample artifact. Worth
auditing whether other 2026-05-08 closures are similarly thin.

---

## 7. References

- [`RUNS.md`](RUNS.md) — chronological per-sweep log
- [`CLAUDE.md`](CLAUDE.md) — Q3I implementation overview
- [`PERFORMANCE.md`](PERFORMANCE.md) — Q3I perf investigation history
- [`../q3/RUNS_ANALYSIS.md`](../q3/RUNS_ANALYSIS.md) — sibling
  analysis for Q3 (no invoice extension; S5 absent)
- Commits referenced: `92336200` (BMJ flush — shared with Q3),
  `426a79cd` (Q3I S1+S2+S4 fairness port), `c482945d` (Q3I SF=15
  post-fix sweep — first datapoint after fairness)
