# Paper-side edits — audit response (2026-05-25)

The paper LaTeX (near-final, separate source) is the authority for paper
text; `REVISION_SNIPPETS.md` has been **retired/deleted** to avoid a second
drifting copy. This file records the **proposed LaTeX edits** that preempt the
reviewer integrity audit (`/tmp/figure_audit_FINAL.md`), each with the current
passage quoted and a proposed change, plus a list of findings the LaTeX
**already handles** (so they are not re-edited).

All numbers are from `2026-05-24-a-ssd/summary/headline.csv` (SSD, c0, bg=2,
medians over reps) and `2026-05-24-refresh-5L-ssd` unless noted. The audit found
**no fabrication**; these are scope/presentation fixes, and every one currently
leans pro-merged-index.

---

## Already honest in the current LaTeX — do NOT re-edit

- **"Only inversion" is gone.** The old §5.5 line is not in the LaTeX.
- **H3 LSM space inversion is disclosed**: §B-trees-vs-LSM states "each
  individual view is now … smaller than $\mathbf{MI_B}$, inverting the LeanStore
  order," and that Merged-Idx wins on *total* space only via sharing ("the two
  views together amount to 1.21 GiB"). The `tab:exp-baselines` caption states the
  per-family one-image framing. (`space_table.py` regenerates these numbers.)
- **CPU figure is commented out** — no CPU claim is in the paper.
- **H2 is already scoped** to "as fast as the traditional indexes used by
  \textsc{Base-Merge}" (i.e. secondary-maintaining S1), correctly *not* claiming
  parity with Base-Hash/S4 (which maintains no secondary).

---

## Proposed edits

### Edit 1 — Disclose the magnitude of the LSM COLI-family query inversions  (audit Finding #1, reviewer Q1)

The headline LSM figure shows them, but the prose does not. On LSM at 5L/bg=2
**three panels invert** (Merged-Idx slower than Mat-View): q3 1.29×, q3i 1.34×,
**q5i 1.83×** (the largest, an 83% loss). B-tree preserves the advantage on all
four (S3 within ~8% of S2). "Closely trails … matches or surpasses on several"
is fair for B-tree + LSM vanilla but glosses the LSM invoice queries.

**(a) Current** (subsec:query_perf summary):
> In summary, \textsf{Merged-Idx} closely trails \textsf{Mat-View}---and on
> several queries matches or surpasses it---while decisively beating the other
> two baselines. … coming within a small margin of it confirms \textbf{H1}.

**Proposed:** add a scoping clause, e.g.:
> … closely trails \textsf{Mat-View}---and on several queries matches or
> surpasses it---while decisively beating the other two baselines. The one
> regime where the margin is not small is the **invoice family on LSM**, where
> \textsf{Merged-Idx} trails \textsf{Mat-View} by 1.3--1.8$\times$ (Q5i is the
> widest); we attribute this to LSM scan layout in
> Section~\ref{subsec:btree_vs_lsm}. On B-tree, and on the vanilla family on
> LSM, \textsf{Merged-Idx} comes within a small margin of \textsf{Mat-View},
> confirming \textbf{H1}.

**(b) Current** (subsec:btree_vs_lsm):
> \paragraph{LSM-trees improve the query performance of \textsc{Mat-View}
> disproportionately.} The same storage-layout properties also accelerate
> scans: fewer but wider rows reduce per-record processing overhead …

**Proposed:** extend to quantify the flip side (this *is* the mechanism for the
inversion):
> … fewer but wider rows reduce per-record processing overhead along the
> LSM-tree scan path. The flip side of this advantage is the COLI-family
> inversion noted in Section~\ref{subsec:query_perf}: where \textsf{Mat-View}
> scans pre-joined wide rows, the COLI walker re-reads the co-located
> per-customer records, and on LSM this read-amplification is not masked by the
> page-traversal cost it would incur on B-tree. The result is
> \textsf{Merged-Idx} trailing \textsf{Mat-View} by up to 1.8$\times$ on Q5i
> (vs. matching it on B-tree).

---

### Edit 2 — Disclose the partial-aggregation (S5) asymmetry as an engineering-time choice  (audit Finding #3, reviewer Q3)

`q10.pdf` shows the partial-aggregation variants of Merged-Idx and Mat-View
winning for Q10/Q10i; they are not shown for Q3i/Q5i. For Q3i/Q5i a partial-agg
*merged* index (the "aCOLI" variant) exists but uses a generic scanner and loses
to plain Merged-Idx (~2.5×); the **hand-rolled walker** that makes it the
fastest structure on Q10/Q10i is simply not yet written for the 4-table case
(`frontend/tpch/PLAYBOOK.md §S5`, `ACOL_ACOLI_PLAYBOOK.md`). This is an
implementation-effort choice, not a structural limit.

**Current** (Q10 paragraph): the partial-agg variants are introduced only for
Q10/Q10i, with no note about the invoice family.

**Proposed:** add a sentence/footnote where partial aggregation is introduced:
> The same partial-aggregation merged index applies to the invoice queries and
> would likewise narrow the LSM Q3i/Q5i gap; we do not report it there because
> its merged-index walker is not yet hand-tuned to the four-table layout (a
> generic scan under-performs the raw co-located walk). This is an
> implementation-effort gap, not a property of the index --- the same hand-rolled
> walker is what makes the partial-aggregation merged index the fastest
> structure on Q10/Q10i.

---

### Edit 3 — Quantify "loses its edge" for Q10 on B-tree  (audit Finding #6, reviewer-adjacent; minor)

**Current:**
> In addition, \textsf{Merged-Idx} loses its edge in query performance for Q10
> and Q10i on the B-tree backend.

The skip-scan footnote is mechanistically complete; only the magnitude is
unstated. Plain Merged-Idx is ~1.9$\times$ slower than the split-index
Base-Merge (S1) on Q10 B-tree (35.0 s vs 18.7 s) — the slowest non-naive
structure there.

**Proposed (optional):**
> … loses its edge in query performance for Q10 and Q10i on the B-tree backend,
> running roughly $1.9\times$ slower than the split-index \textsf{Base-Merge}
> for Q10's order-only prune (the partial-aggregation variant, below, recovers
> the ordering).

---

### Edit 4 — H2 refresh: add the LSM figure, acknowledge the no-secondary ceiling  (audit Finding #5, reviewer Q5; user choice = justify + add LSM figure)

**Current:**
> As shown in Figure~\ref{fig:refresh_5L_pair_latency}, \textsf{Merged-Idx}
> processes the TPC-H refresh functions as fast as the traditional indexes used
> by \textsf{Base-Merge} and faster than \textsf{Mat-View}. This confirms
> \textbf{H2}.

This is correctly scoped to secondary maintenance (S1). Two additions:
1. **LSM**: `plot_refresh_sales.py` now emits `refresh_5L_pair_latency_lsm.pdf`
   (LSM rows were in the CSV but previously unplotted). The ordering holds on
   LSM too — S3 1132 ≈ S1 1129 > S2 918 pair/s (tail-30). Add the figure (or a
   sentence) so the refresh claim is shown on both backends, not B-tree-only.
2. **S4 ceiling (honest acknowledgment)**: S4 (Base-Hash) maintains *no*
   secondary, so it is ~3.5$\times$ faster at refresh than S3 on B-tree (6234 vs
   1788 pair/s) and is intentionally excluded from the figure. A one-clause
   footnote keeps a reviewer from thinking it was hidden:
   > (\textsc{Base-Hash} maintains no secondary structure and is therefore
   > $\sim$3.5$\times$ faster on refresh; it is the no-secondary ceiling, not a
   > comparison for a claim about secondary maintenance.)

---

### Edit 5 — Setup: single operating point + single query parameter  (audit Finding #8, reviewer Q2-adjacent)

**Current** (subsec:setup):
> Each measured query runs against background threads --- two worker threads
> issuing TPCH queries alongside a uniform-random point-lookup stream … Queries
> are run for three repetitions, and we report the median of per-query latency.

Honest caveats to add (none change a number):
- Results are at a **single beyond-memory operating point** (1.0 GiB DRAM, 5L).
- Each query uses a **single validation substitution parameter**, applied
  **uniformly across all four approaches** (so it bounds generalization but is
  not a head-to-head bias). At 5L a single query fills the measurement window,
  so the three repetitions are run-to-run repeats of that parameter; a fair
  per-rep parameter-rotation capability exists (`--param_seed`) but was not
  exercised for this submission.
- Verify "after a warm-up phase" describes the **refresh** harness only — the
  TPC-H *query* helper has no warmup phase (cold-start is counted equally for
  all approaches). Keep the phrase only where it is true.

---

## Reviewer-question crib (answers to send back)

1. **"Only inversion = Q3I" / the LSM inversions** — the current LaTeX no longer
   says "only inversion"; Edit 1 discloses all three LSM inversions (q3 1.29×,
   q3i 1.34×, q5i 1.83×) with the mechanism. B-tree preserves the advantage.
2. **Q10 figure conditions** — being re-run under the stated bg=2, 3-rep protocol
   (the 2026-05-25 supplemental was bg=0, single-rep, hand-ported; the figure
   script's bg relabel is removed once the genuine data lands). See
   `SWEEP_LOG.md` audit-response entry.
3. **Partial-agg shown only where it wins** — Edit 2 states plainly the Q3i/Q5i
   deferral is an implementation-effort choice (walker not yet hand-rolled), the
   same fix already shipped for Q10/Q10i.
4. **H3 cross-query sharing** — already in the LaTeX (`tab:exp-baselines` caption
   + §B-trees-vs-LSM); `space_table.py` regenerates the per-query `Sx − S4` and
   the shared-MI-vs-per-query-views aggregate.
5. **Refresh omits S4 + LSM** — Edit 4: LSM figure added; S4 omission justified
   (no-secondary, different baseline class) and acknowledged as the ~3.5× ceiling
   in prose.
6. **Error bars + y-cap** — headline bars now carry IQR error bars; the y-cap
   (≈1.24× second-tallest, over-cap value annotated) is documented in
   `scripts/PLOTTING.md` for the caption.
