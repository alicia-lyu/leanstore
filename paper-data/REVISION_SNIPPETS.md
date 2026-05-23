# Paper revision — ready-to-paste snippets

Deliverable companion to
[`.claude/plans/looks-good-now-let-s-functional-lollipop.md`](../.claude/plans/looks-good-now-let-s-functional-lollipop.md).
Drop these into `merged_index_interesting_orderings/main.tex`, then
stylise / restructure as the writer sees fit. Load-bearing claims,
figure / table references, and the §5 reordering are pre-baked.

Convention used below:

- `S1` / `S2` / `S3` / `S4` = sweep codes; paper text uses
  `trad_idx_mj` / `mat_view` / `merged_idx` / `trad_idx_hj`
  (matching the existing `tab:hypotheses_comparison` vocabulary).
- Cell labels in plots: `2` (sec 2 GiB / DRAM 0.4 GiB), `5L` (sec 5
  GiB / DRAM 1.0 GiB), `5H` (sec 5 GiB / DRAM 0.4 GiB).
- All numbers in this file are placeholders the writer should
  re-read off `paper-data/2026-05-18-b/summary/stats.csv` (or the
  generated figures) before final submission. The structural claims
  are stable; the absolute numbers may shift on a re-sweep.

---

## §5.1 — Hypotheses and scope (load-bearing rewrite)

Strike the stale "modeled after Query~\ref{lst:tpch_q3}" line in the
current §5.1. Replace with six paragraphs.

```latex
% --- §5.1 Hypotheses and scope ---------------------------------

\subsection{Hypotheses and Experiment Scope}
\label{sec:hypotheses}

% P1 — Scope: query components, not whole suites
We do not benchmark TPC-H or any other suite end-to-end.
Merged indexes accelerate a specific structural pattern: order-sharing
pipelines whose join keys form a hierarchical or tree-shaped prefix
chain (Sections~\ref{sec:mi-hierarchical}, \ref{sec:mi-tree}).
Whole-benchmark numbers would dilute that signal with operators that
are not part of the pattern --- sorts, group-bys on non-prefix keys,
and single-table aggregates. We instead evaluate \emph{query
components} chosen to isolate the pattern:
the geo microbenchmark (Section~\ref{sec:exp-geo}) covers
join-only, join+\textsc{count}, and join+\textsc{count distinct}
at progressively deeper hierarchies; the TPC-H/TPCHI subset
(Section~\ref{sec:exp-tpch}) covers the same patterns inside
full-shape queries with realistic selectivities and dimension
tables.

% P2 — Why Q3 and Q5
TPC-H Q3 exercises the hierarchical 3-table chain
(Customer--Orders--Lineitem) with a \textbf{range} predicate on
\texttt{shipdate} (MI$_B$; Section~\ref{sec:mi-hierarchical}).
TPC-H Q5 exercises the same chain with a \textbf{point} predicate
and three reduce-side hashed dimensions (Region, Nation, Supplier),
showing how merged-index performance composes with small dimension
tables that sit outside the merged structure. Together Q3 and Q5
bracket the range/point selectivity axis on the hierarchical
pattern.

% P3 — Why Q3I and Q5I
Q3I and Q5I extend Q3 and Q5 with an Invoice sibling sub-aggregate,
exercising the \textbf{tree-shaped} MI$_C$
(Section~\ref{sec:mi-tree}). They isolate the cost of co-locating
a sibling sub-aggregate --- Invoice attaches under Customer
alongside Orders --- inside a single merged-index scan. The
extensions are summarised in Table~\ref{tab:tpchi_extension}.

% P4 — Why both backends
We evaluate on B-tree (LeanStore) and LSM-tree (RocksDB) to show
the claims are storage-agnostic. The backends differ on absolute
latency by orders of magnitude; the \emph{relative ranking} of
\textsf{merged\_idx}, \textsf{mat\_view}, \textsf{trad\_idx\_mj},
and \textsf{trad\_idx\_hj} across the two backends is what the
hypothesis predicts.

% P5 — Three sweep cells
Three sweep cells separate two pressure axes:
\textbf{2} (baseline: 0.4\,GiB DRAM, 2\,GiB secondary index),
\textbf{5L} (low memory pressure: 1.0\,GiB DRAM, 5\,GiB secondary),
and \textbf{5H} (high memory pressure: 0.4\,GiB DRAM, 5\,GiB
secondary). The step $2 \rightarrow 5\mathit{L}$ scales the
secondary index with proportional DRAM; the step
$5\mathit{L} \rightarrow 5\mathit{H}$ holds the secondary index
size constant and cuts DRAM, isolating the cache-spill regime.

% P6 — Hypotheses table
Table~\ref{tab:hypotheses_comparison} restates the four
storage-structure baselines and the hypothesised ranking of each,
with an \emph{evidence} column pointing at the figure or section
that supports each row.
```

---

## §5.2 — Geo microbenchmark (first experimental section)

```latex
% --- §5.2 Geo microbenchmark -----------------------------------

\subsection{Geo Microbenchmark}
\label{sec:exp-geo}

% P1 — Workload definition
We use the 5-table geographic hierarchy
(Nation $\rightarrow$ State $\rightarrow$ County $\rightarrow$
City $\rightarrow$ Customer) defined in the artifact
repository~\cite{vldb19-artifact}. Queries cover three depths
(\textsf{join-n}, \textsf{join-ns}, \textsf{join-nsc}) crossed
with three aggregate shapes (raw join, $+$\textsc{count},
$+$\textsc{count distinct}). Figure~\ref{fig:geo_queries} shows
the deepest depth (\textsf{nsc}) across all three aggregate
shapes; root and intermediate depths are deferred to the
appendix because they probe operator behaviour outside the
merged-index pattern.

% P2 — Headline
At the deepest join (\textsf{join-nsc}), \textsf{merged\_idx}
matches \textsf{mat\_view} on query latency while keeping
\textsf{trad\_idx\_mj} within reach. The
$+$\textsc{count} and $+$\textsc{count distinct} variants
preserve the ranking, confirming that the result is not
aggregation-specific.

% P3 — LSM
The same ranking carries to LSM, with absolute latencies
compressed because RocksDB's sequential SST scan is faster than
the B-tree's pointer-chasing path.
```

---

## §5.3 — TPC-H / TPCHI headline (new section)

```latex
% --- §5.3 TPC-H and TPC-HI Queries -----------------------------

\subsection{Real-Query Validation: TPC-H and TPC-HI}
\label{sec:exp-tpch}

% P1 — Claim
On full-shape TPC-H and TPCHI queries (Q3, Q5, Q3I, Q5I; see
Section~\ref{sec:hypotheses}), the merged index matches the
materialised view on query latency while keeping update cost and
space at traditional-index levels
(Section~\ref{sec:exp-update-space}).

% P2 — B-tree headline
Figure~\ref{fig:tpch_btree_headline} shows query duration on
B-tree across the three sweep cells. \textsf{merged\_idx} is tied
with or ahead of \textsf{mat\_view} on Q5, Q3I, and Q5I at every
cell. The gap is narrowest on Q3, where the \texttt{shipdate}
range scan favours the pre-aggregated view's contiguous layout.
\textsf{trad\_idx\_hj} is consistently the slowest, by roughly
an order of magnitude.

% P3 — LSM headline
Figure~\ref{fig:tpch_lsm_headline}, stacked directly below the
B-tree figure with the same panel geometry, mirrors the result on
LSM. LSM compresses absolute latency relative to B-tree; the
relative ranking is preserved with one exception (Q3I, attributed
in Section~\ref{sec:exp-anomalies}).

% P4 — DRAM sensitivity
Reading the figures left-to-right within each panel:
$2 \rightarrow 5\mathit{L}$ doubles data with proportional DRAM
(no change in DRAM/data ratio), and the merged-index advantage
holds. The step $5\mathit{L} \rightarrow 5\mathit{H}$ cuts DRAM
at fixed data size; \textsf{merged\_idx} degrades gracefully on
every query, while \textsf{mat\_view} degrades slightly faster
on Q5 and Q5I because its pre-aggregated working set spills out
of the buffer pool sooner than the merged index's interleaved
layout.

% P5 — Why (single sentence, points at attribution figure)
The per-query LLC-miss attribution in
Figure~\ref{fig:diag_btree_llc_miss} confirms the mechanism:
\textsf{merged\_idx} and \textsf{mat\_view} share an essentially
identical cache profile across all four queries, separating cleanly
from the merge-join and hash-join baselines.

% Space pointer (single sentence, into §5.4)
Table~\ref{tab:space_overhead} reports the space footprint on
real TPC-H data and shows the merged index stays within $\sim$X\%
of \textsf{trad\_idx} while \textsf{mat\_view} inflates by
$\sim$Y$\times$.
```

---

## §5.4 — Update / space (lightly revised)

Replace the section opener with one sentence on scope; keep the
existing update-time material.

```latex
% --- §5.4 Update and Space -------------------------------------

\subsection{Update and Space}
\label{sec:exp-update-space}

% Scope sentence — updates are geo-only by design
We measure update throughput on the geo microbenchmark; the
TPC-H and TPCHI implementations are read-only, since adding
TPCC-style write transactions would entangle the experiment with
workload-mix decisions orthogonal to the merged-index claim. The
dual-write argument in Section~\ref{sec:update-algo} generalises
the geo result: any merged index whose key shape matches a
hierarchy supports the same maintenance pattern.

% [retain the existing update-throughput paragraph and
%  Figure~\ref{fig:update_performance} — no data change.]

% Space — measured on both workloads
Table~\ref{tab:space_overhead} reports static storage footprint
on the TPC-H/TPCHI queries; the pattern observed on geo holds:
\textsf{merged\_idx} stays within $\sim$X\% of \textsf{trad\_idx},
while \textsf{mat\_view} inflates by $\sim$Y$\times$.
```

---

## §5.5 — Anomalies and limitations (new short subsection)

```latex
% --- §5.5 Anomalies and Limitations ----------------------------

\subsection{Anomalies and Limitations}
\label{sec:exp-anomalies}

The Q3I LSM panel of Figure~\ref{fig:tpch_lsm_headline} shows the
only inversion in our results: at \textbf{5L}, \textsf{merged\_idx}
sits slightly above \textsf{mat\_view}. The per-query SST-read
attribution in Figure~\ref{fig:diag_lsm_sst_read} traces this to
a read-amplification path in the COLI walker that scales with the
DRAM-relief working set; the materialised view, having
pre-aggregated, scans less SST data per query in this regime. The
inversion is an implementation gap in the walker, not a structural
limitation of MI$_C$ --- the B-tree backend, which uses a hand-
tuned walker, preserves the merged-index advantage on Q3I (see
Figure~\ref{fig:tpch_btree_headline}, third panel).

Two further limitations bound our claims. First, the
pre-aggregated MI variant (S5 in earlier drafts, called the
\emph{aCOLI} index in the implementation) is deferred from this
evaluation because it inherits the same walker gap and would
re-state the Q3I anomaly rather than resolving it; see the
implementation playbook for details~\cite{leanstore-playbook}.
Second, the geo refresh covering the full $(2, 5\mathit{L},
5\mathit{H})$ matrix is still in progress at the time of this
revision; the current geo figures use the baseline cell only, and
will be re-rendered before the camera-ready with the same
plotter.
```

---

## Tables

### `tab:tpchi_extension` (new)

```latex
\begin{table}[t]
  \centering
  \small
  \begin{tabular}{@{}lp{2.4cm}p{2.6cm}p{2.4cm}l@{}}
    \toprule
    Query & Extends     & New tables          & New aggregate                & MI shape \\ \midrule
    Q3I   & TPC-H Q3    & Invoice (sibling of Orders under Customer) & per-customer \texttt{cust\_open\_due}            & MI$_C$ \\
    Q5I   & TPC-H Q5    & Invoice (same attachment)                  & per-customer payment-status revenue split        & MI$_C$ \\ \bottomrule
  \end{tabular}
  \caption{Invoice extensions to TPC-H Q3 and Q5. Both Q3I and Q5I
  attach Invoice as a sibling of Orders under Customer, exercising
  the tree-shaped layout MI$_C$
  (Section~\ref{sec:mi-tree}).}
  \label{tab:tpchi_extension}
\end{table}
```

### `tab:space_overhead` (new)

Numbers below are placeholders — read from
`paper-data/2026-05-18-b/summary/headline.csv` `size_mib` column
at cell `c0` (the `5L` paper label), grouped by `(binary,
structure)`. The writer should pivot the CSV before pasting.

```latex
\begin{table}[t]
  \centering
  \small
  \begin{tabular}{@{}llrrrr@{}}
    \toprule
    Query & Backend & trad\_idx (GiB) & mat\_view (GiB) & merged\_idx (GiB) & merged / trad \\ \midrule
    Q3   & B-tree & X & X & X & X\% \\
    Q3   & LSM    & X & X & X & X\% \\
    Q5   & B-tree & X & X & X & X\% \\
    Q5   & LSM    & X & X & X & X\% \\
    Q3I  & B-tree & X & X & X & X\% \\
    Q3I  & LSM    & X & X & X & X\% \\
    Q5I  & B-tree & X & X & X & X\% \\
    Q5I  & LSM    & X & X & X & X\% \\ \bottomrule
  \end{tabular}
  \caption{Static storage footprint at the \textbf{5L} cell. The
  merged index stays within a small fraction of the traditional
  index's size while the materialised view inflates because it
  stores fully joined rows.}
  \label{tab:space_overhead}
\end{table}
```

### `tab:merged-indexes` — extra column

Existing rows for MI$_A$ / MI$_B$ / MI$_C$ get an additional
column on the right:

| MI | … existing columns … | Experiment query |
|---|---|---|
| MI$_A$ | … | (not exercised by §5 experiments) |
| MI$_B$ | … | Q3, Q5 (§\ref{sec:exp-tpch}) |
| MI$_C$ | … | Q3I, Q5I (§\ref{sec:exp-tpch}) |

This is the only §3 ↔ §5 cross-reference; §3 prose stays self-contained.

### `tab:hypotheses_comparison` — evidence column

Add a final column "Evidence" to the existing table; populate
each row with a reference like `Fig.~\ref{fig:tpch_btree_headline}`
or `§\ref{sec:exp-tpch}`. No row text changes required.

---

## Figure inclusions

### Headline pair (stack vertically — same width)

```latex
\begin{figure}[t]
  \includegraphics[width=\columnwidth]{figures/paper_tpch_btree_headline.pdf}
  \includegraphics[width=\columnwidth]{figures/paper_tpch_lsm_headline.pdf}
  \caption{TPC-H/TPCHI query duration (seconds, log scale), bg=2
  contention cohort, structures S1--S4. Top: B-tree (LeanStore).
  Bottom: LSM (RocksDB). X-axis tick labels denote the secondary
  index size in GiB with a memory-pressure suffix:
  \textbf{2} = 2\,GiB / DRAM 0.4\,GiB;
  \textbf{5L} = 5\,GiB / DRAM 1.0\,GiB (low pressure);
  \textbf{5H} = 5\,GiB / DRAM 0.4\,GiB (high pressure).
  Each panel uses its own y-range; legend is shared between the
  two sub-figures.}
  \label{fig:tpch_btree_headline}
  \label{fig:tpch_lsm_headline}
\end{figure}
```

(If two `\label` commands inside one figure float is awkward, split
into two adjacent `\begin{figure}` blocks with the lsm caption
reading "(continued)" so the cross-references remain distinct.)

### Geo condensed

```latex
\begin{figure}[t]
  \includegraphics[width=\columnwidth]{figures/paper_geo_condensed.pdf}
  \caption{Geo microbenchmark, depth \textsf{nsc} across the three
  aggregate shapes. Rows: backend; columns: query shape; x-axis as
  in Figure~\ref{fig:tpch_btree_headline}.}
  \label{fig:geo_queries}
\end{figure}
```

### Diagnostic candidates (main-text or appendix)

```latex
\begin{figure}[t]
  \includegraphics[width=\columnwidth]{figures/diag_btree_llc_miss.pdf}
  \caption{Per-query LLC misses per TX on B-tree, bg=2. The cache
  profile of \textsf{merged\_idx} tracks \textsf{mat\_view}
  closely, separating cleanly from the two traditional-index
  baselines and attributing the headline parity to cache
  behaviour.}
  \label{fig:diag_btree_llc_miss}
\end{figure}

\begin{figure}[t]
  \includegraphics[width=\columnwidth]{figures/diag_lsm_sst_read.pdf}
  \caption{Per-query SST read time per TX on LSM, bg=2. The
  \textsf{trad\_idx\_hj} baseline is 10--100$\times$ above the
  rest, dominating LSM latency. \textsf{merged\_idx} reads slightly
  more SST data than \textsf{mat\_view} on Q3I and Q5I at the
  \textbf{5L} cell, attributing the LSM Q3I inversion called out
  in Section~\ref{sec:exp-anomalies}.}
  \label{fig:diag_lsm_sst_read}
\end{figure}
```

### `fig:tpch_q3i_plan` (sibling of `fig:tpch_q3_plan`)

This is a TikZ figure mirroring the existing `fig:tpch_q3_plan`
with an Invoice scan attached as a sibling of the Orders scan
under the Customer node. Stub:

```latex
\begin{figure}[t]
  \centering
  % TODO: extend fig:tpch_q3_plan's TikZ with an additional Invoice
  %       leaf joined into Customer (sibling of Orders); attach a
  %       cust_open_due sub-aggregate node showing the §3.1.3
  %       tree-shaped pattern.
  \input{figures/tpch_q3i_plan.tikz}
  \caption{TPC-H Q3I plan. Q3I extends Q3
  (Figure~\ref{fig:tpch_q3_plan}) with an Invoice sibling
  sub-aggregate (\texttt{cust\_open\_due}), exercising the
  tree-shaped MI$_C$ (Section~\ref{sec:mi-tree}).}
  \label{fig:tpch_q3i_plan}
\end{figure}
```

---

## Build / re-render commands

From `leanstore/`:

```bash
# regenerate all paper figures from the latest sweep
python3 paper-data/scripts/plot_paper_sweep.py \
    --tag 2026-05-18-b --mode paper-figures

# regenerate the per-query diagnostic figures
python3 paper-data/scripts/plot_paper_sweep.py \
    --tag 2026-05-18-b --mode diagnostics-explore
```

Output paths to drop into the paper:

- `paper-data/2026-05-18-b/figures/paper/paper_tpch_btree_headline.pdf`
- `paper-data/2026-05-18-b/figures/paper/paper_tpch_lsm_headline.pdf`
- `paper-data/2026-05-18-b/figures/paper/paper_geo_condensed.pdf`
- `paper-data/2026-05-18-b/figures/diagnostics/diag_btree_llc_miss.pdf`
- `paper-data/2026-05-18-b/figures/diagnostics/diag_lsm_sst_read.pdf`
