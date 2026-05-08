# TPC-H Experiment Plan

Entry point and experiment matrix for the TPC-H workloads in
`frontend/tpch/`. **Per-query `frontend/tpch/<q>/CLAUDE.md` is the
canonical home** for SQL definitions, storage-structure details, plan
descriptions, and implementation phases. This document does not
duplicate them — it indexes them, captures the cross-cutting
experiment matrix (memory pressure, ad-hoc sweeps), and binds reviewer
concerns to current queries.

## Query Status

| Query | Status | Structures | Canonical doc |
|-------|--------|------------|---------------|
| **Q3I** | Active showcase; S1–S4 are the paper-reported axis (cross-structure parity verified at SF=1). S5 (aCOLI MI) is implemented and parity-verified but **deferred from paper sweep** — see [`PLAYBOOK §S5`](frontend/tpch/PLAYBOOK.md) | S1–S4 (S5 in tree, deferred) | [`q3i/CLAUDE.md`](frontend/tpch/q3i/CLAUDE.md) + [`q3i/PERFORMANCE.md`](frontend/tpch/q3i/PERFORMANCE.md) |
| **Q12** | Proof-of-concept; all four structures pass XOR parity at SF=1 (`0x9000007000003c`); production targets wired | S1–S4 | [`q12/CLAUDE.md`](frontend/tpch/q12/CLAUDE.md) |
| **Q3**  | Phase 4 complete; all four `query_by_*` paths pass strict cross-structure XOR parity at SF=1 (rows=10); production targets wired | S1–S4 | [`q3/CLAUDE.md`](frontend/tpch/q3/CLAUDE.md) |
| **Q9**  | Skeleton present; `query_by_*` bodies and `load.tpp` still TODO | (S1–S4 planned) | [`q9/CLAUDE.md`](frontend/tpch/q9/CLAUDE.md) |
| **Q5I** | Design doc only | — | [`q5i/CLAUDE.md`](frontend/tpch/q5i/CLAUDE.md) |
| **Q10I**| Design doc only | — | [`q10i/CLAUDE.md`](frontend/tpch/q10i/CLAUDE.md) |
| **Q5**  | Not started | — | — |
| **Q7**  | Not started | — | — |

Q3I is the active showcase because the COLI 4-table merged index
substrate it exercises is what the §3.1.2 + §3.1.3 hybrid pattern needs
to demonstrate. Q12 is the simpler proof-of-concept. Q3 is the
no-sibling-aggregate baseline of the COL family (Phase 4 complete);
Q9 remains deferred.

## Storage-Structure Conventions

All paper-reported queries use a four-structure mapping (S1–S4). Q3I
also has an S5 (aCOLI MI) variant in tree, but it is **deferred from
the paper sweep** across all queries — see
[`frontend/tpch/PLAYBOOK.md §S5`](frontend/tpch/PLAYBOOK.md). See
per-query CLAUDE.md for filter pushdown, comparison axis, and operator
details.

### Q12 / Q3 / Q9 (4 structures)

| # | Wrapper | Strategy |
|---|---------|----------|
| 1 | `BaseQ{N}`   | Traditional indexes + `BinaryMergeJoin`(O ⋈ L) |
| 2 | `ViewQ{N}`   | Materialised pipeline view of `joined_ol_t` |
| 3 | `MergedQ{N}` | `MI[0]` (`MergedAdapter<orders_t, lineitem_t>`) + `PremergedJoin` |
| 4 | `HashQ{N}`   | Traditional indexes + `HashJoin`(O ⋈ L) |

### Q3I (4 paper-reported structures + S5 deferred)

| # | Strategy | Substrate |
|---|----------|-----------|
| 1 | 4-way custkey-merge over secondaries | `Adapter<{orders,lineitem,invoice}_coli_t>` (custkey-sorted) |
| 2 | Sequential view scan | `q3i_pipeline_view_t` (post-aggregate, mktsegment/threshold hoisted out) |
| 3 | `coli_group_walk` over MI[COLI] | `MergedAdapter<customer_coli_t, orders_coli_t, lineitem_coli_t, invoice_coli_t>` |
| 4 | HashJoin chain baseline | Base tables + two probe hashmaps |
| ~~5~~ | ~~Direct field reads (no accumulator)~~ | ~~`MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>` with `pre_open_due` baked at load time~~ — **deferred from paper sweep** (see [`PLAYBOOK §S5`](frontend/tpch/PLAYBOOK.md)) |

S5 (aCOLI MI) is implemented and parity-verified at SF=1 but excluded
from paper figures: post-G8 measurements show **S3 > S5** at SF=15 on
both backends, traced to S5 lacking the hand-tuned `coli_group_walk`
(A2c fused-emit + A3 customer-level Seek-skip). Closing the gap is
infrastructure work, not per-query, and out of scope for the current
paper. See
[`q3i/CLAUDE.md §Storage Structure Options`](frontend/tpch/q3i/CLAUDE.md)
for the full comparison axis and
[`q3i/PERFORMANCE.md §A1`](frontend/tpch/q3i/PERFORMANCE.md) for the
anomaly investigation.

Storage structure 0 forces a data reload (no recovery).

## COLI / aCOLI MI Substrate

The COLI 4-table merged index interleaves
`customer_coli_t × orders_coli_t × lineitem_coli_t × invoice_coli_t`
keyed by `custkey` with Calcite-style tagged keys. Within each custkey
group, byte-lex order is `customer → invoice* → (orders → lineitem*)+`
so per-customer sub-aggregates (e.g. open-invoice totals) finalise
before the bulk O × L records stream by — the §3.1.2 sibling +
§3.1.3 hierarchical hybrid pattern.

The aCOLI MI is the pre-aggregated 3-type variant
(`MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>`,
with `customer_acoli_t.pre_open_due` baked at load time;
`orders_acoli_t` was retired Step 4b 2026-05-03 once tagged-key
adoption made its bytes identical to `orders_coli_t`). Built and
parity-tested for Q3I S5; **deferred from the paper sweep** — see
[`frontend/tpch/PLAYBOOK.md §S5`](frontend/tpch/PLAYBOOK.md).

Substrate lives in:
- `frontend/tpch/views_coli.hpp` — record types and tagged-key encoding.
- `frontend/tpch/coli_pipeline.{hpp,tpp}` — `COLIPipeline<Backend>`
  with `populate_merged()` and `populate_aggregated()`.

## Memory-Pressure Experiments

The intended cross-cutting experiment that this document owns. Q12
results at SF=40 dram=0.1 GiB on RocksDB show the orders side fits in
the block cache and the `unordered_multimap` build is fully in-memory,
so hash join is competitive (~10–11 TX/s across S1–S4). The MI
locality advantage is masked. To surface it:

| Experiment | SF | dram_gib | Build-side fits? | Purpose |
|---|---|---|---|---|
| Baseline | 40 | 0.1 | Yes | Current results |
| Low DRAM | 40 | 0.01 | Marginal | Block cache eviction |
| High SF | 100 | 0.1 | No | Orders grows ~2.5× |
| High SF + low DRAM | 100 | 0.01 | No | Strongest MI advantage |

**Expected outcomes**:
- MI / merge join: degrade gracefully (streaming sequential I/O).
- Hash join: degrades sharply (build phase triggers random reads when
  orders exceeds the block cache; `unordered_multimap` has no spill).
- Pipeline view: degrades proportionally with row width.

**Implementation note**: `frontend/shared/merge-join/hash_join.hpp`
uses `std::unordered_multimap` with no spill. Accept OOM at very high
SF as evidence of hash join's limitation — this is honest data for the
paper.

A separate counter-narrative is already in hand for Q3I: at SF=40
dram=0.1 GiB on RocksDB, **S3 (COLI MI) is currently the slowest** of
the four real paths (Q3I S1/S2/S3/S4) — see
[`q3i/PERFORMANCE.md`](frontend/tpch/q3i/PERFORMANCE.md) for the full
investigation. That regression is a Reviewer 2 D2 asset and is bound
in the mapping below.

## Reviewer Critique Mapping

### Reviewer 1 — More TPC-H

| Concern | Experiment |
|---------|------------|
| "Evaluate on standard benchmarks" | Q3I + Q12 end-to-end across S1–S4; Q3/Q9 once their bodies land |
| "Show generality beyond geo" | Complexity gradient: Q12 (2 tables, OL pipeline) → Q3I (4 tables, COLI MI) |

### Reviewer 2 — Honest Trade-offs

| Concern | Experiment |
|---------|------------|
| D2 "Show where MI loses" | Q3I S3 RocksDB regression at SF=40 dram=0.1 — see [`q3i/PERFORMANCE.md`](frontend/tpch/q3i/PERFORMANCE.md) |
| Maintenance overhead | Insert/delete latency: MI vs view vs traditional (TODO) |
| Space overhead | Per-structure storage size already reported by load tests; cross-checked via `compare_mi_and_view` for Q12 |

### Reviewer 3 — Specific Experiments

| Concern | Experiment |
|---------|------------|
| W2/D5 "Larger scan ranges" | Vary Q12 receipt-date filter window; vary Q3I `params.orderdate` |
| W3/D6 "Single-table scan overhead in MI" | LINEITEM-only scan in `Adapter<lineitem_t>` vs MI[0]; CUSTOMER-only scan in `Adapter<customerh_t>` vs MI[COLI] |
| D3–D4 "MI vs materialised views" | Every Q3I/Q12 already runs MI (S3), view (S2), and traditional (S1, S4); compare latency, maintenance, space |
| "B-tree vs LSM deeper analysis" | Run all queries on both `_btree` (LeanStore) and `_lsm` (RocksDB) executables |

## Ad-Hoc Experiments

These do not require new query implementations:

1. **Scan selectivity sweep** — vary the date-filter window (Q12 `l_receiptdate` or Q3I `params.orderdate`) across 1 month, 1 year, 3 years; plot latency vs selectivity per structure.
2. **Single-table scan overhead** — full-scan LINEITEM via the standalone adapter vs via MI[0]; the gap is the cost of skipping ORDERS records during the scan.
3. **MI density** — for each query's MI, measure the fraction of records belonging to each table type. High skew means single-table scans pay high overhead.

## Running Experiments

Targets are emitted by `generate_targets.py`. Sweeps:

```bash
# Sweep paper-reported structures (S1..S4) for one query
make q3i_lsm scale=15
make q12_btree scale=15

# Single structure
make q3i_lsm_3 dram=0.1
make q12_btree_3 dram=0.1
# Q3I S5 (aCOLI) target exists (`make q3i_lsm_5`) but is deferred
# from the paper sweep — see PLAYBOOK §S5.

# LLDB debug
make q12_btree_lldb_3
```

`scale` is the TPC-H scale factor; `dram` is the buffer-pool size in
GiB. See [`LINUX_SETUP.md`](LINUX_SETUP.md) for fresh-Linux-node
bring-up (perf_event_paranoid sysctl, NVMe partitioning, smoke tests).
After editing `STRUCTURE_OPTIONS` in `generate_targets.py`, regenerate
with `python3 generate_targets.py > targets.mk`.

CSV output lands in `build/<exec>/<scale>-in-<dram>/` (latency,
throughput, maintenance, storage size, scan/IO counters).

## Out of Scope

These belong to per-query CLAUDE.md, not this document:

- SQL definitions and substitution parameters → per-query `§TPC-H Definition`.
- Plan descriptions / DOT files → `frontend/tpch/<q>/plans/` and per-query `§Plan Descriptions`.
- Per-structure operator details and comparison axes → per-query `§Storage Structure Options` and `§How the four approaches differ`.
- COLI / aCOLI byte layouts and tagged-key encoding → `views_coli.hpp` and `frontend/tpch/CLAUDE.md`.
- Implementation phases and remaining work → per-query `§Implementation Phases` / `§Implementation Status`.
- TPC-H schema / data-generation internals → `frontend/tpch/CLAUDE.md §Layout`.
