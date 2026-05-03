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

Full evidence: archive `PERFORMANCE-2026-05-03b.md` §3.

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
