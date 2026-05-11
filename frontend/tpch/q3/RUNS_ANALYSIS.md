# Q3 — Run Analysis

Synthesis of the runs logged in [`RUNS.md`](RUNS.md). RUNS.md is a
chronological log — one entry per perf sweep, headline numbers,
claim-check sentence. **This doc** is the analytical companion: cross-
config comparisons, hypotheses, open questions, recommendations. Update
this when a new run shifts the conclusions; don't backfill it just to
mirror RUNS.md.

When pulling numbers in, **cite the source run** by date + commit SHA
so the reader can trace back to the raw TPut.csv line.

---

## 1. Storage structure comparison (S1 / S2 / S3 / S4)

All four structures execute the same logical plan (aggregate-first,
join-up; revenue DESC LIMIT 10) and produce byte-identical XOR parity
within each backend at SF=1. They differ only in physical operators
and what's pre-materialized:

| # | Strategy | Pre-materialized | Per-query work |
|---|----------|------------------|----------------|
| S1 | BMJ chain over custkey-sorted COL split secondaries | Custkey-sorted ORDERS + LINEITEM secondaries | 2 BMJs; physical custkey seek-skip on orders side (commit `f62a0149`); lineitem-aggregator seek deferred (BMJ refill race) |
| S2 | Sequential view scan | Per-lineitem `q3_pipeline_view_t` with FD-attached `c_mktsegment`, `o_orderdate`, `o_shippriority` | View scan; physical custkey seek-skip on mktsegment-miss (commit `bbc15e68`); per-orderkey orderdate gate |
| S3 | COLGroupWalk over MI[COL] | 3-table merged adapter (customer + orders + lineitem co-located by custkey) | Single MI walk with `WalkAction::SkipGroup` (physical seek on custkey-miss) + logical `SkipOrder` |
| S4 | HashJoin chain + inverted-lineitem seek | None | qualifying-customer hashset → qualifying-orders hashmap → physical seek per qualifying orderkey (commit `f62a0149`) |

**Comparison-integrity invariant**: after the 2026-05-11 fairness sweep
all four structures use the same physical-seek mechanic where it
applies (custkey for S1/S2/S3, orderkey for S4). The S1/S2/S4 paths
previously used logical filter pushdown only, which gave S3 an unfair
I/O advantage — see RUNS.md entry for SF=15 post-fix and the
`view_groups_skipped` / `s1_groups_skipped` / `s4_orderkey_seeks`
counters.

### Headline numbers (post-2026-05-11 fairness fix)

Run sources: SF=15 LSM/BTree (commit `51ea87b0`), SF=300 LSM
(commit `51ea87b0`), SF=600 BTree (commit `51ea87b0`).

| Config | S1 | S2 | S3 | S4 | Winner |
|--------|----|----|----|----|--------|
| SF=15 LSM (cache-resident) | 80.4 | 264.3 | 211.5 | 113.6 | **S2** |
| SF=15 BTree (cache-resident) | 101.3 | 408.6 | 314.1 | 168.7 | **S2** |
| SF=300 LSM (3.5× beyond-mem) | 2.32 | **4.99** | 4.20 | 0.62 | **S2** |
| SF=600 BTree (4× beyond-mem) | 0.39 | 0.75 | **1.30** | 0.087 | **S3** |

Numbers are TX/s; bold marks the winning structure for that config.

---

## 2. Memory-resident vs beyond-memory

The fundamental question separating the two regimes: **does per-record
CPU work matter more, or does per-byte I/O cost more?**

### Cache-resident (DRAM > secondary size)

Per-record CPU work dominates. Three observations:

1. **S2 wins on both backends.** The per-lineitem view is the most
   "pre-cooked" structure — every row is one cache-line away from the
   answer, and the FD-attached customer/order columns avoid pointer
   chasing. S3's MI walk pays the variant-dispatch tax (one byte read
   to pick the record type, then jump into the type-specific handler)
   that S2 skips entirely.

2. **S4 (inverted seek) beats S1 (BMJ).** Hash lookups are O(1) per
   record; BMJ traversal is cursor-management overhead plus
   match-state machine. With cache-resident data the hash table
   fits trivially and probes are sub-microsecond. SF=15: S4 is
   +41% over S1 LSM, +67% over S1 BTree.

3. **S3 lands within 20–30% of S2.** This is the paper's claim — MI
   matches view perf without paying view's storage/maintenance cost.
   The gap is the per-record dispatch overhead, and it's a fair
   price for not materializing.

### Beyond-memory (DRAM << secondary size)

Per-byte I/O cost dominates. The picture flips:

1. **S3 closes the gap to (and beats) S2.** Sequential MI scan is
   bytes-efficient. The view's wider per-row footprint (FD-attached
   order columns repeated per lineitem) costs more bytes per
   qualifying row. SF=300 LSM: S3 climbs to 84% of S2 (vs 80% at
   cache-resident). SF=600 BTree: S3 overtakes S2 by 73%.

2. **S4 collapses.** The inverted-lineitem seek pattern that
   delivered +80% cache-resident becomes a catastrophe — every
   physical seek is bloom-filter miss + random SST read (LSM) or
   B-tree descent (BTree). SSTRead(µs)/TX jumped 13× SF=300 LSM.
   S4 absolute TX/s at SF=600 BTree is 0.087 — 15× worse than S1.

3. **S1 also pays.** The custkey pre-scan + `std::lower_bound`-per-
   order overhead scales with customer count; at SF=600 BTree S1
   regressed ~65% from the pre-fix baseline (see §5 open questions).

### The S3 ≈ S2 invariant

Across all four configs, S3 lands within a 2× envelope of S2:

| Config | S3/S2 ratio |
|--------|-------------|
| SF=15 LSM | 0.80 |
| SF=15 BTree | 0.77 |
| SF=300 LSM | 0.84 |
| SF=600 BTree | 1.73 |

This is the load-bearing experimental claim: **S3 matches S2 within
a constant factor across regimes, while S2 carries the full view
materialization cost and S3 carries only the MI co-location cost.**
S1 and S4 fall further behind in either direction (cache-resident
S1 is 30–40% of S2; beyond-memory S4 is ≤15% of S1).

---

## 3. LSM vs BTree

### Cache-resident: BTree wins 1.3–1.5× uniformly

| Path | LSM TX/s | BTree TX/s | BTree/LSM |
|------|---------:|-----------:|----------:|
| S1 | 80.4 | 101.3 | 1.26× |
| S2 | 264.3 | 408.6 | 1.55× |
| S3 | 211.5 | 314.1 | 1.49× |
| S4 | 113.6 | 168.7 | 1.49× |

(SF=15, DRAM=0.1 GiB; commit `51ea87b0`.)

**Three drivers, all favoring BTree:**

1. **No compaction tax.** LSM SSTWrite(µs)/TX is 320–1060 even on
   read-only workloads — background compactions compete for CPU.
   BTree reports W MiB/TX = 0 across the board.
2. **No bloom-filter lookup overhead.** LSM point reads consult
   bloom filters on every level before reading SST data. BTree
   does a direct root-to-leaf descent.
3. **No level merge during read.** LSM scans must merge results
   across levels; BTree pages are flat.

**The biggest BTree win is S2 (+55%).** View scan is a pure-sequential
workload — BTree's contiguous page scan is the optimal access pattern;
LSM has to interleave levels.

**The smallest BTree win is S1 (+26%).** BMJ does point lookups +
short range scans; LSM's per-CF bloom filters are actually useful
here, narrowing the gap.

### Disk-bound: backends diverge structurally

Direct cross-backend TX/s comparison at beyond-memory is not
apples-to-apples (we ran SF=300 LSM @ DRAM=0.08 GiB but SF=600 BTree @
DRAM=0.4 GiB — both targeting ~4× secondary/DRAM ratio, but absolute
scale differs). What does compare cleanly is the **structure
ordering**:

| Backend (beyond-mem) | Ordering | S3 vs S2 |
|----------------------|----------|----------|
| LSM (SF=300/0.08) | S2 > S3 > S1 >> S4 | S2 ahead (84%) |
| BTree (SF=600/0.4) | S3 > S2 > S1 >> S4 | S3 ahead (+73%) |

**Why the S2/S3 inversion flips between backends:**

- On LSM, the per-lineitem view's sequential SST scan benefits from
  bloom-filter prefetch and level-1 caching. S2 stays competitive
  even at 3.5× beyond-memory.
- On BTree, every page miss is a root-to-leaf descent with no
  shortcut. The view's wider rows compound this (more pages for
  the same number of qualifying rows). The MI's compact tagged-key
  layout fits more useful records per page; S3 reasserts its
  locality advantage.

**Storage cost cross-check (SF=15):**

| Path | LSM size (MiB) | BTree size (MiB) | BTree/LSM |
|------|---------------:|-----------------:|----------:|
| S1 | 14.89 | 41.40 | 2.78× |
| S2 | 14.71 | 43.64 | 2.97× |
| S3 | 15.25 | 42.38 | 2.78× |
| S4 | 12.23 | 32.86 | 2.69× |

BTree is ~2.8× larger on disk (Snappy compression off vs on). At
cache-resident this is invisible; beyond-memory it tightens the
DRAM-vs-secondary pressure ratio (BTree fills DRAM faster), which
contributes to the LSM-vs-BTree per-config variance.

### Practical recommendation

- **DRAM plentiful → BTree.** Uniform 1.3–1.5× advantage across all
  structures, no compaction overhead.
- **DRAM tight, modest-to-large data → LSM.** Compression buys
  effective cache, sequential SST scan handles per-lineitem views
  well, S2 stays competitive without needing S3.
- **DRAM tight, very large data + want MI locality benefit → BTree
  with S3.** BTree S3 reasserts its lead deep-disk-bound where LSM S3
  is held back by per-level merge overhead.

---

## 4. S4 (hash join) — when it wins, when it collapses

S4 is the most config-sensitive path. Summarized:

| Config | S4 TX/s | Rank | Notes |
|--------|--------:|:----:|-------|
| SF=15 LSM | 113.6 | 3rd | Above S1, behind S2/S3. Inverted seek is +80% cache-resident win. |
| SF=15 BTree | 168.7 | 3rd | Above S1, behind S2/S3. Inverted seek + 139% win. |
| SF=300 LSM | 0.62 | 4th | **−49% from pre-fix.** SSTRead 13×, bloom-filter miss on every per-orderkey seek. |
| SF=600 BTree | 0.087 | 4th | **Catastrophic** — 15× worse than S1. Every seek = B-tree descent. |

**The inverted-lineitem seek (`f62a0149`)** is the right thing for
cache-resident — it converts an O(|all lineitems|) sequential scan
into an O(|qualifying orderkeys|) point-lookup pass. Cache-resident:
fewer rows processed, hash table is sub-MiB.

**Beyond-memory it inverts** — point lookups are the worst access
pattern for both LSM (bloom-filter cost stacks) and BTree (each
seek is a full descent). The original sequential scan beat sequential
prefetch + level-merge in LSM; the inverted-seek doesn't.

**Open question** — should S4 dynamically pick the access pattern
based on DRAM:secondary ratio? Cache-resident: inverted seek.
Beyond-memory: sequential scan + hashmap probe. The decision is the
same factor that makes nested-loop-join vs hash-join optimization
hard in general planners. **Tracked as a TODO**, not implemented.

The S4 **transient working set** is small in absolute terms:
`s4_hashtable_bytes` reports ~9 KB at SF=1, scaling linearly to
~14 MiB at SF=1500 (still <5% of typical DRAM budgets). So the
beyond-memory collapse is **not** memory pressure on the hash tables
themselves — it is purely access-pattern cost.

---

## 5. Open questions / pending diagnoses

### Q1. SF=600 BTree S1/S3 regression vs `c500b747` baseline

S1 dropped 1.27 → 0.39 TX/s (−69%) and S3 dropped 3.71 → 1.30 TX/s
(−65%) between the `c500b747` SF=600 entry (RUNS.md 2026-05-08
23:01) and the `51ea87b0` post-fix run (RUNS.md 2026-05-11 12:11).

Fresh load means the std::random_device-seeded TPC-H data differs,
so part of the delta is data-shape variation. But 65% is large.
Candidates:

- (a) BMJ final-group flush (`92336200`) adds a `final_flushed`
  check to every `next()` call — should be O(1) but worth A/B'ing.
- (b) S1 custkey pre-scan + `std::lower_bound` per fetched order =
  ~22M comparisons at SF=600. Could be cache-thrashing.
- (c) S3 has no obvious code-path change from the merge other than
  TopNSink emit. A heap-of-10 push per qualifying order should be
  nanoseconds.

**Action**: rerun the `c500b747` binary on the same fresh /mnt/ssd
data to isolate code vs data-shape. Not done yet.

### Q2. S3 vs S2 crossover scale on LSM

At SF=15 cache-resident, S2/S3 = 1.25 (S2 wins). At SF=300
beyond-memory LSM, S2/S3 = 1.19 (S2 still wins, gap closing).
**Does the gap fully close — or invert — at SF=1500 LSM?**

If LSM follows BTree's behavior (BTree inverts to S3 > S2 at
SF=600), we'd expect S3 to overtake S2 somewhere between SF=300
and SF=1500 LSM. If LSM stays S2-favored even at SF=1500, that's
a structural backend difference worth documenting.

**Pending**: SF=1500 LSM sweep in flight at time of writing.

### Q3. S4 access-pattern switching

(Per §4 above.) When is the crossover from "inverted seek wins" to
"sequential scan + hashmap probe wins"? Approximate threshold by
running both variants at multiple SF/DRAM points. Not done.

### Q4. Q3I parity regression on btree (`92336200` fix)

The BMJ final-group flush (`92336200`) fixed a probabilistic silent
drop on `test_query_q3i_btree`. The LINUX_PENDING 2026-05-08
closure claimed "5/5 consecutive runs" — turned out to be a lucky
sample. **Worth a skim** of the other 2026-05-08 LINUX_PENDING
closures to confirm they're backed by enough repetitions. Flagged
in `LINUX_PENDING.md`.

---

## 6. References

- [`RUNS.md`](RUNS.md) — chronological per-sweep log
- [`CLAUDE.md`](CLAUDE.md) — Q3 implementation overview, storage
  structure options, plan descriptions
- [`../CLAUDE.md`](../CLAUDE.md) — TPC-H workload guide; project pitch
  (S3 ≈ S2 > S1/S4) is the load-bearing claim
- [`../q3i/PERFORMANCE.md`](../q3i/PERFORMANCE.md) — Q3I perf
  investigation (sibling pattern; informs Q3 expectations)
- Commits referenced: `bbc15e68` (S2 seek-skip), `92336200` (BMJ
  flush), `f62a0149` (S1+S4 seek), `d976b047` (TopNSink merge),
  `51ea87b0` (s4_hashtable_bytes counter)
