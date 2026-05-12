# TPC-H Conventions Reference

Cross-cutting rules referenced from per-query files and PLAYBOOK.md.
This file holds the **timeless conventions** — the "always" and "never"
rules that apply to every query, every storage structure, and every new
piece of code in `frontend/tpch/`. `PLAYBOOK.md` is the step-by-step
bring-up walkthrough; read CONVENTIONS first, then use PLAYBOOK as a
recipe book.

---

## §Sort-key wildcard semantics — `WILDCARD_KEY`, `matching_keys()`, `match()`

The convention is codified in `frontend/shared/wildcard_key.hpp`:

- `WILDCARD_KEY` is the sentinel constant (`= 0` — TPC-H and geo IDs
  are 1-based) for a key field intentionally left unset to act as a
  prefix anchor or wildcard match.  Never write a bare `0` in a
  `Key{...}` constructor or in a match()/matching_keys() comparison.
- `wildcard_match(a, b)` is a 3-way comparison helper that returns 0
  when either side equals `WILDCARD_KEY`, otherwise the usual signed
  ordering.  Use it inside `match()` instead of hand-rolling the
  `if (a == 0 || b == 0) return 0;` branch.

Three pieces of machinery work together for wildcard-keyed joins:

1. **`match()`** chains `wildcard_match` calls per field (see
   `q5_sort_key_t::match` in `q5/views.hpp` for the canonical 3-field
   shape, `geo::sort_key_t::match` in `frontend/geo/views.hpp` for the
   5-field shape).  Used by BMJ's `refresh_join_state`, by HashJoin's
   probe-side equal-range check, and by `join_state::join_and_clear`
   (which compares `next_jk.match(SKBuilder<JK>::project<R>(jk_to_join))`
   to decide whether each per-record-type buffer needs flushing).

2. **`matching_keys()`** enumerates the prefix-anchor keys a probe-side
   row must look up, walking up the hierarchy (each non-WILDCARD_KEY
   field, set to WILDCARD_KEY one level at a time, plus self).  HashJoin
   probe walks the returned vector and consults each bucket via
   `equal_range`, because the hashmap (`std::unordered_multimap`)
   cannot itself ignore trailing slots.

3. **`SKBuilder<JK>::project<R>(jk)`** strips a JK to record R's
   natural granularity, filling absent fields with `WILDCARD_KEY`.
   Pure granularity stripping — has nothing to do with wildcards in
   isolation; the wildcard semantics come from `match()` honouring
   the WILDCARD_KEY sentinel.  But the two mechanisms together are
   what make wildcard-keyed BMJ work: when one input lacks a finer
   field (e.g. `q5_jr1_t` has no linenumber), `project<q5_jr1_t>`
   strips it to WILDCARD_KEY, and `join_and_clear`'s match-vs-projected
   call returns 0 → the build-side buffer survives across probe-side
   fan-out instead of being cleared per probe row.

**Hash convention**: wildcard-blind by default.  `operator%` /
`std::hash` hash all fields uniformly; the probe-side `matching_keys()`
fan-out is what bridges build/probe bucket placement.  See
`q5_sort_key_t` and `geo::sort_key_t` for the canonical pattern.  The
OLD-DESIGN exception is `tpch_family/views_ol.hpp::ol_sort_key_t`,
which uses a wildcard-AWARE hash (orderkey-only) and enumerates only
one anchor in `matching_keys()` — kept working as-is because no
current consumer needs the canonical pattern there, but slated for
retirement; do not mirror it in new code.

**Audit checklist for any SK with >1 field**:

1. Does every consumer of this SK project every field with a concrete
   value? If yes, `return {*this}` is sound and `match()` can stay
   strict.
2. If any consumer projects WILDCARD_KEY into a trailing slot, three
   things are required (all, not any):
   - **`match()`** chains `wildcard_match` per field.
   - **`matching_keys()`** enumerates every prefix anchor walking up
     the hierarchy.
   - **`SKBuilder<JK>::project<R>`** strips fields R doesn't carry to
     WILDCARD_KEY (granularity stripping; needed by BMJ via
     `join_state::join_and_clear`).
3. `operator<=>` stays strict (default).  `std::hash` / `operator%`
   hash all fields uniformly (wildcard-blind); the probe-side
   `matching_keys()` fan-out handles the bucket asymmetry.
4. Don't mint a narrower SK type to "drop" wildcardable fields.
   Trailing fields stay in the SK because the underlying secondary
   structure relies on them for sort uniqueness; teach the existing
   SK its wildcard wiring instead.  (Q5's `q5_co_jk_t` was a
   workaround that got retired in favour of the proper
   `q5_sort_key_t`.)

**Canonical reference implementations**: `q5/views.hpp::q5_sort_key_t`
(3-field, mirrors geo style) and `frontend/geo/views.hpp::sort_key_t`
(5-field).  Both expose all three pieces (per-field `match`, full
prefix-anchor enumeration, wildcard-blind hash) plus their
`SKBuilder` projections.

**Layering principle — don't bake query semantics into record-type
defaults.**  A record-type definition predates its consumers.  A
`match()` / `matching_keys()` design that relies on "current callers
happen to project to the same granularity" is a **latent assumption**,
not a correct minimal design — the failure mode (silent miss when a
new consumer probes with a coarser anchor) looks like a query bug,
not a record-type design debt.  When you choose
`matching_keys() = {*this}` deliberately, write the assumption inline
with a pointer to `frontend/shared/wildcard_key.hpp` and the canonical
wildcard-aware impls so the next maintainer gets a hint instead of a
silent miss.

Currently load-bearing examples (Q3-family aggregate keys; the strict
default is sound today only because both BMJ sides project to the
full key):
`tpch::q3_family::lineitem_agg_t::Key`,
`tpch::q3i::cust_open_due_t::Key`,
`tpch::q3::q3_cust_jk_t::Key`.

> **PITFALL — `joined_ol_t::unfoldKey`** (commit `be8b9bba`): The generic
> `joined_t::unfoldKey(fold_pks=false)` path assumes each constituent
> type has a `Key(JK)` constructor. If yours doesn't, you must add an
> explicit `unfoldKey` override on your join result type. The symptom is
> a compile error in `RocksDBAdapter<your_view_t>` instantiation.

---

## §Post-pipeline OutClass: small-buffer sink

The pipeline → result boundary is owned by an **OutClass** — a
small-buffer sink the visitor / scan loop / per-emit lambda pushes
rows into one at a time.  The sink decides storage shape internally;
the caller drains it once after the pipeline runs.

This is the project convention codified in commit `458bcaf0`.  It
replaces the older "push every qualifying row into
`std::vector<AggRow>& out`, then truncate via `apply_topN`" shape,
which buffered O(qualifying rows) in memory just to discard 99.999%
of them at SF=15+ — see anti-pattern #30 in `PLAYBOOK.md §13`.

### Two canonical implementations (use these; don't reinvent)

| OutClass | File | Used by | Output bound |
|---|---|---|---|
| `NNameRevenueAggregator` | `frontend/tpch/q5/query.tpp` | Q5 S1/S2/S3/S4 | `\|nation_set\|` ≈ 5 (intrinsic to the GROUP BY) |
| `TopNSink<R, Cmp>` | `frontend/tpch/operators.hpp` | Q3 / Q3I S1–S4(/S5) | `K` (LIMIT — 10 for Q3 family) |

Both expose the same shape:

- **Push API** — one row at a time
  (`agg.accumulate(l, n_name)` for Q5; `sink.offer(std::move(row))`
  for Q3 family).
- **Memory** — bounded by output cardinality, not by walk size.
  `NNameRevenueAggregator` is a `std::unordered_map<string, Numeric>`
  (~5 buckets); `TopNSink` is a `std::priority_queue` of size `K`.
- **Drain API** — single materialisation pass at the end
  (`agg.emit(out, sides)` for Q5; `sink.drain_sorted(out)` for Q3
  family).  `out` ends up holding exactly the answer, in result order.

The **same OutClass instance must be used across S1/S2/S3/S4(/S5)**
for a given query — this is the OPERATORS.md §6.1
comparison-integrity contract applied to the post-pipeline boundary.
Q5's `NNameRevenueAggregator` and Q3's `TopNSink` already obey this:
all four (or five) `query_by_*` bodies in each query construct one
sink and push through it.

### Two flavours of OutClass

- **Blocking small-buffered** (the two canonical impls above) —
  output cardinality is bounded by the GROUP BY shape (Q5) or by an
  explicit `LIMIT` (Q3 family).  The sink internally aggregates /
  sorts; rows beyond the bound are evicted (TopNSink) or folded
  into existing buckets (NNameRevenueAggregator).  Memory is
  O(output_cardinality), not O(qualifying_rows).
- **Streaming pass-through** — operator transforms / filters per
  row without a blocking step (filter, project, witness).  The
  OutClass receives each row, computes whatever it needs (parity
  digest, correctness check, optional retain-for-debug), and either
  forwards or discards.  Memory is O(1) per row plus whatever metric
  state the OutClass owns.  No live consumer of this flavour exists
  in `frontend/tpch/` today; the canonical reference for the shape
  is `query_proc_w_merged_index/operators/witness.hpp`.

### Authoring rules

- One row at a time on the push side.
- Memory bounded by `output_cardinality` (blocking case) or by a
  metric-only constant (streaming case).
- Drain materialises the final answer once; **no inside-the-pipeline
  buffering** with O(N) growth in `qualifying_rows`.
- Same OutClass instance shared across all storage variants of the
  query.  Asymmetric sinks across S1/S2/S3/S4 break OPERATORS.md
  §6.1 — the answer comparison stops measuring the storage substrate
  and starts measuring sink-shape divergence.

### Comparator-as-Cmp template parameter

`TopNSink<R, Cmp>` takes the result-ordering comparator as a
template parameter; callers pass a per-row lambda wrapping the
shared `q3_family::q3_agg_row_base_t::cmp`:

```cpp
auto cmp = [](const q{{N}}_agg_row_t& a, const q{{N}}_agg_row_t& b) {
   return q3_family::q3_agg_row_base_t::cmp(a, b);
};
TopNSink<q{{N}}_agg_row_t, decltype(cmp)> sink({{K}}, cmp);
```

The comparator MUST include a unique tiebreaker field as the final
key — `q3_agg_row_base_t::cmp` already does this
(`revenue DESC, o_orderdate ASC, o_orderkey ASC`).

> **PITFALL — Single-key comparator in top-K** (commit `a4f5a4bc`):
> Using only `revenue DESC` as the sort key leaves the heap (or, in
> the legacy `apply_topN`, `std::partial_sort`) free to resolve ties
> by input order, which differs across paths (S2 scans by
> `(custkey, orderkey)`, S4 iterates an `unordered_map`).
> On data states with revenue ties at the boundary, this produces
> different top-10 sets and thus different XOR digests.
> **Symptom**: 3 or 4 distinct digests despite correct query logic.
> **Fix**: always include enough tiebreaker fields to make the comparator
> a strict weak ordering with no residual ambiguity. A unique key
> (e.g. `o_orderkey`) as the last field guarantees this.

### When to use plain `apply_topN` instead

`apply_topN(out, K, cmp)` (also in `operators.hpp`) is kept for
callers whose result set is **intrinsically small** before any
truncation — e.g. Q12's per-shipmode `HashAggregate` output (~7
buckets, bounded by SHIPMODE cardinality).  In that case the buffer
never grows past the bound, so the choice between
`apply_topN(small_vec, K, cmp)` and `TopNSink::offer + drain` is
purely stylistic — `apply_topN` is shorter.

For any sink whose input cardinality grows with the walk (every
`query_by_*` in Q3 / Q3I / Q10), use `TopNSink`.  Anti-pattern #30
spells out the failure mode of getting this wrong.

### Forward-direction note

The fully general operator-tree refactor
(`Iterator<Output>::open/next/close` per
`query_proc_w_merged_index/operators/CLAUDE.md`) is a strict
super-set of the OutClass pattern: every OutClass becomes a leaf
`Iterator` operator, the pipeline becomes a tree of operators
above it.  Out of scope until a real correctness or performance
forcing function arrives; the OutClass convention is the interim.

---

## §Performance instrumentation (standard machinery)

Four flags + two header-level utilities are inherited for free if the
new query's executable is wired the same way as Q3I's. Each was
introduced during a Q3I performance investigation; new queries should
plumb them all from day one rather than back-fill when a perf surprise
surfaces (and it will — H1, H4, H8 all surfaced this way; see
Anti-Pattern #23).

### `--micro_perf=true`

Per-query scanner-internal timing.

- **RocksDB**: `frontend/tpch/q3i/perf_context_capture.hpp` snapshots
  `rocksdb::PerfContext` (e.g. `user_key_comparison_count`,
  `iter_next_cpu_nanos`) and `IOStatsContext` (`bytes_read`) before
  and after each query; emits per-TX averages.
- **LeanStore**: B-tree has no PerfContext analog. The chrono hook in
  `frontend/shared/adapter-scanner/scanner_perf_hook.hpp`
  (`tpch::scanner_perf::iter_next_ns_acc`) accumulates wall-clock
  per `MergedScanner::next()` call. The hook costs ~50 ns/call;
  read absolute numbers as upper bounds, ratios as robust.

### `--cfstats=true` (RocksDB only)

Pre/post `helper.run()` per-CF stats diff via the shared
`RocksDBLogger`. Useful for block-cache hit rate, SST read/write
attribution.

> **`SST_WRITE_MICROS` baseline-subtract**: the histogram
> accumulates over DB lifetime including post-load compaction. For
> read-only query experiments this inflates the reported
> SSTWrite(µs)/TX. Call `RocksDBLogger::capture_baseline()` once
> before `helper.run()`; each snapshot then reports
> `(current – baseline)`.

### `--load_only_structure=N`

Populate only the secondary needed for `--storage_structure=N` at
load time. Used by isolated-DB experiments (Q3I A5) to remove
cross-structure cache pollution. Default `-1` = load all.

`generate_targets.py::run_isolated_experiment` emits per-structure
make targets `q{N}_lsm_iso_M` and `q{N}_btree_iso_M` (`M` = storage
structure 1–5), plus an aggregate `q{N}_lsm_iso` / `q{N}_btree_iso`
target (commit `9da4f295` consolidated the on-disk layout):

- Image dirs:  `$(data_disk)/{exec}_iso/iso_{N}/{scale}` (one per
  storage structure).
- Runtime dir: `build/{exec}_iso/{scale}-in-{dram}/` — **shared
  across all five iso structures**, so `build/{exec}_iso/TPut.csv`
  carries one row per `N`, parallel to the non-iso
  `build/{exec}/TPut.csv`. Don't override `csv_path` per-structure.

Register your query the same way and the iso targets appear
automatically.

### `--coli_walker_variant={baseline,fused_emit}`

Walker dispatch choice — see `PLAYBOOK.md §7.1`. **Default**: `fused_emit`
(post-A2c, set in Makefile `coli_walker_variant ?= fused_emit`).
Use `coli_walker_variant=baseline` to reproduce the regression A/B.

### `--use_seek_skip={-1,0,1}`

Walker Seek-skip override (commit `9da4f295`). `-1` (default) defers
to `Backend::USE_PHYSICAL_SEEK_SKIP`; `0`/`1` force forward-iter /
Seek-skip respectively. Both production binaries and the Makefile
expose this for regression A/Bs (`use_seek_skip ?= -1`).

---

## §Size diagnostics: content-walk pattern

`50fd2052` (RocksDB per-CF size cache fix) and `83870b48` (LeanStore
`content_bytes_walk`) together establish the
`[content/row]` + `[overhead]` + `[fill]` reporting convention. New
queries adding a custom secondary should follow it — the alternative
(reasoning from `get_size()` MiB alone) cost the entire H1 cycle to
root-cause when a stale-cache bug misreported one CF's size.

- **RocksDB**: typed scan summing `key_bytes + value_bytes` per row;
  divide by `get_size()` to get content-vs-overhead split. Healthy
  tagged-record overhead ~5–25% (per-CF SST metadata + bloom filter
  + index blocks); shrinks at higher SF.
- **LeanStore**: call
  `LeanStoreMergedAdapter::content_bytes_walk()` (drives
  `next_raw()`, no variant construction) or
  `LeanStoreAdapter::content_bytes_walk()` (typed scan summing
  `maxFoldLength + sizeof(Record)`). Compare to
  `estimatePages × page_size` for fill ratio. Healthy ratio
  ~0.5–0.7; far below 0.5 implies a measurement bug or extreme
  fragmentation. See `frontend/shared/adapter-scanner/
  LeanStoreMergedAdapter.hpp` and `LeanStoreAdapter.hpp` for the
  exact signatures.

---

## §Operator framing rules

*Surfaced during Q5 Phase 10A semi-join refactor (commits `1263e7e2`,
`a076d31f`, `614240dd`, `347c3c44`, `6b69b3fd`, `6ab7ad15`). Apply
before writing any join body in any new query.*

**Rule 1 — Operator class is decided by downstream consumption, not
by build shape.** Use `HashJoin` (inner) when right-side columns flow
past the operator. Use `HashSemiJoin` only when the right side is a
pure existence test with no columns consumed downstream. Build
payload shape (empty / PK-only / wide) is independent — a build
that happens to carry no payload does NOT make the operator a
semi-join. Q5: CUSTOMER ⋈ NATION is inner (`n_name` flows
downstream); SUPPLIER ⋉ nation_set is semi (no NATION column past
the supplier arm).

**Rule 2 — "Probe" and "Build" are edge labels on a HashJoin node,
never standalone operator nodes.** Draw one HashJoin box; annotate
its two input edges as "build" and "probe". Never write a separate
"Build" or "Probe" step as if it were its own operator class.

**Rule 3 — Probe-side seek-on-miss is a HashJoin physical
implementation choice, not a Sort operator.** Hash builds are
inherently unordered (sets / hashmaps), so neither the build
itself nor a sorted vector view of its keys is needed. When the
**probe side is naturally ordered on the join key** (e.g.,
LINEITEM is sorted on `l_orderkey`), one valid physical
implementation is: stream the probe; probe the hashset on each
row; on a miss at probe-side key `K`, seek the probe scanner to
`K + 1` and continue. The natural sort guarantees
every probe row in the miss group is skipped without per-row hash
lookups. Draw it as `HashJoin`; the seek-on-miss strategy belongs
in a code comment, not as a separate `Sort` operator.

**Rule 4 — Build payload = primary key only.** Standard hash builds
carry only the build relation's PK; downstream consumers fetch
additional columns via the primary index at consumption time. Q5:
`nation_set = unordered_set<n_nationkey>` (dimension semi-join);
`orders_set = unordered_set<o_orderkey>` (fact-side build). No
forwarded payload columns.

**Rule 5 — Join outputs are first-class relations; multi-column
equi-joins are NOT cross-equality filters.** Once two tables join,
the output is a relation with its own combined schema. Predicates
that relate columns from already-joined inputs ARE join conditions
on that combined relation — pack them into the composite key of
the next build, probe with the corresponding columns from the
joined-side row. Q5: SUPPLIER ⋉ nation_set produces a relation
keyed by `(s_nationkey, s_suppkey)`; the lineitem-side probe sends
`(c_nationkey, l_suppkey)` from the JOINED-COL row (c_nationkey is
a column on that row, not "a column from a different table"). One
hashset probe is the physical implementation of this multi-
column equi-join — no separate "cross-equality filter"
downstream.
`supplier_nation_set: unordered_set<tuple<n_nationkey, s_suppkey>>`
is the composite-key PK of the restricted-supplier relation.

**Rule 6 — Aggregator keys on the output column (GROUP BY column),
not on intermediate IDs.** Resolve dimension IDs to output columns
at the join point that introduces them; carry the resolved value
downstream. The aggregator stays decoupled from dimension adapters.
Q5: `NNameRevenueAggregator` keys on `n_name` string; resolution
happens at the CUSTOMER ⋈ NATION survival point in every query
body.

**Rule 7 — Lookup columns ride on join-output records (mental
model); the C++ type is only minted when an operator template
demands it.** Think of every join as producing a relation whose
schema is the union of fields surviving from each side — that's
the mental model. In code, that "join-output record" is usually
*not* a defined struct; it's the payload of whatever container
the next operator consumes (a hashmap value, a view-row payload,
a BMJ intermediate). Widen the existing carrier to add the
looked-up field. Q5: `n_name` widens into `q5_jr1_t`,
`q5_jr2_t`, and `q5_pipeline_view_t`; S4's hand-rolled
`cust_map` payload is a local anonymous struct carrying
`{c_nationkey, n_name}`. Do NOT mint a new top-level
`customer_rn_t` (or similar "named output of CUSTOMER ⋈ NATION")
type — the mental model doesn't require one. The only forcing
function for a real type is an operator template that needs it
as a parameter (e.g., `HashJoin<JK, JR, R1, R2>` — see Rule 9).
Hand-rolled chains express the joined record inline. A per-query
`nationkey_to_name` cache is implementation glue to avoid
duplicate PK lookups; it is NOT the canonical column carrier
either.

**Rule 8 — S4 baselines use base tables only — no `col.split_*`.**
A hash-join baseline that consumes a custkey-sorted split secondary
borrows the merged-index family's locality and produces an unfair
comparison against S3. Use the `customer` / `orders` / `lineitem`
adapter members directly. See Q5 Phase 10B for the explicit fix.

**Rule 9 — Hand-roll vs reuse `HashJoin<…>` is a boilerplate trade-
off, not a star-schema / reduce-side rule.** The shared
`HashJoin<JK, JR, R1, R2>` in
`frontend/shared/merge-join/hash_join.hpp` requires (a) a `JK` type
satisfying `match()` / `matching_keys()` / `std::hash` / `operator%`
(the wildcard-key contract from §Sort-key wildcard semantics), and
(b) a `JR` join-result type. If you'd have to mint these wrappers
specifically for this join — that is, no existing SK/JR pair fits
— hand-writing the hashmap + probe loop costs the same or less
than the wrapper. Decide on syntax cost only:

- **Reuse `HashJoin<…>`** when an existing SK type already
  carries the join key (e.g., Q12's `ol_sort_key_t`; geo's
  hierarchical `sort_key_t`).
- **Hand-roll** when the join key would force a bespoke
  composite (tuple, multi-column custom struct) AND no other
  caller needs that key shape (e.g., Q3 / Q3I / Q5 dimension
  filters, S4 build chains keyed by PK).

The decision has nothing to do with whether the schema is "star"
or "snowflake" or how many dimension tables exist. It's purely:
will defining the wrapper types cost more boilerplate than the
inline hashmap probe?

---

## §Anti-Pattern Reference

| # | Anti-pattern | Source commit | Symptom | Fix |
|---|-------------|--------------|---------|-----|
| 1 | `BinaryMergeJoin` for view population | `f74b67da` | ~1 row per order group instead of N; view cardinality wrong | Manual two-pointer merge |
| 2 | Post-join Filter nodes | design rule | Filters not pushed down; perf loss and semantic divergence | Fuse into fetch lambdas (S1/S4) or Visitor hooks (S3) |
| 3 | Single-key comparator in `apply_topN` | `a4f5a4bc` | 3–4 distinct digests despite correct logic; ties resolved by input order | Multi-key comparator ending in a unique field |
| 4 | Reusing `--ssd_path` without wipe | `835b4f0a` | Lineitem count grows across re-runs; 4 distinct digests | `remove_all(ssd_path)` before `rocks_db.open()` |
| 5 | Per-structure loading in test | `721771bc` | RNG data drift; different lineitem counts per structure | Load once, populate all secondaries up front |
| 6 | Hash-aggregate inside MI family pipeline | OPERATORS.md §7 | Violates comparison-integrity; S1/S3 results are not comparable | Use SortedAggregate / inline accumulator; hash-aggregate only in S4 |
| 7 | Missing `accepts_key` for tagged types | `3ce2bf38` | `toType()` falls back to fold-length heuristic, misclassifying records | Explicit `static bool accepts_key(...)` on all `_coli_t` / `_col_t` types |
| 8 | Duplicate record type `id` | — | Silent data corruption: one type's records overwrite another's | `grep 'static constexpr int id'` before allocating |
| 9 | Zero-revenue orders in `flush_order` | `4dc93ec6` | S3 emits orders without matching lineitems; row count too high | Guard `revenue <= 0` at top of `flush_order` |
| 10 | ~~View missing baked-in filter~~ (RETIRED — see #24) | `4dc93ec6` reversed by `db60d49b` (Q3I) | Originally diagnosed as "S2 includes unfiltered rows; digest diverges". The actual root cause was that the view was the wrong shape (per-orderkey + pre-aggregated revenue); making the view per-lineitem + unaggregated dissolves the symptom. | Per-lineitem view, no filter baking; see #24 |
| 11 | S2 missing zero-revenue guard | `8fdcdda1` | S2 emits zero-revenue view rows other paths suppress | Add `revenue <= 0` check in `query_by_view` |
| 12 | `reinterpret_cast` on RocksDB values | `d8980426` | Alignment UB on platforms with unaligned value buffers | Use `memcpy` into a stack local instead |
| 13 | Load order: lineitems before orders | `6edcf2ca` | `order_dates` map empty during lineitem generation; dates default to 0 | Load order: customer → orders → lineitem |
| 14 | Raw indices as orderkeys | `f74b67da` | ~75% of lineitems are orphans with no matching order | Use `orderkey_from_index()` for sparse key generation |
| 15 | `load()` populating only one secondary | `47405bec` | 3 of 4 structures read empty adapters; fantasy throughput | Populate ALL secondaries unconditionally in `load()` |
| 16 | Hardcoded row-count assertion | `c077236f` | False test failures when data yields fewer than LIMIT rows | Assert cross-structure agreement + range `(0, K]` instead |
| 17 | No secondary cardinality check in test | `739ebf63` | Empty secondaries produce 0-row "fast" queries silently | Verify each secondary has nonzero rows after `populate_*` |
| 18 | ~~Physical Seek in skip path on RocksDB~~ (RETIRED — macOS-only artefact) | `8d10782b` reversed by `9da4f295` | Original macOS A/B saw SSTRead/TX rise 5× and was attributed to SST prefetch invalidation; Linux re-A/B refuted this — was a macOS page-cache artefact. RocksDB Seek-skip lifts SF=15 +700% / SF=40 +64% on Linux | Default both backends to `USE_PHYSICAL_SEEK_SKIP = true`; if a macOS regression resurfaces, override via `--use_seek_skip=0` rather than flipping the trait |
| 19 | `wants_skip_group()` alongside `bool on_order` | (post-`128f6d44`) | Dead code — `on_order → false` already clears `customer_active`; `wants_skip_group()` guard can never fire after that | Use `on_order → false` directly; remove `wants_skip_group()` when cleaning up |
| 20 | File-local `USE_PHYSICAL_SEEK_SKIP` constexpr instead of Backend trait | `83870b48` | Hard-coded constexpr makes per-backend tuning impossible and makes regression A/Bs (`--use_seek_skip=0`) require a recompile | Read `Backend::USE_PHYSICAL_SEEK_SKIP` from `frontend/tpch/backend.hpp` and let `--use_seek_skip` override it at runtime |
| 21 | Custom walker calling `MergedScanner::next()` for performance-critical paths | A2c (`6402ba97`) | Per-record `std::variant` construction (memcpy of widest-payload + dispatch tag setup) — 18–50% TX/s tax at SF=15 cache-resident on LeanStore; +28% on RocksDB | Use `scanner->next_raw()` returning `(tag_byte, key_slice, value_slice)`; dispatch via tag-byte switch + `memcpy` into the typed buffer the visitor needs |
| 22 | Reusing shared DB image for cross-structure perf comparison | A5 (`200ee0ae`) | Differential cache pollution at cache-resident SFs: structures with the largest secondary footprints are evicted disproportionately. Q3I SF=15 LeanStore: shared S3-vs-S1 gap = 36.6% but isolated gap = 13.7% — most of the gap was a benchmarking artefact | Use `--load_only_structure=N` + per-structure iso make targets (`q{N}_lsm_iso_M`); compare iso numbers, not shared |
| 23 | Skipping `--micro_perf` / `--cfstats` plumbing during bring-up | `b7ebebc8` | When perf surprises surface (and they will — H1, H4, H8 all did), no instrumentation means a round-trip to add it before any test can be run | Wire both flags into the executable scaffold; ~30 lines using `perf_context_capture.hpp` (RocksDB) + `scanner_perf_hook.hpp` (LeanStore) |
| 24 | Baking a parameterised filter into a secondary | Q3I S2/S5 audit (2026-05-03; commits `db60d49b`, `8ac423dd`) | Wrong answers for any param other than the validation value; surfaced (or hidden) by `[SKIP]` parity guards rather than fixed | Store unaggregated source rows in the secondary; apply parameterised filters live in `query_by_*`. Aggregates may bake **only** spec-hardcoded constants |
| 25 | Pinning `Params::defaults()` across the entire `helper.run()` loop | (post-2026-05-03 fix in commits `f3573b0f`, `dbcce8d8`, `b9ef4947`) | Bugs that depend on a particular param value (e.g. shipdate-baked aggregate) survive long benchmark runs without ever firing | `wrapper.set_params_for_iter(count)` before each `wrapper.query(out)`; per-query rotation through a deterministic param table covering all SUBSTITUTION-PARAMETER domain values |
| 26 | `[SKIP X]` parity guards in the cross-structure test harness | Q3I S5 (`tests/q3i/test_query_q3i_leanstore.cpp` guard retired by `8ac423dd`) | A storage variant that diverges at non-default params is excused as "baked-in filter mismatch", masking unsoundness | A `[SKIP]` is a structural-soundness alarm. Treat it as a fix-blocker, not a documented exception. If the variant cannot match parity at all params, the variant's design is wrong — rebuild it (don't bypass the check) |
| 27 | Reusing one cardinality framing across pure-hierarchical and sibling-aggregate queries | Q3 Phase 0 design draft (commit `dad7ccce` reverted by `d50cc33a`) | "3-way M:N with no sibling shortcut" framing imported into a query that has no sibling at all — undersells the hierarchical-prefix story and confuses reviewers | Use the typology in `PLAYBOOK.md §3.5 §4`: pure hierarchical, hierarchical + sibling sub-aggregate, OR genuine tree. Never import (2)'s "no sibling shortcut" wording into (1) or (3) |
| 28 | Silently continuing on an illegal hook return instead of throwing | Q5/Q3 walker bring-up (2026-05-09) | Wrong-but-plausible answers: a visitor arm that returns `SkipOrder` when no order is open is a programming error, not a runtime condition; swallowing it silently produces subtly wrong aggregates that still pass non-zero parity | `throw std::logic_error` immediately — see §"Contract violations & fail-fast" below |
| 29 | Default `matching_keys() { return {*this}; }` on a sort key with a wildcard-able trailing slot | Q5 stage-2 join bring-up (2026-05-09) | HashJoin probe hashes to a different bucket than the build-side anchor; `equal_range` returns empty; query silently emits zero rows. BMJ's analogous failure surfaces via `match()` returning a non-zero compare instead of treating the WILDCARD_KEY slot as a wildcard | All three pieces required: per-field `wildcard_match` in `match()`, full prefix-anchor enumeration in `matching_keys()`, AND `SKBuilder<JK>::project<R>` stripping absent fields to WILDCARD_KEY for BMJ's `join_state::join_and_clear`. Hash stays wildcard-blind. See §Sort-key wildcard semantics above for the audit checklist; `q5_sort_key_t` and `geo::sort_key_t` are the canonical reference impls |
| 30 | `std::vector<AggRow>& out` as the visitor's emit target paired with a trailing `apply_topN` | Q3 / Q3I pre-`458bcaf0` | Buffers every qualifying row before truncation: ~150K rows at SF=1 → ~6M (~192 MiB) at SF=40, just to discard 99.999% of them. Cache-thrashes the discard pile and defeats the merged-index streaming benefit at the post-pipeline boundary | Use a small-buffer OutClass instead — `TopNSink<R, Cmp>` for LIMIT queries, a HashAggregate-style aggregator (`NNameRevenueAggregator`) for global aggregates. See §Post-pipeline OutClass above for the contract and the two canonical impls. `apply_topN` itself is kept for intrinsically-small result sets (e.g. Q12's per-shipmode HashAggregate output) where the buffer never grows past the bound |

---

## §Contract violations & fail-fast

**Rule**: any operator that detects a contract violation in its inputs —
an illegal hook return, a malformed record, a broken structural invariant
— **must throw** rather than silently continuing.  Silent fallback masks
bugs and produces wrong-but-plausible answers that may pass non-zero
parity checks.

The canonical example is the col/coli group-walk walkers
(`col_pipeline.tpp`, `coli_pipeline.tpp`): when an `on_customer` or
`on_invoice` arm returns `WalkAction::SkipOrder`, no order is open at
that point in the byte-lex scan, so `SkipOrder` is semantically
meaningless.  The walkers throw immediately:

```cpp
// col_pipeline.tpp — on_customer arm
if (action == tpch::WalkAction::SkipOrder)
    throw std::logic_error(
        "col_group_walk: on_customer returned SkipOrder — "
        "no order is open; use SkipGroup instead");
```

The full per-hook contract is documented in
[`tpch_family/walk_action.hpp`](tpch_family/walk_action.hpp).
The same principle applies everywhere: a helper that detects a missing
REGION row, an out-of-range enum, or an impossible join state must throw,
not return a sentinel or silently skip.
