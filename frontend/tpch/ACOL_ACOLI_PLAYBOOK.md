# aCOL / aCOLI Playbook — adding a pre-aggregated merged index (S5)

How to add the **S5** storage structure: a *pre-aggregated* merged index that
bakes a parameter-independent aggregate at load time, sitting between raw
co-location (S3) and full materialisation (S2) on the pre-computation spectrum.

This is the companion to [`PLAYBOOK.md`](PLAYBOOK.md) (vanilla S1–S4 bring-up).
Read the main playbook first; this doc is **only** about the S5 variant.

Two reference implementations teach opposite lessons:

| | aCOLI (Q3I) | aCOL (Q10) |
|---|---|---|
| MI | `MergedAdapter<customer_acoli_t, orders_coli_t, lineitem_acoli_t>` | `MergedAdapter<customer_coli_t, orders_acol_t>` |
| bakes | per-**customer** invoice scalar `pre_open_due` | per-**order** `returned_revenue` |
| children | keeps lineitems raw | **drops** lineitems |
| walker | **generic** `getScanner`+`std::visit` | **hand-rolled** `acol_group_walk` |
| result | **S5 lost to S3** (deferred) | **S5 fastest structure** |

The difference between those last two rows is the whole point of this doc.

---

## §0 — When to read

- Adding S5 to a new query (Q5/Q5I, …).
- Deciding the pre-aggregation **grain** (per-order vs per-customer).
- Revisiting the Q3I aCOLI deferral ([`PLAYBOOK.md §S5`](PLAYBOOK.md)).

If you only need S1–S4, stop — you don't need S5. S5 is worth it only when (a)
there's a *soundly bakeable* aggregate (§2) **and** (b) you will *hand-write the
walker* (§3). Skipping (b) reproduces the aCOLI deferral.

---

## §1 — What an aCOL/aCOLI is

A merged index that stores **pre-computed aggregate values** instead of (or
alongside) the raw child rows, exposed as `--storage_structure=5` via the
shared `AggregatedStructure` wrapper (`per_structure_workload.hpp`) →
`query_by_aggregated`.

It is the **fair merged-index answer to the pre-aggregated S2 view**: the view
gets to pre-aggregate; the MI should too. When done right (§3) the aCOL even
*beats* the view, because it co-locates the parent payload once per group
instead of duplicating it per child row (see `q10/PERFORMANCE.md §7`).

Spectrum: **S3** (raw co-location, recompute every query) → **S5** (bake the
parameter-independent part, recompute the rest) → **S2** (fully materialise).

---

## §2 — Soundness rule (the #1 gate; grain-dependent)

Canonical rule, verbatim from [`PLAYBOOK.md §3.5 step 5`](PLAYBOOK.md):

> **Soundness rule**: a secondary may bake aggregates **only** when every
> predicate in the aggregate's filter expression is a constant hardcoded by the
> TPC-H spec for this query (e.g. `i_status='O'` for Q3I's `cust_open_due`).
> Aggregates derived from parameterised predicates (`l_shipdate > $DATE`,
> `c_mktsegment = $SEGMENT`, etc.) **must not** be baked. Storing the
> unaggregated source rows and recomputing at query time is the only sound
> choice. Anti-pattern #10 / #24 for the historical bug.

**Grain corollary (Q10 D8 overturn).** Soundness is **grain-dependent**: the
same measure may be bakeable at a fine grain but not a coarser one, if a
parameterised filter applies *between* the two grains.

- Q10 returned revenue = `SUM(l_extendedprice·(1−l_discount))` WHERE
  `l_returnflag='R'`. The only lineitem-level filter is the spec constant
  `returnflag='R'`, so **per-order** returned revenue is bakeable.
- The `o_orderdate ∈ window` filter is parameterised but applies at **order**
  grain, *above* the per-order aggregate. So per-order is sound; **per-customer**
  pre-totalling the same revenue is **un**sound (you'd have to subtract
  out-of-window orders from a baked customer total — impossible).

This is exactly why Q10's D8 originally (wrongly) said "no S5": it only
considered the per-customer grain. **Always ask: at what grain is every filter
a spec constant?** Bake at *that* grain; recompute everything above it.

---

## §3 — The hand-rolled walker requirement (centerpiece)

**If you ship an S5, you MUST hand-write its walker.** A pre-aggregated MI with
a *generic* scanner is not worth shipping — it loses to S3.

This is the documented cause of the aCOLI deferral. From
[`PLAYBOOK.md §S5`](PLAYBOOK.md) (and `q3i/PERFORMANCE.md §A1`, H13/H15):

> post-G8 measurements show S3 > S5 on both backends … the most pre-computed MI
> variant *loses* to the raw COLI walk by ~2.5×. … The remaining plausible
> explanation is that the gap comes from **S3's hand-tuned `coli_group_walk`** …
> versus S5's generic MergedAdapter scan that lacks the equivalent tuning.

The fix Q10 demonstrated: mirror `col_group_walk_fused_emit`
(`tpch_family/col_pipeline.tpp`) — **`scanner->next_raw()` → switch on the
trailing idx byte → `memcpy` into the typed record → typed visitor hook. No
`std::variant`, no `std::visit`.**

Contrast the two, side by side:

- **Generic (slow) — `q3i/query.tpp::query_by_aggregated`:**
  `mi.getScanner()` → `while (auto kv = scanner->next())` →
  `std::visit([&](auto&& val){ if constexpr (is_same<V,…>) … }, kv->second)`.
  Per record: a `std::variant` is materialised and double-dispatched. Over the
  whole MI that overhead dominates the (tiny) aggregate compute → S5 < S3.

- **Hand-rolled (fast) — `q10_family/acol_walk.tpp::acol_group_walk`:**
  `while (auto raw = scanner->next_raw()) { auto [tag,k,v]=*raw; switch(tag){
  case TAG_CUSTOMER: memcpy(&rec,v…); visitor.on_customer(…); … } }`. No variant.
  Custkey-transition fires `on_group_end`. → S5 beats S3 *and* the view.

Per-group finalisation (NATION INL → `q10_agg_row_t` → `TopNSink`) is shared
with S3 — only the *scan/dispatch* is rewritten. Reuse the S3 visitor's
finalize shape; just drop the child hook the aggregate replaces.

> Rule of thumb: the walker is ~80% of the S5 value and ~80% of the work. Budget
> for it. A `std::visit` version is an acceptable correctness stepping-stone but
> is **not** shippable — measure both if unsure (the `coli_walker_variant` A/B
> mindset).

---

## §4 — Anatomy & the design axis

### The bake-vs-drop axis

- **aCOLI (bake-a-sibling, keep-children):** reduce an out-of-chain *sibling*
  table (INVOICE) to a per-customer scalar baked onto the customer record;
  keep the O×L children raw (revenue still parameterised by shipdate). 3-type.
- **aCOL (reduce-children, drop-them):** reduce the *child* level (LINEITEM) to
  a per-parent aggregate folded into the parent (ORDER) record; **drop** the
  children entirely. 2-type. Strictly smaller than S3 — this is the win.

Pick by where the soundly-bakeable aggregate lives (§2) and whether the raw
children are still needed at query time.

### Record types

New pre-agg record type per baked level, in the family's `views_*.hpp`
(`views_col.hpp` for COL, `views_coli.hpp` for COLI). Each needs:
`tagged_path<IDX, tag_field_step<…>…>`, a distinct trailing **`idx_id`** byte,
an `accepts_key` hook reading that byte, the manual
`keyfold`/`unfoldKey`/`maxFoldLength` delegators, and a `from_base` factory.
Mirror the sibling type exactly (e.g. `orders_acol_t` mirrors `orders_coli_t`,
swapping `o_shippriority` for the baked `returned_revenue` and idx 1→37).

**Reuse the base type when no extra baked field is needed.** Q10's aCOL reuses
`customer_coli_t` verbatim (id=30) — Q10 has no per-customer baked scalar. Only
mint a new customer type (like `customer_acoli_t`, id=49) when you bake a field
onto it. Reusing a type across two coexisting MergedAdapters is **safe** (§6).

**idx_id allocation** (the trailing dispatch byte; must be unique *within* an
MI). In use: `customer=0, orders=1, lineitem=2, invoice=3` (COLI domain),
`customer_coli=30`, `lineitem_col=36`, `orders_acol=37`, `customer_acoli=49`,
`lineitem_acoli=53`; `50–52` retired. Pick any free byte (next: 54, or 38–48).
Define COL-family ids as `static_cast<coli_idx_id>(N)` constexpr in
`views_col.hpp` (don't extend the upstream enum).

**SKMatcher.** Each new co-resident type needs `SKMatcher<R1,R2>`
specializations for every pair it forms in the MI (self-pair + both directions
vs each existing member) — the MergedAdapter instantiation requires them. See
the `orders_acol_t` block in `views_col.hpp` (3 specializations: self,
`customer_coli_t×orders_acol_t`, reverse).

**Projection + uniformity** (links, non-negotiable): pre-agg secondaries carry
**only query-required columns** ([`CLAUDE.md §Project pushdown`](CLAUDE.md));
every co-resident key must use `tagged_path` encoding
([`CLAUDE.md §Tagged-key uniformity`](CLAUDE.md)).

---

## §5 — Step-by-step recipe

0. **Phase 0 decide.** Is there an aggregate whose filter is all spec-constants
   at some grain (§2)? If no → no S5, stop. If yes → which grain? bake-and-drop
   (aCOL) or bake-a-sibling-keep-children (aCOLI)? Record it in `q{N}/CLAUDE.md`
   Storage Structure Options + a decision.
1. **Record type(s) + SKMatcher** in `views_{col,coli}.hpp` (§4).
2. **Loader `populate_{aggregated,q{N}_acol}`.** Cheapest path: reuse the S3
   walk to compute the aggregate. Q10 reuses `col_group_walk` in
   `Q10FilterMode::ViewLoadPreagg` with a thin sink that inserts the agg record
   (`q10_family/view_loaders.hpp`) + a separate customer base scan. aCOLI uses a
   multi-pass base scan (`tpchi_family/coli_pipeline.tpp::populate_aggregated`:
   sibling→map, then customer/orders/lineitem inserts).
3. **Hand-rolled walker + `query_by_aggregated`** (§3). New
   `q{N}_family/acol_walk.tpp` mirroring `col_group_walk_fused_emit`; a visitor
   that snapshots parent FD cols, folds the baked aggregate, finalises per group
   (reuse the S3 sink/NATION-INL/TopN).
4. **Wire S5.** `AggregatedQ{N}` alias (`per_structure_workload.hpp`); aCOL MI
   member + ctor param in `workload.hpp`; `load()` populates it + `get_size()`
   case 5 (`load.tpp`); `case 5:` in both executables; `generate_targets.py`
   `STRUCTURE_OPTIONS[q{N}_*] += [5]` (surgical — touch only your query's entry).
5. **Parity** (correctness before perf — discipline). Add an **S5-vs-S3 A/B
   check** to `test_query_q{N}_{lsm,btree}` (digest ≡ S3 at both param iters),
   mirroring the S2-preagg / S3-physical check blocks in `tests/q10/`. Both
   backends green at SF=1 before any perf.
6. **Perf A/B** at the iteration cell (`make q{N}_btree_5 scale=… dram=…
   q10_stats=true`). S5 must beat the **fair** S2 view, not just raw S3 — a slow
   per-lineitem view is a strawman (`q10/PERFORMANCE.md §1`). If S5 isn't clearly
   winning, profile dispatch (the §3 lesson) before concluding. Confirm at 5L.

---

## §6 — Pitfalls (from building the aCOL)

- **LeanStore btree ops (incl. `MergedAdapter::size()`) must run inside a
  worker** (`crm.scheduleJobSync`), never from `main()` — they touch
  `cr::Worker::my()` and segfault otherwise. This bit the aCOL test (the crash
  *looked* non-deterministic across iterations purely from stdout buffering; gdb
  pinned it to a `.size()` call in `main`). RocksDB `.size()` works from `main`,
  so LSM won't catch it — test btree.
- **Reusing a record type across two coexisting MergedAdapters is safe.** q3i's
  COLI MI and aCOLI MI both contain `orders_coli_t`; Q10's COL MI and aCOL MI
  both contain `customer_coli_t`. Separate btrees (registered by name), no
  collision.
- **2-type MergedAdapters work** (`<customer_coli_t, orders_acol_t>`); no minimum.
- **Tagged-key uniformity is mandatory** — every co-resident type uses
  `tagged_path` with a distinct trailing idx byte (§4).
- **Don't cargo-cult a scan/insert "decouple."** An early aCOL hypothesis blamed
  concurrent COL-scan + aCOL-insert and buffered the inserts; the real bug was
  the `.size()` call above. The direct-insert-during-scan loader (same pattern
  as the preagg-view loader) is correct — keep it simple.

---

## §7 — Worked examples & cross-references

- **Q10 aCOL — implemented, fastest structure** (the template to copy):
  [`q10/CLAUDE.md`](q10/CLAUDE.md) D8 (revised) + Storage Structure Options;
  [`q10/PERFORMANCE.md §7`](q10/PERFORMANCE.md) (live numbers — cite, don't copy:
  iteration-cell SF=150 S5 9.98 ms/q vs view 21.5 vs raw S3 1,017). Code:
  `q10_family/acol_walk.tpp`, `q10_family/view_loaders.hpp::populate_q10_acol`,
  `q10/query.tpp::query_by_aggregated`, `views_col.hpp::orders_acol_t`.
- **Q3I aCOLI — deferred, the cautionary tale** (what NOT to do for the walker):
  [`q3i/CLAUDE.md`](q3i/CLAUDE.md) §Phase 4; [`q3i/PERFORMANCE.md §A1`](q3i/PERFORMANCE.md)
  (the S3 > S5 anomaly, H13/H15). Code: `tpchi_family/coli_pipeline.tpp::populate_aggregated`,
  `q3i/query.tpp::query_by_aggregated` (generic `std::visit` scan).
- **Rules:** [`PLAYBOOK.md §3.5 step 5`](PLAYBOOK.md) (soundness),
  [`PLAYBOOK.md §S5`](PLAYBOOK.md) (the deferral framing — this playbook is the
  "S5 done right" counterpart), [`CLAUDE.md §Project pushdown`](CLAUDE.md),
  [`CLAUDE.md §Tagged-key uniformity`](CLAUDE.md), `CONVENTIONS.md` anti-patterns
  #10 / #24 (baked-parameterised-filter bug).
- **Future candidates:** Q5 (aCOL — per-nation/region grain?), Q5I (aCOLI).
  Apply §2 first: find the spec-constant grain.
