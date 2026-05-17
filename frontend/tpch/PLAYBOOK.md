# TPC-H Query Implementation Playbook

Step-by-step guide for implementing a new TPC-H query across all four
storage structures (S1 merge join, S2 pipeline view, S3 merged index,
S4 hash join) with cross-structure XOR parity verification.

**Canonical reference implementation**: Q3I (`q3i/`).
Every template and pattern in this document is derived from Q3I's
implementation and the bugs fixed during its 3-phase bring-up.

---

## §0 — Prerequisites

Before starting, read (in order):

1. This directory's `CLAUDE.md` — shared infrastructure, file conventions,
   completed work.
2. `OPERATORS.md` — per-operator strategy, filter pushdown rules,
   comparison-integrity constraints.
3. The target query's `CLAUDE.md` (e.g. `q5i/CLAUDE.md`) — SQL, extension
   rationale, open questions.
4. `MULTI_TABLE_MI_ANALYSIS.md §6` — dual-track rationale (COL vs COLI).
5. `INVOICE_EXTENSION_CANDIDATES.md` — per-query analysis.
6. `q3i/PERFORMANCE.md` — performance investigation worklist format
   (forward-looking hypotheses + A-tests + archive cycle). Read if
   you expect to instrument the new query.
7. `q3i/archive/PERFORMANCE-2026-05-03b.md` — example evidence trail
   (A1 + A5 + A2c + A3); skim to see how a hypothesis is prosecuted
   end-to-end.

### Phase dependency graph

```
Phase 0   (design doc — REQUIRED before any code)
  → Phase 1   (skeleton + schema + pipeline-owned load.
  │            8-file shape, all query_by_* stubs, real ctor + load()
  │            populating base + splits + merged unconditionally;
  │            test_query_*_lsm reports [OK] cardinality on base +
  │            splits + merged; pipeline_view rows == 0 [OK] deferred;
  │            parity gate: digest 0x0 across empty stubs)
      → Phase 4a (join-FREE query bodies, ordered:
      │           §7.1 S3 walker FIRST;
      │           then §6 populate_q{N}_view (Pattern B reuses S3 walker);
      │           then §7.3 S2 view scan;
      │           then §7.4 S5 if applicable)
        → Phase 4b (join-DRIVEN query bodies: §7.5 S4 hash,
        │           §7.2 S1 BMJ chain)
          → Phase 5 (wiring + tests + docs:
                     per_structure_workload.hpp, executables,
                     test harness, CMake/targets, doc refresh)
```

> **Phase numbering note (2026-05-09 consolidation; updated
> 2026-05-15).** Phase numbers 2, 3, 6, 7, 8, 9 are intentionally
> retired — the underlying work is now folded into Phase 1 and Phase 5
> (wiring tail). **Phase 0.5 is also retired (2026-05-15)**: its
> skeleton deliverables (8-file stub set, executable links, digest-0x0
> parity gate) now land at the start of Phase 1 rather than as a
> separate commit gate. Per-query docs that reference "Phase 0.5"
> describe completed work accurately under the old model. **Phase 4 is
> preserved** because per-query docs and code comments reference its
> sub-§ headings as "Phase 4 §7.1" (S3 merged), "Phase 4 §7.2" (S1
> merge join), etc. Renumbering would churn dozens of cross-references
> for no architectural gain. Sub-section labels "(cont.)" identify
> continuations of the same phase.

> **Phase 4a / 4b split (2026-05-09; anti-pattern #29 driver).** Phase
> 4 splits along the SK-validation axis. Phase 4a lands the join-free
> paths (S3 walker, S2 view scan, S5 if applicable) — these exercise
> record types and aggregators but never call `BinaryMergeJoin` or
> `HashJoin`, so they don't depend on the SK's `matching_keys()` /
> `match()` wildcard wiring being correct. Phase 4b lands the
> join-driven paths (S4 hash, S1 BMJ) — these exercise every JK
> consumer pair. **Phase 4b's entry criterion is an SK wildcard
> smoke test**: for each JK consumer pair feeding a join, assert
> `SKBuilder<SK>::create(left)` either equals
> `SKBuilder<SK>::create(right)` for known-matching synthetic rows,
> or that the asymmetry is covered by the SK's `matching_keys()` +
> `match()` wildcard wiring (§4 "Sort-key wildcard semantics"). The
> Q5 stage-2 zero-rows bug (2026-05-09) was a Phase 1 SK-design
> error that hid until Phase 4 §7.5 — four phases of latency. The
> 4a/4b split shortens that to one sub-phase.

> **Process rule (NON-NEGOTIABLE).** No code change for the new
> query lands before Phase 0 is committed. Phase 0 produces the
> per-query design doc (`q{N}/CLAUDE.md`) — SQL, storage-structure
> table, plan descriptions, query shapes, open questions. Phase 1
> opens with the 8-file skeleton (stub bodies, compiles clean, digest
> 0x0 parity gate) as its first commit, followed in the same phase by
> schema widening and unconditional secondary population. The skeleton
> deliverables are no longer a separate phase gate. Skipping Phase 0
> has, historically, made the first real-code commit a 2000-line
> "kitchen sink" diff that's impossible to review.

---

## §1 — Decision Tree: Classify the Query

### Step 1: Which track?

| Question | If yes | If no |
|----------|--------|-------|
| Does the query join C+O+L+I (with Invoice)? | **Track 2 (COLI)** | → next |
| Does the query join C+O+L (no Invoice)? | **Track 1 (COL)** | Not covered here |

### Step 2: Query-specific parameters

| Parameter | Options | Affects |
|-----------|---------|---------|
| Sibling sub-aggregate (Invoice) | yes (Q3I, Q5I, Q10I) / no (Q3, Q5, Q10) | Visitor shape, accumulator count |
| Extra dimension tables | 0 (Q3/Q3I), 3+ (Q5/Q5I: S+N+R), 1 (Q10/Q10I: N) | Outside-pipeline hash lookups |
| Aggregate shape | per-order (Q3/Q3I), per-nation (Q5/Q5I), per-customer (Q10/Q10I) | GROUP BY key, result cardinality |
| Top-K present | yes (Q3: 10, Q10: 20) / no (Q5) | `apply_topN` vs full sort |
| Result cardinality | bounded by LIMIT / bounded by dim table (~5 nations) | Sort strategy |

### Step 3: Reference mapping

All queries use Q3I (`q3i/`) as the canonical reference. Adjust:
- **Track 1 (COL)**: same patterns, remove `on_invoice` hook, remove invoice
  accumulator, remove invoice split adapter, use COL pipeline instead of COLI.
- **Per-nation aggregate (Q5/Q5I)**: replace per-order `flush_order` with
  per-nation accumulation in `on_group_end` + outside-pipeline nation hash.
- **Per-customer aggregate (Q10/Q10I)**: emit one row per customer in
  `on_group_end` instead of per-order in `flush_order`.

---

## §2 — Infrastructure: COL vs COLI Pipeline

### COLI pipeline (exists — `coli_pipeline.hpp`)

4-table MI: `customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`,
`invoice_coli_t`. Provides `coli_group_walk`, `populate_merged()`,
`populate_split()`. Used by Q3I; reuse for Q5I, Q10I.

### COL pipeline (does NOT exist yet)

3-table MI without Invoice. **Must be built before any Track 1 query.**
Structurally identical to COLI minus the invoice dimension:

- `views_col.hpp`: 3 tagged record types (`customer_col_t`, `orders_col_t`,
  `lineitem_col_t`), domain tags without invoice, 6 `SKMatcher` specs
  (vs COLI's 12).
- `col_pipeline.hpp`: `CustomerOrdersLineitemPipeline<Backend>` with
  `col_group_walk`, `populate_merged()`, `populate_split()` (2 split
  adapters instead of 3).
- Derive both files from `views_coli.hpp` and `coli_pipeline.hpp` by
  removing all invoice references.

The Visitor pattern, accumulator pattern, and all 4 storage-structure
body shapes are identical between COL and COLI — the only difference is
whether `on_invoice` exists and whether the pipeline owns an invoice
split adapter.

**Building the COL pipeline is a separate infrastructure task, not part
of per-query implementation.** This playbook assumes the pipeline exists.

### S5 (aCOLI / pre-aggregated MI) — DEFERRED for the current paper (all queries)

The aCOLI MI (`MergedAdapter<customer_acoli_t, orders_coli_t,
lineitem_acoli_t>` with `pre_open_due` baked at load time) is
**deferred indefinitely** from the paper sweep, **for all queries
including Q3I**. The Q3I S5 implementation stays in tree (it builds,
parity-passes, and remains useful as a working aCOLI design
reference), but it is not part of any reported figure or sweep.
New queries should land **S1–S4 only**.

**Reason** (`q3i/PERFORMANCE.md §A1`, hypothesis ledger H13/H15):
post-G8 measurements show S3 > S5 on both backends at SF=15
cache-resident — the most pre-computed MI variant *loses* to the raw
COLI walk by ~2.5×. Code inspection refuted "S5's revenue
accumulator dominates" (S3 and S5 share `LineitemRevenueAccumulator`
over identically-projected lineitem payloads). The remaining
plausible explanation is that the gap comes from **S3's hand-tuned
`coli_group_walk`** (A2c fused-emit + A3 customer-level Seek-skip,
massive measured wins) versus S5's generic MergedAdapter scan that
lacks the equivalent tuning.

Closing the gap would require **hand-writing a second walker** for
the aCOLI MergedAdapter family with the same dispatch tuning and
Seek-skip facility. That is not a per-query task — it's
infrastructure work on the order of the original COLI walker.

**For the paper's story, S1–S4 are sufficient**. The pitch is **S3
matches S2 while beating S1/S4** — i.e. raw co-location with a tuned
walker reaches the throughput of full materialisation without paying
materialisation's storage cost or maintenance burden, and does so
while comfortably beating split-index merge-join (S1) and hash-join
(S4) baselines. S5 (MI-as-aggregate-store) addresses a separate
reviewer concern (R2-D1) and can be re-investigated later when the
second walker is justified by a query whose semantics the S3 walker
cannot serve.

**What this means for new queries** (Track 1 / Track 2):

- Implement S1 / S2 / S3 / S4 only. No `query_by_aggregated`. No
  aCOLI adapter members. No `populate_aggregated()` call site.
- Per-structure wrappers stop at `HashQ{N}`; do not add
  `AggregatedQ{N}`.
- Storage-structure dispatch in executables and `load()` covers
  cases 1–4.
- The aCOLI record types (`customer_acoli_t`, `lineitem_acoli_t`)
  remain in `views_coli.hpp` for Q3I's existing S5 path (kept as a
  working design reference, not reported); new queries do not
  reference them.

When S5 is later revisited, the work item is "build aCOLI walker as
a tuned analogue of `coli_group_walk`," not "wire S5 into a new
query." Track that as infrastructure, not per-query. Re-enabling S5
in the paper sweep would also require re-running the Q3I S5
measurements end-to-end against the new walker.

---

## §3 — Directory and File Scaffold

Create `frontend/tpch/q{N}/` (or `q{N}i/` for invoice-extended) with
these 8 files:

| File | Purpose |
|------|---------|
| `views.hpp` | Record types: pipeline view, agg row, intermediate join types |
| `workload.hpp` | Workload class, Params, predicates, Stats |
| `per_structure_workload.hpp` | Alias-only: `BaseQ{N}`, `ViewQ{N}`, `MergedQ{N}`, `HashQ{N}` |
| `load.tpp` | Constructor, `load()`, `get_size()`, `populate_q{N}_view` |
| `query.tpp` | `Params::defaults()`, `print()`, predicates, accumulators, 4 `query_by_*` |
| `executable_rocksdb.cpp` | Production RocksDB main |
| `executable_leanstore.cpp` | Production LeanStore main (`#ifndef ROCKSDB_ONLY`) |
| `CLAUDE.md` | SQL, plan descriptions, implementation status |

**Namespace**: `tpch::q{N}` (e.g. `tpch::q5i`).

### Record type ID allocation

IDs must be globally unique. Before choosing:

```bash
grep -rn 'static constexpr int id' frontend/tpch/ --include='*.hpp' | sort -t= -k2 -n
```

Pick the next free block (Q3I uses 42–48; start from 50 for Q5I, 60 for
Q10I, etc.). Allocate one ID per struct that participates in an adapter.

> **PITFALL — Duplicate record type IDs** (from `views_coli.hpp` bring-up):
> Two record types sharing an `id` value will collide in the same RocksDB
> column family. The symptom is silent data corruption at load time — one
> type's records overwrite the other's. The `id` grep above is the only
> reliable check.

---

## §3.5 — Phase 0: Per-query design doc (REQUIRED before any code)

The first thing that lands for a new query is **`q{N}/CLAUDE.md`**.
No `.hpp`/`.cpp`/`.tpp` file appears in the tree until this doc has
been committed and reviewed. The doc captures the big-picture design
so the implementation can be reviewed against an explicit target,
not against the implementer's evolving mental model.

### Required sections

1. **TPC-H definition (extended)** — the SQL with substitution params
   in their canonical form. For Track 2 (invoice-extended) queries,
   show both the original and the extended SQL side by side.
2. **Substitution parameters** — table: parameter | domain |
   description | validation value.
3. **Motivation** — *why* this query was picked. For invoice-extended
   queries this is also where the §3.1.x pattern (sibling
   sub-aggregate / hierarchical / etc. — see
   `MULTI_TABLE_MI_ANALYSIS.md`) is named. For Track 1, why
   merged-index co-location is interesting on this join shape.
4. **Cardinality structure** — true M:N joins vs scalar attachments
   vs lookups. Q3I's "not a true 4-way M:N" section is the template:
   surface any asymmetry that affects what the merged-index pitch
   is actually claiming. This is the single most important
   anti-overclaiming safeguard.

   **Three distinct framings (pick one):**

   1. **Pure hierarchical (linear chain)**: every table in the join
      chain contributes many-to-many rows along a strict prefix
      hierarchy (e.g., `custkey ⊃ orderkey ⊃ linenumber` for Q3:
      customer × orders × lineitem). The MI benefit is hierarchical-
      prefix scan locality, full stop. **Do not** describe the schema
      as "M:N with no sibling shortcut" — there is no sibling.
   2. **Hierarchical + sibling sub-aggregate**: a sibling table
      (Q3I's INVOICE) is reduced to a per-key scalar before the main
      join — the §3.1.2 sibling sub-aggregate pattern. The "not-a-
      true-N-way-M:N" caveat applies here: only the chain is genuinely
      M:N; the sibling collapses to a scalar attachment.
   3. **Genuine tree (sibling-with-children)**: the sibling is itself
      a sub-hierarchy (e.g., invoice × invoiceitem) and contributes
      M:N rows that are NOT pre-aggregated. The MI co-locates the
      full tree under the root key; both branches stream
      independently inside the walker. This is the strongest §3.1.3
      showcase — every record in the MI participates in the join
      output, no scalar reductions, no asymmetry to disclaim. None
      of Q3 / Q3I / Q12 is this shape; it would belong to a future
      query that joins multiple branches of a customer's data
      structure (e.g., orders AND invoices AND their line/payment
      children) without collapsing any branch to an aggregate.

   Cross-reference the chosen framing in §Motivation. Confusing
   the framings undersells (1)/(3) or overclaims (2). Importing
   (2)'s "no sibling shortcut" wording into (1) is the specific
   mistake the Q3 Phase 0 audit caught (anti-pattern #27 in §13).

5. **Storage Structure Options** — the 4–5 row table (S1 / S2 / S3 /
   S4 / [S5]) listing for each: strategy, secondary structure, join
   strategy, and **Params baked in** (required column — value is
   "none" or a reference to the spec-hardcoded constant, e.g.
   `i_status='O'` per TPC-H Q3I §3.2.3).

   **Soundness rule**: a secondary may bake aggregates **only** when
   every predicate in the aggregate's filter expression is a constant
   hardcoded by the TPC-H spec for this query (e.g.
   `i_status='O'` for Q3I's `cust_open_due`). Aggregates derived from
   parameterised predicates (`l_shipdate > $DATE`,
   `c_mktsegment = $SEGMENT`, etc.) **must not** be baked. Storing
   the unaggregated source rows and recomputing at query time is the
   only sound choice. Anti-pattern #10 / #24 (§13) for the historical
   bug.

6. **Plan Descriptions (query shapes)** — operator graphs for each
   storage structure. Either inline DOT diagrams or `plans/*.dot`
   files referenced from the doc. **Each shape names the explicit
   logical plan and the physical specialisation it ships.** This is
   what gets compared against the implementation in code review;
   absence here = freelance code in `query.tpp`.
7. **Filter pushdown principle** — query-specific application of the
   canonical rule (`OPERATORS.md §Filter Pushdown`). State which
   filters fuse with which TableScan, which fuse with aggregators,
   which fuse with Visitor hooks. The doc is the source of truth;
   if `query.tpp` does something else, that's a review comment.

   - **No parameterised filter may be baked into any secondary.** The
     COL MI, COL custkey-sorted secondaries, and `q{N}_pipeline_view_t`
     are all loaded **without** applying SEGMENT / DATE / THRESHOLD /
     any other parameter. This is non-negotiable: a baked param ties
     the secondary to its load-time value and the test harness's
     `[SKIP]` guard becomes the only thing that hides the
     wrong-answer bug. (Q3I S2/S5 audit, 2026-05-03.)

8. **Required Record Types** — `q{N}_pipeline_view_t`,
   `q{N}_agg_row_t`, intermediate join types (if S1 uses BMJ chain).
   Just the shape and key — full bodies land in Phase 1.

   **Composition with sibling queries**. If this query has a sibling
   in the same family (Q3 / Q3I, hypothetical Q5 / Q5I, etc.), aim
   for the new record types to be a strict subset / projection of the
   sibling's types so a single `views_*.hpp` definition serves both.
   Concretely: `customer_col_t` ≡ `customer_coli_t` minus invoice-aware
   sentinel ordering; `LineitemRevenueAccumulator` is reused verbatim
   across both queries' walkers. Where types must diverge (Q3I-only
   `CustomerOpenDueAccumulator`), they live in Q3I-only files. The
   relationship is **composition**, not "Q3I extends Q3" — the goal
   is DRY (a fix in one place doesn't have to be applied twice), not
   inheritance.

9. **Open questions** — whatever wasn't pinned down. Better to log
   them up front than discover mid-Phase-4.

### Why this gating exists

Q3I's bring-up tried to land design and code in parallel. The result
was three rounds of large refactors after most of the code existed,
each driven by realisations that should have surfaced in a doc
review. Having a frozen design doc means the rest of the playbook
fills in named blanks; deviations are explicit decisions, not
discoveries.

### Exit criterion

The design doc is committed; a reviewer signs off; this commit
contains zero `.hpp` / `.cpp` / `.tpp` changes for the new query.

---

## §3.6 — Phase 1: Skeleton + schema + pipeline-owned load

Phase 1 is the first real implementation phase. It opens with the
8-file skeleton as its first commit and closes when all secondaries
populate cleanly and the test harness asserts `[OK]` on base + split +
merged cardinalities, `pipeline_view rows == 0 [OK]` (deferred), and
digest 0x0 parity across empty stubs.

### 8-file source tree

- All 8 files from §3 created in `q{N}/`.
- `views.hpp`: `q{N}_pipeline_view_t` with real Key shape (see below),
  real payload, `operator<=>`, `ADD_RECORD_TRAITS`, and `print()`
  stub. `q{N}_agg_row_t` real shape. Intermediate join-result types
  (`q{N}_jr*_t`, sort-key types) deferred to Phase 4b — they depend
  on the BMJ design and must not be pre-speculated. See §Design
  constraint: tree-shaped COLI keys in per-query CLAUDE.md.
- `workload.hpp`: `Q{N}Workload<Backend>` class with all adapter
  members, `Params` struct with `defaults()` real body,
  `set_params_for_iter` real body, `Q{N}Stats` struct declared outside
  the class (but inside the namespace), `Q{N}Stats* stats = nullptr;`
  member, predicate declarations, four `query_by_*` declarations. All
  bodies in `query.tpp` are stubs (`out.clear(); return 0;`).
- `per_structure_workload.hpp`: alias-only, completed.
- `load.tpp`: ctor + `load()` + `get_size()` real bodies.
- `query.tpp`: `Params::defaults()` real body; predicate implementations
  real; four `query_by_*` stubs; `print()` stub.
- `executable_{rocksdb,leanstore}.cpp`: full `main()` mirroring Q3I —
  loads data and executes each storage structure (returning empty
  results) without crashing.
- `tests/q{N}/test_query_q{N}_rocksdb.cpp`: extended harness with
  strict-equality cardinality block (see §Exit criterion below) plus
  the digest-0x0 parity check from the skeleton.
- `q{N}/CLAUDE.md`: Implementation Status section updated.

### `q{N}_pipeline_view_t` Key shape

The Key mirrors the leading record-type key of the MI's deepest record
type. For COLI queries (Q3I, Q5I, Q10I) the deepest type is
`lineitem_coli_t`, whose Key is `(custkey, orderkey, invoicekey,
linenumber)`. The view Key must match so the MI and view are sortable
in the same order and the view loader can emit in key order.

```cpp
struct Key {
   static constexpr int id = {{ID}};
   Integer custkey;
   Integer orderkey;
   Integer invoicekey;   // mirrors lineitem_coli_t
   Integer linenumber;
   ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey,
                  &Key::invoicekey, &Key::linenumber)
   auto operator<=>(const Key&) const = default;
};
```

### `q{N}_pipeline_view_t` payload

Carries all FD-attached columns needed at query time, including
`n_name` when the query groups by nation:

```cpp
Numeric     l_extendedprice;
Numeric     l_discount;
Integer     l_suppkey;
Integer     c_nationkey;
Varchar<25> n_name;    // FD-attached at view-load time; MUST be Varchar
                       // (POD), NOT std::string — record_traits uses
                       // memcpy; libstdc++ std::string is non-standard-
                       // layout and corrupts across insert/getScanner.
Timestamp   o_orderdate;
Varchar<1>  i_status;
```

### `q{N}_agg_row_t` shape

Final output row. Key = the SQL GROUP BY key.

```cpp
struct q{{N}}_agg_row_t {
   static constexpr int id = {{ID}};
   struct Key {
      {{Integer o_orderkey;}}   // or n_name, c_custkey — depends on GROUP BY
      ADD_KEY_TRAITS({{&Key::o_orderkey}})
   };
   {{Numeric revenue;}}
   {{Timestamp o_orderdate;}}
   // ...all SELECT columns
   ADD_RECORD_TRAITS(q{{N}}_agg_row_t)
   void print(std::ostream& os) const;
};
```

### `Q{N}Stats` struct

```cpp
struct Q{{N}}Stats {
   long customers_scanned    = 0;
   long orders_scanned       = 0;
   long lineitems_scanned    = 0;
   long invoices_scanned     = 0;    // Track 2 only
   long customers_passing_nation   = 0;
   long orders_passing_date        = 0;
   long lineitems_passing_supp     = 0;
   long aggregator_rows_out        = 0;
   long mi_records_visited   = 0;
   long mi_groups_skipped    = 0;
   long view_rows_scanned    = 0;
};
```

Place outside the workload class but inside the `tpch::q{N}` namespace.
Counters are populated by Phase 4 bodies; in Phase 1 the struct exists
but no body writes to it.

### Predicate declarations

```cpp
inline bool q{{N}}_predicate_orders(const orders_t& o, const Params& p);
inline bool q{{N}}_predicate_lineitem(const lineitem_t& l, const Params& p);
inline bool q{{N}}_predicate_invoice(const invoice_t& i);  // Track 2 only
```

Bodies go in `query.tpp`.

### Workload class

```cpp
template <typename Backend>
class Q{{N}}Workload {
   TPCHWorkload<typename Backend::template Adapter>& tpch;
   typename Backend::template Adapter<customerh_t>&  customer;
   typename Backend::template Adapter<orders_t>&     orders;
   typename Backend::template Adapter<lineitem_t>&   lineitem;
   {{typename Backend::template Adapter<invoice_t>&  invoice;}}  // Track 2
   CustomerOrdersLineitemInvoicePipeline<Backend> coli;           // Track 2
   // or CustomerOrdersLineitemPipeline<Backend> col;             // Track 1
   typename Backend::template Adapter<q{{N}}_pipeline_view_t>& pipeline_view;
  public:
   Params params;
   Q{{N}}Stats* stats = nullptr;

   // Constructor (body in load.tpp)
   Q{{N}}Workload(/* all adapter refs */);

   // Pipeline accessor for test harness
   auto& coli_pipeline() { return coli; }

   void load();
   double get_size() const;

   long query_by_base  (std::vector<q{{N}}_agg_row_t>& out);
   long query_by_view  (std::vector<q{{N}}_agg_row_t>& out);
   long query_by_merged(std::vector<q{{N}}_agg_row_t>& out);
   long query_by_hash  (std::vector<q{{N}}_agg_row_t>& out);
};

// Include template bodies (IWYU keep)
#include "load.tpp"
#include "query.tpp"
```

### `Params` + `set_params_for_iter` + `defaults()` real bodies

One field per SQL substitution parameter. `set_params_for_iter` rotates
through a static table of valid SUBSTITUTION-PARAMETER tuples per the
TPC-H spec (covering at minimum every distinct domain value listed in
the query's §Substitution parameters). `params = table[iter % table.size()]`.
Test harness pins `Params::defaults()` for parity checks; production
executables go through `set_params_for_iter`.

**Why required**: a fixed-param loop hid the Q3I `pre_revenue` bug for
weeks. Per-query rotation surfaces baked-in-param secondaries within a
10-second `helper.run()`.

### `load()` dispatch

```cpp
template <typename Backend>
void Q{{N}}Workload<Backend>::load() {
   tpch.load();
   // Populate ALL secondaries unconditionally.
   // The Makefile flow loads once then runs 4 --recover invocations
   // selecting S1/S2/S3/S4 — if only one secondary is populated,
   // three of four read empty adapters.
   populate_q{{N}}_view<Backend>(/* adapter refs */);  // for S2
   coli.populate_split();                               // for S1
   coli.populate_merged();                              // for S3
   // S4 needs no secondary
}
```

> **PITFALL — `load()` populating only one secondary** (commit `47405bec`):
> An earlier Q3I design dispatched on `FLAGS_storage_structure` inside
> `load()`, populating exactly one of {split, view, merged}. The production
> Makefile loads once (default `FLAGS_storage_structure=1`) then runs four
> `--recover` invocations selecting S1/S2/S3/S4. Three of four read empty
> adapters — producing fantasy throughput (S2 ≈ 247k TX/s, S3 ≈ 274k TX/s
> vs S1/S4 ≈ 30 TX/s at SF=15) and 0 MiB secondary sizes.
> **Symptom**: one or more structures report absurdly high throughput with
> 0-row results and 0 MiB secondary size.
> **Fix**: populate ALL secondaries unconditionally in `load()`.

### `populate_q{N}_view` — Pattern A vs Pattern B

The implementation strategy depends on the join arity of the pipeline:

**Pattern A — prefix-chain join (≤2 base tables in the pipeline)**:
applies to Q3/Q12. Hand-roll via two-pointer merge (see Q3I
`load.tpp` lines 44–112):

1. **Invoice scan** (Track 2 only): build `unordered_map<Integer, Numeric>`
   per-custkey aggregate with `i_status` filter fused.
2. **Customer scan** (if needed): build per-custkey lookup map for
   FD-attached columns (e.g. `c_mktsegment`).
3. **Two-pointer merge** over orders + lineitem (both sorted by orderkey):
   for each order, advance lineitem pointer while `l_orderkey == orderkey`;
   accumulate revenue; emit one `q{N}_pipeline_view_t` row per
   `(custkey, orderkey)`.

Pattern A lands in Phase 1.

**Pattern B — wider join (≥3 base tables in the pipeline)**:
applies to Q5I (canonical landed reference) and the still-design-only
Q10I. Q3I is actually Pattern A — invoice reduces to a per-customer
scalar — so the "≥3 base tables" trigger means 3 base tables flowing
columns into the pipeline output, not just appearing in the join
graph. Hand-rolling a 4-way join here would duplicate the entire
query body in `load.tpp`. Instead, defer `populate_q{N}_view` to
Phase 4a, where it reuses the S3 group-walk with parameterised
filters disabled and an emit callback that inserts into the view
adapter:

> **Canonical Pattern B implementation**: `frontend/tpch/q5i/visitor.hpp`
> (`Q5IGroupWalkVisitor<Sides, Sink, Q5IFilterMode>` with `ViewLoad`
> mode) + `q5i/load.tpp` (`Q5IViewLoadSink` + `populate_q5i_view`).
> One visitor, two sinks (OutClass for query, view-insert for load),
> parameterised filters dropped in `ViewLoad` mode while FD-attached
> fields (`c_nationkey`, `o_orderdate`, `i_status`) flow through.

- FD-attached filters (e.g. `i_status` extraction from invoice) — keep.
- Parameterised filters (region/date/nation/supplier) — drop. The view
  must be predicate-hoisted so one load serves all param combinations.
- A `view_load_mode` flag (or equivalent) on `Q{N}IWorkload` routes
  the walker to a view-insert sink instead of the normal post-pipeline
  OutClass.

For Pattern B, the S2 branch in `load.tpp` at Phase 1 carries only a
comment:

```cpp
if (load_all || only == 2) {
   // Pattern B (PLAYBOOK §3.6): view loader reuses S3 group-walk
   // (Phase 4a) with parameterised filters dropped. Hand-rolling
   // a {N}-way join here would duplicate query_by_merged.
}
```

> **PITFALL — BinaryMergeJoin for view population (Pattern A only)**
> (commit `f74b67da`): Do NOT use `BinaryMergeJoin` for view loading
> in Pattern A. The hierarchical-key wildcard semantics on
> `ol_sort_key_t` cause the join-state machine to emit ~1 row per
> order group instead of N rows per (order, lineitem) pair. Use the
> manual two-pointer merge instead.
> **This PITFALL does not apply to Pattern B**: the Pattern B view
> loader reuses S3's walker, which has its own correctness contract
> (sentinel ordering, invoice buffer completeness). Any BMJ-related
> wildcard bugs are simply not in the code path.

> **PITFALL — View missing baked-in filter (Pattern A only)** (commit
> `4dc93ec6`): if `query_by_view` assumes a filter was baked into the
> view at load time but `populate_q{N}_view` forgot to apply it, S2
> produces more rows than S1/S3/S4.
> **This PITFALL does not apply to Pattern B**: the same walker code
> that bakes filters in S3 also drives the view loader; a filter
> omission shows up in both paths simultaneously, making it detectable
> by cross-structure parity.

### `get_size()` dispatch

```cpp
template <typename Backend>
double Q{{N}}Workload<Backend>::get_size() const {
   double base = customer.size() + orders.size() + lineitem.size()
               {{+ invoice.size()}};
   switch (FLAGS_storage_structure) {
      case 1: return base + coli.get_split_size();
      case 2: return pipeline_view.size();
      case 3: return base + coli.get_merged_size();
      case 4: return base;
      default: throw std::runtime_error("invalid --storage_structure");
   }
}
```

### SKBuilder / std::hash specializations (deferred to Phase 4b)

`std::hash` specializations (one per intermediate Key type, needed by
`HashJoin`) and `SKBuilder` specializations (one per join key type)
are deferred to Phase 4b. They depend on the BMJ design and the
chosen join-result type shapes — which for tree-shaped COLI queries
differ from the prefix-chain pattern and must be designed explicitly.
Q5I lands the canonical pattern (plain 2-field `q5i_co_jk_t` JK for
BMJ #2 over a tree-shaped lineitem key; see `q5i/views.hpp` ids 63–66
and the per-emit `CustkeyInvoiceBuffer` in `q5i/query.tpp`); Q10I
will diverge again and gets its own design. See §Invoice ⋈ spine:
Phase 4b decision space in the per-query CLAUDE.md.

→ see `CONVENTIONS.md §Sort-key wildcard semantics`

### Exit criterion

- `make -C build/frontend test_query_q{N}_lsm q{N}_lsm -j$(nproc)`
  builds clean.
- `test_query_q{N}_lsm --tpch_scale_factor=1` exits 0 with:
  - Strict-equality `[OK]` on `split_orders`, `split_lineitem`,
    `split_invoice`, and `merged_coli` (per-type breakdown +
    total via `coli_group_walk`).
  - Sentinel-ordering `[OK]` (customer before invoice/orders/lineitems
    within each custkey group).
  - `pipeline_view rows == 0 [OK]` with an explicit message
    "deferred to Phase 4a" so future agents don't mistake it for a
    regression. (Pattern A queries assert `rows == |lineitem|` here
    instead.)
  - Parity `[OK]` at digest 0x0 across empty stubs.

### Side-table templating + include order (invoice-extended queries)

Pattern B equi-join queries (Q5I and later Q10I) need a post-pipeline
side-tables struct (REGION/NATION/SUPPLIER hashmaps in Q5I) that the
OutClass and the COLI group-walk visitor both consume. The naive
shape — `Q5IOutClass` and `Q5IGroupWalkVisitor` both `#include
"side_tables.hpp"` and reference `Q5ISideTables` by name —
introduces a forward-declaration cycle when `side_tables.hpp` itself
needs `workload.hpp` types.

**Canonical fix (Q5I)**: template both consumers on a `Sides` type
parameter rather than naming the side-tables struct directly:

```cpp
template <typename Sides>
struct Q5IOutClass { … };

template <typename Sides, typename Sink, typename Mode>
struct Q5IGroupWalkVisitor { … };
```

`Sides` is instantiated at the call site (`workload.hpp` after both
`side_tables.hpp` and the visitor header are included), which lets
the template definitions compile without seeing the concrete
side-tables type.

**Include order**: `workload.hpp` must include `side_tables.hpp`
*before* it includes the visitor header. The visitor header includes
`workload.hpp` (cycle-free now that the visitor is templated). See
`q5i/workload.hpp` for the canonical ordering and
[`CONVENTIONS.md §Rule 10`](CONVENTIONS.md) Pattern B row for the
record-buffering contract that motivates this layout.

### Phase rollout and parity gating (S2≡S3 strict → 4-way strict)

Multi-structure queries land in three parity stages, each gating the
next. Skip a stage and a digest mismatch later in bring-up will
masquerade as a wrong-fix-in-the-wrong-structure puzzle.

1. **Stage 0 — digest 0x0**: all four `query_by_*` bodies are empty
   stubs; the test harness asserts `[OK]` parity at the zero digest.
   Surfaces wiring bugs (test harness, scratch dirs, RocksDB reload)
   before any query logic exists.

2. **Stage 1 — S2≡S3 strict** (Phase 4a). The view path (`query_by_view`)
   and the COLI group-walk path (`query_by_merged`) share the same
   per-record assembly; landing them first and asserting strict
   equality between them (no `[SKIP]`) validates the OutClass + the
   group-walk visitor + view loader together. S1 and S4 stay
   stubbed; parity over (S2, S3) only.

3. **Stage 2 — strict 4-way** (Phase 4b). S1 (BMJ chain) and S4
   (hash chain) land last; strict XOR parity across all four
   structures at non-zero digest is the exit criterion. Any
   `[SKIP]` tolerance at this stage is an anti-pattern (#26).

**Why this ordering**: S2 and S3 share the most code (the view's
emit callback IS the visitor's emit row in Pattern B per
[CONVENTIONS Rule 10](CONVENTIONS.md)) and validate the pipeline
boundary cheaply. S1 and S4 then validate the join-operator side
independently; if S4 diverges, the bug is in the hash build, not in
the OutClass. The Q5I bring-up (commits `469ffae9`, `15cfa612`,
`65bdb7a4`, `19104306`) is the canonical reference for this
sequencing.

---

## §7 — Phase 4: `query.tpp` (The Core)

Phase 4 is split into **4a (join-free paths)** and **4b (join-driven
paths)** along the SK-validation axis — see §0 "Phase 4a / 4b split"
for rationale.

| Sub-phase | Sub-§ | Path | Uses join primitives? |
|-----------|-------|------|-----------------------|
| **4a** | §7.1 | S3 (group walk over MI) | No — visitor pattern, no JK |
| **4a** | §7.3 | S2 (sequential view scan) | No — pure scan + filter |
| **4a** | §7.4 | S5 (aCOLI MI, if applicable) | No — group walk variant |
| **4b** | §7.5 | S4 (HashJoin chain) | **Yes** — `HashJoin<SK,...>` |
| **4b** | §7.2 | S1 (BMJ chain) | **Yes** — `BinaryMergeJoin<SK,...>` |

**Land Phase 4a first** — these paths work even if the SK's wildcard
wiring is wrong, so they validate data shape, accumulators, and
filter pushdown without dragging in JK bugs. Flip the test harness
to strict-on-{S2, S3, S5} at the end of 4a; S1 / S4 stay
`[SKIP]`-tolerant.

**Within Phase 4a, Pattern-B queries must follow this order**:
S3 walker (§7.1) → `populate_q{N}_view` (view loader reuses S3
walker) → S2 view scan (§7.3) → S5 if applicable (§7.4). The view
loader depends on the S3 walker's group-walk infrastructure, so S3
must land first. S2's `query_by_view` depends on the populated view,
so the view loader must land before S2.

### Pattern-B `populate_q{N}_view` (Phase 4a)

For Pattern-B queries (≥3 base tables flowing columns into the
pipeline output; see §3.6), `populate_q{N}_view` is implemented in
Phase 4a by driving the S3 group-walk with parameterised filters
disabled. The canonical landed reference is Q5I — see
`frontend/tpch/q5i/visitor.hpp` (`Q5IGroupWalkVisitor` with
`Q5IFilterMode::ViewLoad`) plus `q5i/load.tpp::populate_q5i_view`:

- Add a `view_load_mode` flag (or equivalent) to `Q{N}IWorkload`.
- In view-load mode the walker routes assembled records to a
  view-insert sink instead of the normal post-pipeline OutClass.
- FD-attached filters (e.g. `i_status` routing from invoice) — keep.
  These are structural properties of the view schema, not query params.
- Parameterised filters (region/date/nation/supplier) — drop. The
  view must be predicate-hoisted so one load serves all param combos.
- Wire the call into `Q{N}Workload::load()` in the S2 branch,
  replacing the Phase 1 TODO comment.
- Update the test harness: flip `pipeline_view rows == 0 [OK] deferred`
  to `pipeline_view rows == |lineitem| [OK]`.

The correctness contract for the view loader is the same as S3's
walker: sentinel ordering, invoice buffer completeness (CONVENTIONS.md
Rule 10), no parameterised filter baked in. The shared code means a
bug in one path shows up in both paths simultaneously, which is the
main advantage over a hand-rolled two-pointer merge.

**Then land Phase 4b** with this entry criterion: an SK wildcard
smoke test — for each `(SK, left_record_type, right_record_type)`
triple feeding a join, build matching synthetic rows from each side
and assert
`SKBuilder<SK>::create(left).match(SKBuilder<SK>::create(right)) == 0`,
plus that walking
`SKBuilder<SK>::create(probe_side).matching_keys()` and looking up
each anchor via `equal_range` finds the build-side row.  If either
fails, fix the SK's `matching_keys()` / `match()` /
`SKBuilder::project<R>` wiring (§4 "Sort-key wildcard semantics")
**before writing the join body**.  Then implement S4 and S1, and flip
the harness to strict 4-way (or 5-way) parity.

### Preamble

```cpp
#pragma once
#include <ostream>
#include <string_view>
#include <unordered_map>
#include "../operators.hpp"
#include "../../shared/merge-join/binary_merge_join.hpp"
#include "../../shared/merge-join/hash_join.hpp"
#include "../../shared/scanner_helpers.hpp"

namespace tpch::q{{N}} {

inline Params Params::defaults() {
   return { /* validation values from SQL spec */ };
}

inline void q{{N}}_agg_row_t::print(std::ostream& os) const {
   os << {{field1}} << "\t" << {{field2}} << "\n";
}
```

### Predicate bodies

```cpp
inline bool q{{N}}_predicate_orders(const orders_t& o, const Params& p) {
   return o.o_orderdate < p.orderdate;
}
inline bool q{{N}}_predicate_lineitem(const lineitem_t& l, const Params& p) {
   return l.l_shipdate > p.shipdate;
}
```

### Namespace-scope accumulators

Factor filter-fused arithmetic into standalone structs. **Both `_t`
(base-table) and `_coli_t` (tagged) overloads are required** so S1
(reads `_coli_t` from split adapters) and S4 (reads `_t` from base
adapters) share the same per-record logic with S3. This is what makes
the cross-structure comparison apples-to-apples (OPERATORS.md §7).

```cpp
struct CustomerOpenDueAccumulator {  // Track 2 only
   Numeric value = 0;
   void consume_invoice(const invoice_t& i) {
      if (i.i_status != 'O') return;  // filter fused
      value += i.i_totaldue;
   }
   void consume_invoice(const invoice_coli_t& i) {
      // identical logic, _coli_t variant
   }
   void reset() { value = 0; }
};

struct LineitemRevenueAccumulator {
   Numeric revenue = 0;
   bool consume(const lineitem_t& l, const Params& p) {
      if (l.l_shipdate <= p.shipdate) return false;  // filter fused
      revenue += l.l_extendedprice * (Numeric(1) - l.l_discount);
      return true;
   }
   bool consume(const lineitem_coli_t& l, const Params& p) {
      // identical logic, _coli_t variant
   }
   void reset() { revenue = 0; }
};
```

---

### §7.1 — S3: Merged Index (Group Walk)

The merged-index path is the **oracle** — implement and debug it first.
S1/S2/S4 are validated against S3.

Use `coli_group_walk<Backend>(coli.merged_adapter(), visitor)` (or
`col_group_walk` for Track 1). The Visitor struct handles all query
logic.

#### Walker variants (`--coli_walker_variant`)

Two walkers expose the same Visitor interface and produce
**byte-identical XOR digests**:

- `coli_group_walk` (baseline) — drives `MergedScanner::next()` which
  returns `std::variant<customer_coli_t, orders_coli_t,
  lineitem_coli_t, invoice_coli_t>` and dispatches via `std::visit`.
- `coli_group_walk_fused_emit` (A2c, commit `6402ba97`) — drives
  `MergedScanner::next_raw()` which returns
  `(tag_byte, key_slice, value_slice)` with no variant construction;
  dispatches via tag-byte switch + `memcpy` into the typed buffer the
  visitor needs. Closes per-record dispatch overhead at SF=15
  cache-resident (~3% of S1's per-call cost; +18-50% TX/s on
  LeanStore SF=15, +28% on RocksDB SF=15).

Production binaries dispatch via `--coli_walker_variant={baseline,
fused_emit}` (default `fused_emit` post-A2c, set in Makefile
`coli_walker_variant ?= fused_emit`). Plumbed through `tpch_flags.hpp`
and `generate_targets.py`. **Do not write a new walker that uses
`next()` instead of `next_raw()`** — see Anti-Pattern #21.

#### Multi-level active markers (target design)

The walker supports a hierarchical `xxx_active` flag per non-leaf schema
level. Leaves (invoice, lineitem) have no active marker of their own —
they are dispatched only when all ancestor active markers are true.

| Level | Active marker | Set by | Resets to `true` when |
|-------|--------------|--------|-----------------------|
| custkey group | `customer_active` | `on_customer` return value | next custkey boundary |
| order | `order_active` | `on_order` return value | next order record within the group |

**Dispatch rules:**

- `on_customer(ck, c) → bool`: fires for every custkey group. Return value
  sets `customer_active`. If false, `on_invoice` / `on_order` / `on_lineitem`
  are suppressed for the rest of this group.
- `on_invoice(k, v) → void`: dispatched when `customer_active`. Leaf — no
  active marker.
- `on_order(k, v) → bool`: dispatched when `customer_active`. Return value
  sets `order_active`. Use case: return false when `o_orderdate` fails or
  when the custkey threshold fails (at the first order boundary, once all
  invoices have been accumulated).
- `on_lineitem(k, v) → void`: dispatched when `customer_active && order_active`.
  Leaf — no active marker.

**What this replaces:**

| Old mechanism | Replaced by |
|--------------|-------------|
| `group_active` (walker state) | `customer_active` |
| `wants_skip_group()` + `skip_group_pending` | `on_order → false` (threshold fail at first order) |
| `wants_skip_order()` + `skip_order_pending` | `on_order → false` (date filter fail) |

**Why `wants_skip_group()` is redundant:** once `on_order` supports bool
returns, `on_order → false` immediately sets `customer_active = false` at
the dispatch site. The post-dispatch `wants_skip_group()` SFINAE check
then can't fire (`if (customer_active && ...)` is already false). Both
paths produce identical walker state; `wants_skip_group()` is dead code
once the bool-returning `on_order` design lands.

**Customer-level Seek-skip is Backend-gated** (A3, commits `83870b48`
+ `9da4f295`): when `customer_active` becomes false (mktsegment
fails, or threshold fails on first `on_order`), the walker can either
forward-iterate past every record in the rejected custkey group OR
physically Seek to the next custkey boundary. Each backend declares
its preferred default via a static constexpr trait in
`frontend/tpch/backend.hpp`:

```cpp
struct RocksDBBackend   { static constexpr bool USE_PHYSICAL_SEEK_SKIP = true; };
struct LeanStoreBackend { static constexpr bool USE_PHYSICAL_SEEK_SKIP = true; };
```

Both default `true` after the Linux re-A/B refuted the original macOS
regression (which turned out to be a macOS page-cache artefact, not
RocksDB SST prefetch invalidation as originally hypothesised). Linux
RocksDB iso S3 fused_emit dram=0.1: SF=15 1.81 → 14.46 TX/s (+700%),
SF=40 0.90 → 1.48 (+64%). LeanStore lift remains +356% / +116×.

**Runtime override**: the walker actually reads `--use_seek_skip`
(declared in `tpch_flags.hpp`) which takes one of:

- `-1` (default): defer to `Backend::USE_PHYSICAL_SEEK_SKIP`.
- `0`: force forward-iter (regression A/B).
- `1`: force Seek-skip (regression A/B).

So the trait is the production default and the flag is the override
for losing-branch regression runs.

**Customer-level only** — order-level skip remains forward iteration
on both backends because order groups (~4 lineitems) are too small
to amortise a tree descent.

**Order-level rejection** still suppresses `on_lineitem` dispatch
within that order via `order_active=false`; the walker may also
forward-iterate past those lineitems without `std::visit`/dispatch
overhead. Walker implementation detail; not part of the visitor
interface.

```cpp
struct Q{{N}}GroupWalkVisitor {
   const Params& params;
   std::vector<q{{N}}_agg_row_t>& out;

   // Accumulators (same structs as S1/S4)
   CustomerOpenDueAccumulator open_due;  // Track 2 only
   LineitemRevenueAccumulator rev;

   // Per-custkey gate (customer_active in the multi-level design).
   bool mktsegment_ok = false;
   // Track 2: set on first on_order once invoices are accumulated.
   bool threshold_ok = false;

   // Per-order register
   bool have_open_order = false;
   Integer cur_orderkey;
   Timestamp cur_orderdate;
   Integer cur_shippriority;

   void flush_order() {
      if (!have_open_order) return;
      have_open_order = false;
      if (rev.revenue <= Numeric(0)) { rev.reset(); return; }
      out.push_back({cur_orderkey, rev.revenue, ...});
      rev.reset();
   }

   bool on_customer(Integer ck, const customer_coli_t& c) {
      mktsegment_ok = /* check filter */;
      return mktsegment_ok;
   }

   void on_invoice(const invoice_coli_t::Key&, const invoice_coli_t& i) {
      // Track 2 only: accumulate sibling aggregate
      open_due.consume_invoice(i);
   }

   // bool return: false suppresses on_lineitem for this order AND — on the
   // first call per custkey — skips all further orders via customer_active.
   bool on_order(const orders_coli_t::Key& k, const orders_coli_t& o) {
      flush_order();
      if (!threshold_ok) {
         // First order: invoices fully accumulated; evaluate threshold now.
         threshold_ok = (open_due.value > params.threshold);
         if (!threshold_ok) return false;  // kills customer_active → skip rest of group
      }
      if (o.o_orderdate >= params.orderdate) return false;  // skips this order's lineitems
      have_open_order  = true;
      cur_orderkey     = k.orderkey;
      cur_orderdate    = o.o_orderdate;
      cur_shippriority = o.o_shippriority;
      return true;
   }

   void on_lineitem(const lineitem_coli_t::Key&, const lineitem_coli_t& l) {
      rev.consume(l, params);
   }

   void on_group_end(Integer ck) {
      if (threshold_ok) flush_order();
      mktsegment_ok = false;
      threshold_ok  = false;  // Track 2
      open_due.reset();       // Track 2
   }
};
```

Then call:

```cpp
out.clear();
Q{{N}}GroupWalkVisitor v{params, out, ...};
coli_group_walk<Backend>(coli.merged_adapter(), v);
apply_topN(out, {{K}}, {{comparator}});
return static_cast<long>(out.size());
```

#### Stage attribution and timing

`StageTimer` RAII helper (defined in `q3i/query.tpp`):

```cpp
{ StageTimer t(stats ? &stats->stage_us_join : nullptr); /* work */ }
```

Wraps a `long*` accumulator, starts `high_resolution_clock` on
construction, adds elapsed µs on destruction. Pass `nullptr` for
stat-disabled paths.

**Attribution policy** (important for cross-structure comparison):

- S1/S3/S4: scanner-wrapper aggregators and join drivers fuse
  scan + filter + aggregate into the join chain; all attributed to
  `stage_us_join`. Zeros in `stage_us_scan_filter` /
  `stage_us_aggregator` are honest — those stages don't exist as
  separate operators in these paths.
- S2: `stage_us_scan_filter` covers the view scan + per-row filters.
  `stage_us_aggregator` and `stage_us_join` are zero (pre-materialised).
- **Compare per-query averages** (`stage_us / tx_count`), NOT raw
  totals — raw totals accumulate proportionally to TX/s across the 15s
  window, which differs across structures.

`TpchExecutableHelper` exposes `tx_count()` after `run()`. Print
per-query averages alongside totals in the cardinality/timing block.

> **PITFALL — Zero-revenue orders emitted** (commit `4dc93ec6`):
> `flush_order` must suppress orders where `revenue <= 0`. This happens
> when all of an order's lineitems fail the `l_shipdate` filter. SQL
> requires a matching lineitem for the order to appear in the result.
> **Symptom**: S3 produces more rows than S4/S1; parity fails.
> **Fix**: add `if (rev.revenue <= Numeric(0)) { reset; return; }` at the
> top of `flush_order`.

> **PITFALL — Threshold evaluated too late** (Q3I Phase 1 learning):
> For Track 2, the threshold filter on `cust_open_due` must be evaluated
> as soon as invoices are done (i.e. at the first `on_order` call), NOT
> at `on_group_end`. The COLI byte-lex order guarantees all invoices
> arrive before any order within a custkey group. Evaluating at
> `on_group_end` would waste time processing O×L records for customers
> that will be dropped.

---

### §7.2 — S1: Merge Join (Split Indexes)

S1 uses `BinaryMergeJoin` chains over custkey-sorted split adapters.
The key building blocks:

**Scanner-wrapper aggregators** (see Q3I `query.tpp` lines 267–442):
Each wraps a split adapter's scanner, drives it internally, and emits
one aggregate row per group boundary.

```cpp
template <typename Backend>
class CustomerOpenDueAggregator {  // Track 2 only
   // Wraps coli.split_invoice().getScanner()
   // Accumulates via CustomerOpenDueAccumulator
   // Emits one cust_open_due_t per custkey transition
   // Threshold filter at emit: only emit if value > threshold
public:
   std::optional<std::pair<cust_open_due_t::Key, cust_open_due_t>> next();
};

template <typename Backend>
class LineitemRevenueAggregator {
   // Wraps coli.split_lineitem().getScanner()
   // Accumulates via LineitemRevenueAccumulator
   // Emits one lineitem_agg_t per (custkey, orderkey) transition
public:
   std::optional<std::pair<lineitem_agg_t::Key, lineitem_agg_t>> next();
};
```

**BMJ chain** (Track 2, 3 joins):
```
BMJ#1: customerh_t ⋈ cust_open_due_t on custkey       → jr1_t
BMJ#2: jr1_t       ⋈ orders_coli_t   on custkey       → jr2_t
BMJ#3: jr2_t       ⋈ lineitem_agg_t  on (custkey, ok) → jr3_t
```

**BMJ chain** (Track 1, 2 joins — no invoice):
```
BMJ#1: customerh_t ⋈ orders_col_t    on custkey       → jr1_t
BMJ#2: jr1_t       ⋈ lineitem_agg_t  on (custkey, ok) → jr2_t
```

Each BMJ feeds the next via `.as_scanner()`. The final BMJ's `.run()`
callback extracts fields and pushes to `out`. Then `apply_topN`.

Filters are pushed into fetch lambdas on the scanner-wrappers (e.g.
`make_filtered_scanner(customer.getScanner(), [&](auto& c) { ... })`).

> **PITFALL — Last group dropped by BinaryMergeJoin** (commit `9a2e393d`):
> Both `BinaryMergeJoin` and `PremergedJoin` have a flush bug: when the
> scanner exhausts, the final key group's records remain in
> `records_to_join` un-joined. The fix is to call
> `join_state.refresh(JK::max())` after the main loop. This bug has been
> fixed in the shared infrastructure, but if you create custom join
> drivers, be aware of the pattern.

> **PITFALL — `SKBuilder::create` on variant operands** (commit `241a7184`):
> If you touch `PremergedJoin`, the tentative-skip path must use
> `jk_from_variants<JK>` — NOT `SKBuilder<JK>::create` directly on
> `std::variant` operands.

---

### §7.3 — S2: Pipeline View Scan

> **S3 chain-join counters suppressed**: `join1_output_rows`,
> `join2_output_rows`, `join3_output_rows` are S1/S4 abstractions for 3
> sequential binary joins. S3 is a single fused walk — suppress these
> counters (leave at zero) and render `–` in output tables. This is not
> a cardinality bug; it means "not applicable to a fused walk".

**Post-audit shape** (Q3I commit `db60d49b`; Q3 design follows the same
pattern):

- View row granularity is **per-lineitem**, keyed by
  `(custkey, orderkey, linenumber)`. NOT per-orderkey with
  pre-aggregated revenue.
- View payload carries the unaggregated lineitem fields plus FD-attached
  order/customer columns.
- `populate_q{N}_view` does NOT apply mktsegment, orderdate, shipdate, or
  any parameterised filter. It does NOT pre-aggregate.
- `query_by_view` applies all filters live, runs a per-orderkey
  `LineitemRevenueAccumulator` over qualifying lineitems, then emits.

```cpp
template <typename Backend>
long Q{{N}}Workload<Backend>::query_by_view(std::vector<q{{N}}_agg_row_t>& out)
{
   out.clear();
   LineitemRevenueAccumulator acc;
   auto scanner = pipeline_view.getScanner();
   while (auto kv = scanner->next()) {
      const auto& row = kv->second;
      if (row.c_mktsegment != params.mktsegment) continue;   // parameterised
      if (row.o_orderdate >= params.orderdate) continue;       // parameterised
      if (!q{{N}}_predicate_lineitem(row.line(), params)) continue;
      // Track 2: if (row.cust_open_due <= params.threshold) continue;
      acc.accumulate(kv->first.orderkey, row);
   }
   acc.flush(out);
   apply_topN(out, {{K}}, {{comparator}});
   return static_cast<long>(out.size());
}
```

> **Why per-lineitem, not per-orderkey**: a per-orderkey view with
> pre-aggregated revenue must bake the shipdate filter at load time. Any
> param other than the load-time value produces wrong answers. The
> per-lineitem shape defers all parameterised filters to query time,
> matching the shape of S1/S3/S4. (Q3I S2/S5 audit, 2026-05-03 —
> anti-pattern #24 in §13.)

---

### §7.4 — S5: aCOLI MI (Pre-Aggregated Variant)

> **Status (2026-05-04): deferred from paper sweep across all queries.**
> The Q3I S5 implementation is retained as design + working code for
> future revisit, but is excluded from reported figures. The "When to
> use" guidance below still applies *as a future-work design note*; do
> not stand up an aCOLI MI for a new query as part of paper work. See
> §S5 deferral above for rationale.

**When to use**: the query has per-custkey aggregates whose filter predicates
are constants hardcoded by the TPC-H spec (e.g. `i_status='O'` for Q3I's
`cust_open_due`), AND the COLI MI scan is cache-bound at target scale factors
(i.e. S3 is paying per-record dispatch overhead for data that fits in cache).

**When NOT to use**: when the aggregate filter is parameterised (e.g.
`l_shipdate > $DATE`, `c_mktsegment = $SEGMENT`). Parameterised aggregates
must NOT be baked — store the unaggregated source rows and recompute at query
time. The `[SKIP S5 — baked-in filter mismatch]` guard is not an acceptable
workaround; it masks a soundness bug. (Q3I S2/S5 audit, 2026-05-03 —
anti-pattern #24 / #26 in §13.)

**Post-audit pattern** (Q3I commits `8ac423dd`, `b9ef4947`; reference
implementation in `coli_pipeline.tpp::populate_aggregated` and
`q3i/query.tpp::query_by_aggregated`):

- aCOLI is a **3-type** `MergedAdapter<customer_acoli_t, orders_coli_t,
  lineitem_acoli_t>`. `orders_acoli_t` (id=50) was retired Step 4b
  (2026-05-03): switching all aCOLI types to tagged-key encoding made its
  key byte-identical to `orders_coli_t`, so the type was collapsed via
  `using orders_acoli_t = orders_coli_t`. Lineitems are stored unaggregated
  so that parameterised filters (shipdate, mktsegment) are applied at query
  time.
- Pre-aggregated columns are permitted **only** when the aggregate filter is
  a TPC-H-spec hardcoded constant. Example: `customer_acoli_t.pre_open_due`
  from `i_status='O'` in Q3I. The removed `orders_acoli_t.pre_revenue`
  (dropped 2026-05-03) is the canonical counter-example.
- `populate_aggregated()` is two passes: Pass A scans invoices and builds
  a per-custkey aggregate map for spec-constant aggregates; Pass B scans
  customer/orders/lineitem base adapters and inserts all three record types
  into the aCOLI `MergedAdapter`.
- `query_by_aggregated` walks the 3-type adapter using the same
  `LineitemRevenueAccumulator` as S1/S3 — re-enforcing OPERATORS.md §6.1
  comparison-integrity. Pre-aggregated spec-constant columns (e.g.
  `pre_open_due`) are read directly; parameterised aggregates are recomputed.
- The `[SKIP S5 — baked-in filter mismatch]` parity guard is **retired**.
  S5 must produce byte-identical XOR digests to S1/S3/S4 across all param
  sets.

**Spectrum position** (cross-reference `q3i/CLAUDE.md §Phase 4`):

```
S1/S3 (raw co-location, full recompute each query)
  → S5 (aCOLI: spec-constant aggregates pre-stored; parameterised filters
         and revenue recomputed from unaggregated lineitems at query time)
    → S2 (per-lineitem view; only parameterised filters at query time)
```

S5 sits between S3 and S2 on the pre-computation spectrum: smaller scan
footprint than S3 (no invoice rows in the MI — 486 aCOLI records vs 10918
COLI records at SF=1 Q3I, a 22× reduction), fully sound across all param
sets (no baked-in date or segment filter).

**Paper angle**: S5 is the concrete "MI-as-aggregate-store" example for
reviewer R2-D1 (MULTI_TABLE_MI_ANALYSIS.md §6 / INVOICE_EXTENSION_CANDIDATES.md).
It demonstrates that merged indexes can store pre-computed spec-constant
included columns alongside raw records — a point distinct from raw
co-location.

---

### §7.5 — S4: Hash Join Baseline

Pre-build hashmaps, then probe. S4 is the no-MI baseline — hash-aggregates
and hashmaps are acceptable here (unlike the MI family).

Pattern (Track 2):

1. **Pre-aggregate INVOICE** → `unordered_map<custkey, Numeric>` with
   `i_status` filter and threshold filter at emit.
2. **Build customer map** → `unordered_map<custkey, bool>` or
   `unordered_set<custkey>` with mktsegment filter.
3. **Build orders map** → `unordered_map<orderkey, OrderSlot>` filtered
   by `o_orderdate` + custkey membership in both maps above.
4. **Probe lineitems** → for each lineitem, look up orders map; accumulate
   revenue per orderkey via `LineitemRevenueAccumulator`.
5. **Collect results** → iterate the per-orderkey map, emit rows with
   `revenue > 0`, then `apply_topN`.

See Q3I `query.tpp` lines 620–740 for the full pattern.

> **PITFALL — Hash-aggregate inside the MI family pipeline** (OPERATORS.md §7):
> S1/S2/S3 share the family logical plan and MUST NOT use hash-aggregates
> inside the pipeline. Only S4 (the no-MI baseline) is allowed hash-aggregates,
> because it doesn't claim merged-index benefits. If you need a post-join
> aggregate in S4 (e.g. grouping by orderkey after a HashJoin whose output
> is unsorted), use an `unordered_map` — that's the "hashmap tax" S4 pays.

---

### §7.6 — Post-pipeline OutClass: small-buffer sink

→ see `CONVENTIONS.md §Post-pipeline OutClass`

---

## §8 — Phase 5: Wiring + tests + docs

Sections §8–§12 are all **Phase 5**, broken out by file rather than
by phase. They are typically co-developed in one or two commits
because each piece is mechanical: per-structure aliases mirror the
template, executables mirror Q3I's, the test harness mirrors Q3's,
CMake entries mirror neighbouring queries, and the doc refresh is a
diff against the design doc. Splitting them into separate phases
inflated the bring-up commit count without adding review value.

**Phase 5 exit criterion**: production binaries build clean (`q{N}_lsm`
/ `q{N}_btree`); `test_query_q{N}_lsm` passes with strict
cross-structure XOR parity at SF=1 (and at least one off-default
param set per §10 "Off-default param verification"); `q{N}/CLAUDE.md`
reflects the implemented state; `frontend/tpch/CLAUDE.md` "Completed"
section gains an entry; `RUNS.md` is appended after the first Linux
perf sweep.

### `per_structure_workload.hpp`

Alias-only file. Copy from Q3I and change names:

```cpp
#pragma once
#include "../per_structure_workload.hpp"
#include "workload.hpp"

namespace tpch::q{{N}} {

template <typename Backend>
using BaseQ{{N}}   = ::tpch::BaseStructure  <Q{{N}}Workload<Backend>, q{{N}}_agg_row_t>;
template <typename Backend>
using ViewQ{{N}}   = ::tpch::ViewStructure  <Q{{N}}Workload<Backend>, q{{N}}_agg_row_t>;
template <typename Backend>
using MergedQ{{N}} = ::tpch::MergedStructure<Q{{N}}Workload<Backend>, q{{N}}_agg_row_t>;
template <typename Backend>
using HashQ{{N}}   = ::tpch::HashStructure  <Q{{N}}Workload<Backend>, q{{N}}_agg_row_t>;

}
```

---

## §9 — Phase 5 (cont.): Executables

### `executable_rocksdb.cpp`

Mirror Q3I `executable_rocksdb.cpp`. Key sections:

1. **Adapter declarations**: all 8+1 base adapters + pipeline view + merged
   COLI (or COL) adapter + split adapters.
2. `rocks_db.open()` — AFTER all adapter declarations.
3. Construct `TPCHWorkload` and `Q{N}Workload`.
4. `q{N}.load()` — if `!FLAGS_recover`, return after load.
5. Switch on `FLAGS_storage_structure` 1–4, each case constructing the
   corresponding wrapper (`BaseQ{N}`, `ViewQ{N}`, `MergedQ{N}`, `HashQ{N}`)
   and passing it to `TpchExecutableHelper::run()`.

### `executable_leanstore.cpp`

Same structure, `#ifndef ROCKSDB_ONLY` guarded. Uses `LeanStoreBackend`.

---

## §10 — Phase 5 (cont.): Test Harness

> **SSTWrite baseline-subtract**: `RocksDB::SST_WRITE_MICROS` histogram
> accumulates over DB lifetime, including post-load compaction. For
> read-only query experiments this inflates the reported SSTWrite(µs)/TX.
> Fix: call `RocksDBLogger::capture_baseline()` once before `helper.run()`
> starts. Each snapshot then reports `(current – baseline)`.



File: `tests/q{N}/test_query_q{N}_rocksdb.cpp`

### XOR digest function

```cpp
static uint64_t row_digest(const q{{N}}_agg_row_t& r) {
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   // ... one line per output field, same rotl64 + XOR pattern
   return d;
}

static uint64_t digest_rows(const std::vector<q{{N}}_agg_row_t>& rows) {
   uint64_t d = 0;
   for (auto& r : rows) d ^= row_digest(r);
   return d;
}
```

### Harness `main()` structure

```cpp
int main(int argc, char** argv) {
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // 1. Defensive wipe
   if (fs::exists(ssd) && !fs::is_empty(ssd)) {
      std::cout << "=== Wiping prior --ssd_path ===\n";
      fs::remove_all(ssd);
   }
   fs::create_directories(ssd);

   // 2. Open DB, declare ALL adapters
   // ...
   rocks_db.open();

   // 3. Load ONCE
   tpch.load();

   // 4. Populate ALL secondaries (NOT through q{N}.load() dispatch)
   populate_q{{N}}_view<B>(/* ... */);   // for S2
   q{{N}}.coli_pipeline().populate_split();    // for S1
   q{{N}}.coli_pipeline().populate_merged();   // for S3
   // S4 needs no secondary

   // 5. Run all four paths
   std::vector<q{{N}}_agg_row_t> r_base, r_view, r_merged, r_hash;
   q{{N}}.query_by_base(r_base);
   q{{N}}.query_by_view(r_view);
   q{{N}}.query_by_merged(r_merged);
   q{{N}}.query_by_hash(r_hash);

   // 6. Sort deterministically for digest stability
   auto by_key = [](const auto& a, const auto& b) {
      return a.o_orderkey < b.o_orderkey;  // or appropriate unique key
   };
   for (auto* v : {&r_base, &r_view, &r_merged, &r_hash})
      std::sort(v->begin(), v->end(), by_key);

   // 7. Compute digests, compare, print [OK]/[FAIL]
   uint64_t d_base = digest_rows(r_base);
   // ... same for view, merged, hash
   // S3 (merged) is the oracle
   bool all_ok = (d_base == d_merged && d_view == d_merged && d_hash == d_merged);

   // Row-count check: cross-structure agreement + range
   size_t n = r_merged.size();
   bool counts_ok = (r_base.size() == n && r_view.size() == n && r_hash.size() == n);
   bool range_ok  = (n > 0 && n <= {{K}});  // at least 1 match, at most LIMIT
   all_ok = all_ok && counts_ok && range_ok;

   return all_ok ? 0 : 1;
}
```

> **PITFALL — Per-structure loading in test harness** (commit `721771bc`):
> Do NOT load data separately for each storage structure. TPC-H data
> generation is not deterministic across reloads in one process — the
> global RNG state advances with each `tpch.load()` call, producing
> different lineitem counts (e.g. 6025 vs 5942 in the same harness run).
> The four query paths then see four different datasets, making parity
> fail by construction.
> **Symptom**: four distinct digests despite correct query logic; lineitem
> counts differ across structures in diagnostic output.

### Off-default param verification

The harness's parity check (XOR digest agreement across S1–S5) must run
at **at least one param set other than `Params::defaults()`** before
declaring success. The simplest implementation: after the default-param
parity round, call `q{{N}}.set_params_for_iter(1)` (one tick into the
rotation table), re-run all `query_by_*` paths, and re-check digest
agreement. Without this, a baked-in-param secondary will pass the harness
silently. (Q3I S5 was hidden this way until the 2026-05-03 audit —
anti-pattern #25 / #26 in §13.)
> **Fix**: load ONCE, populate ALL secondaries up front, run all four
> paths against the same in-memory state.

> **PITFALL — Hardcoded row-count expectation** (commit `c077236f`):
> Do NOT assert `row_count == K` (e.g. `== 10`). The actual count is
> data-dependent — at SF=1 the dataset can yield fewer than K qualifying
> orders. The correct invariant is: (a) all four paths agree on count,
> (b) count is in `(0, K]`. Cross-structure disagreement is a hard fail;
> an exact-count assertion just creates false negatives.

> **PITFALL — Empty secondaries in production** (commit `739ebf63`):
> Add secondary cardinality + size checks to the test harness. After
> populating all secondaries, verify each has a nonzero row count
> (pipeline view, each split adapter, merged MI). An empty secondary
> means a load bug — the query will "succeed" with 0 rows and fast
> throughput, which is misleading. See Q3I test harness for the pattern.

> **PITFALL — Reusing `--ssd_path` without wiping** (commit `835b4f0a`):
> RocksDB does not cleanly overwrite an existing DB. Calling `tpch.load()`
> writes new records on top of prior state, growing the lineitem count
> across consecutive runs (e.g. 6051 → 7765 → 10017 at SF=1). The four
> query paths then read subtly inconsistent loaded states.
> **Symptom**: four distinct digests that change each time you re-run the
> harness; lineitem count grows monotonically.
> **Fix**: `remove_all(FLAGS_ssd_path)` before `rocks_db.open()`.

---

## §10.5 — Performance instrumentation (standard machinery)

→ see `CONVENTIONS.md §Performance instrumentation`

---

## §10.6 — Size diagnostics: content-walk pattern

→ see `CONVENTIONS.md §Size diagnostics`

---

## §11 — Phase 5 (cont.): CMake + Makefile Targets

### `frontend/CMakeLists.txt`

Add following the Q3I pattern:

```cmake
# Test target (macOS + Linux)
add_executable(test_query_q{{N}}_lsm
    tpch/tests/q{{N}}/test_query_q{{N}}_rocksdb.cpp)
target_link_libraries(test_query_q{{N}}_lsm PRIVATE ...)
target_compile_definitions(test_query_q{{N}}_lsm PRIVATE ROCKSDB_ONLY)

# Production RocksDB target (macOS + Linux)
add_executable(q{{N}}_lsm tpch/q{{N}}/executable_rocksdb.cpp)
target_link_libraries(q{{N}}_lsm PRIVATE ...)
target_compile_definitions(q{{N}}_lsm PRIVATE ROCKSDB_ONLY)

# Production LeanStore target (Linux only)
if(NOT APPLE)
   add_executable(q{{N}}_btree tpch/q{{N}}/executable_leanstore.cpp)
   target_link_libraries(q{{N}}_btree PRIVATE leanstore ...)
endif()
```

### `generate_targets.py`

Add `q{N}_lsm` and `q{N}_btree` to `exec_names`, `DIFF_DIRS`, and
`STRUCTURE_OPTIONS`. Then regenerate:

```bash
python3 generate_targets.py > targets.mk
```

This also refreshes `.vscode/launch.json`.

---

## §12 — Phase 5 (cont.): Documentation

### `q{N}/CLAUDE.md`

Must contain: SQL (original + extended if Track 2), substitution params,
storage structure table, plan descriptions, implementation status.

### `frontend/tpch/CLAUDE.md`

Update:
- §Layout: add `q{N}/` entry.
- §Tests: add `test_query_q{N}_lsm` row.
- §Completed: add completion entry with date and summary.

### `OPERATORS.md`

Update only if the query introduces a new operator pattern not already
documented.

---

## §12.5 — Operator Framing Rules

→ see `CONVENTIONS.md §Operator framing rules`

---

## §13 — Anti-Pattern Reference

→ see `CONVENTIONS.md §Anti-Pattern Reference`

---

## §"Contract violations & fail-fast"

→ see `CONVENTIONS.md §Contract violations & fail-fast`

---

## §14 — Verification Checklist

Run after completing all phases:

- [ ] `grep -rn 'static constexpr int id' frontend/tpch/ --include='*.hpp' | sort -t= -k2 -n` — no duplicate IDs
- [ ] `make -C build/frontend test_query_q{N}_lsm -j$(nproc)` — builds clean
- [ ] Run with fresh `--ssd_path`:
  ```bash
  # Scratch under build/scratch/<tag>/ (gitignored). Never test_data*/ in root.
  mkdir -p build/scratch/q{N}/{data,csv}
  ./build/frontend/test_query_q{N}_lsm \
      --ssd_path=./build/scratch/q{N}/data \
      --csv_path=./build/scratch/q{N}/csv \
      --tpch_scale_factor=1
  ```
  All `[OK]`, zero `[FAIL]`, exit 0.
- [ ] 4 identical digests printed (values are seed-dependent; agreement is the invariant)
- [ ] Row count matches expected (10 for LIMIT 10, 20 for LIMIT 20, ~5 for per-nation, etc.)
- [ ] Re-run in place (without manual wipe) — still `[OK]` (harness wipes its own `ssd_path`)
- [ ] `make -C build/frontend q{N}_lsm -j$(nproc)` — production executable builds
- [ ] `make q{N}_lsm scale=1` — runs all four structures, emits CSV metrics
- [ ] `test_query_q{N}_lsm` re-runs all `query_by_*` paths after
  `set_params_for_iter(1)`; XOR digests still agree across all structures
  (off-default param verification — anti-pattern #25 / #26 guard).

---

## §15 — Performance investigation cycle (when bring-up wraps)

Once Phases 1–9 land and parity is green, the next surface — and the
playbook's remit ends here — is the **per-query performance
investigation**. Q3I is the canonical example. The convention:

### Worklist + archive

- `q{N}/PERFORMANCE.md` is the **active worklist**: hypothesis ledger
  (§2) + open A-tests (§3) + reviewer-relevance summary. Forward-
  looking only; ~150 lines max.
- `q{N}/archive/PERFORMANCE-YYYY-MM-DD[-letter].md` snapshots the
  active doc when it grows past ~150 lines or when a major round of
  evidence lands. The `-b`, `-c` letter suffix disambiguates multiple
  archives on the same day. Archived docs gain a one-line header
  `> Archived YYYY-MM-DD — superseded by ../PERFORMANCE.md`.

### Hypothesis ledger format

§2 of `PERFORMANCE.md` is a single table with one row per hypothesis
(`H1`, `H2`, …):

| ID | Hypothesis | Status | One-line takeaway |

Status values: `OPEN`, `CONFIRMED`, `REFUTED`,
`CONFIRMED + REMEDIATED at <regime> (<commit>)`. Full evidence
trails live in the archive; the ledger only carries the verdict and
the takeaway sentence.

### A-test format

§3 entries name a **mechanism**, a **prediction** the mechanism
makes, and the **remediation** if confirmed:

```text
### A{n} — short title

Mechanism. Why this could be the bottleneck (≤2 sentences).

- WHAT: the variant being measured / changed.
- WHERE: file paths + flag names.
- MEASURE: which Q{N}Stats fields / PerfContext counters.
- WIN: the specific signal that confirms; the fallback if refuted.
```

Keep each entry ≤30 lines. If an A-test grows beyond that, it gets
its own follow-up plan file in `.claude/plans/`.

### Discipline

- Active investigations only. Sweeps (e.g. dram ∈ {0.05, 0.1, 0.5,
  1.0, full}) are **confirmation runs after a hypothesis-driven
  test**, not the test itself. A worklist that reads "sweep until
  something works" is a doc-keeping pattern, not an investigation.
- Each completed A-test promotes the matching H-row in §2 with a
  commit SHA pointer — so the ledger stays the source of truth.
- Refuted candidates (e.g. Q3I H9 per-record-width tax, H10
  compression-masking-locality) get recorded in §3 so they aren't
  re-investigated.
