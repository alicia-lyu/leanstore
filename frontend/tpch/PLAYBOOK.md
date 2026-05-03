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

### Phase dependency graph

```
Phase 1 (views.hpp)
  → Phase 2 (workload.hpp)
    → Phase 3 (load.tpp)
      → Phase 4 (query.tpp)   ← the bulk of the work
        → Phase 5 (per_structure_workload.hpp)
          → Phase 6 (executables)
            ├→ Phase 7 (test harness)    ← can start once Phase 5 is done
            ├→ Phase 8 (CMake + targets) ← can start once Phase 6 files exist
            └→ Phase 9 (documentation)   ← can start once Phase 7 passes
```

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

## §4 — Phase 1: `views.hpp` (Record Types)

### `q{N}_pipeline_view_t`

A real struct (not an alias) keyed by `(custkey, orderkey)`. One row per
order — cardinality ≈ |orders|. Carries the inside-pipeline aggregate
output:

```cpp
struct q{{N}}_pipeline_view_t {
   static constexpr int id = {{ID}};
   struct Key {
      Integer custkey;
      Integer orderkey;
      ADD_KEY_TRAITS(&Key::custkey, &Key::orderkey)
   };
   Numeric     revenue;        // SUM(l_extendedprice * (1-l_discount))
   {{Numeric  cust_open_due;}} // Track 2 only
   Varchar<10> c_mktsegment;   // or whatever outside-pipeline filter needs
   Timestamp   o_orderdate;
   Integer     o_shippriority;
   // ...other FD-attached columns the outside-pipeline filter/output needs
   ADD_RECORD_TRAITS(q{{N}}_pipeline_view_t)
   void print(std::ostream& os) const;
};
```

### `q{N}_agg_row_t`

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

### Intermediate types for S1 BMJ chain

Only needed if S1 uses `BinaryMergeJoin`. See `q3i/views.hpp` lines 65–241.

**Per-custkey invoice aggregate** (Track 2 only):
```cpp
struct cust_open_due_t {
   static constexpr int id = {{ID}};
   struct Key { Integer custkey; /* fold/unfold + match + matching_keys */ };
   Numeric cust_open_due;
   ADD_RECORD_TRAITS(cust_open_due_t)
};
```

**Per-(custkey, orderkey) lineitem aggregate**:
```cpp
struct lineitem_agg_t {
   static constexpr int id = {{ID}};
   struct Key { Integer custkey; Integer orderkey; /* fold/unfold + match + matching_keys */ };
   Numeric revenue;
   ADD_RECORD_TRAITS(lineitem_agg_t)
};
```

**Join result types** (`q{N}_jr1_t`, `q{N}_jr2_t`, `q{N}_jr3_t`):
Each wraps `joined_t<id, JK, fold, Left, Right>` with:
- Two Key constructors: one from constituent keys (for `join_current`),
  one from JK only (for the unfold path with `fold_pks=false`).
- Accessor methods to extract nested records.

See Q3I `views.hpp` lines 154–241 for the exact pattern.

**`std::hash` specializations**: one per intermediate Key type (needed by
`HashJoin`). See Q3I `views.hpp` lines 276–292.

**`SKBuilder` specializations**: one per join key type. Each needs
`create(Key, Record)` for every record type that participates in
merge-joins on that key, plus `project<R>` and `to_key<R>`. See Q3I
`views.hpp` lines 309–375.

> **PITFALL — `joined_ol_t::unfoldKey`** (commit `be8b9bba`): The generic
> `joined_t::unfoldKey(fold_pks=false)` path assumes each constituent
> type has a `Key(JK)` constructor. If yours doesn't, you must add an
> explicit `unfoldKey` override on your join result type. The symptom is
> a compile error in `RocksDBAdapter<your_view_t>` instantiation.

---

## §5 — Phase 2: `workload.hpp` (Class + Params + Predicates)

### Params

One field per SQL substitution parameter, plus `static Params defaults()`.

```cpp
struct Params {
   {{Varchar<10> mktsegment;}}
   {{Timestamp orderdate;}}
   {{Timestamp shipdate;}}
   {{Numeric threshold;}}
   static Params defaults();
};
```

### Predicates

Declare one per single-table filter + one for the joined row:

```cpp
inline bool q{{N}}_predicate_orders(const orders_t& o, const Params& p);
inline bool q{{N}}_predicate_lineitem(const lineitem_t& l, const Params& p);
inline bool q{{N}}_predicate_invoice(const invoice_t& i);  // Track 2 only
```

Bodies go in `query.tpp`.

### Stats (optional)

```cpp
struct Q{{N}}Stats {
   long customers_scanned = 0;
   long orders_scanned = 0;
   long lineitems_scanned = 0;
   // ...per-path counters
};
```

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

---

## §6 — Phase 3: `load.tpp` (Constructor + Load Dispatch)

### Constructor

Wire adapter refs into members, construct the pipeline, init params:

```cpp
template <typename Backend>
Q{{N}}Workload<Backend>::Q{{N}}Workload(
    TPCHWorkload<typename Backend::template Adapter>& tpch,
    /* adapter refs... */)
    : tpch(tpch), customer(customer), orders(orders), lineitem(lineitem),
      {{invoice(invoice),}}
      coli(customer, orders, lineitem, {{invoice,}} merged_coli,
           split_orders, split_lineitem {{, split_invoice}}),
      pipeline_view(pipeline_view),
      params(Params::defaults())
{}
```

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

### `populate_q{N}_view` free function

Three-step pattern (see Q3I `load.tpp` lines 44–112):

1. **Invoice scan** (Track 2 only): build `unordered_map<Integer, Numeric>`
   per-custkey aggregate with `i_status` filter fused.
2. **Customer scan** (if needed): build per-custkey lookup map for
   functionally-dependent columns (e.g. `c_mktsegment`).
3. **Two-pointer merge** over orders + lineitem (both sorted by orderkey):
   - For each order, advance lineitem pointer while `l_orderkey == orderkey`.
   - Accumulate revenue per orderkey (fuse `l_shipdate` filter here).
   - Emit one `q{N}_pipeline_view_t` row per `(custkey, orderkey)`.

**Predicate hoisting**: the view is loaded WITHOUT parameterised filters
(mktsegment, threshold, orderdate) so it's reusable across param sets.
Bake in only constant or single-table filters that are invariant across
runs (e.g. `l_shipdate > DATE_1995_03_15` with the default date constant,
`i_status = 'O'`).

> **PITFALL — BinaryMergeJoin for view population** (commit `f74b67da`):
> Do NOT use `BinaryMergeJoin` for view loading. Its hierarchical-key
> wildcard semantics (`linenumber==0` matches any linenumber) cause the
> join-state machine to emit ~1 row per order group instead of N rows per
> (order, lineitem) pair. Use a manual two-pointer merge instead.
> **Symptom**: view has ~150K rows (one per order) instead of ~600K
> (one per lineitem), and the parity test fails with S2 producing
> different results from S1/S3/S4.

> **PITFALL — View missing baked-in filter** (commit `4dc93ec6`):
> If `query_by_view` assumes a filter was baked into the view at load time
> (e.g. `l_shipdate > DATE`), but `populate_q{N}_view` forgot to apply it,
> the view will contain unfiltered rows and S2 will produce different
> results from the other three paths.
> **Symptom**: S2 returns more rows than S1/S3/S4, or includes rows with
> revenue from lineitems that should have been filtered out.

---

## §7 — Phase 4: `query.tpp` (The Core)

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

**Walker-internal forward-iterate optimization:** when `order_active`
becomes false (date filter rejects an order), the walker MAY
forward-iterate past that order's lineitems without calling `std::visit`
on each one — skipping the variant-dispatch overhead for the
rejected-order lineitem tail. This is a walker implementation detail, not
part of the visitor interface. Controlled by `USE_PHYSICAL_SEEK_SKIP =
false` (physical Seek was A/B tested at SF=40: physical Seek invalidates
RocksDB's prefetch buffer, raising SSTRead/TX ~5×; forward iteration
without physical Seek is competitive and the default).

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

The simplest body. Scan `pipeline_view`, apply parameterised filters
per-row, emit, `apply_topN`:

```cpp
template <typename Backend>
long Q{{N}}Workload<Backend>::query_by_view(std::vector<q{{N}}_agg_row_t>& out)
{
   out.clear();
   auto scanner = pipeline_view.getScanner();
   while (auto kv = scanner->next()) {
      const auto& row = kv->second;
      if (row.c_mktsegment != params.mktsegment) continue;   // parameterised
      if (row.o_orderdate >= params.orderdate) continue;       // parameterised
      // Track 2: if (row.cust_open_due <= params.threshold) continue;
      if (row.revenue <= Numeric(0)) continue;                 // zero-revenue guard
      out.push_back({kv->first.orderkey, row.revenue, ...});
   }
   apply_topN(out, {{K}}, {{comparator}});
   return static_cast<long>(out.size());
}
```

> **PITFALL — S2 missing zero-revenue guard** (commit `8fdcdda1`):
> `query_by_view` must suppress rows where `revenue <= 0`, just like
> `flush_order` in S3. Orders whose lineitems all failed the baked-in
> `l_shipdate` filter at view-load time have `revenue == 0` in the view.
> S1/S3/S4 naturally exclude these (the accumulator produces no output),
> but S2 sees them as pre-existing view rows.
> **Symptom**: S2 returns more rows than the other three paths; digests diverge.

---

### §7.4 — S5: aCOLI MI (Pre-Aggregated Variant)

**When to use**: the query has simple per-custkey and per-order aggregates
(e.g. `SUM(i_totaldue)`, `SUM(l_extendedprice*(1-l_discount))`) whose filter
predicates are constant or invariant across the param sets you care about, AND
the COLI MI scan is cache-bound at target scale factors (i.e. S3 is paying
per-record dispatch overhead for data that fits in cache).

**When NOT to use**: when the aggregate filter parameter changes between
production runs (e.g. a different `shipdate` cutoff per experiment). Baking
in a date constant defeats reuse — S5 results diverge from S1–S4 for any
non-default params, and the test harness emits `[SKIP S5 — baked-in filter
mismatch]` rather than failing.

**Pattern** (Q3I S5, reference implementation):

- Define `customer_acoli_t` (id=N) and `orders_acoli_t` (id=N+1) in
  `views_coli.hpp`. Each carries the base record payload plus one or more
  pre-aggregated `Numeric` included columns.
- `populate_aggregated()` in `coli_pipeline.tpp`: multi-pass algorithm —
  Pass A builds per-custkey aggregate map (invoice scan, constant filter
  fused), Pass B builds per-(custkey,orderkey) aggregate map (lineitem scan,
  constant filter fused), Pass C inserts `customer_acoli_t` and
  `orders_acoli_t` records into a `MergedAdapter<customer_acoli_t,
  orders_acoli_t>`.
- `query_by_aggregated` in `q{N}/query.tpp`: scan the 2-type aCOLI MI,
  apply parameterised filters (mktsegment, threshold, orderdate) per-row
  as direct field comparisons — no accumulators needed.

**Spectrum position** (cross-reference `q3i/CLAUDE.md §Phase 4`):

```
S1/S3 (raw co-location, full recompute each query)
  → S5 (aCOLI: pre-aggregated, no per-query accumulation, reusable across
         mktsegment/threshold/orderdate param sets)
    → S2 (fully pre-computed view, only parameterised filters at query time)
```

S5 sits between S3 and S2 on the pre-computation spectrum: smaller scan
footprint than S3 (no invoice/lineitem rows in the MI), more param-reuse
than S2 (parameterised filters not baked in). At SF=1 Q3I: 486 aCOLI
records scanned vs 10918 COLI records (22× reduction) and ~1.5M view rows.

**Paper angle**: S5 is the concrete "MI-as-aggregate-store" example for
reviewer R2-D1 (MULTI_TABLE_MI_ANALYSIS.md §6 / INVOICE_EXTENSION_CANDIDATES.md).
It demonstrates that merged indexes can store not just raw records but
pre-computed included columns — a point distinct from raw co-location.

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

### §7.6 — Common Epilogue: `apply_topN`

Every `query_by_*` body ends with:

```cpp
apply_topN(out, {{K}}, [](const q{{N}}_agg_row_t& a, const q{{N}}_agg_row_t& b) {
   if (a.revenue != b.revenue) return a.revenue > b.revenue;  // DESC
   if (a.o_orderdate != b.o_orderdate) return a.o_orderdate < b.o_orderdate;  // ASC
   return a.o_orderkey < b.o_orderkey;  // ASC — unique tiebreaker
});
return static_cast<long>(out.size());
```

The comparator MUST include a unique tiebreaker field as the final key.

> **PITFALL — Single-key comparator in top-K** (commit `a4f5a4bc`):
> Using only `revenue DESC` as the sort key leaves `std::partial_sort`
> free to resolve ties by input order, which differs across paths
> (S2 scans by `(custkey, orderkey)`, S4 iterates an `unordered_map`).
> On data states with revenue ties at the boundary, this produces
> different top-10 sets and thus different XOR digests.
> **Symptom**: 3 or 4 distinct digests despite correct query logic.
> **Fix**: always include enough tiebreaker fields to make the comparator
> a strict weak ordering with no residual ambiguity. A unique key
> (e.g. `o_orderkey`) as the last field guarantees this.

---

## §8 — Phase 5: `per_structure_workload.hpp`

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

## §9 — Phase 6: Executables

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

## §10 — Phase 7: Test Harness

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

## §11 — Phase 8: CMake + Makefile Targets

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

## §12 — Phase 9: Documentation

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

## §13 — Anti-Pattern Reference

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
| 10 | View missing baked-in filter | `4dc93ec6` | S2 includes unfiltered rows; digest diverges from S1/S3/S4 | Apply constant filters in `populate_q{N}_view` |
| 11 | S2 missing zero-revenue guard | `8fdcdda1` | S2 emits zero-revenue view rows other paths suppress | Add `revenue <= 0` check in `query_by_view` |
| 12 | `reinterpret_cast` on RocksDB values | `d8980426` | Alignment UB on platforms with unaligned value buffers | Use `memcpy` into a stack local instead |
| 13 | Load order: lineitems before orders | `6edcf2ca` | `order_dates` map empty during lineitem generation; dates default to 0 | Load order: customer → orders → lineitem |
| 14 | Raw indices as orderkeys | `f74b67da` | ~75% of lineitems are orphans with no matching order | Use `orderkey_from_index()` for sparse key generation |
| 15 | `load()` populating only one secondary | `47405bec` | 3 of 4 structures read empty adapters; fantasy throughput | Populate ALL secondaries unconditionally in `load()` |
| 16 | Hardcoded row-count assertion | `c077236f` | False test failures when data yields fewer than LIMIT rows | Assert cross-structure agreement + range `(0, K]` instead |
| 17 | No secondary cardinality check in test | `739ebf63` | Empty secondaries produce 0-row "fast" queries silently | Verify each secondary has nonzero rows after `populate_*` |
| 18 | Physical Seek in skip path | `8d10782b` | SSTRead/TX rises 5× — prefetch buffer invalidated by Seek | Set `USE_PHYSICAL_SEEK_SKIP = false`; forward iterate instead |
| 19 | `wants_skip_group()` alongside `bool on_order` | (post-`128f6d44`) | Dead code — `on_order → false` already clears `customer_active`; `wants_skip_group()` guard can never fire after that | Use `on_order → false` directly; remove `wants_skip_group()` when cleaning up |

---

## §14 — Verification Checklist

Run after completing all phases:

- [ ] `grep -rn 'static constexpr int id' frontend/tpch/ --include='*.hpp' | sort -t= -k2 -n` — no duplicate IDs
- [ ] `make -C build/frontend test_query_q{N}_lsm -j$(nproc)` — builds clean
- [ ] Run with fresh `--ssd_path`:
  ```bash
  mkdir -p test_data_q{N} test_csv_q{N}
  ./build/frontend/test_query_q{N}_lsm \
      --ssd_path=./test_data_q{N} \
      --csv_path=./test_csv_q{N} \
      --tpch_scale_factor=1
  ```
  All `[OK]`, zero `[FAIL]`, exit 0.
- [ ] 4 identical digests printed (values are seed-dependent; agreement is the invariant)
- [ ] Row count matches expected (10 for LIMIT 10, 20 for LIMIT 20, ~5 for per-nation, etc.)
- [ ] Re-run in place (without manual wipe) — still `[OK]` (harness wipes its own `ssd_path`)
- [ ] `make -C build/frontend q{N}_lsm -j$(nproc)` — production executable builds
- [ ] `make q{N}_lsm scale=1` — runs all four structures, emits CSV metrics
