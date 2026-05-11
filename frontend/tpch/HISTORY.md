# TPC-H Implementation History

Append-only log of completed work. Never edit existing entries.
Rotate entries older than 3 months to `HISTORY-YYYYHN.md` (e.g.
`HISTORY-2026H1.md`) when this file exceeds ~400 lines.

---

## Completed (post-skeleton)

- **Q3I S5 aCOLI MI + wrap-up** (2026-05-02; revised 2026-05-03):
  `customer_acoli_t` / `orders_acoli_t` / `lineitem_acoli_t` in
  `views_coli.hpp` (ids 49/50/53); `populate_aggregated()` 2-pass algorithm
  in `coli_pipeline.tpp` (Pass B removed — `pre_revenue` dropped so S5 is
  reusable across all DATE param sets); `query_by_aggregated` in
  `q3i/query.tpp` (uses `LineitemRevenueAccumulator` shared with S1/S3;
  `pre_open_due` read directly). `[SKIP S5]` parity guard removed — all five
  paths produce identical digest at SF=1 (6 rows).
  `acoli_total=744 (c=150 o=202 l=392)` vs `mi_records_visited~1547`
  (S5 skips all invoice rows). Also landed this session:
  multi-level active markers design (`customer_active` / `order_active`)
  documented in `PLAYBOOK.md §7.1` — superseded 2026-05-09 by the
  uniform `WalkAction` (`Continue` / `SkipOrder` / `SkipGroup`) hook
  protocol now used by `col_group_walk` and `coli_group_walk`;
  `RocksDBLogger::capture_baseline()` for SSTWrite baseline-subtract
  (documented in `PLAYBOOK.md §10`); `StageTimer` per-stage attribution and
  per-query averages in production stats output.

- **`views_ol.hpp` fully implemented** (2026-04-29): `ol_sort_key_t` (match,
  first_diff, matching_keys), `joined_ol_t` (constructors, accessors),
  `SKBuilder<ol_sort_key_t>` (create, project, to_key). Unit-tested via
  `test_views_ol.cpp` (CMake target `test_views_ol`, 12 tests).
- **Shared merge-join infrastructure refactored** (2026-04-29):
  `PremergedJoin` decoupled from record-type `get_jk()` / `Key(JK)` via
  `jk_from_variants` free function and `SKBuilder::to_key<R>`.
  `SKBuilder::get<R>` renamed to `project<R>` across all files.
  Geo backward compatibility verified (`geo_lsm` builds clean).
- **`ol_pipeline` trimmed to truly-shared scope; load-test executables added**
  (2026-04-29): `OrdersLineitemPipeline` now only owns `populate_merged` and
  `get_merged_size`. Operator drivers and view loading removed (moved to
  per-query dirs in follow-up). `test_load_merged_rocksdb.cpp` and
  `test_load_merged_leanstore.cpp` added as standalone MI[0] load-test binaries
  (CMake targets `test_load_merged_lsm` / `test_load_merged_btree`).
- **Shared `per_structure_workload.hpp` hoisted; per-query files collapse to
  aliases** (2026-04-30): `BaseStructure` / `ViewStructure` / `MergedStructure`
  / `HashStructure` templates with inline forwarder bodies moved to
  `frontend/tpch/per_structure_workload.hpp`. Q12/Q3/Q9 `per_structure_workload.hpp`
  files are now single-line `using` aliases.
- **`views.hpp` narrowed to row types; `Params` and predicates moved to
  `workload.hpp`** (2026-04-30): Q12 `Params` struct, `Params::defaults()`,
  `q12_predicate_lineitem`, and `q12_predicate_joined` now live in
  `q12/workload.hpp`. `q12/views.hpp` contains only `q12_pipeline_view_t` and
  `q12_agg_row_t`.
- **`joined_ol_t::unfoldKey` override added** (2026-04-30): `joined_ol_t`
  changed from a `using` alias to a derived struct with an explicit `unfoldKey`
  that uses the existing `joined_ol_t::Key(const ol_sort_key_t&)` constructor.
  Unblocks `RocksDBAdapter<joined_ol_t>` instantiation (pipeline view adapter).
- **Lineitem sparse-key data generation bug fixed** (2026-04-30):
  `loadPartsuppLineitem` and `loadLineitem` in `tpch_workload.hpp` now call
  `orderkey_from_index()` at all three call sites. Previously raw indices were
  used, making ~75% of lineitems orphans.
- **Q12 `load.tpp` fully implemented; `populate_q12_view` uses manual merge**
  (2026-04-30): ctor, `load()`, `get_size()`, and free function
  `populate_q12_view` all have real bodies. View loading uses a two-pointer
  manual merge (not `BinaryMergeJoin`) because the merge-join's wildcard
  semantics emitted ~1 row per order group instead of N rows per (order,
  lineitem) pair.
- **Q12 view load-test added** (2026-04-30): `test_load_merged_stats.hpp`
  refactored to return a `MergedOlStats` struct. New
  `q12/test_load_view_stats.hpp` provides `ViewStats<Backend>`,
  `dump_view_stats<Backend>`, and `compare_mi_and_view` (cross-check with
  [OK]/[FAIL] tags). New binaries `test_load_q12_rocksdb.cpp` and
  `test_load_q12_leanstore.cpp` build MI[0] + pipeline view and cross-check
  cardinality and orderkey ranges. CMake targets: `test_load_q12_lsm`
  (macOS + Linux) and `test_load_q12_btree` (Linux only). All [OK] checks
  pass at scale factor 1.
- **Q12 query bodies + cross-structure parity test** (2026-04-30):
  All four `query_by_*` methods in `q12/query.tpp` implemented (S1
  `BinaryMergeJoin` with filter-pushdown, S2 view scan + post-join
  filter, S3 `PremergedJoin` over MI[0], S4 `HashJoin` with the same
  filter-pushdown as S1). `q12_predicate_lineitem` /
  `q12_predicate_joined` implemented; the joined variant delegates to
  the lineitem variant via `j.line()` to keep S1–S4 semantically
  identical. New `test_query_q12_lsm` / `test_query_q12_btree` binaries
  run all four paths in one process and check XOR parity over the
  aggregate result rows. Parity at SF=1: all four paths produce digest
  `0x9000007000003c` (`[OK]`). Limitation: digest is over aggregate
  fields only, not per-orderkey — see `q12/CLAUDE.md §XOR parity scope`.
  Also fixed a latent variant-dispatch bug in
  `shared/merge-join/premerged_join.hpp`: the tentative-skip path now
  uses `jk_from_variants<JK>` instead of calling
  `SKBuilder<JK>::create` on variant operands.
- **F1 admission filter in PremergedJoin** (2026-04-30):
  `PremergedJoin::scan_next` accepts an optional `admit` callback; records
  rejected before emplace. `query_by_merged` threads `q12_predicate_lineitem`
  through it. Closes the ~2× merged-vs-hash perf gap to ~5–10%.
- **Q12Stats cardinality counters** (2026-04-30): `Q12Stats` struct in
  `q12/workload.hpp` tracks scanned/admitted/join/agg counters per path.
  All `query_by_*` paths increment when `stats != nullptr`; test binaries
  print `[card]` lines. Confirmed at SF=1: 6004 scanned → 30 passed → 30
  join callbacks → 2 agg rows across all four paths.
- **OPERATORS.md aligned with current code** (2026-04-30): updated §3 op 1
  (TableScan ownership moved to per-query drivers), §3 op 4 (S1/S3 named the
  actual primitives plus a new "load vs query" paragraph), §4 Q12 (unfiltered
  view + manual two-pointer merge), §5 Q12 example (concrete primitive
  instantiations), §8 pointers (trimmed pipeline contract). Added
  cross-reference comments pointing to OPERATORS.md from each per-query
  `workload.hpp` / `query.tpp` / `load.tpp` so programmers filling in
  `query_by_*` bodies land on the right operator strategy.
- **View size reporting fixed; MI/view size cross-check added** (2026-04-30):
  `RocksDB.hpp::get_size<Record>()` now uses `SizeApproximationOptions` with
  `include_memtables=true`, `include_files=true`, `files_size_error_margin=0.1`,
  fixing spurious 0.00 MiB readings for sub-CF prefix ranges at small scale
  factors. `compare_mi_and_view` in `q12/test_load_view_stats.hpp` prints an
  `[OK]/[FAIL]` bound (0.3×–5× MI[0] size). At SF=1 the view reports 0.28 MiB
  and the cross-check is `[OK]`. The `get_size` fix benefits all per-Record
  sizing calls across the codebase, not just Q12.
- **TPC-H §4.2.3 derived fields now computed** (2026-04-30): `part_t::computeRetailPrice(partkey)`
  factored out as a pure function; `lineitem_t::generateRandomRecord` now
  computes `l_extendedprice = l_quantity * p_retailprice` instead of
  `randomNumeric(0,100)`. `TPCHWorkload` accumulates a per-orderkey
  `OrderAggregate {totalprice, line_count, ostatus_count}` while generating
  lineitems; `loadOrders` then materializes orders with finalized
  `o_totalprice = Σ l_extendedprice * (1+l_tax) * (1-l_discount)` and
  `o_orderstatus` derived from lineitem statuses ('O'/'F'/'P'). `loadOrders`
  was moved after `loadPartsuppLineitem` and a new `prepopulate_order_dates`
  step seeds `order_dates` first so lineitems still have access to
  `o_orderdate`. The placeholder fields flagged in the
  `data-generator-evolution` plan are gone; only `o_clerk` and various
  `randomastring` comment fields remain placeholders (none are query-critical).
- **`invoice_t` schema + loader** (2026-05-01): `invoice_t` added to
  `tpch_tables.hpp` (keyed by `i_invoicekey`). `lineitem_t` gained
  `l_invoicekey` payload. `TPCHWorkload::loadInvoiceAndLinkLineitem()`
  runs after `loadOrders`, creates ~1.5× orders count invoices, and
  back-fills `l_invoicekey` on each lineitem.
- **COLI 4-table merged index pipeline** (2026-05-01): `views_coli.hpp`
  defines `customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`,
  `invoice_coli_t` with Calcite-style tagged keys (`tagged_path` helper,
  pointer-to-member `tag_field_step` / `tag_fields2_step`). Sentinel
  tag `index=0` ensures parent rows sort before children within each key
  group (`customer=1`, `invoice=2`, `orders=3`, `lineitem=4`). Twelve
  explicit `SKMatcher<R1,R2>` specializations cover all 16 ordered pairs.
  `COLIPipeline<Backend>` in `coli_pipeline.{hpp,tpp}` owns the
  `MergedAdapter<...>` and resolves `custkey` for lineitems via an
  in-memory orderkey→custkey map built during the orders pass.
  `test_load_coli_lsm` (`tests/test_load_coli_rocksdb.cpp`) verifies row
  counts, FK resolution, and hierarchical scan order at SF=1 — all [OK].
- **Merged-adapter `accepts_key` dispatch hook** (2026-05-01, renamed from
  `matches` 2026-05-02 to disambiguate from `SKMatcher::match` join-pair
  matching): `LeanStoreMergedAdapter::toType()` now tries
  `Record::accepts_key(key, key_len)` via SFINAE before falling back
  to the `(maxFoldLength, sizeof(payload))` heuristic. Existing OL/Q12
  records opt out silently; tagged COLI records opt in via explicit
  `static bool accepts_key(...)`. Additive — existing tests pass byte-for-byte.
  `toType` also `memcpy`s the value bytes (instead of `reinterpret_cast`) to
  guard against potential alignment UB on platforms where RocksDB value
  buffers aren't 8-byte aligned.
- **`SKMatcher` abstraction + `sk_for_t` rename** (2026-05-01):
  `SKMatcher<R1,R2>` per-pair join-matching abstraction introduced in
  `frontend/shared/view_templates.hpp`. Default specialization wraps legacy
  `SKBuilder<JK>::create + JK::match` so OL pipelines require no changes.
  `SortKeyFor<R>` opt-in trait and `HasSharedSKBuilder` concept gate the
  default. Sort-key alias renamed from `sort_key_t<R>` to `sk_for_t<R>` to
  avoid collision with `geo::sort_key_t`. `test_sk_matcher_compat.cpp`
  (4 tests) proves sign/zero equivalence for OL records.
  `test_views_coli.cpp` (6 tests) covers tagged-key encoding, `matches`
  dispatch, and sentinel ordering. Both test suites pass.
  `test_query_q12_lsm` XOR-parity digest unchanged (`0x90000070006039`)
  confirming the fold-length fallback path is unaffected.
- **Data-generation load order fixed** (2026-04-30): `TPCHWorkload::load()`
  now runs `loadCustomer → loadOrders → loadPartsuppLineitem` (previously
  partsupp+lineitem ran before orders/customer). The earlier order left
  `order_dates` empty during lineitem generation, so `o_orderdate`
  defaulted to 0 and lineitem dates were in `[2, 151]` instead of
  `[orderdate+2, orderdate+151]` — collapsing Q12's receipt-date filter
  to zero matches. After the fix, `test_query_q12_lsm` reports ~25–30
  matching lineitems at SF=1 and all shape + parity checks pass.
  Spec-compliant date math in `lineitem_t::generateRandomRecord` is
  preserved.

---

## Q12 Completed — Lessons for Q3/Q9

### Reusable Infrastructure (copy directly)

- `OrdersLineitemPipeline<Backend>` — same MI(ORDERS, LINEITEM), same
  `populate_merged()` / `get_merged_size()`
- Join drivers: `BinaryMergeJoin`, `PremergedJoin`, `HashJoin` all typed on
  `ol_sort_key_t` / `joined_ol_t` from `views_ol.hpp`
- `TpchExecutableHelper` for throughput measurement
- Per-structure wrapper aliases (`per_structure_workload.hpp`)
- `load.tpp` structure: `tpch.load()` → `populate_view()` →
  `ol.populate_merged()`
- F1 admission filter pattern in `PremergedJoin::scan_next` via `admit`
  callback

### What must be reimplemented per query

- **View types + view loading**: Q12 uses manual two-pointer merge in
  `populate_q12_view`; each query needs its own view type and loading
  function
- **Predicates and aggregates**: Different filter conditions, different
  grouping keys, different output shape (`q{N}_agg_row_t`)
- **`Params` struct**: Different substitution parameters per query
- **Downstream joins**: Q3 needs CUSTOMER hash join; Q9 needs PART,
  SUPPLIER, PARTSUPP, NATION lookups
- **Post-aggregate processing**: Q3 needs ORDER BY + LIMIT 10; Q9 needs
  GROUP BY (nation, year) with no LIMIT

### Performance observations (SF=40)

| Structure | TX/s | SSTRead(us)/TX | Size (MiB) |
|-----------|------|----------------|------------|
| base_merge_join (S1) | 10.74 | 24845 | ~37 |
| pipeline_view (S2) | 10.40 | 31579 | 35.50 |
| mi_premerged (S3) | 11.51 | 25143 | 32.15 |
| base_hash_join (S4) | 11.21 | 26269 | ~37 |

- MI advantage modest (~7%) for 2-table join — expect similar for Q3/Q9
  since the OL pipeline dominates and extra joins are small-table lookups
- Hash beats merge because build side fits in memory — need memory-pressure
  experiments (see `TPCH_experiments.md §Memory-Pressure Experiments`)
- Pipeline view slowest — fat rows increase scan cost proportionally
- MI advantage should widen under I/O pressure (higher SF, lower DRAM)
- MI is actually *smaller* than base tables because it shares the orderkey
  prefix

### Checklist for implementing Q3

1. Fix `q3/load.tpp` — replace removed `ol.populate_view()` /
   `ol.get_view_size()` with per-query `populate_q3_view()` and local size
2. Define `q3_pipeline_view_t` (replace `using = joined_ol_t` placeholder)
3. Implement `Params::defaults()` (mktsegment=BUILDING,
   orderdate < 1995-03-15, shipdate > 1995-03-15)
4. Implement predicates: `q3_predicate_orders`, `q3_predicate_lineitem`,
   `q3_predicate_joined`
5. Implement all 4 `query_by_*` bodies — same monolithic post-join pattern
   as Q12, adding CUSTOMER hash lookup inside the callback
6. Add top-10 selection (priority queue or partial sort)
7. Add XOR parity test (`test_query_q3_lsm`)
8. Add CMake targets and `generate_targets.py` entries
