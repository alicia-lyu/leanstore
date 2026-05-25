# Q10I final sanity-check audit — 2026-05-25

End-of-implementation audit of Q10I (Phases 1–4) on worktree branch
`worktree-agent-a86745538133069de`. Goal: confirm what landed
matches the plan, the per-query design doc, the playbook, and the
project conventions — and flag any drift before the worktree is
parked for the paper-revision window.

## Bases consulted

- **Plan**: `.claude/plans/good-plan-since-you-delightful-rossum.md`
  (Phase 1 plan with commits 0–3 and decisions E1–E3).
- **Design doc**: [`q10i/CLAUDE.md`](./CLAUDE.md) — decisions D1–D14,
  Phase 4 query-body specifications.
- **Playbook**: [`../PLAYBOOK.md`](../PLAYBOOK.md) — esp. §3.6
  (Phase 1 recipe), §S5 (rationale for S5 deferral), Pattern B view
  loader recipe, record-layout PITFALLs.
- **Conventions**: [`../CONVENTIONS.md`](../CONVENTIONS.md).
- **Parent docs**: [`../CLAUDE.md`](../CLAUDE.md) (TPC-H tree),
  repo-root [`../../CLAUDE.md`](../../CLAUDE.md) (workflow rules,
  COLI MI showcase).
- **Cousin implementations**: `frontend/tpch/q3i/`, `q5i/`, `q10/`
  used as established patterns.
- **Implementation commits** on worktree branch:
  `d6025348` (Phase 1 commit 0 — widening),
  `b4c24011` (Phase 1 commit 1 — skeleton),
  `360013c9` (Phase 1 commits 2+3 — harness + schema),
  `38787939` (Phase 4 — query bodies),
  `a337f89a` (LINUX_PENDING Phase 5 entry).

## Aligned with plan / docs (no action)

- **Sentinel ids**: `q10i_pipeline_view_t` = 68;
  `q10i_agg_row_t` = 69 (in-memory). Matches plan §"Sentinel id
  allocation".
- **View Key shape**: `(custkey, orderkey, invoicekey, linenumber)`
  mirrors `lineitem_coli_t::Key` per memory
  `feedback-view-key-mirrors-mi` and PLAYBOOK §3.6.
- **Track**: `CustomerOrdersLineitemInvoicePipeline<Backend> coli`
  (Track 2 / COLI), not COL. Matches plan + D-cluster.
- **D14 widening** of `lineitem_coli_t` with `l_returnflag`
  (Varchar<1>) lives in `tpch_family/views_coli.hpp`. `from_base`,
  `print`, and `ADD_RECORD_TRAITS` are all updated. Q3I / Q5I never
  read `l_returnflag`, so cousin digests unchanged (verified at
  Phase 1 commit 0 gate).
- **Pattern B view loader**: `populate_q10i_view` in `load.tpp`
  uses `coli_group_walk` and emits one row per lineitem with
  FD-attached customer payload, `o_orderdate`, and pre-resolved
  `i_status` from `invoice_buf` (D8). Matches PLAYBOOK §3.6
  Pattern B recipe and memory
  `feedback-view-loader-reuses-query`.
- **D9 NATION INL at emit**: view payload omits `n_name`; resolved
  via per-customer point lookup at emit (`query.tpp` ~ line 477).
- **All four `query_by_*` real (not stubs)**:
  - S1 (`query_by_base`): chain-scan over the four custkey-sorted
    split COLI indexes + per-emit invoice point-seek.
  - S2 (`query_by_view`): view scan + per-customer map aggregator.
  - S3 (`query_by_merged`): bespoke `Q10IGroupWalkVisitor` over
    `coli_group_walk` (D6 — bespoke on top of util, NOT subclass
    of `Q3FamilyVisitor`, per memory
    `feedback-col-walk-is-shared-util`).
  - S4 (`query_by_hash`): hash build on orders set + LINEITEM
    streaming probe + chained INL (Rule 13).
  - All four funnel into shared `drain_to_sink` with a
    deterministic TopN-20 comparator (c_custkey tiebreaker).
- **PARAM_TABLE**: 24 entries Feb 1993 – Jan 1995, byte-identical
  to Q10's table (D-shared).
- **Predicates**: `q10i_predicate_orders` = orderdate window
  `[date_lo, date_lo + 90)`; `q10i_predicate_lineitem` =
  `l_returnflag == 'R'`.
- **`Q10IStats`** declares all 12 plan-listed counters
  (customers_scanned, orders_scanned, orders_passing_date,
  lineitems_scanned, lineitems_passing_returnflag, invoices_scanned,
  aggregator_rows_out, topn_offers, topn_evictions,
  mi_records_visited, mi_groups_skipped, view_rows_scanned).
- **Build wiring**:
  - `frontend/CMakeLists.txt` registers `q10i_lsm` (macOS + Linux),
    `q10i_btree` (Linux), `test_query_q10i_lsm` (macOS + Linux),
    `test_query_q10i_btree` (Linux).
  - `generate_targets.py` lists q10i across `exec_names`,
    `query_dirs`, and `storage_structures: [1, 2, 3, 4]` —
    **S5 deferred** per D3 and the global S5-deferral note.
  - `targets.mk` regenerated to match.
- **Test harness assertions**: split / merged-per-type / view
  cardinality, sentinel-ordering with the invoice tag
  (customer=1 < invoice=2 < orders=3 < lineitem=4), and a strict
  4-way XOR digest parity gate.
- **`q10i/RUNS.md`** carries the Phase-4 multi-SF entry with
  digests for SF=1, SF=5, SF=10.
- **`LINUX_PENDING.md`** Phase 5 entry queues the 5L cell sweep on
  the worktree branch with the no-merge warning and the digest to
  reproduce on the btree backend.
- **Worktree** is fully pushed to its remote tracking branch; no
  unpushed commits. E1 worktree-only discipline honored.
- **No `std::string`** in any q10i record payload — all `Varchar<N>`
  per PLAYBOOK PITFALL on `record_traits` / non-standard-layout.
- **No S5 leakage**: aCOLI absent from query bodies, makefile
  structure list, and generator map.
- **No merge to main**: confirmed via branch state; E1 still in
  force through paper revision.

## Deviations found (doc-only)

Two stale prose lines in higher-level docs, both fixed in this same
worktree as part of the audit:

1. **`frontend/tpch/CLAUDE.md`** Tests section, the per-query
   Q10I bullet still claimed "Phase 1: stubs return empty, digests
   all 0x0". The q10i top-of-file bullet 30 lines earlier already
   called out Phase 4 complete. Inconsistent within the same file
   — **edited** to reflect Phase 4 parity at SF=1/5/10 with a
   pointer to `q10i/RUNS.md`.
2. **Repo-root `CLAUDE.md`** "COLI MI + Invoice-Extended Queries"
   paragraph still said "Q5I and Q10I are design-doc only" and
   listed only `q3i_lsm` / `q3i_btree` as wired into CMake.
   **Edited** to: Q3I / Q5I / Q10I targets are all wired; Q3I and
   Q5I are production on main; Q10I is Phase 4 complete on the
   worktree branch with paper-deferred merge per E1.

No code, build, or test changes recommended or made — the
implementation matches plan and design doc end-to-end.

## Non-deviations explicitly verified

- View payload composition matches D8 (FD-attached `i_status`,
  customer payload, `o_orderdate`) and excludes `n_name` (D9).
- TopN-20 comparator is deterministic (revenue desc, then
  c_custkey asc tiebreak) — map-order vs streaming-order produce
  identical output, justifying the shared-aggregator design.
- S5 absence is consistent across queries, harness, makefile, and
  generator map.
- Cousin Q3I / Q5I tests not regressed by the widening — Phase 1
  commit 0 gate green; digests unchanged.

## Outstanding work (out of scope here)

- **Linux Phase 5** (queued in `LINUX_PENDING.md`): check out
  worktree branch on a Linux node (no merge to main), build
  `q10i_lsm` + `q10i_btree`, reproduce the SF=1 digest on the
  btree backend, then run the 5L cell sweep at SF=15, S1–S4,
  unlimited DRAM. Pair-fates with Q10's 5L cell. Expected story:
  S3 ≥ S2 > S1/S4.
- **Worktree merge** to main is paper-deferred per E1 — not part
  of this audit and not part of Phase 5.

## References

- Plan file:
  `.claude/plans/good-plan-since-you-delightful-rossum.md`
- Q10I design doc: [`./CLAUDE.md`](./CLAUDE.md)
- Q10I run log: [`./RUNS.md`](./RUNS.md)
- TPC-H tree doc: [`../CLAUDE.md`](../CLAUDE.md)
- Repo-root doc: [`../../CLAUDE.md`](../../CLAUDE.md)
- Playbook: [`../PLAYBOOK.md`](../PLAYBOOK.md)
- Conventions: [`../CONVENTIONS.md`](../CONVENTIONS.md)
- Linux follow-up queue:
  [`../../LINUX_PENDING.md`](../../LINUX_PENDING.md)
