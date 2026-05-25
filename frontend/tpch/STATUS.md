# TPC-H Query Status

Current implementation state of all tracked queries. Keep this doc
to current state only — completed history belongs in `HISTORY.md`.
Update in place; do not append.

## Per-Query Status Table

| Query | S1 | S2 | S3 | S4 | S5 | Notes |
|-------|----|----|----|----|----|-|
| Q12 | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1. Production targets wired. |
| Q3  | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (10 rows). Production targets wired. Phase 9 doc refresh + Linux perf sweep remaining — see [`q3/CLAUDE.md §Implementation Phases`](q3/CLAUDE.md#implementation-phases). |
| Q3I | ✅ | ✅ | ✅ | ✅ | ✅ (deferred) | S1–S4 paper-reported axis. S5 aCOLI implemented (generic `std::visit` walker), parity-verified, deferred from paper sweep. The deferral is an **engineering-time choice, not a structural limit**: the generic walker loses to S3 (~2.5×); the hand-rolled walker that *wins* on Q10/Q10i (below) is simply not yet written for the 4-table aCOLI. **Internal note only — not in the paper:** per the 2026-05-25 decision the paper does not discuss this S5 (PAPER_EDITS.md Edit 2 withdrawn); the code is an archived in-tree reference, kept partly because Q10i's aCOLI reuses its record types. See [`PLAYBOOK.md §S5`](PLAYBOOK.md). |
| Q5  | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (LSM 0x85b9b6291f258258, BTree 0x2fa49bb120). First Linux sweep SF=15 DRAM=0.1: LSM **S2 (205) > S3 (156) > S1 (43) ≈ S4 (32)**; BTree **S2 (331) > S3 (250) > S1 (67) > S4 (9.5)** TX/s. View materialization beats MI scan (same shape as Q3I S2/S3 inversion — infrastructure, not Q5-specific). |
| Q9  | ❌ | ❌ | ❌ | ❌ | — | `load.tpp` ctor/`load()`/`get_size()` bodies still reference removed pipeline methods. `query_by_*` bodies, predicates, `Params::defaults()`, CMake targets all pending. |
| Q5I | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (S1 ≡ S2 ≡ S3 ≡ S4 strict at non-zero digest). S5 deferred (PLAYBOOK §S5) — same engineering-time choice as Q3I (hand-rolled aCOLI walker not yet written), not a structural limit. **Not in the paper** (2026-05-25 decision; PAPER_EDITS.md Edit 2 withdrawn) — archived in-tree reference. Production targets wired. Linux perf sweep pending — see [`LINUX_PENDING.md`](../../LINUX_PENDING.md). |
| Q10 | ✅ | ✅ | ✅ | ✅ | ✅ (supplemental) | Parity verified SF=1 (strict 4-way XOR across two distinct param sets — iter=0 + iter=1 off-default for the param-bake guard). **S5 aCOL** (`customer_coli_t + orders_acol_t`, per-order returned revenue baked, **hand-rolled** `acol_group_walk`) **is the fastest structure** in the 2026-05-25-q10 investigation (btree ~160 ms/q vs S3 35,024). This supersedes the earlier "no parameter-independent aggregate to bake (D8)" note: per-order returned revenue *is* bakeable (`l_returnflag='R'` is a spec constant; the `orderdate` window is applied at query time over surviving orders). Not in the paper sweep (hand-ported supplemental). Production S1–S4 targets wired. |
| Q10I| ✅ | ✅ | ✅ | ✅ | ✅ (supplemental) | Phase 4 complete + merged to calcite-integration (2026-05-25): all four `query_by_*` bodies parity-verified at SF=1/5/10 (strict 4-way XOR digest). **S5 aCOLI** (`customer_coli_t + orders_acoli_q10i_t`, per-order paid/open/late baked, **hand-rolled** `acoli_group_walk`) **is the fastest structure** in the 2026-05-25-q10i investigation (btree S5 181 ms vs S3 47,296). Demonstrates that the Q3I/Q5I aCOLI deferral is engineering-time, not structural. Not in the paper sweep (hand-ported supplemental). |

## Q3I S3 Performance Headline

**S3 ≥ S2 > S1/S4** at SF=15 on both backends (cache-resident and
disk-bound). Post-A2c + A3:

- RocksDB SF=15 iso: S3 `fused_emit` = 14.46 TX/s (+700% vs forward-iter).
- LeanStore SF=15: A3 delivered +356% TX/s; SF=40 disk-bound: +116×.
- S3 matches/beats fully-materialised S2 without paying its storage /
  maintenance cost, while comfortably beating split-merge (S1) and hash (S4).

Full investigation worklist and per-hypothesis evidence: [`q3i/PERFORMANCE.md`](q3i/PERFORMANCE.md).

## What's Needed Next

- **Q9**: fix `load.tpp` references to removed pipeline methods first;
  then `query_by_*` bodies, predicates, CMake/generate_targets.py entries.
  Q9-specific: NATION + SUPPLIER hashmaps before the OL scan; PART +
  PARTSUPP merge joins inside per-row callback; LIKE filter on `p_name`.
- **Q3 Linux perf sweep**: Phase 9 doc refresh (see q3/CLAUDE.md) then
  a Linux SF=15/40 sweep for the paper.
- **Q10/Q10I paper-protocol rerun**: the 2026-05-25 supplemental data is
  bg=0, single-rep, hand-ported. To match the paper's stated setup
  (bg=2, 3 reps), rerun via the paper harness once q10/q10i are added to
  `analyze_paper_sweep.py` + `sweep.yaml`. Tracked in the audit-response
  plan and `paper-data/SWEEP_LOG.md`.
