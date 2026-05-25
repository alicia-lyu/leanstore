# Q10 Audit — 2026-05-25

End-of-implementation read-only sanity audit against the Q10 plan,
docs, `frontend/tpch/PLAYBOOK.md`, `frontend/tpch/CONVENTIONS.md`,
and top-level `CLAUDE.md` workflow rules. Conducted after the Linux
5L perf sweep landed and S5 aCOL came online (D8 reversal, commit
`fd771c13`). Read-only — code was confirmed correct; only doc drift
remained.

## Scope of audit

- All Q10 implementation files (`q10/workload.hpp`, `query.tpp`,
  `load.tpp`, `views.hpp`, `executable_*.cpp`, harness sources under
  `tests/q10/`).
- All Q10-family shared substrate (`q10_family/admit.hpp`,
  `q10_family/acol_walk.tpp`).
- Q10 doc surfaces (`q10/CLAUDE.md`, `q10/RUNS.md`,
  `q10/PERFORMANCE.md`) and parent indices (`frontend/tpch/CLAUDE.md`,
  `STATUS.md`, `HISTORY.md`, top-level `CLAUDE.md`, `LINUX_PENDING.md`,
  `LINUX_HISTORY.md`).
- Cross-references: PLAYBOOK §S5, §8–§12, §10 anti-pattern #25/#26;
  CONVENTIONS Rules 4 and 13; D1–D8.

## Findings

| ID | Severity | Surface | Finding | Disposition |
|----|----------|---------|---------|-------------|
| A1 | MED | `q10/workload.hpp:17-18` | Header preamble claimed *"S5 is omitted (Decision D8) — Q10 has no parameter-independent aggregate to bake"* while the same file declared the aCOL adapter (line 145) and `query_by_aggregated` (line 182). D8 was revised on 2026-05-25. | **Fixed** in this audit: preamble now documents the D8 reversal, names the per-order returned-revenue bake, points to `acol_walk.tpp`, and links the RUNS.md / PERFORMANCE.md sweep results. |
| A2 | MED | `q10/CLAUDE.md §Status` | Headline read *"all four `query_by_*` paths live; … Linux 5L perf sweep done"* but did not name S5 aCOL despite it being the **fastest structure on both backends** per the 2026-05-25 sweep. | **Fixed** in this audit: §Status now names S5 aCOL explicitly and quotes the headline ms/query numbers (btree 160 ms; LSM 1.37 s). |
| A3 | LOW | `q10/CLAUDE.md §Sibling Docs` | `PERFORMANCE.md` link verified present (line 65). No action needed. | No action. |
| A4 | LOW | `LINUX_PENDING.md` / `LINUX_HISTORY.md` | Q10 5L sweep already rotated to `LINUX_HISTORY.md §"Closed by 2026-05-25 — Q10 first Linux 5L sweep + refresh LSM 9 GiB"`. Pending file is clean. | No action. |
| A5 | LOW (withdrawn) | `q10_family/acol_walk.tpp` header | Initial audit flagged the walker for not naming the NATION INL substitution per CONVENTIONS Rule 13. On re-reading, the NATION INL is realised in `q10_family/admit.hpp::q10_finalize_aggregator`, not in the walker — the walker just emits per-customer/per-order records. The Rule 13 site is correctly labeled at the admit boundary. | Withdrawn; no action. |

## No genuine correctness / paper-claim risks

The audit confirmed clean state on the following gates:

- **PLAYBOOK §10 anti-pattern #25/#26 (off-default param re-run).**
  `test_query_q10_rocksdb.cpp` and `test_query_q10_leanstore.cpp`
  both invoke `set_params_for_iter(0)` then `set_params_for_iter(1)`
  and re-assert strict 4-way XOR parity. PARAM_TABLE rotation
  exercises all 24 valid month starts in [1993-02-01, 1995-01-01].
- **5-way parity in production.** RUNS.md 2026-05-25 5L entries
  record S1==S2==S3==S4==S5 across both backends (parity gated by
  `test_query_q10_*` first at SF=1 then by the production binaries
  at SF=1550 btree / 3850 lsm).
- **CONVENTIONS Rule 4 (PK-only secondaries).** Customer-sorted COL
  split secondaries do not duplicate non-PK columns; INL substitution
  recovers them at record-assembly time per `admit.hpp`.
- **CONVENTIONS Rule 13 (INL at record-assembly).** Realised at
  three sites: (1) `q10_finalize_aggregator` resolves `n_name` via
  NATION PK probe at customer emit (D6); (2) S4's
  `query_by_hash` recovers `c_custkey` via `orders.lookup1` on
  per-orderkey transition + full customer via `customer.lookup1`
  (cached per orderkey); (3) `customer_inl_lookups` and
  `orders_inl_lookups` Q10Stats counters wire the same boundary for
  diagnostics.
- **Per-path Q10Stats split.** Phase 5 commit 1 confirmed clean
  single-path cardinalities (e.g. `customers_scanned == 150` at
  SF=1, not the doubled-up 600 from the pre-split shared-stats
  pattern).
- **Record-type IDs** (`q10_pipeline_view_t` id=70,
  `q10_pipeline_view_preagg_t` id=72, `orders_acol_t` id=37) — no
  collisions with sibling Q3/Q5/Q3I/Q5I families.
- **PLAYBOOK §S5 rationale satisfied.** S5 aCOL ships a dedicated
  walker (`acol_walk.tpp`) instead of falling back to the generic
  merged scanner — exactly the missing piece that drove the Q3I S3>S5
  anomaly. No anomaly in Q10's 5L numbers.

## Provenance

- Audit conducted by Opus 4.7 on 2026-05-25.
- Worktree: `q10-audit-may25` (branch off `calcite-integration` at
  commit `71536eeb`).
- Fixes A1 + A2 land in this worktree as a single doc-only commit.
- No code paths touched; no test rebuild required.
