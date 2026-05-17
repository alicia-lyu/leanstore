# TPC-H Query Status

Current implementation state of all tracked queries. Keep this doc
to current state only — completed history belongs in `HISTORY.md`.
Update in place; do not append.

## Per-Query Status Table

| Query | S1 | S2 | S3 | S4 | S5 | Notes |
|-------|----|----|----|----|----|-|
| Q12 | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1. Production targets wired. |
| Q3  | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (10 rows). Production targets wired. Phase 9 doc refresh + Linux perf sweep remaining — see [`q3/CLAUDE.md §Implementation Phases`](q3/CLAUDE.md#implementation-phases). |
| Q3I | ✅ | ✅ | ✅ | ✅ | ✅ (deferred) | S1–S4 paper-reported axis. S5 aCOLI implemented, parity-verified, but deferred from paper sweep. See [`PLAYBOOK.md §S5`](PLAYBOOK.md) for rationale. |
| Q5  | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (LSM 0x85b9b6291f258258, BTree 0x2fa49bb120). First Linux sweep SF=15 DRAM=0.1: LSM **S2 (205) > S3 (156) > S1 (43) ≈ S4 (32)**; BTree **S2 (331) > S3 (250) > S1 (67) > S4 (9.5)** TX/s. View materialization beats MI scan (same shape as Q3I S2/S3 inversion — infrastructure, not Q5-specific). |
| Q9  | ❌ | ❌ | ❌ | ❌ | — | `load.tpp` ctor/`load()`/`get_size()` bodies still reference removed pipeline methods. `query_by_*` bodies, predicates, `Params::defaults()`, CMake targets all pending. |
| Q5I | ✅ | ✅ | ✅ | ✅ | — | Parity verified SF=1 (S1 ≡ S2 ≡ S3 ≡ S4 strict at non-zero digest). S5 deferred (PLAYBOOK §S5). Production targets wired. Linux perf sweep pending — see [`LINUX_PENDING.md`](../../LINUX_PENDING.md). |
| Q10I| — | — | — | — | — | Design doc only; no skeleton yet. |

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
- **Q10I**: design doc exists; skeleton not yet started.
