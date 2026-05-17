# Buffer — deferred follow-ups

Date-stamped, self-contained items the user has flagged for later. Each
entry is independent. Prune entries when handled.

## 2026-05-16 — Q5I S4 rare parity flake

**Context**: during Phase 5 doc-only verification, one
`test_query_q5i_lsm` run at SF=1 (fresh `--ssd_path`) produced a
1-bit-region divergence between S4 and S1/S2/S3:

- S1/S2/S3 digest `0x303b3bcc95cc0c73` (2 rows)
- S4 digest      `0x303b3a4c95cc0c73` (2 rows)
- XOR diff       `0x0000018000000000` — looks like a small single-row
  field difference (possibly `i_status` or revenue cent value).

Five subsequent fresh-load runs all pass strict 4-way parity at
matching non-zero digests, so the bug is rare and data-dependent
(SF=1 random generator). Phase 4b commit 2 (`19104306`) shipped on
a clean run; this surfaced in a separate roll of the dice.

**Hypothesis space**:

1. S4 caches `c_nationkey` per orderkey transition. If the random
   generator produces an orderkey whose lineitems span more than
   one custkey via some FK-violating data quirk, S4 would
   miscache. Unlikely given the loader contract, but worth
   asserting.
2. Invoice INL via `invoice.lookup1(l_invoicekey)` could fall
   through silently when the key is missing — `lookup1` may not
   throw on miss in this RocksDB adapter. `i_status` would then
   stay default-initialized (`'\0'`) and route only into
   `nominal`, not into any of the realised/open/late buckets.
   That mismatches S1/S2/S3 which actively throw on invoice FK
   miss. Worth checking adapter semantics.
3. Tie-break in `q5i_sort_cmp` if two buckets have equal
   nominal_revenue — but S4 and S1/S2/S3 share the comparator, so
   this is unlikely.

**Action**: add a `--check_invoice_lookup_throws` smoke or simply
make S4's `invoice.lookup1` callback assert the callback fired
(`bool found=false` flipped inside). Re-run with `--tpch_seed=…`
sweeps to find a reproducer.

Not blocking Phase 5 (doc-only) — but should land before any
Linux SF=15 sweep, since one bad run in 15+ TX/s × 15s = a few
hundred queries means several bucket mismatches per run.

---

## 2026-05-09 — Direct unit test for `tpch::lineitem_revenue` (un-gated path)

**Context**: refactor commit `93ab0bd3` lifted the revenue arithmetic
into `frontend/tpch/tpch_family/revenue.hpp` as
`tpch::lineitem_revenue<L>(const L&)`. It's currently exercised only
indirectly:

- `frontend/tpch/tests/test_q3_family.cpp` — covers it via
  `LineitemRevenueAccumulator::consume`, which always pairs it with
  the `l_shipdate > params.shipdate` gate (Q3-family-specific).
- `test_query_q3_lsm` / `test_query_q3i_lsm` — end-to-end parity
  also gated.

**Gap**: no direct test of `lineitem_revenue` against an *un-gated*
lineitem — the Q5 use case (Q5 has no shipdate filter; it calls
`lineitem_revenue` directly from a per-`n_name` accumulator).

**When to address**: at Q5 Phase 1, when Q5 lands its first real
accumulator that calls `tpch::lineitem_revenue` directly. Add the
test colocated with the Q5 accumulator work, not retroactively.

**What to test**: a handful of `lineitem_t` / `lineitem_col_t` /
`q5_pipeline_view_t` instances with known `(extendedprice, discount)`
pairs, asserting `lineitem_revenue(l) == extendedprice * (1 - discount)`
to a sensible `Numeric` precision. Spot-check at boundary discounts
(0.0, 0.10) since Q5's per-`n_name` aggregate sums many of these.
