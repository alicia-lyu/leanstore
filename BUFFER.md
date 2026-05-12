# Buffer — deferred follow-ups

Date-stamped, self-contained items the user has flagged for later. Each
entry is independent. Prune entries when handled.

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
