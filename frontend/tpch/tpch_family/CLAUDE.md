# `tpch_family/` — Track-1 vanilla-set-shared substrate

Set-shared infrastructure used by **all Track-1 vanilla queries**
(Q3, Q5, Q10 future, Q12, Q9). Independent of pipeline choice
(OL / COL / COLI). The horizontal twin of the per-pair `qN_family/`
directories — see the membership decision tree below.

## Where this directory fits

The `frontend/tpch/` source tree organises shared logic on two
axes:

- **Vertical pairs** — `q3_family/`, future `q5_family/`,
  `q10_family/`. One per Qx ↔ Qxi pair. Owns shape that's specific
  to that pair's join footprint (e.g. `lineitem_revenue_aggregator`
  templated on lineitem record type so Q3 plugs in `lineitem_col_t`
  and Q3I plugs in `lineitem_coli_t`).
- **Horizontal sets** — `tpch_family/` (this directory) and
  `tpchi_family/` (Track-2 invoice-extended). Own shape that all
  queries on one side of the i-axis reuse but the other side may
  not.

Generic infrastructure used by **everything** (vanilla and
extended) lives one level up at `frontend/tpch/` directly:
`tpch_tables.hpp`, `tpch_workload.hpp`, `tpch_flags.hpp`,
`per_structure_workload.hpp`, `backend.hpp`, etc.

## Membership decision tree

Use this when adding a new piece while implementing a query:

1. Used **only** by Qx and Qxi → `qx_family/`.
2. Used by Qx, Qy, Qz but **not** any Qxi → `tpch_family/` (this
   directory).
3. Used by Qxi, Qyi, Qzi but **not** any Qx → `tpchi_family/`.
4. Used by **all six** → `tpch_family/`, with `tpchi_family/`
   consumers depending on it (extended set is additive on top
   of vanilla, composition not duplication).
5. Used by Qx and Qyi (cross-axis odd cases) → keep it in the
   more central of the two homes (usually `tpch_family/`); flag
   for re-evaluation when Qy or Qxi joins as a consumer.

## File inventory

| File | Role |
|------|------|
| `views_ol.hpp` | OL pipeline record types (`ol_sort_key_t`, `joined_ol_t`, `SKBuilder` specialisations). Used by Q12, Q9; Q3I cross-axis-uses `joined_ol_t` for sort-key shape (the type is misnamed — it's really a tagged-key sort utility). |
| `views_col.hpp` | COL pipeline record types (`lineitem_col_t`; reuses `customer_coli_t` and `orders_coli_t` from `views_coli.hpp`). Used by Q3, Q5. |
| `views_coli.hpp` | COLI pipeline record types (`customer_coli_t`, `orders_coli_t`, `lineitem_coli_t`, `invoice_coli_t`). **Half its types are reused by Q3 vanilla** (the customer/orders pair) — the file lives here despite its name because the reuse spans both tracks. The `lineitem_coli_t` + `invoice_coli_t` portion is genuinely Track-2-only; a future Phase-3 split will move those into a separate `views_invoice.hpp` under `tpchi_family/`. |
| `ol_pipeline.hpp` / `.tpp` | `OrdersLineitemPipeline<Backend>` — 2-table OL merged index loader. Used by Q12, Q9. |
| `col_pipeline.hpp` / `.tpp` | `CustomerOrdersLineitemPipeline<Backend>` — 3-table COL merged index loader. Used by Q3, Q5. |
| `revenue.hpp` | `tpch::lineitem_revenue<L>(const L&)` — pure revenue arithmetic `l_extendedprice * (1 - l_discount)`, generic over any lineitem-shaped record. No date gate (that is Q3-family-specific; see `q3_family/accumulators.hpp`). Used by Q3 (via `LineitemRevenueAccumulator`) and will be used directly by Q5 / Q10 accumulators. |
| `family_stats.hpp` | `tpch::TPCHFamilyStats` — workload-agnostic cardinality and timing counters (scan/filter/join/MI-walk/stage timing) shared by all COL-family vanilla queries. `q3_family::Q3FamilyStats` derives from it and adds `topN_candidates` / `stage_us_topN` (Q3-specific LIMIT machinery). Q5 will derive `Q5Stats` from it in Phase 4. |
| `col_two_pointer_merge.hpp` | `tpch::col_two_pointer_merge<OrdersAdapter, LineitemAdapter, EmitFn>` — two-pointer ORDERS × LINEITEM merge kernel keyed by `(custkey, orderkey, linenumber)`. Caller supplies an emit callback; no Q3-specific row construction inside. Used by Q3 / Q3I / Q5 via `q3_family/view_loaders.hpp` (re-export shim) and directly by future Q10. |

## Future work

- **Phase 3 of the family-dirs refactor** (deferred): split
  `views_coli.hpp` along the actual axis. The vanilla-shared bits
  (`customer_coli_t`, `orders_coli_t`, the `tagged_path` helpers,
  the SKMatcher specialisations that don't reference invoice)
  stay here under a more honest filename
  (`views_tagged.hpp` or similar); the genuinely-extended bits
  (`lineitem_coli_t`, `invoice_coli_t`, the aCOLI variants,
  invoice-aware SKMatcher specialisations) move to
  `tpchi_family/views_invoice.hpp`.
- **`q5_family/`** will be created when Q5I starts and surfaces a
  Q5 ↔ Q5I shareable piece that doesn't fit the wider `tpch_family/`
  set scope.
- **`q10_family/`** likewise for Q10 ↔ Q10I.
