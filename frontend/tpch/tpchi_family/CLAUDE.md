# `tpchi_family/` — Track-2 invoice-extended-set-shared substrate

Set-shared infrastructure used by **all Track-2 invoice-extended
queries** (Q3I, Q5I future, Q10I future). Composes on top of
`tpch_family/` — extended is additive, not a fork.

## Where this directory fits

The `frontend/tpch/` source tree organises shared logic on two
axes:

- **Vertical pairs** — `q3_family/`, future `q5_family/`,
  `q10_family/`. One per Qx ↔ Qxi pair.
- **Horizontal sets** — `tpch_family/` (Track-1 vanilla) and
  `tpchi_family/` (this directory). Each owns shape that all
  queries on one side of the i-axis reuse but the other side
  may not.

Generic infrastructure used by **everything** stays at top
level: `tpch_tables.hpp`, `tpch_workload.hpp`, `backend.hpp`,
etc. See [`../tpch_family/CLAUDE.md`](../tpch_family/CLAUDE.md)
for the full membership decision tree.

## File inventory

| File | Role |
|------|------|
| `tpchi_tables.hpp` | Invoice schema. Defines `lineitem_i_t`-style invoice base record types layered on top of `tpch_tables.hpp`'s base TPC-H schema. The genuine Track-2 schema delta. |
| `tpchi_workload.hpp` | `TPCHIWorkload` — alias / extension of `TPCHWorkload` that loads `invoice_t` and back-fills `l_invoicekey` on lineitems. Pulled in by the top-level `tpch_workload.hpp` so any query can opt into invoice loading. |
| `coli_pipeline.hpp` / `.tpp` | `COLIPipeline<Backend>` — 4-table COLI merged index (CUSTOMER × ORDERS × LINEITEM × INVOICE). The extended-side analogue of `tpch_family/col_pipeline.{hpp,tpp}`. Owns `populate_merged()` and `populate_aggregated()` (S5 aCOLI variant). |

## Composition rule

Each piece in `tpchi_family/` should depend on `tpch_family/`
where vanilla-shared types are involved (e.g. `coli_pipeline.hpp`
includes `../tpch_family/views_coli.hpp` because half its types
are vanilla-shared and live there). The reverse is **forbidden**:
`tpch_family/` must not depend on `tpchi_family/` — that would
make extended pieces a load-bearing dependency of vanilla
queries, defeating the composition.

There is a planned exception (Phase 3 of the family-dirs
refactor): split `tpch_family/views_coli.hpp` into
`tpch_family/views_tagged.hpp` (vanilla-shared customer/orders
tagged-key types) and `tpchi_family/views_invoice.hpp`
(genuinely-extended `lineitem_coli_t`, `invoice_coli_t`, aCOLI
variants). After that split, `tpch_family/views_invoice.hpp`
will not exist and the composition becomes one-directional.

## Future work

- **Phase 3 of the family-dirs refactor**: see above; split
  `views_coli.hpp` along the actual axis.
- **`q5_family/`** when Q5I starts and surfaces a Q5 ↔ Q5I
  shareable piece.
- **`q10_family/`** likewise for Q10 ↔ Q10I.
