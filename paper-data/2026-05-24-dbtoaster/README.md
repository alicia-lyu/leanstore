# 2026-05-24-dbtoaster — DBToaster IVM baseline for refresh_sales

Generic incremental-view-maintenance baseline for the **Q3 and Q5 pipeline
views** under a TPC-H RF1/RF2 update stream, as a comparison point for
LeanStore's S2 materialized pipeline view. Engine: **DBToaster 2.3**; source in
[`../../dbtoaster/`](../../dbtoaster/) (`refresh_sales.sql`, `main.cpp`). Data:
TPC-H `dbgen` (base `.tbl` + `-U 1` refresh set).

## What was run

One DBToaster program maintains **both** pipeline views (unaggregated
`C×O×L` and `C×O×L×NATION` joins, one row per lineitem) simultaneously.
Base + RF1 inserts stream from the CSV files (measured in the
`process_stream_event` override); RF2 deletes are fired via the generated
`on_delete_*` triggers. **Unlimited memory** — no `ulimit` sweep, because
DBToaster is pure in-memory (no eviction / SSD spill), so a cap below the
working set just OOMs rather than producing a pressure curve.

Three scale factors (`summary/refresh_sales_dbtoaster_throughput.csv`):
SF=0.01 and 0.1 establish the linear RSS-vs-SF scaling; **SF=0.36 ≈ 5 GiB
working set** is the paper 5-GiB-secondary anchor.

## Results

| SF | view rows (Q3=Q5) | RF1 orders/s | RF2 orders/s | working set | peak RSS |
|----|-------------------|--------------|--------------|-------------|----------|
| 0.01 | 60,174 | 105,000 | 41,112 | 140 MiB | 209 MiB |
| 0.1 | 600,578 | 91,316 | 17,349 | 1.35 GiB | 2.25 GiB |
| 0.36 | 2,160,142 | 86,253 | 16,417 | **4.86 GiB** | **8.25 GiB** |

(orders/s: one "order" = insert/delete of 1 order + its 1–7 lineitems, with
both views maintained per event.)

## Reading

- **Correctness**: at every SF, `q3_view_rows = q5_view_rows = base_lineitems +
  rf1_lineitems − rf2_lineitems` (size-stable delta) — e.g. SF=0.36:
  2,160,128 + 2,157 − 2,143 = 2,160,142. NATION is 1:1 so Q5 = Q3 row count.
- **The point — memory requirement gap**: DBToaster maintains both views at
  ~86k/16k orders/s but must hold the entire ~4.9 GiB working set resident
  (peak ~8.25 GiB, ≈1.7× the working set), with no spill. LeanStore S2 serves
  the equivalent 5 GiB secondary from a **0.4–1.0 GiB DRAM buffer pool** by
  spilling to SSD (see `../2026-05-23-refresh-5L`, `../2026-05-23-refresh-sf1`:
  btree 22.6k / lsm 4.2k RF1 inserts/s @ SF=1). Generic IVM needs RAM ≥ its
  full in-memory footprint to maintain the same views.

## Caveats

- **Unlimited-memory only**: RSS is a *loose lower bound* on the RAM
  requirement; actual need is a multiple of the working set (≈1.7× peak here;
  the precise factor under sustained RF1/RF2 is left for later).
- `DECIMAL → double` (DBToaster has no fixed-point) vs LeanStore `Numeric`.
- dbgen `-U` orderkeys ≠ LeanStore's sparse `orderkey_from_index` grid — both
  honor the disjoint-keyspace, size-stable-pair invariant, so per-update cost
  is comparable even though exact keys differ.
- **RF1 and RF2 run as separate phases by default**, so the default `pair` rate
  is *derived* (`1/(1/rf1+1/rf2)`). A **measured interleaved pair** is now also
  available via `RF_INTERLEAVE=1` (`make build` then `RF_INTERLEAVE=1
  ./build/refresh_sales`): it captures the RF1 tail and replays it interleaved
  with RF2 through the typed `on_insert/on_delete` triggers — the same 1-insert/
  1-delete loop LeanStore runs. See `summary/refresh_sales_dbtoaster_interleaved.csv`.
  **The measured pair (≈33,200 pairs/s @ SF=0.36) is 2.4× the derived 13,792**:
  the derived value leaned on the *phased* RF1, which ran through the generic
  stream/event-dispatch path (event-arg unpacking + virtual routing = ingestion
  overhead), understating the trigger-only maintenance cost. The interleaved
  typed-trigger number is the apples-to-apples figure against LeanStore's direct
  maintenance calls, and it **beats** the LeanStore in-memory btree pairs
  (S1 20.5k / S3 18.5k) — so the in-memory *throughput* edge is DBToaster's; the
  LeanStore contrast is memory (it runs the same views at 0.4–1.0 GiB by spilling,
  whereas DBToaster needs ≈8 GiB resident and OOMs below its working set).

## raw/ (gitignored)

Per-SF harness logs (`sf*.log`), the SF=0.36 `/usr/bin/time -v` capture
(`sf0.36.time.log`), and the harness CSV (`RefreshTPut.dbtoaster.sf0.36.csv`).
Regenerate with `cd ../../dbtoaster && make data SF=<sf> && make build && make experiment`.
