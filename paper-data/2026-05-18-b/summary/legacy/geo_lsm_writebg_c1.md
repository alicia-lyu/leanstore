# Legacy write-bg headline — geo_lsm

- Cell: **c1** (DRAM=0.4 GiB, SF=206)
- BG semantics: **write-only** (insert/erase TXs on customer2; pre-redesign)
- bg=2 (single bg thread; effectively bg=1 since flag was bool)
- Reps captured: 3 (c1-bg2-r1, c1-bg2-r2, c1-bg2-r3)
- Geo source SHA (most recent touching frontend/geo): `6c771892`

## ms/query — median (IQR) across reps

| tx | S1 | S2 | S3 | S4 |
|----|---|---|---|---|
| `join-ns` | 107.64 (88.18–138.12) | 56.63 (51.30–63.19) | 87.57 (75.02–105.15) | 107.99 (97.47–121.07) |
| `join-nsc` | 49.78 (39.09–68.49) | 8.48 (7.14–10.46) | 55.16 (47.84–65.13) | 57.87 (43.56–86.17) |
| `mixed-ns` | 151.75 (136.15–171.38) | 37.26 (34.80–40.09) | 143.68 (103.41–235.29) | 170.07 (129.28–248.45) |
| `mixed-nsc` | 74.29 (45.22–208.12) | 8.88 (7.59–10.71) | 65.70 (37.96–244.20) | 79.55 (58.45–124.53) |
| `distinct-ns` | 228.83 (158.10–414.08) | 140.85 (123.15–164.47) | 363.64 (227.79–900.90) | 210.97 (166.25–288.60) |
| `distinct-nsc` | 145.14 (106.55–227.53) | 9.64 (8.04–12.04) | 76.98 (58.17–113.77) | 112.99 (86.81–161.81) |
| `maintain` | 32.26 (17.70–181.82) | 3.34 (3.15–3.56) | 10.75 (10.15–11.43) | 20.00 (13.07–42.55) |

## Interpretation (write-bg era, c1 only)

- S2 (mat_view) wins across most tx because the view is small enough to cache in DRAM despite SF=206.
- S3 (merged_idx) is competitive but pays cartesian-product buffering inside PremergedJoin.

This table is frozen for reference. The dirs `c1-bg2-r*` under this binary's root were moved to `legacy-writebg/` because the bg-thread semantics changed (write→read) and re-running under new semantics will produce non-comparable numbers in the canonical location.
