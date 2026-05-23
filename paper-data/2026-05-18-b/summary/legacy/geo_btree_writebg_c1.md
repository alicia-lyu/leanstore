# Legacy write-bg headline — geo_btree

- Cell: **c1** (DRAM=0.4 GiB, SF=78)
- BG semantics: **write-only** (insert/erase TXs on customer2; pre-redesign)
- bg=2 (single bg thread; effectively bg=1 since flag was bool)
- Reps captured: 2 (c1-bg2-r1, c1-bg2-r2)
- Geo source SHA (most recent touching frontend/geo): `6c771892`

## ms/query — median (IQR) across reps

| tx | S1 | S2 | S3 | S4 |
|----|---|---|---|---|
| `join-ns` | 1031.99 (992.36–1074.92) | 4156.28 (3703.70–4734.85) | 1261.59 (1112.44–1456.93) | 1082.95 (1044.28–1124.61) |
| `join-nsc` | 30.30 (30.25–30.36) | 48.36 (42.34–56.37) | 17.40 (17.33–17.47) | 27.99 (27.41–28.59) |
| `mixed-ns` | 1062.36 (975.04–1166.86) | 1333.87 (1100.29–1693.34) | 1086.01 (909.42–1347.71) | 993.74 (839.03–1218.40) |
| `mixed-nsc` | 31.21 (30.85–31.57) | 36.21 (35.13–37.36) | 17.89 | 29.41 (28.56–30.32) |
| `distinct-ns` | 1341.02 (1278.04–1410.54) | 3376.10 (3291.10–3465.60) | 1185.96 | 1146.79 (952.74–1440.09) |
| `distinct-nsc` | 32.59 (29.52–36.38) | 51.55 (48.76–54.67) | 17.42 | 30.33 (30.05–30.61) |
| `maintain` | 3.78 (3.74–3.82) | 19.08 (18.91–19.26) | 500.00 | 3.73 (3.66–3.80) |

## Interpretation (write-bg era, c1 only)

- S2 (mat_view) *loses* on geo_btree at this DRAM/scale — view scan is expensive when the working set doesn't cache well, and base-idx merge join wins.
- S3 is competitive with S1 on join-* but slower on distinct-*.

This table is frozen for reference. The dirs `c1-bg2-r*` under this binary's root were moved to `legacy-writebg/` because the bg-thread semantics changed (write→read) and re-running under new semantics will produce non-comparable numbers in the canonical location.
