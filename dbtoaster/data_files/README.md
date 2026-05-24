# data_files/ — TPC-H dbgen output for the DBToaster refresh_sales baseline

This directory holds the generated TPC-H data. Everything except this README
is git-ignored (regenerate with `dbgen`).

## Prerequisite

`dbgen` from the TPC-H tools kit must be on `PATH` (Phase 0; see
[`../../LINUX_SETUP.md`](../../LINUX_SETUP.md)). The `electrum/tpch-dbgen`
mirror works:

```bash
git clone https://github.com/electrum/tpch-dbgen.git
make -C tpch-dbgen CC=gcc DATABASE=DB2 MACHINE=LINUX WORKLOAD=TPCH
export PATH=$PWD/tpch-dbgen:$PATH         # dbgen also needs dists.dss in CWD or DSS_CONFIG
```

## Generate (`make data SF=<sf>` does all of this)

```bash
dbgen -s <SF> -f                # base: customer/orders/lineitem/nation/region.tbl (pipe-delimited)
dbgen -s <SF> -U 1 -f           # refresh: orders.tbl.u1 + lineitem.tbl.u1 (RF1 inserts), delete.1 (RF2 orderkeys)

# Concatenate base + RF1 into the ORDERS/LINEITEM stream files (CUSTOMER/NATION/
# REGION have no refresh and are read straight from .tbl):
cat orders.tbl   orders.tbl.u1   > ORDERS.csv
cat lineitem.tbl lineitem.tbl.u1 > LINEITEM.csv
```

The SQL uses `LINE DELIMITED CSV (delimiter := '|')`. dbgen's trailing `|` is
harmless — the adaptor stops once the declared schema is consumed — so no
stripping is needed. (Note: the adaptor ignores a `fields := ...` option and
silently defaults to `,`; `delimiter := '|'` is the correct keyword.)

## File → relation mapping

| file | relation | role |
|------|----------|------|
| `nation.tbl` | NATION (TABLE) | static, loaded at init |
| `region.tbl` | REGION (TABLE) | static, loaded at init |
| `customer.tbl` | CUSTOMER (STREAM) | base load (warmup) |
| `ORDERS.csv` (= orders.tbl + .u1) | ORDERS (STREAM) | base (warmup) + RF1 tail (measured) |
| `LINEITEM.csv` (= lineitem.tbl + .u1) | LINEITEM (STREAM) | base (warmup) + RF1 tail (measured) |
| `orders.tbl` / `lineitem.tbl` | — | kept: the harness reads their line count for the warmup boundary |
| `delete.1` | — (not in SQL) | RF2 delete orderkeys, fired by the harness |

## Scale factor

For the headline run, pin SF so the maintained working set ≈ 5 GB (the paper's
5 GiB secondary anchor); see [`../results/README.md`](../results/README.md).
DBToaster's in-memory maps are larger per row than LeanStore's packed view, so
the ~5 GB point lands at a smaller SF (expected ~0.5–1). Use a small SF
(0.01–1) for correctness checks first.

## Caveat

dbgen's `-U` orderkey allocation differs from LeanStore's `orderkey_from_index`
sparse grid, but both honor the disjoint-RF1/RF2-keyspace, size-stable-pair
invariant — so per-update **cost** is comparable even though exact keys differ.
dbgen emits a trailing `|` per line (trailing empty field); the DBToaster CSV
adaptor tolerates it. If codegen rejects the field count, add a trailing dummy
`VARCHAR` column per relation in `../refresh_sales.sql`.
