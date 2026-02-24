# Experiment Workload Specification

## Schema and Data Generation

The experimental workload utilizes a synthetic dataset. The schema consists of five relations: `Nation`, `States`, `County`, `City`, and `Customer`.

The geographical hierarchy has a repeated pattern: each child table extends the composite primary key with its local key and declares a foreign key to its parent's composite key. To avoid redundancy, the following snippet shows `Nation`, `States`, and `Customer` in full. `County` and `City` follow the same pattern as `States`, with the obvious key extensions.

```sql
-- Schema Definitions
CREATE TABLE Nation (
    nationkey INT PRIMARY KEY,
    n_name STRING,
    n_comment STRING,
    last_statekey INT
);

CREATE TABLE States (
    nationkey INT,
    statekey INT,
    s_name STRING,
    s_comment STRING,
    last_countykey INT,
    PRIMARY KEY (nationkey, statekey),
    FOREIGN KEY (nationkey) REFERENCES Nation(nationkey)
);

-- County and City follow the same pattern

CREATE TABLE Customer (
    custkey INT PRIMARY KEY,
    nationkey INT,
    statekey INT,
    countykey INT,
    citykey INT,
    c_name STRING,
    c_address STRING,
    c_mktsegment STRING,
    FOREIGN KEY (nationkey, statekey, countykey, citykey)
        REFERENCES City(nationkey, statekey, countykey, citykey)
);
```

Data generation is performed with custom code. It populates these tables with random numbers or characters and maintains referential integrity. For each parent relation, a randomized number of child relations are generated based on configured cardinality parameters.

To simulate realistic data skew, the generator marks specific cities as "hot" candidates with a 1% probability, assigning a disproportionately higher number of customers to these locations.

## Query and View Definitions

The workload evaluates three categories of queries: multi-table joins, multi-table count aggregations, and multi-table distinct count aggregations. All queries share a common join path connecting all five tables from `Nation` to `Customer`.

### Multi-table Join Queries (Q1 & Q2)

These queries reconstruct the full hierarchy for a selected range of data. Q1 represents a medium-range scan (filtering by State), while Q2 represents a short-range scan (filtering by County).

```sql
-- Join Queries (Q1 & Q2)
SELECT *
FROM Nation
JOIN States   ON Nation.nationkey  = States.nationkey
JOIN County   ON States.nationkey  = County.nationkey
             AND States.statekey   = County.statekey
JOIN City     ON County.nationkey  = City.nationkey
             AND County.statekey   = City.statekey
             AND County.countykey  = City.countykey
JOIN Customer ON City.nationkey    = Customer.nationkey
             AND City.statekey     = Customer.statekey
             AND City.countykey    = Customer.countykey
             AND City.citykey      = Customer.citykey
WHERE Nation.nationkey = ? AND States.statekey = ?;
-- Q2 adds: AND County.countykey = ?
```

The materialized view for these queries pre-joins all five tables and is indexed by `(nationkey, statekey, countykey, citykey)` to support efficient retrieval.

### Aggregation Queries (Q3–Q6)

These queries perform grouping on the geographical hierarchy keys. Q3 and Q4 calculate the total number of customers per city, while Q5 and Q6 calculate the number of distinct market segments.

```sql
-- Aggregation Queries (Q3–Q6)
SELECT Nation.nationkey, Nation.n_name,
       States.statekey, States.s_name,
       County.countykey, County.c_name,
       City.citykey, City.ci_name,
       COUNT(*) -- Q5/Q6: change to COUNT(DISTINCT Customer.c_mktsegment)
FROM Nation
JOIN States   ON Nation.nationkey  = States.nationkey
JOIN County   ON States.nationkey  = County.nationkey
             AND States.statekey   = County.statekey
JOIN City     ON County.nationkey  = City.nationkey
             AND County.statekey   = City.statekey
             AND County.countykey  = City.countykey
JOIN Customer ON City.nationkey    = Customer.nationkey
             AND City.statekey     = Customer.statekey
             AND City.countykey    = Customer.countykey
             AND City.citykey      = Customer.citykey
WHERE Nation.nationkey = ? AND States.statekey = ?
-- Q4/Q6 adds: AND County.countykey = ?
GROUP BY Nation.nationkey, Nation.n_name,
         States.statekey, States.s_name,
         County.countykey, County.c_name,
         City.citykey, City.ci_name;
```

We use two materialized views for Q3 and Q4: a join view and an aggregate view. The join view pre-joins all tables except `Customer`. The aggregate view pre-groups the `Customer` table by the geographical keys and calculates the customer count. At query time, a join between the two views produces the final result. The reasoning for using two views is to have the aggregate view over the `Customer` table maintained at a lower cost [gupta1999selection]. Specifically, each update to the `Customer` table only requires an update to the aggregate view, without the need to re-join with the other tables. This is a widely employed strategy, especially for star schema (see "aggregation navigation" [kimball2013data]).

For Q5 and Q6, the materialized view pre-joins all tables except `Customer`. A secondary index is created on the `Customer` table by the geographical keys. At query time, the `Customer` table is first grouped by the geographical keys for calculation of distinct market segment count. This intermediate result is then joined with the pre-joined view to produce the final result. The reason why `COUNT DISTINCT` is not included in the materialized view is that it is not incrementally maintainable and thus would require a full re-computation of the view for each update. This is a standard practice; for example, SQL Server forbids the use of `DISTINCT` in indexed views [indexedviews].

# Additional Experiment Results

## Difference between B-trees and LSM-trees

The figure below shows the CPU utilization percentage across queries and indexing methods, with B-tree storage and LSM-tree storage.

![CPU utilization with B-tree storage: all indexing methods use roughly 30 to 45 percent CPU across six queries.](figures/btree_cpu_utilization.png)
*With B-tree storage. Different indexing methods consistently utilize 30–45% CPU across 6 queries.*

![CPU utilization with LSM-tree storage: range queries use roughly 60 to 80 percent CPU, point queries use 5 to 20 percent CPU.](figures/lsm_cpu_utilization.png)
*With LSM-tree storage. Q1, Q3, and Q5 (range queries) utilize 60–80% CPU, while Q2, Q4, and Q6 (point queries) utilize 5–20% CPU.*

*CPU utilization percentage across queries and indexing methods. While B-tree utilization remains relatively stable and moderate across workloads, LSM-tree storage utilizes CPU heavily for range queries and lightly for point queries.*

We suspect that the reason for LSM-tree's bi-modal CPU utilization (as pointed out in Section "lsm_analysis") is the following. On one hand, the LSM-tree's use of SSTables, which implement compaction and compression, accelerates sequential scans of medium ranges compared with B-trees. On the other hand, point retrievals in an LSM-tree require multiple lookups across the levels (i.e., read amplification), thus it is more IO-intensive than a B-tree.

Their most notable differences in queries and updates between B-trees and LSM-trees are:

- Range queries: Higher CPU utilization of LSM-tree for range queries penalizes disproportionately hash-based query-time execution (`trad_idx_hj`). Meanwhile, it benefits materialized views in a more subtle way—fast range scan offered by LSM-trees alleviates materialized views' disadvantage in scan volume, allowing them to even surpass merged indexes in Q1. In this specific case, the merged index may be occasionally CPU-bound due to record assembly (with 76.4% average CPU utilization), while the materialized view remains IO-bound (with 25.1% average CPU utilization).
- Updates: LSM-trees significantly improve update performance for all methods, but they also pose a penalty for large updates due to compaction and write amplification, which disproportionately affects materialized views.

-------

Original README of LeanStore:

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling

Install dependencies:

`sudo apt-get install cmake libtbb2-dev libaio-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev liblmdb-dev libwiredtiger-dev liburing-dev`

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## Benchmark Examples
### TPC-C

`build/frontend/tpcc --tpcc_warehouse_count=100 --notpcc_warehouse_affinity --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=240 --csv_path=./log --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`

check `build/frontend/tpcc --help` for other options

### YCSB
`build/frontend/tpcc --ycsb_read_ratio=50 --target_gib=10 --ssd_path=./ssd_block_device_or_file --worker_threads=120 --pp_threads=4 --dram_gib=5 --csv_path=./log --free_pct=1 --contention_split --xmerge --print_tx_console --run_for_seconds=60 --isolation_level=si`
check `build/frontend/ycsb --help` for other options

## Implement Your Own Workload

LeanStore offers a flexible transactional Key/Value interface similar to WiredTiger and RocksDB.
A table is a B-Tree index where keys and values are stored in a normalized format, i.e., lexicographically ordered strings.
For convenience, frontend/shared offers templates that take care of (un)folding common types.
The best starting points are frontend/minimal-example and frontend/ycsb.
The required parameters at runtime are: `--ssd_path=/block_device/or/filesystem --dram_gib=fixed_in_gib`.
The default tranasction isolation level is `--isolation_level=si`. You can lower it to Read Committed or Read Uncommitted by replaced `si` with `rc` or `ru` respectively.
You can set the transaction isolation level using `--isolation_level=si` and enable the B-Tree techniques from CIDR202 with `--contention_split --xmerge`.

### Metrics Reporting
LeanStore emits several metrics per second in CSV files: `log_bm.csv, log_configs.csv, log_cpu.csv, log_cr.csv, log_dt.csv`.
Each row has a c_hash value, which is calculated by chaining and hashing all the configurations that you passed to the binary at runtime.
This gives you an easy way to identify your run and join all relevant information from the different CSV files using SQLite, for example."

## Implemented Featuers

- [x] Lightweight buffer manager with pointer swizzling [ICDE18]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE19]
- [x] Contention and Space Management in B-Trees [CIDR21]
- [x] Variable-length key/values B-Tree with prefix compression and hints  [BTW23]
- [x] Scalable and robust out-of-memory Snapshot Isolation (OSIC protocol, Graveyard and FatTuple) [VLDB23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD20, BTW23]
- [ ] Recovery [SIGMOD20]
- [ ] What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance {I/O} for High-Performance Storage Engines [VLDB23] [branch](https://github.com/leanstore/leanstore/tree/io)

## Cite

The code we used for our VLDB 2023 that covers alternative SI commit protocols is in a different [branch](https://github.com/leanstore/leanstore/tree/mvcc).

```BibTeX
@inproceedings{alhomssi23,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
    booktitle = {VLDB},
    year      = {2023}
}
```

The code we used for our VLDB 2023 that covers the fast I/O implementation is in a different [branch](https://github.com/leanstore/leanstore/tree/io).

```BibTeX
@article{haas23,
  author       = {Gabriel Haas and Viktor Leis},
  title        = {What Modern NVMe Storage Can Do, And How To Exploit It: High-Performance {I/O} for High-Performance Storage Engines},
  journal      = {Proc. {VLDB} Endow.},
  year         = {2023}
}
```

BTW 2023 [branch](https://github.com/leanstore/leanstore/tree/btw) that covers alternative dependency tracking.

```BibTeX
@inproceedings{leanstore23,
    author    = {Adnan Alhomssi, Michael Haubenschild and Viktor Leis},
    title     = {The Evolution of LeanStore},
    booktitle = {BTW},
    year      = {2023}
}
```

CIDR 2021 [branch](https://github.com/leanstore/leanstore/tree/cidr) (outdated).

```BibTeX
@inproceedings{alhomssi21,
    author    = {Adnan Alhomssi and Viktor Leis},
    title     = {Contention and Space Management in B-Trees},
    booktitle = {CIDR},
    year      = {2021}
}
```
