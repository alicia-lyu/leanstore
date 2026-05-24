-- ===========================================================================
-- TPC-H refresh_sales DBToaster baseline.
--
-- Maintains BOTH the Q3 and Q5 *pipeline views* (the LeanStore S2
-- intermediates: unaggregated, unfiltered multi-way joins, one row per
-- lineitem) incrementally under a TPC-H RF1 (insert) / RF2 (delete) stream.
-- Quantifies generic-IVM maintenance cost vs LeanStore's hand-rolled S2 view.
--
-- Streams vs tables (per the experiment constraint "all tables may change
-- except nation and region"):
--   nation, region            -> static  CREATE TABLE
--   customer, orders, lineitem -> dynamic CREATE STREAM (delta-maintained)
--   part / supplier / partsupp -> absent from both pipeline views (out of scope)
--
-- Column lists match dbgen .tbl POSITIONAL order (canonical TPC-H spec order),
-- NOT the LeanStore struct order. Numerics use DECIMAL (DBToaster maps to
-- double — no fixed-point; a documented caveat, fine for throughput/memory).
--
-- No "SELECT * FROM R" passthroughs: they would force trivial passthrough maps
-- to be maintained per event, inflating per-event cost/memory and
-- contaminating the measurement. Only the two join-view maps are maintained.
--
-- Mirror sources in this repo:
--   frontend/tpch/q3/views.hpp:39-63   (q3_pipeline_view_t)
--   frontend/tpch/q5/views.hpp:160-191 (q5_pipeline_view_t)
--   frontend/tpch/tpch_tables.hpp      (authoritative column types)
-- ===========================================================================

-- dbgen emits pipe-delimited .tbl with a trailing '|'; the CSV adaptor's
-- delimiter is set via `delimiter := '|'` (NOT `fields`, which it ignores —
-- the default ',' would mis-parse every row). The trailing '|' is harmless:
-- the parser stops once the declared schema is consumed. NATION/REGION/CUSTOMER
-- have no refresh set, so they read the base .tbl directly; ORDERS/LINEITEM read
-- the base+RF1 concatenated stream files (see ../Makefile `data`).
CREATE TABLE NATION (
    N_NATIONKEY INT, N_NAME VARCHAR(25), N_REGIONKEY INT, N_COMMENT VARCHAR(152)
) FROM FILE './data_files/nation.tbl' LINE DELIMITED CSV (delimiter := '|');

-- REGION: static per the constraint, but referenced by neither pipeline view
-- (region is query-time in LeanStore Q5). Declared for fidelity; loaded once
-- at init, zero maintenance.
CREATE TABLE REGION (
    R_REGIONKEY INT, R_NAME VARCHAR(25), R_COMMENT VARCHAR(152)
) FROM FILE './data_files/region.tbl' LINE DELIMITED CSV (delimiter := '|');

CREATE STREAM CUSTOMER (
    C_CUSTKEY INT, C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY INT,
    C_PHONE VARCHAR(15), C_ACCTBAL DECIMAL, C_MKTSEGMENT VARCHAR(10), C_COMMENT VARCHAR(117)
) FROM FILE './data_files/customer.tbl' LINE DELIMITED CSV (delimiter := '|');

CREATE STREAM ORDERS (
    O_ORDERKEY INT, O_CUSTKEY INT, O_ORDERSTATUS VARCHAR(1), O_TOTALPRICE DECIMAL,
    O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR(15), O_CLERK VARCHAR(15),
    O_SHIPPRIORITY INT, O_COMMENT VARCHAR(79)
) FROM FILE './data_files/ORDERS.csv' LINE DELIMITED CSV (delimiter := '|');

CREATE STREAM LINEITEM (
    L_ORDERKEY INT, L_PARTKEY INT, L_SUPPKEY INT, L_LINENUMBER INT, L_QUANTITY DECIMAL,
    L_EXTENDEDPRICE DECIMAL, L_DISCOUNT DECIMAL, L_TAX DECIMAL, L_RETURNFLAG VARCHAR(1),
    L_LINESTATUS VARCHAR(1), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE,
    L_SHIPINSTRUCT VARCHAR(25), L_SHIPMODE VARCHAR(10), L_COMMENT VARCHAR(44)
) FROM FILE './data_files/LINEITEM.csv' LINE DELIMITED CSV (delimiter := '|');

-- QUERY_1: Q3 pipeline view — CUSTOMER x ORDERS x LINEITEM, one row per
-- lineitem (mirrors q3_pipeline_view_t). Unaggregated, unfiltered
-- (predicate-hoisted: mktsegment/orderdate/shipdate are query-time params in
-- LeanStore and are NOT baked here, keeping the view param-reusable).
SELECT C_CUSTKEY, O_ORDERKEY, L_LINENUMBER, L_EXTENDEDPRICE, L_DISCOUNT,
       L_SHIPDATE, C_MKTSEGMENT, O_ORDERDATE, O_SHIPPRIORITY
FROM   CUSTOMER, ORDERS, LINEITEM
WHERE  C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY;

-- QUERY_2: Q5 pipeline view — CUSTOMER x ORDERS x LINEITEM x NATION, one row
-- per lineitem (mirrors q5_pipeline_view_t). NATION is static; n_name is
-- FD-attached via c_nationkey = n_nationkey. No SUPPLIER / REGION (those are
-- query-time in LeanStore S2, not part of the pipeline view).
SELECT C_CUSTKEY, O_ORDERKEY, L_LINENUMBER, L_EXTENDEDPRICE, L_DISCOUNT,
       L_SUPPKEY, C_NATIONKEY, N_NAME, O_ORDERDATE
FROM   CUSTOMER, ORDERS, LINEITEM, NATION
WHERE  C_CUSTKEY = O_CUSTKEY AND O_ORDERKEY = L_ORDERKEY AND C_NATIONKEY = N_NATIONKEY;
