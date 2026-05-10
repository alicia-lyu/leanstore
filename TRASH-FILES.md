calcite-integration-info/test-plans - moved to TRASH/ - replaced with symlink to calcite test-dot-output
frontend/tpch/q12/views.hpp - moved to TRASH/q12_views.hpp - replaced by new skeleton (cargo-cult geo design, template-param naming bug)
frontend/tpch/q12/workload.hpp - moved to TRASH/q12_workload.hpp - replaced by new skeleton (four-template-param explosion, now uses Backend traits)
frontend/tpch/q12/per_structure_workload.hpp - moved to TRASH/q12_per_structure_workload.hpp - replaced by new skeleton (virtual dispatch removed, plain structs)
frontend/tpch/q12/load.tpp - moved to TRASH/q12_load.tpp - replaced by new skeleton (template-param bug)
build/vendor/tabluate - moved to TRASH/tabluate-stale-build-cache - stale ExternalProject cache pointing to build2 dir
build/log - moved to TRASH/build-log-stale-file - regular file blocking create_directories(./log)
build/log - moved to TRASH/build-log-stale-file-2 - second stale RocksDB log blocking Logger
test_data{2,4,5,6,7} - moved to TRASH/ - stale RocksDB test instance dirs from earlier load-test runs
test_csv{2,4,5,6,7} - moved to TRASH/ - stale CSV log dirs paired with the above
test_q12_data - moved to TRASH/ - stale test data from prior Q12 run
test_q12_csv - moved to TRASH/ - stale test CSV from prior Q12 run
test_q12.json - moved to TRASH/ - stale persist file from prior Q12 run
test_data_coli_run1 - moved to TRASH/ - temporary test RocksDB data from first COLI load test run
test_csv_coli_run1 - moved to TRASH/ - temporary test CSV/log dir from first COLI load test run
test_data_coli_run2 - moved to TRASH/ - second COLI load test run data
test_csv_coli_run2 - moved to TRASH/ - second COLI load test run CSV
test_data_coli_run3 - moved to TRASH/ - third COLI load test run data
test_csv_coli_run3 - moved to TRASH/ - third COLI load test run CSV
test_data_coli_run4 - moved to TRASH/ - diagnostic run 4
test_csv_coli_run4 - moved to TRASH/ - diagnostic run 4 CSV
test_data_coli_run5 - moved to TRASH/ - diagnostic run 5
test_csv_coli_run5 - moved to TRASH/ - diagnostic run 5 CSV
test_data_coli_run6 - moved to TRASH/ - diagnostic run 6
test_csv_coli_run6 - moved to TRASH/ - diagnostic run 6 CSV
test_coli_data - moved to TRASH/test_coli_data_<ts> - stale RocksDB test instance from prior coli load-test run
test_{coli,q12}_{data,csv}, test_{data,csv}{,2,3,_card,_coli,_diag1,_diag2,_final}, test_{data,csv}_run{1..10}, test_q12.json - moved to TRASH/test_artifacts_$(date)/ - bulk cleanup of accumulated load/query test runtime artifacts from project root
frontend/tpch/q9i/ - moved to TRASH/q9i_skeleton/ - per INVOICE_EXTENSION_CANDIDATES.md, Q9 has no Customer; contrived extension. Rejection rationale is in the candidate doc itself.
frontend/tpch/q12i/ - moved to TRASH/q12i_skeleton/ - per INVOICE_EXTENSION_CANDIDATES.md, Q12 has no Customer; contrived extension. Rejection rationale is in the candidate doc itself.
test_data_coli2 - moved to TRASH/ - COLI load-test run verifying 2× invoice cardinality and tag reorder (Phase 1 prerequisites)
test_csv_coli2 - moved to TRASH/ - CSV/log dir paired with test_data_coli2
test_data_q12_verify - moved to TRASH/ - Q12 parity regression test run after COLI tag reorder
test_csv_q12_verify - moved to TRASH/ - CSV/log dir paired with test_data_q12_verify
test_data_q3i - moved to TRASH/ - q3i phase 1 test harness rocksdb data
test_csv_q3i - moved to TRASH/ - q3i phase 1 test harness logs
test_data_q3i - moved to TRASH/ - q3i phase 1 harness rocksdb data (multiple iterations during debug)
test_csv_q3i - moved to TRASH/ - q3i phase 1 harness logs (multiple iterations during debug)
test_data_q12 - moved to TRASH/ - q12 verification rocksdb data
test_csv_q12 - moved to TRASH/ - q12 verification logs
test_data_q3i - moved to TRASH/test_data_q3i_1777692050 - stale q3i RocksDB data from pre-fix run
test_csv_q3i - moved to TRASH/test_csv_q3i_1777692050 - stale q3i CSV/log dir from pre-fix run
test_data_q3i - moved to TRASH/test_data_q3i_prestep4 - stale q3i data loaded before Phase 2A Step 4 (threshold_ok + apply_topN)
test_csv_q3i - moved to TRASH/test_csv_q3i_prestep4 - stale q3i CSV/log paired with above
test_query_q3i_phase1_rocksdb.cpp - moved to TRASH/ - temporary Phase 1 harness retired after Phase 2C parity gate passed; replaced by test_query_q3i_lsm
test_data_q3i - moved to TRASH/test_data_q3i_stale - stale Q3I RocksDB data before S3 bug investigation
test_csv_q3i - moved to TRASH/test_csv_q3i_stale - stale Q3I CSV/log dir before S3 bug investigation
.git/index.lock - moved to TRASH/git-index.lock - stale git lock blocking commit
.git/index.lock - moved to TRASH/git-index.lock-2 - stale git lock blocking commit (second occurrence)

/tmp/leanstore/q3i_lsm - moved to TRASH/q3i_lsm-pre-a1-* - prior run data, wiping for A1 baseline
build/q3i_lsm - moved to TRASH/build-q3i_lsm-pre-a1-* - prior structure logs, wiping for A1 baseline
build/q3i_btree_iso{1..5} - moved to TRASH/ - replaced by build/q3i_btree_iso/ unified tree (storages nested as iso_N/ on the data_disk side)
build/q3i_lsm_iso3 - moved to TRASH/ - replaced by build/q3i_lsm_iso/ unified tree
/mnt/ssd/q3i_{btree,lsm}_iso{1..5} - left in place (out-of-project; permission-blocked) - stale per-N image dirs replaced by /mnt/ssd/q3i_{btree,lsm}_iso/iso_N/; safe to delete manually
test_data_*/ test_csv_*/ (60 dirs total, project root) - moved to TRASH/scratch-2026-05-04/ - leftover scratch from per-tag test/load runs (`_a1`/`_a2c`/`_p1b`/`_v8`/`_baseline`/`_prod`/`_s1..s4`/etc.); convention now puts scratch under build/scratch/<tag>/{data,csv} (gitignored via build*/) so it never lands in the project root again
build/scratch/q5_parity/ - moved to TRASH/ - stale RocksDB scratch from prior parity test run; rebuilding fresh for wildcard-key cleanup verification
