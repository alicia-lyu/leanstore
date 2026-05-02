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
