// Standalone load-test binary: drives OrdersLineitemPipeline<RocksDBBackend>
// to verify MI[0] loading end-to-end without depending on per-query state.

#include <gflags/gflags.h>
#include <iostream>

#include "../shared/RocksDB.hpp"
#include "../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../shared/logger/rocksdb_logger.hpp"
#include "backend.hpp"
#include "ol_pipeline.hpp"
#include "tpch_tables.hpp"
#include "tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("OrdersLineitemPipeline load-test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // All 8 TPC-H base tables (TPCHWorkload requires them all).
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // The merged structure under test.
   B::MergedAdapter<orders_t, lineitem_t> merged_ol(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch.load();  // base tables only

   tpch::OrdersLineitemPipeline<B> ol(orders, lineitem, merged_ol);
   ol.populate_merged();

   std::cout << "MI[0] size (MiB): " << ol.get_merged_size() << std::endl;
   return 0;
}
