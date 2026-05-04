// RocksDB entry point for Q3 workload.
//
// Shared scaffolding (gflags + structure dispatch) lives in
// `frontend/tpch/tpch_flags.hpp` and `frontend/tpch/tpch_executable.hpp`.

#include <gflags/gflags.h>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
#include "../backend.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable.hpp"

#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // Base TPC-H tables (vanilla schema — Q3 uses lineitem_t, not lineitem_i_t)
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // Q3-specific adapters
   B::Adapter<tpch::q3::q3_pipeline_view_t> pipeline_view(rocks_db);

   // COL 3-table merged index (S3) + custkey-sorted split indexes (S1)
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem);

   if (!FLAGS_recover) {
      q3.load();
      return 0;
   }
   tpch.recover_last_ids();

   std::vector<tpch::q3::q3_agg_row_t> result;
   return tpch::dispatch_storage_structure<
       tpch::q3::BaseQ3, tpch::q3::ViewQ3,
       tpch::q3::MergedQ3, tpch::q3::HashQ3, B>(q3, result);
}
