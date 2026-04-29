// RocksDB entry point for Q12 workload.
//
// Wires gflags, opens a RocksDB TransactionDB, declares the adapters Q12
// needs, constructs the TPCHWorkload and Q12Workload, then dispatches to
// the chosen per-structure wrapper via tpch::dispatch_storage_structure.
//
// Shared scaffolding lives in `frontend/tpch/tpch_flags.hpp` (gflags) and
// `frontend/tpch/tpch_executable.hpp` (dispatch helper).

#include <gflags/gflags.h>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
#include "../backend.hpp"
#include "../tpch_tables.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable.hpp"

#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q12 workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // Base TPC-H tables
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   // Q12-specific adapters
   B::Adapter<tpch::q12::q12_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<orders_t, lineitem_t>     merged_ol(rocks_db);

   rocks_db.open();  // must be called after all adapters register their CFs

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q12::Q12Workload<B> q12(tpch, orders, lineitem, pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      q12.load();
      return 0;
   }
   tpch.recover_last_ids();

   std::vector<tpch::q12::q12_agg_row_t> result;
   return tpch::dispatch_storage_structure<
       tpch::q12::BaseQ12, tpch::q12::ViewQ12,
       tpch::q12::MergedQ12, tpch::q12::HashQ12, B>(q12, result);
}
