// RocksDB entry point for Q9 workload.
//
// NATION, SUPPLIER, PART, PARTSUPP are base-table adapters used at query
// time (loaded into hashmaps or merge-joined); they are not part of any
// merged index.
//
// Shared scaffolding (gflags + structure dispatch) lives in
// `frontend/tpch/tpch_flags.hpp` and `frontend/tpch/tpch_executable.hpp`.

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
   gflags::SetUsageMessage("Q9 workload — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   B::Adapter<tpch::q9::q9_pipeline_view_t> pipeline_view(rocks_db);
   B::MergedAdapter<orders_t, lineitem_t>   merged_ol(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q9::Q9Workload<B> q9(tpch, orders, lineitem, nation, supplier,
                                part, partsupp, pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      q9.load();
      return 0;
   }
   tpch.recover_last_ids();

   std::vector<tpch::q9::q9_agg_row_t> result;
   return tpch::dispatch_storage_structure<
       tpch::q9::BaseQ9, tpch::q9::ViewQ9,
       tpch::q9::MergedQ9, tpch::q9::HashQ9, B>(q9, result);
}
