#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q9 workload.
//
// Shared scaffolding (gflags + structure dispatch) lives in
// `frontend/tpch/tpch_flags.hpp` and `frontend/tpch/tpch_executable.hpp`.

#include <gflags/gflags.h>

#include "../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../backend.hpp"
#include "../tpch_tables.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_executable.hpp"

#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q9 workload — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;
   B::Adapter<invoice_t>   invoice;

   B::Adapter<tpch::q9::q9_pipeline_view_t> pipeline_view;
   B::MergedAdapter<orders_t, lineitem_t>   merged_ol;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part          = B::Adapter<part_t>(db, "part");
      supplier      = B::Adapter<supplier_t>(db, "supplier");
      partsupp      = B::Adapter<partsupp_t>(db, "partsupp");
      customer      = B::Adapter<customerh_t>(db, "customer");
      orders        = B::Adapter<orders_t>(db, "orders");
      lineitem      = B::Adapter<lineitem_t>(db, "lineitem");
      nation        = B::Adapter<nation_t>(db, "nation");
      region        = B::Adapter<region_t>(db, "region");
      invoice       = B::Adapter<invoice_t>(db, "invoice");
      pipeline_view = B::Adapter<tpch::q9::q9_pipeline_view_t>(db, "q9_pipeline_view");
      merged_ol     = B::MergedAdapter<orders_t, lineitem_t>(db, "q9_merged_ol");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);
   tpch::q9::Q9Workload<B> q9(tpch, orders, lineitem, nation, supplier,
                                part, partsupp, pipeline_view, merged_ol);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q9.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   std::vector<tpch::q9::q9_agg_row_t> result;
   return tpch::dispatch_storage_structure<
       tpch::q9::BaseQ9, tpch::q9::ViewQ9,
       tpch::q9::MergedQ9, tpch::q9::HashQ9, B>(q9, result);
}

#endif  // ROCKSDB_ONLY
