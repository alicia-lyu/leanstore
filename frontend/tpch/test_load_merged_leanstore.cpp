#ifndef ROCKSDB_ONLY
// Standalone load-test binary: drives OrdersLineitemPipeline<LeanStoreBackend>
// to verify MI[0] loading end-to-end without depending on per-query state.

#include <gflags/gflags.h>
#include <iostream>

#include "../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "backend.hpp"
#include "ol_pipeline.hpp"
#include "test_load_merged_stats.hpp"
#include "tpch_tables.hpp"
#include "tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("OrdersLineitemPipeline load-test — LeanStore backend");
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

   B::MergedAdapter<orders_t, lineitem_t> merged_ol;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part      = B::Adapter<part_t>(db, "part");
      supplier  = B::Adapter<supplier_t>(db, "supplier");
      partsupp  = B::Adapter<partsupp_t>(db, "partsupp");
      customer  = B::Adapter<customerh_t>(db, "customer");
      orders    = B::Adapter<orders_t>(db, "orders");
      lineitem  = B::Adapter<lineitem_t>(db, "lineitem");
      nation    = B::Adapter<nation_t>(db, "nation");
      region    = B::Adapter<region_t>(db, "region");
      merged_ol = B::MergedAdapter<orders_t, lineitem_t>(db, "test_merged_ol");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      leanstore::cr::Worker::my().commitTX();
   });

   tpch::OrdersLineitemPipeline<B> ol(orders, lineitem, merged_ol);
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      ol.populate_merged();
      leanstore::cr::Worker::my().commitTX();
   });

   const Integer expected_orders =
       TPCHWorkload<B::Adapter>::ORDERS_SCALE * FLAGS_tpch_scale_factor;
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::OLAP);
      tpch::dump_merged_ol_stats<B>(merged_ol, expected_orders, tpch.last_order_id);
      leanstore::cr::Worker::my().commitTX();
   });
   return 0;
}

#endif  // ROCKSDB_ONLY
