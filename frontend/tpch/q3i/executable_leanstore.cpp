#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3I workload.
//
// Same shape as executable_rocksdb.cpp but uses LeanStoreBackend. Shared
// scaffolding (gflags + dispatch) lives in tpch_flags.hpp /
// tpch_executable_helper.hpp.
//
// Phase 1 requires only --storage_structure=3 to fully work.

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
#include "../tpch_executable_helper.hpp"

#include "../coli_pipeline.hpp"
#include "per_structure_workload.hpp"
#include "workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I workload — LeanStore backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   leanstore::LeanStore db;
   using B = tpch::LeanStoreBackend;

   // Base TPC-H tables (default-constructed; assigned inside crm callback).
   B::Adapter<part_t>      part;
   B::Adapter<supplier_t>  supplier;
   B::Adapter<partsupp_t>  partsupp;
   B::Adapter<customerh_t> customer;
   B::Adapter<orders_t>    orders;
   B::Adapter<lineitem_t>  lineitem;
   B::Adapter<nation_t>    nation;
   B::Adapter<region_t>    region;
   B::Adapter<invoice_t>   invoice;

   // Q3I-specific adapters
   B::Adapter<tpch::q3i::q3i_pipeline_view_t> pipeline_view;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;

   // S1 custkey-sorted secondary indexes.
   B::Adapter<tpch::orders_coli_t>   orders_secondary;
   B::Adapter<tpch::lineitem_coli_t> lineitem_secondary;
   B::Adapter<tpch::invoice_coli_t>  invoice_secondary;

   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      part              = B::Adapter<part_t>(db, "part");
      supplier          = B::Adapter<supplier_t>(db, "supplier");
      partsupp          = B::Adapter<partsupp_t>(db, "partsupp");
      customer          = B::Adapter<customerh_t>(db, "customer");
      orders            = B::Adapter<orders_t>(db, "orders");
      lineitem          = B::Adapter<lineitem_t>(db, "lineitem");
      nation            = B::Adapter<nation_t>(db, "nation");
      region            = B::Adapter<region_t>(db, "region");
      invoice           = B::Adapter<invoice_t>(db, "invoice");
      pipeline_view     = B::Adapter<tpch::q3i::q3i_pipeline_view_t>(db, "q3i_pipeline_view");
      merged_coli       = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                           tpch::lineitem_coli_t, tpch::invoice_coli_t>(db, "q3i_merged_coli");
      orders_secondary   = B::Adapter<tpch::orders_coli_t>(db, "q3i_orders_sec");
      lineitem_secondary = B::Adapter<tpch::lineitem_coli_t>(db, "q3i_lineitem_sec");
      invoice_secondary  = B::Adapter<tpch::invoice_coli_t>(db, "q3i_invoice_sec");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);
   tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                   pipeline_view, merged_coli,
                                   orders_secondary, lineitem_secondary, invoice_secondary);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q3i.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   using AggRow = tpch::q3i::q3i_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q3i::BaseQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "base_merge_join");
         helper.run();
         break;
      }
      case 2: {
         tpch::q3i::ViewQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "pipeline_view");
         helper.run();
         break;
      }
      case 3: {
         tpch::q3i::MergedQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "mi_coli_walk");
         helper.run();
         break;
      }
      case 4: {
         tpch::q3i::HashQ3I<B> w{q3i};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "base_hash_join");
         helper.run();
         break;
      }
      default:
         std::cerr << "Invalid storage_structure: " << FLAGS_storage_structure << std::endl;
         return 1;
   }
   return 0;
}

#endif  // ROCKSDB_ONLY
