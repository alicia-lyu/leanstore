#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q12 workload.
//
// Same shape as executable_rocksdb.cpp but uses LeanStoreBackend. Shared
// scaffolding (gflags + dispatch) lives in `frontend/tpch/tpch_flags.hpp`
// and `frontend/tpch/tpch_executable.hpp`.

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
   gflags::SetUsageMessage("Q12 workload — LeanStore backend");
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

   // Q12-specific adapters
   B::Adapter<tpch::q12::q12_pipeline_view_t> pipeline_view;
   B::MergedAdapter<orders_t, lineitem_t>     merged_ol;

   // COLI pipeline substrate (compiled here; used by Q3I/Q9I/Q12I extensions).
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli;

   // S1 custkey-sorted secondary indexes for the COLI pipeline.
   // Q12 does not use these at query time; declared so the COLI pipeline
   // compiles cleanly and is ready for Q3I S1 use.
   B::Adapter<tpch::orders_coli_t>   coli_orders_sec;
   B::Adapter<tpch::lineitem_coli_t> coli_lineitem_sec;
   B::Adapter<tpch::invoice_coli_t>  coli_invoice_sec;

   // S5 aCOLI MI: required by the COLI pipeline ctor (added with Q3I S5).
   B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_acoli_t> coli_acoli;

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
      pipeline_view = B::Adapter<tpch::q12::q12_pipeline_view_t>(db, "q12_pipeline_view");
      merged_ol     = B::MergedAdapter<orders_t, lineitem_t>(db, "q12_merged_ol");
      merged_coli   = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_coli_t, tpch::invoice_coli_t>(db, "coli_merged");
      coli_orders_sec   = B::Adapter<tpch::orders_coli_t>(db, "coli_orders_sec");
      coli_lineitem_sec = B::Adapter<tpch::lineitem_coli_t>(db, "coli_lineitem_sec");
      coli_invoice_sec  = B::Adapter<tpch::invoice_coli_t>(db, "coli_invoice_sec");
      coli_acoli        = B::MergedAdapter<tpch::customer_acoli_t, tpch::orders_acoli_t>(
                              db, "coli_acoli");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, invoice, logger);
   tpch::q12::Q12Workload<B> q12(tpch, orders, lineitem, pipeline_view, merged_ol);
   tpch::CustomerOrdersLineitemInvoicePipeline<B> coli_pipeline(
       customer, orders, lineitem, invoice, merged_coli,
       coli_orders_sec, coli_lineitem_sec, coli_invoice_sec, coli_acoli);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         q12.load();
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   tpch.recover_last_ids();

   using AggRow = tpch::q12::q12_agg_row_t;
   switch (FLAGS_storage_structure) {
      case 1: {
         tpch::q12::BaseQ12<B> w{q12};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "base_merge_join");
         helper.run();
         break;
      }
      case 2: {
         tpch::q12::ViewQ12<B> w{q12};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "pipeline_view");
         helper.run();
         break;
      }
      case 3: {
         tpch::q12::MergedQ12<B> w{q12};
         tpch::TpchExecutableHelper<decltype(w), AggRow, B::Adapter> helper(crm, std::move(w), tpch, "mi_premerged");
         helper.run();
         break;
      }
      case 4: {
         tpch::q12::HashQ12<B> w{q12};
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
