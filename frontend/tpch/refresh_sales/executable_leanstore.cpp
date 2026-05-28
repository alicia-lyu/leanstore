#ifndef ROCKSDB_ONLY
// LeanStore entry point for the refresh_sales (RF1/RF2) update experiment.
// Mirrors q3/executable_leanstore.cpp adapter setup; the foreground loop is
// the RF1+RF2 refresh pair, not a read-query helper. See
// refresh_sales/executable_rocksdb.cpp for the RF1/RF2 design notes.

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include <gflags/gflags.h>

#include "../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../backend.hpp"
#include "../tpch_workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../tpch_flags.hpp"
#include "../tpch_family/refresh.hpp"
#include "../tpch_vanilla_family.hpp"

#include "../q3/per_structure_workload.hpp"
#include "../q3/workload.hpp"
#include "../q5/per_structure_workload.hpp"
#include "../q5/workload.hpp"
#include "../q10/per_structure_workload.hpp"
#include "../q10/workload.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");
DEFINE_int32(update_size,     1, "RF1+RF2 batch size (orders per refresh op)");
DEFINE_int32(refresh_seconds, 15, "Total seconds to run the RF1/RF2 loop");
DEFINE_bool(prewarm, false,
            "Scan the entire DB into the buffer pool before the timed RF loop, "
            "so the measured run is warm (in-memory). Use with a buffer pool "
            "large enough to hold the footprint (e.g. dram_gib >= image size); "
            "the prewarm time itself is not measured.");

namespace {

template <typename B>
void apply_rf1_vanilla(
    tpch::CustomerOrdersLineitemPipeline<B>& col,
    typename B::template Adapter<orders_t>&   orders,
    typename B::template Adapter<lineitem_t>& lineitem,
    tpch::q3::Q3Workload<B>& q3,
    tpch::q5::Q5Workload<B>& q5,
    const orders_t::Key& ok, const orders_t& ov,
    const std::vector<lineitem_t>& lines)
{
   const Integer custkey = ov.o_custkey;
   orders.insert(ok, ov);
   for (size_t j = 0; j < lines.size(); ++j) {
      lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
      lineitem.insert(lk, lines[j]);
   }
   switch (FLAGS_storage_structure) {
      case 1:
         col.insert_order_to_split(custkey, ok, ov);
         for (size_t j = 0; j < lines.size(); ++j) {
            lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
            col.insert_lineitem_to_split(custkey, lk, lines[j]);
         }
         break;
      case 3:
         col.insert_order_to_merged(custkey, ok, ov);
         for (size_t j = 0; j < lines.size(); ++j) {
            lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
            col.insert_lineitem_to_merged(custkey, lk, lines[j]);
         }
         break;
      case 2: case 4: default: break;
   }
   q3.maintain_rf1(ok, ov, lines);
   q5.maintain_rf1(ok, ov, lines);
}

template <typename B>
void apply_rf2_vanilla(
    tpch::CustomerOrdersLineitemPipeline<B>& col,
    typename B::template Adapter<orders_t>&   orders,
    typename B::template Adapter<lineitem_t>& lineitem,
    tpch::q3::Q3Workload<B>& q3,
    tpch::q5::Q5Workload<B>& q5,
    const orders_t::Key& ok, Integer custkey,
    const std::vector<Integer>& linenumbers)
{
   q3.erase_rf2(ok, custkey, linenumbers);
   q5.erase_rf2(ok, custkey, linenumbers);
   switch (FLAGS_storage_structure) {
      case 1:
         for (Integer ln : linenumbers)
            col.erase_lineitem_from_split(custkey, lineitem_t::Key{ok.o_orderkey, ln});
         col.erase_order_from_split(custkey, ok);
         break;
      case 3:
         for (Integer ln : linenumbers)
            col.erase_lineitem_from_merged(custkey, lineitem_t::Key{ok.o_orderkey, ln});
         col.erase_order_from_merged(custkey, ok);
         break;
      case 2: case 4: default: break;
   }
   for (Integer ln : linenumbers)
      lineitem.erase(lineitem_t::Key{ok.o_orderkey, ln});
   orders.erase(ok);
}

}  // namespace

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("refresh_sales — TPC-H RF1/RF2 update experiment (LeanStore)");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // refresh_sales maintenance is wired for S1/S2/S3/S4 only. Q10 owns the
   // native S5/S7 read variants in the vanilla family cohort; they have no
   // RF1/RF2 maintenance path here.
   if (FLAGS_storage_structure == 5 || FLAGS_storage_structure == 7) {
      std::cerr << "refresh_sales_btree does not support --storage_structure="
                << FLAGS_storage_structure
                << " (refresh_sales has no S5/S7 maintenance; the Q10 native S5/S7"
                << " variants are read-only)." << std::endl;
      return 1;
   }

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

   B::Adapter<tpch::q3::q3_pipeline_view_t> q3_view;
   B::Adapter<tpch::q5::q5_pipeline_view_t> q5_view;
   B::Adapter<tpch::q10::q10_pipeline_view_t>        q10_view;
   B::Adapter<tpch::q10::q10_pipeline_view_preagg_t> q10_view_preagg;

   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t> q10_acol;
   B::Adapter<tpch::col_shared_view_t>    shared_view;  // S6 (unused by refresh)

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
      q3_view       = B::Adapter<tpch::q3::q3_pipeline_view_t>(db, "q3_pipeline_view");
      q5_view       = B::Adapter<tpch::q5::q5_pipeline_view_t>(db, "q5_pipeline_view");
      q10_view      = B::Adapter<tpch::q10::q10_pipeline_view_t>(db, "q10_pipeline_view");
      q10_view_preagg = B::Adapter<tpch::q10::q10_pipeline_view_preagg_t>(db, "q10_pipeline_view_preagg");
      merged_col    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_col_t>(db, "col_merged");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "col_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "col_split_lineitem");
      q10_acol       = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_acol_t>(db, "q10_acol");
      shared_view    = B::Adapter<tpch::col_shared_view_t>(db, "col_shared_view");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               q3_view, merged_col, split_orders, split_lineitem,
                               shared_view);
   tpch::q5::Q5Workload<B> q5(tpch, customer, orders, lineitem,
                               supplier, nation, region,
                               q5_view, merged_col, split_orders, split_lineitem,
                               shared_view);
   tpch::q10::Q10Workload<B> q10(tpch, customer, orders, lineitem, nation,
                                  q10_view, q10_view_preagg,
                                  merged_col, split_orders, split_lineitem,
                                  q10_acol, shared_view);

   if (!FLAGS_recover) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
         tpch::load_vanilla_family<B>(tpch, q3, q5, q10);
         leanstore::cr::Worker::my().commitTX();
      });
      return 0;
   }
   // recover_last_ids() scans base tables (scanDesc) to recover last_*_id.
   // LeanStore adapter scans need a Worker TLS context, so run it inside a
   // scheduleJobSync TX — the main thread has no Worker (RocksDB tolerates
   // this via thread-local txn; LeanStore SEGVs without it).
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      tpch.recover_last_ids();
      leanstore::cr::Worker::my().commitTX();
   });

   // Optional prewarm: scan the entire DB so the timed RF loop runs warm
   // (in-memory), matching an in-memory view engine's resident footprint.
   // Random RF access alone never warms the footprint within a short run, so
   // a bigger buffer pool is wasted without this. Not measured.
   if (FLAGS_prewarm) {
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX();
         const auto t0 = std::chrono::steady_clock::now();
         long n = 0, mrows = 0;
         auto warm = [&](auto& ad, auto key) {
            ad.scan(key, [&](const decltype(key)&, const auto&) { ++n; return true; },
                    []() {});
         };
         // Base tables the RF path reads (partsupp/customer are the random-access
         // RF1 bottleneck; orders/lineitem/nation for RF2 + Q5 maintain).
         warm(partsupp,       partsupp_t::Key{});
         warm(customer,       customerh_t::Key{});
         warm(orders,         orders_t::Key{});
         warm(lineitem,       lineitem_t::Key{});
         warm(nation,         nation_t::Key{});
         // Only the active structure's secondary is touched during its run.
         switch (FLAGS_storage_structure) {
            case 1:
               warm(split_orders,   tpch::orders_coli_t::Key{});
               warm(split_lineitem, tpch::lineitem_col_t::Key{});
               break;
            case 2:
               warm(q3_view, tpch::q3::q3_pipeline_view_t::Key{});
               warm(q5_view, tpch::q5::q5_pipeline_view_t::Key{});
               break;
            case 3: {
               auto [mbytes, mr] = merged_col.content_bytes_walk();
               (void)mbytes; mrows = mr; n += mr;
               break;
            }
            case 4: default: break;  // base only
         }
         leanstore::cr::Worker::my().commitTX();
         const double s = std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - t0).count();
         std::cout << "prewarm(S" << FLAGS_storage_structure << "): scanned " << n
                   << " rows (" << mrows << " merged) in " << s << "s\n";
      });
   }

   tpch::RefreshState<B::Adapter> refresh(tpch);

   // bg=2 contention cohort (optional). N parallel CRM cohort workers
   // (one per registered Q3/Q5/... step) plus an optional point-lookup
   // CRM worker. The foreground RF loop runs on MAIN_WORKER, distinct
   // from all bg workers (CRMG.hpp: one mutex/cv/job per worker). bg
   // results are discarded — this is a contention axis, not a parity
   // check.
   LeanStoreTraits db_traits(crm);
   std::atomic<bool> keep_running = true;
   std::atomic<long> bg_cohort_count = 0;
   std::atomic<long> bg_lookup_count = 0;
   std::vector<tpch::BgCatalogFn> bg_catalog =
       FLAGS_bg_query_thread
           ? tpch::register_vanilla_bg_catalog<B>(db_traits, tpch, q3, q5, q10,
                                                 FLAGS_storage_structure,
                                                 FLAGS_bg_point_lookups)
           : std::vector<tpch::BgCatalogFn>{};
   tpch::BgCatalogFn bg_lookup_step =
       (FLAGS_bg_query_thread && FLAGS_bg_point_lookups)
           ? tpch::make_tpch_point_lookup_step<B>(db_traits, tpch)
           : tpch::BgCatalogFn{};
   std::vector<std::thread> bg_cohort_threads;
   std::thread bg_lookup_thread;
   if (FLAGS_bg_query_thread) {
      const size_t cohort_size = bg_catalog.size();
      const u64 needed_max_id = cohort_size == 0
          ? std::max<u64>(MAIN_WORKER, FLAGS_bg_point_lookups ? BG_LOOKUP_WORKER : 0)
          : std::max<u64>(
                std::max<u64>(MAIN_WORKER, FLAGS_bg_point_lookups ? BG_LOOKUP_WORKER : 0),
                tpch::max_cohort_worker_id(cohort_size));
      const u32 needed_workers = static_cast<u32>(needed_max_id + 1);
      if (FLAGS_worker_threads < needed_workers) {
         throw std::runtime_error(
             "refresh_sales bg=2 needs --worker_threads >= "
             + std::to_string(needed_workers)
             + "; got " + std::to_string(FLAGS_worker_threads));
      }
      std::cout << "  (--bg_query_thread=true: cohort = " << cohort_size
                << " parallel threads; lookup thread = "
                << (FLAGS_bg_point_lookups ? "on (BG_LOOKUP_WORKER)" : "off") << ")\n";
      bg_cohort_threads.reserve(cohort_size);
      for (size_t i = 0; i < cohort_size; ++i) {
         const u64 wid = tpch::cohort_worker_id(i);
         bg_cohort_threads.emplace_back([&, i, wid]() {
            while (keep_running.load()) {
               jumpmuTry()
               {
                  bg_catalog[i](wid);
                  bg_cohort_count++;
               }
               jumpmuCatchNoPrint() { db_traits.rollback_tx(wid); }
            }
            db_traits.cleanup_thread(wid);
         });
      }
      if (FLAGS_bg_point_lookups) {
         bg_lookup_thread = std::thread([&]() {
            while (keep_running.load()) {
               jumpmuTry()
               {
                  bg_lookup_step(BG_LOOKUP_WORKER);
                  bg_lookup_count++;
               }
               jumpmuCatchNoPrint() { db_traits.rollback_tx(BG_LOOKUP_WORKER); }
            }
            db_traits.cleanup_thread(BG_LOOKUP_WORKER);
         });
      }
   }

   // With bg on, the foreground takes MAIN_WORKER so BG_WORKER (0) is free for
   // the cohort; with bg off, stay on worker 0 (byte-identical to prior runs).
   const u64 fg_worker = FLAGS_bg_query_thread ? MAIN_WORKER : 0;
   long total_rf1 = 0, total_rf2 = 0;
   crm.scheduleJobSync(fg_worker, [&]() {
      const auto t_start  = std::chrono::steady_clock::now();
      const auto deadline = t_start + std::chrono::seconds(FLAGS_refresh_seconds);
      std::ofstream csv("RefreshTPut.s" + std::to_string(FLAGS_storage_structure) + ".csv");
      csv << "elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s\n";

      while (std::chrono::steady_clock::now() < deadline) {
         auto t_rf1 = std::chrono::steady_clock::now();
         for (int i = 0; i < FLAGS_update_size; ++i) {
            leanstore::cr::Worker::my().startTX();
            auto r = refresh.next_rf1();
            apply_rf1_vanilla<B>(q3.col_pipeline(), orders, lineitem, q3, q5,
                                  r.key, r.order, r.lines);
            leanstore::cr::Worker::my().commitTX();
            ++total_rf1;
         }
         auto rf1_us = std::chrono::duration<double, std::micro>(
                           std::chrono::steady_clock::now() - t_rf1).count();

         auto t_rf2 = std::chrono::steady_clock::now();
         int rf2_done = 0;
         for (int i = 0; i < FLAGS_update_size; ++i) {
            leanstore::cr::Worker::my().startTX();
            auto r = refresh.next_rf2();
            if (r.exhausted) { leanstore::cr::Worker::my().commitTX(); break; }
            if (r.linenumbers.empty()) { leanstore::cr::Worker::my().commitTX(); continue; }
            apply_rf2_vanilla<B>(q3.col_pipeline(), orders, lineitem, q3, q5,
                                  r.key, r.custkey, r.linenumbers);
            leanstore::cr::Worker::my().commitTX();
            ++rf2_done; ++total_rf2;
         }
         auto rf2_us = std::chrono::duration<double, std::micro>(
                           std::chrono::steady_clock::now() - t_rf2).count();

         double elapsed_s = std::chrono::duration<double>(
                                std::chrono::steady_clock::now() - t_start).count();
         double rf1_rate = FLAGS_update_size / (rf1_us / 1e6);
         double rf2_rate = rf2_done           / std::max(rf2_us / 1e6, 1e-9);
         double pair_rate = (FLAGS_update_size + rf2_done) /
                            std::max((rf1_us + rf2_us) / 1e6, 1e-9);
         csv << elapsed_s << "," << rf1_rate << "," << rf2_rate << "," << pair_rate << "\n";
      }
   });

   // Foreground deadline reached — stop and join all bg threads.
   keep_running = false;
   for (auto& t : bg_cohort_threads) {
      if (t.joinable()) t.join();
   }
   if (bg_lookup_thread.joinable()) bg_lookup_thread.join();

   std::cout << "refresh_sales done — total RF1=" << total_rf1
             << " RF2=" << total_rf2
             << " over " << FLAGS_refresh_seconds << "s"
             << " (storage_structure=" << FLAGS_storage_structure << ")";
   if (FLAGS_bg_query_thread) {
      std::cout << " | bg_cohort_thread: " << bg_cohort_count.load()
                << " TXs | bg_lookup_thread: " << bg_lookup_count.load() << " TXs";
   }
   std::cout << "\n";
   return 0;
}

#endif  // ROCKSDB_ONLY
