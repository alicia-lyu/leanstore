// RocksDB entry point for the refresh_sales (RF1/RF2) update experiment.
//
// Mounts the same loaded image as the q3/q5 vanilla-family read binaries
// (declares the identical adapter set; recovers via `--recover=true`). When
// `--recover=false` the binary loads the family image so a single invocation
// from a fresh node can stand in for the q3/q5 loader.
//
// The refresh loop runs --update_size RF1 inserts (above last_order_id),
// then --update_size RF2 deletes (from the bottom of the loaded keyspace —
// disjoint from RF1; see refresh_sales/CLAUDE.md). Per-structure
// maintenance: base + col-pipeline writes happen ONCE per op in the family
// helpers below; q3.maintain_rf1 + q5.maintain_rf1 are no-ops unless
// --storage_structure=2, in which case each maintains its own pipeline view.

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#include <gflags/gflags.h>

#include "../../shared/RocksDB.hpp"
#include "../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../shared/logger/rocksdb_logger.hpp"
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

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");
DEFINE_int32(update_size,     1, "RF1+RF2 batch size (orders per refresh op)");
DEFINE_int32(refresh_seconds, 15, "Total seconds to run the RF1/RF2 loop");
DEFINE_bool(prewarm, false,
            "Scan the whole DB (base tables + active structure's secondary) "
            "into RocksDB's block cache before the timed RF loop, so with "
            "--dram_gib >= image size the loop runs in-memory (no SST reads — "
            "reads use_direct_reads, bypassing the OS page cache). Mirrors the "
            "LeanStore --prewarm; the prewarm time itself is not measured.");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

namespace {

// Family helper: insert one RF1 order's base + S1/S3 secondary rows, then
// delegate per-query view maintenance. Encapsulates the "write the shared
// substrate once" rule that prevents q3+q5 double-writes.
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
   // View first (per-query); then col secondary; then base.
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
   gflags::SetUsageMessage("refresh_sales — TPC-H RF1/RF2 update experiment (RocksDB)");
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

   B::Adapter<tpch::q3::q3_pipeline_view_t> q3_view(rocks_db);
   B::Adapter<tpch::q5::q5_pipeline_view_t> q5_view(rocks_db);

   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col(rocks_db);
   B::Adapter<tpch::orders_coli_t>        split_orders(rocks_db);
   B::Adapter<tpch::lineitem_col_t>       split_lineitem(rocks_db);

   // S6 shared view adapter — not exercised by refresh_sales (RF1/RF2 touch
   // S1/S2/S3/S4 only), but the Q3/Q5 ctors require it.
   B::Adapter<tpch::col_shared_view_t>    shared_view(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               q3_view, merged_col, split_orders, split_lineitem,
                               shared_view);
   tpch::q5::Q5Workload<B> q5(tpch, customer, orders, lineitem,
                               supplier, nation, region,
                               q5_view, merged_col, split_orders, split_lineitem,
                               shared_view);

   if (!FLAGS_recover) {
      tpch::load_vanilla_family<B>(tpch, q3, q5);
      return 0;
   }
   tpch.recover_last_ids();

   // Optional prewarm: scan the whole DB into RocksDB's block cache so the timed
   // RF loop runs warm (in-memory) when --dram_gib >= the image. RocksDB sets
   // use_direct_reads (RocksDB.cpp:16), so the OS page cache never serves reads;
   // the block cache (sized by --dram_gib) is the only resident layer, and only
   // this scan fills it ahead of the short RF run. Mirrors the LeanStore prewarm.
   // Not measured.
   if (FLAGS_prewarm) {
      const auto t0 = std::chrono::steady_clock::now();
      long n = 0, mrows = 0;
      auto warm = [&](auto& ad, auto key) {
         ad.scan(key, [&](const decltype(key)&, const auto&) { ++n; return true; },
                 []() {});
      };
      // Base tables the RF path reads (partsupp/customer are the random-access
      // RF1 bottleneck; orders/lineitem/nation for RF2 + Q5 maintain).
      warm(partsupp, partsupp_t::Key{});
      warm(customer, customerh_t::Key{});
      warm(orders,   orders_t::Key{});
      warm(lineitem, lineitem_t::Key{});
      warm(nation,   nation_t::Key{});
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
            auto scanner = merged_col.template getScanner<
                tpch::customer_coli_t::Key, tpch::customer_coli_t>();
            while (auto kv = scanner->next()) { (void)kv; ++mrows; ++n; }
            break;
         }
         case 4: default: break;  // base only
      }
      const double s = std::chrono::duration<double>(
                           std::chrono::steady_clock::now() - t0).count();
      std::cout << "prewarm(S" << FLAGS_storage_structure << "): scanned " << n
                << " rows (" << mrows << " merged) in " << s << "s\n";
   }

   tpch::RefreshState<B::Adapter> refresh(tpch);

   // bg=2 contention cohort (optional). N parallel cohort threads (one
   // per registered Q3/Q5/... step) plus an optional point-lookup thread,
   // mirroring TpchExecutableHelper. Each cohort query runs on its own
   // CRM worker so a heavy structure (S4 all cells, btree S1 5H/5HH) only
   // slows its own thread without un-rotating the others. RocksDB::txn is
   // thread_local, so each thread's TXs are independent of foreground RF
   // writes. bg results are discarded — this is a contention axis, not a
   // parity check.
   RocksDBTraits db_traits(rocks_db);
   std::atomic<bool> keep_running = true;
   std::atomic<long> bg_cohort_count = 0;
   std::atomic<long> bg_lookup_count = 0;
   std::vector<tpch::BgStepFn> bg_steps =
       FLAGS_bg_query_thread
           ? tpch::register_vanilla_bg_steps<B>(db_traits, tpch, q3, q5,
                                                 FLAGS_storage_structure,
                                                 FLAGS_bg_point_lookups)
           : std::vector<tpch::BgStepFn>{};
   tpch::BgStepFn bg_lookup_step =
       (FLAGS_bg_query_thread && FLAGS_bg_point_lookups)
           ? tpch::make_tpch_point_lookup_step<B>(db_traits, tpch)
           : tpch::BgStepFn{};
   std::vector<std::thread> bg_cohort_threads;
   std::thread bg_lookup_thread;
   if (FLAGS_bg_query_thread) {
      const size_t cohort_size = bg_steps.size();
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
                  bg_steps[i](wid);
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

   const auto t_start = std::chrono::steady_clock::now();
   const auto deadline = t_start + std::chrono::seconds(FLAGS_refresh_seconds);

   // CSV next to the binary unless --csv_path overrides. Mirrors logger.
   std::ofstream csv("RefreshTPut.s" + std::to_string(FLAGS_storage_structure) + ".csv");
   csv << "elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s\n";

   long total_rf1 = 0, total_rf2 = 0;
   while (std::chrono::steady_clock::now() < deadline) {
      auto t_rf1 = std::chrono::steady_clock::now();
      for (int i = 0; i < FLAGS_update_size; ++i) {
         auto r = refresh.next_rf1();
         apply_rf1_vanilla<B>(q3.col_pipeline(), orders, lineitem, q3, q5,
                              r.key, r.order, r.lines);
         ++total_rf1;
      }
      auto rf1_us = std::chrono::duration<double, std::micro>(
                        std::chrono::steady_clock::now() - t_rf1).count();

      auto t_rf2 = std::chrono::steady_clock::now();
      int rf2_done = 0;
      for (int i = 0; i < FLAGS_update_size; ++i) {
         auto r = refresh.next_rf2();
         if (r.exhausted) break;
         if (r.linenumbers.empty()) continue;  // hole — skip
         apply_rf2_vanilla<B>(q3.col_pipeline(), orders, lineitem, q3, q5,
                              r.key, r.custkey, r.linenumbers);
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
