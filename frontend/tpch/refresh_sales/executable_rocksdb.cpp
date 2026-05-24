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

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               q3_view, merged_col, split_orders, split_lineitem);
   tpch::q5::Q5Workload<B> q5(tpch, customer, orders, lineitem,
                               supplier, nation, region,
                               q5_view, merged_col, split_orders, split_lineitem);

   if (!FLAGS_recover) {
      tpch::load_vanilla_family<B>(tpch, q3, q5);
      return 0;
   }
   tpch.recover_last_ids();

   tpch::RefreshState<B::Adapter> refresh(tpch);

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

   std::cout << "refresh_sales done — total RF1=" << total_rf1
             << " RF2=" << total_rf2
             << " over " << FLAGS_refresh_seconds << "s"
             << " (storage_structure=" << FLAGS_storage_structure << ")\n";
   return 0;
}
