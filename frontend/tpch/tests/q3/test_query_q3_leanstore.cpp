#ifndef ROCKSDB_ONLY
// LeanStore entry point for Q3 query parity test.
// Mirrors test_query_q3_rocksdb.cpp for the LeanStore backend.

#include <gflags/gflags.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include "../../../shared/adapter-scanner/LeanStoreAdapter.hpp"
#include "../../../shared/adapter-scanner/LeanStoreMergedAdapter.hpp"
#include "../../../shared/logger/leanstore_logger.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/Transaction.hpp"
#include "leanstore/concurrency-recovery/Worker.hpp"
#include "../../backend.hpp"
#include "../../tpch_workload.hpp"
#include "../../q3/workload.hpp"
#include "../../tpch_family/refresh.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 4096, "Tentative skip bytes for smart skipping");

static uint64_t row_digest(const tpch::q3::q3_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderdate));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_shippriority));
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q3::q3_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3 unified query test — LeanStore backend");
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

   B::Adapter<tpch::q3::q3_pipeline_view_t> pipeline_view;
   B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                    tpch::lineitem_col_t>  merged_col;
   B::Adapter<tpch::orders_coli_t>        split_orders;
   B::Adapter<tpch::lineitem_col_t>       split_lineitem;
   B::Adapter<tpch::col_shared_view_t>    shared_view;  // S6

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
      pipeline_view = B::Adapter<tpch::q3::q3_pipeline_view_t>(db, "q3_pipeline_view");
      merged_col    = B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                                       tpch::lineitem_col_t>(db, "q3_merged_col");
      split_orders   = B::Adapter<tpch::orders_coli_t>(db, "q3_split_orders");
      split_lineitem = B::Adapter<tpch::lineitem_col_t>(db, "q3_split_lineitem");
      shared_view    = B::Adapter<tpch::col_shared_view_t>(db, "q3_shared_view");
   });

   LeanStoreLogger logger(db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);

   tpch::q3::Q3Workload<B> q3(tpch, customer, orders, lineitem,
                               pipeline_view, merged_col,
                               split_orders, split_lineitem, shared_view);

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX(leanstore::TX_MODE::INSTANTLY_VISIBLE_BULK_INSERT);
      tpch.load();
      q3.col_pipeline().populate_split();
      tpch::q3::populate_q3_view<B>(customer, orders, lineitem, pipeline_view);
      q3.col_pipeline().populate_merged();
      tpch::populate_col_shared_view<B>(merged_col, shared_view);  // S6
      leanstore::cr::Worker::my().commitTX();
   });

   std::vector<tpch::q3::q3_agg_row_t> r_base, r_view, r_merged, r_hash, r_shared;
   tpch::q3::Stats st_base, st_view, st_merged, st_hash, st_shared;

   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      q3.stats = &st_base;   q3.query_by_base       (r_base);
      q3.stats = &st_view;   q3.query_by_view       (r_view);
      q3.stats = &st_merged; q3.query_by_merged     (r_merged);
      q3.stats = &st_hash;   q3.query_by_hash       (r_hash);
      q3.stats = &st_shared; q3.query_by_shared_view(r_shared);
      q3.stats = nullptr;
      leanstore::cr::Worker::my().commitTX();
   });

   uint64_t d_base   = digest_rows(r_base);
   uint64_t d_view   = digest_rows(r_view);
   uint64_t d_merged = digest_rows(r_merged);
   uint64_t d_hash   = digest_rows(r_hash);
   uint64_t d_shared = digest_rows(r_shared);

   std::cout << "\n=== Results ===\n";
   auto print_digest = [](const char* name, size_t n, uint64_t d) {
      std::cout << std::left << std::setw(14) << name
                << " rows=" << std::setw(4) << n
                << " digest=0x" << std::hex << d << std::dec << "\n";
   };
   print_digest("S1 (base)",   r_base.size(),   d_base);
   print_digest("S2 (view)",   r_view.size(),   d_view);
   print_digest("S3 (merged)", r_merged.size(), d_merged);
   print_digest("S4 (hash)",   r_hash.size(),   d_hash);
   print_digest("S6 (shared)", r_shared.size(), d_shared);

   std::cout << "\n=== Parity check ===\n";
   uint64_t ref = d_merged;  // S3 is the canonical oracle.

   auto status = [&](uint64_t d) -> const char* {
      return d == ref ? "[OK]   " : "[FAIL] ";
   };
   auto parity_line = [&](const char* tag, uint64_t d, long n) {
      const char* s = status(d);
      std::ostringstream ss; ss << std::hex << ref;
      bool show_expected = (s[1] == 'F');
      std::cout << s << tag
                << " rows=" << std::dec << n
                << " digest=0x" << std::hex << d << std::dec
                << (show_expected ? "  (expected 0x" + ss.str() + ")" : "")
                << "\n";
   };
   parity_line("S1 base  ", d_base,   (long)r_base.size());
   parity_line("S2 view  ", d_view,   (long)r_view.size());
   parity_line("S3 merged", d_merged, (long)r_merged.size());
   parity_line("S4 hash  ", d_hash,   (long)r_hash.size());
   parity_line("S6 shared", d_shared, (long)r_shared.size());

   // Skip-seek counters across all four paths.  S1 has two physical streams
   // (orders + lineitem), so s1_groups_skipped can be up to ~2× the
   // single-stream counters (S2/S3); what matters is that it is > 0 and of
   // the same order of magnitude.  S4 seeks per qualifying orderkey, not
   // per custkey, so its counter is naturally larger.
   std::cout << "\n=== Per-customer skip-seek (S1 vs S2 vs S3 vs S4) ===\n"
             << "S1 s1_groups_skipped   = " << st_base.s1_groups_skipped
             << "    (orders_scanned = " << st_base.orders_scanned << ")\n"
             << "S2 view_groups_skipped = " << st_view.view_groups_skipped
             << "    (lineitems_scanned = " << st_view.lineitems_scanned << ")\n"
             << "S3 mi_groups_skipped   = " << st_merged.mi_groups_skipped
             << "    (mi_records_visited = " << st_merged.mi_records_visited << ")\n"
             << "S4 s4_orderkey_seeks   = " << st_hash.s4_orderkey_seeks
             << "    (lineitems_scanned = " << st_hash.lineitems_scanned << ")\n"
             << "  S4 s4_hashtable_bytes=" << st_hash.s4_hashtable_bytes
             << " (" << std::fixed << std::setprecision(2)
             << (double(st_hash.s4_hashtable_bytes) / 1048576.0) << " MiB)\n";

   if (r_merged.empty()) {
      std::cout << "\n[FAIL] S3 returned 0 rows — query_by_merged body broken.\n";
      return 1;
   }
   bool pre_ok = (d_base == ref) && (d_view == ref) && (d_hash == ref)
              && (d_shared == ref);
   if (!pre_ok) {
      std::cout << "\n[FAIL] pre-update parity broken — stopping before RF1/RF2.\n";
      return 1;
   }
   const uint64_t pre_update_ref = ref;

   // ------------------------------------------------------------------
   // RF1 + RF2 round-trip parity (refresh_sales experiment). Mirror of
   // test_query_q3_rocksdb.cpp; every adapter op runs inside a Worker TX
   // (scheduleJobSync) because LeanStore reads/writes need Worker TLS.
   //
   // Insert N orders into base + ALL secondaries; verify all four query
   // paths still agree. Delete the same orderkeys; verify all four paths
   // agree AND the digest matches the pre-update value (true round-trip).
   constexpr int N = 10;
   std::cout << "\n=== RF1+RF2 round-trip (N=" << N << ") ===\n";

   tpch::RefreshState<B::Adapter> refresh(tpch);
   struct Inserted {
      orders_t::Key key;
      Integer       custkey;
      std::vector<Integer> linenumbers;
   };
   std::vector<Inserted> inserted;
   inserted.reserve(N);

   // Insert into base + every secondary (the test owns all four structures
   // simultaneously, unlike the production refresh_sales binary).
   auto apply_rf1_all = [&](const orders_t::Key& ok, const orders_t& ov,
                             const std::vector<lineitem_t>& lines) {
      const Integer custkey = ov.o_custkey;
      orders.insert(ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         lineitem.insert(lk, lines[j]);
      }
      // S1 split
      q3.col_pipeline().insert_order_to_split(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q3.col_pipeline().insert_lineitem_to_split(custkey, lk, lines[j]);
      }
      // S3 merged
      q3.col_pipeline().insert_order_to_merged(custkey, ok, ov);
      for (size_t j = 0; j < lines.size(); ++j) {
         lineitem_t::Key lk{ok.o_orderkey, static_cast<Integer>(j + 1)};
         q3.col_pipeline().insert_lineitem_to_merged(custkey, lk, lines[j]);
      }
      // S2 view (reuse build_q3_view_row from q3/load.tpp)
      Varchar<10> mktseg{};
      customer.lookup1(customerh_t::Key{custkey},
                       [&](const customerh_t& c) { mktseg = c.c_mktsegment; });
      for (size_t j = 0; j < lines.size(); ++j) {
         auto [vk, vv] = tpch::q3::build_q3_view_row(
             custkey, ok.o_orderkey, static_cast<Integer>(j + 1),
             ov, lines[j], mktseg);
         pipeline_view.insert(vk, vv);
      }
   };

   auto apply_rf2_all = [&](const orders_t::Key& ok, Integer custkey,
                             const std::vector<Integer>& linenumbers) {
      // Per-structure secondary first; then base last.
      for (Integer ln : linenumbers)
         q3.col_pipeline().erase_lineitem_from_split(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q3.col_pipeline().erase_order_from_split(custkey, ok);
      for (Integer ln : linenumbers)
         q3.col_pipeline().erase_lineitem_from_merged(custkey,
             lineitem_t::Key{ok.o_orderkey, ln});
      q3.col_pipeline().erase_order_from_merged(custkey, ok);
      for (Integer ln : linenumbers)
         pipeline_view.erase(tpch::q3::q3_pipeline_view_t::Key{
             custkey, ok.o_orderkey, ln});
      for (Integer ln : linenumbers)
         lineitem.erase(lineitem_t::Key{ok.o_orderkey, ln});
      orders.erase(ok);
   };

   // RF1: generate + apply inside one Worker TX.
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      for (int i = 0; i < N; ++i) {
         auto r = refresh.next_rf1();
         Inserted rec;
         rec.key     = r.key;
         rec.custkey = r.order.o_custkey;
         rec.linenumbers.reserve(r.lines.size());
         for (size_t j = 0; j < r.lines.size(); ++j)
            rec.linenumbers.push_back(static_cast<Integer>(j + 1));
         apply_rf1_all(r.key, r.order, r.lines);
         inserted.push_back(std::move(rec));
      }
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[ok] applied " << N << " RF1 inserts\n";

   // Re-run all four queries; verify 4-way agreement.
   {
      std::vector<tpch::q3::q3_agg_row_t> a, b, c, d;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX();
         q3.query_by_base(a); q3.query_by_view(b);
         q3.query_by_merged(c); q3.query_by_hash(d);
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      std::cout << "[post-RF1] S1=0x" << std::hex << da
                << " S2=0x" << db << " S3=0x" << dc
                << " S4=0x" << dd << std::dec << "\n";
      if (!(da == dc && db == dc && dd == dc)) {
         std::cout << "[FAIL] post-RF1 parity broken across structures.\n";
         return 1;
      }
      std::cout << "[ok]   post-RF1 4-way digest agrees\n";
   }

   // RF2: delete the same orderkeys (test-only round-trip property).
   crm.scheduleJobSync(0, [&]() {
      leanstore::cr::Worker::my().startTX();
      for (const auto& rec : inserted)
         apply_rf2_all(rec.key, rec.custkey, rec.linenumbers);
      leanstore::cr::Worker::my().commitTX();
   });
   std::cout << "[ok] applied " << N << " RF2 deletes (round-trip)\n";

   {
      std::vector<tpch::q3::q3_agg_row_t> a, b, c, d;
      crm.scheduleJobSync(0, [&]() {
         leanstore::cr::Worker::my().startTX();
         q3.query_by_base(a); q3.query_by_view(b);
         q3.query_by_merged(c); q3.query_by_hash(d);
         leanstore::cr::Worker::my().commitTX();
      });
      uint64_t da = digest_rows(a), db = digest_rows(b),
               dc = digest_rows(c), dd = digest_rows(d);
      std::cout << "[post-RF2] S1=0x" << std::hex << da
                << " S2=0x" << db << " S3=0x" << dc
                << " S4=0x" << dd << std::dec << "\n";
      bool agree    = (da == dc && db == dc && dd == dc);
      bool restored = (da == pre_update_ref);
      if (!agree) {
         std::cout << "[FAIL] post-RF2 parity broken across structures.\n";
         return 1;
      }
      if (!restored) {
         std::cout << "[FAIL] post-RF2 digest 0x" << std::hex << da
                   << " != pre-update 0x" << pre_update_ref << std::dec
                   << " — round-trip property broken.\n";
         return 1;
      }
      std::cout << "[ok]   post-RF2 digest matches pre-update — RF1+RF2 round-trip OK\n";
   }

   return 0;
}

#endif  // ROCKSDB_ONLY
