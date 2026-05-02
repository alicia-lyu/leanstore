// Unified Phase 2 harness for Q3I: loads each storage structure in turn and
// verifies cross-structure result parity.
//
// For each structure in {1, 2, 3, 4}:
//   - Wipes --ssd_path (re-opens a fresh RocksDB) to avoid stale-load traps.
//   - Loads via Q3IWorkload::load() (dispatches on FLAGS_storage_structure).
//   - Runs the matching query_by_* path.
//   - Computes an XOR digest over the result rows (order-independent).
//
// All four structures must produce the same digest.  Top-10 rows are printed
// for visual inspection (structure 3 = reference oracle).
//
// Usage:
//   mkdir -p test_data_q3i2 test_csv_q3i2
//   ./build/frontend/test_query_q3i_lsm \
//       --ssd_path=./test_data_q3i2 \
//       --csv_path=./test_csv_q3i2 \
//       --tpch_scale_factor=1

#include <gflags/gflags.h>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <vector>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../coli_pipeline.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q3i/workload.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------
// XOR digest over result rows (order-independent parity check).
//
// Mixes each row's fields via rotate-left + XOR so single-field errors flip
// the digest.  The digest is order-independent because XOR is commutative.

static uint64_t row_digest(const tpch::q3i::q3i_agg_row_t& r)
{
   auto rotl64 = [](uint64_t v, int s) { return (v << s) | (v >> (64 - s)); };
   uint64_t d = 0;
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderkey));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.revenue) * 1e6);
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_orderdate));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<int>(r.o_shippriority));
   d = rotl64(d, 13) ^ static_cast<uint64_t>(static_cast<double>(r.cust_open_due) * 1e6);
   return d;
}

static uint64_t digest_rows(const std::vector<tpch::q3i::q3i_agg_row_t>& rows)
{
   uint64_t acc = 0;
   for (const auto& r : rows) acc ^= row_digest(r);
   return acc;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q3I unified query test — RocksDB backend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   using B = tpch::RocksDBBackend;

   // Collect per-structure results for parity check.
   std::vector<tpch::q3i::q3i_agg_row_t> results[5];  // index = storage_structure
   uint64_t digests[5] = {};
   bool     all_ok     = true;

   const std::string base_ssd  = FLAGS_ssd_path;
   const std::string base_csv  = FLAGS_csv_path;

   for (int s : {3, 1, 2, 4}) {
      // Wipe ssd_path to ensure a clean load (guards against stale-data bugs
      // arising from re-use of a previous structure's on-disk state).
      std::filesystem::remove_all(base_ssd);
      std::filesystem::create_directories(base_ssd);
      std::filesystem::create_directories(base_csv);

      FLAGS_storage_structure = s;

      RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);

      // All 8 TPC-H base tables.
      B::Adapter<part_t>      part(rocks_db);
      B::Adapter<supplier_t>  supplier(rocks_db);
      B::Adapter<partsupp_t>  partsupp(rocks_db);
      B::Adapter<customerh_t> customer(rocks_db);
      B::Adapter<orders_t>    orders(rocks_db);
      B::Adapter<lineitem_t>  lineitem(rocks_db);
      B::Adapter<nation_t>    nation(rocks_db);
      B::Adapter<region_t>    region(rocks_db);
      B::Adapter<invoice_t>   invoice(rocks_db);

      // Q3I-specific adapters.
      B::Adapter<tpch::q3i::q3i_pipeline_view_t> pipeline_view(rocks_db);
      B::MergedAdapter<tpch::customer_coli_t, tpch::orders_coli_t,
                       tpch::lineitem_coli_t, tpch::invoice_coli_t> merged_coli(rocks_db);
      B::Adapter<tpch::orders_coli_t>   split_orders(rocks_db);
      B::Adapter<tpch::lineitem_coli_t> split_lineitem(rocks_db);
      B::Adapter<tpch::invoice_coli_t>  split_invoice(rocks_db);

      rocks_db.open();

      RocksDBLogger logger(rocks_db);
      TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                     orders, lineitem, nation, region, invoice, logger);

      tpch::q3i::Q3IWorkload<B> q3i(tpch, customer, orders, lineitem, invoice,
                                      pipeline_view, merged_coli,
                                      split_orders, split_lineitem, split_invoice);

      std::cout << "\n=== S" << s << ": loading SF=" << FLAGS_tpch_scale_factor << " ===\n";
      q3i.load();

      std::vector<tpch::q3i::q3i_agg_row_t> out;
      tpch::q3i::Q3IStats st;
      q3i.stats = &st;

      long rows = 0;
      switch (s) {
         case 1: rows = q3i.query_by_base(out);   break;
         case 2: rows = q3i.query_by_view(out);   break;
         case 3: rows = q3i.query_by_merged(out); break;
         case 4: rows = q3i.query_by_hash(out);   break;
      }

      results[s] = out;
      digests[s] = digest_rows(out);

      std::cout << "S" << s << " rows=" << rows
                << "  digest=0x" << std::hex << digests[s] << std::dec << "\n";

      // Print top-10 for S3 (reference oracle) and for every path as a visual check.
      std::cout << "  o_orderkey\trevenue\to_orderdate\to_shippriority\tcust_open_due\n";
      for (const auto& r : out) r.print(std::cout);
   }

   std::cout << "\n=== Parity check ===\n";
   uint64_t ref = digests[3];  // S3 is the reference oracle
   for (int s : {1, 2, 3, 4}) {
      bool ok = (digests[s] == ref);
      if (!ok) all_ok = false;
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S" << s << " digest=0x" << std::hex << digests[s] << std::dec
                << (ok ? "" : "  (expected 0x" + [&]{ std::ostringstream ss; ss << std::hex << ref; return ss.str(); }() + ")")
                << "\n";
   }

   // Shape check: all structures must return exactly 10 rows (SF=1 has > 10
   // qualifying rows after the BUILDING / threshold=0 filters).
   bool shape_ok = true;
   for (int s : {1, 2, 3, 4}) {
      bool ok = (results[s].size() == 10);
      if (!ok) { shape_ok = false; all_ok = false; }
      std::cout << (ok ? "[OK]   " : "[FAIL] ")
                << "S" << s << " row_count==" << results[s].size()
                << " (expected 10)\n";
   }
   (void)shape_ok;

   return all_ok ? 0 : 1;
}
