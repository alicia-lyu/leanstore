// Unit test for tpch::q5::build_q5_side_tables.
//
// Loads all 8 TPC-H base tables (scale_factor=1) via TPCHWorkload, then calls
// build_q5_side_tables for two regions and asserts:
//   1. region_key resolves correctly (ASIA → 2, AFRICA → 0).
//   2. |nation_set| == 5 for each region.
//   3. n_name_map has 5 entries and each key is in nation_set.
//   4. |supplier_nation| matches the number of suppliers whose s_nationkey
//      is in nation_set (i.e., every inserted entry belongs to the region).
//
// Does NOT require any merged or secondary index — only the three base-table
// adapters (region, nation, supplier) are exercised.
//
// Usage:
//   mkdir -p build/scratch/q5_side_tables/{data,csv}
//   ./build/frontend/test_q5_side_tables \
//       --ssd_path=./build/scratch/q5_side_tables/data \
//       --csv_path=./build/scratch/q5_side_tables/csv \
//       --tpch_scale_factor=1
//
// Expected: all assertions pass, exit 0.

#include <gflags/gflags.h>
#include <cassert>
#include <iostream>
#include <stdexcept>

#include "../../../shared/RocksDB.hpp"
#include "../../../shared/adapter-scanner/RocksDBAdapter.hpp"
#include "../../../shared/adapter-scanner/RocksDBMergedAdapter.hpp"
#include "../../../shared/logger/rocksdb_logger.hpp"
#include "../../backend.hpp"
#include "../../tpch_tables.hpp"
#include "../../tpch_workload.hpp"
#include "../../q5/workload.hpp"
#include "../../q5/side_tables.hpp"

#define TPCH_DEFINE_FLAGS
#include "../../tpch_flags.hpp"

DEFINE_int32(tentative_skip_bytes, 12288, "Tentative skip bytes for smart skipping");

thread_local rocksdb::Transaction* RocksDB::txn = nullptr;

// ---------------------------------------------------------------------------

static const char* pass(bool ok) { return ok ? "[OK]  " : "[FAIL]"; }

// ---------------------------------------------------------------------------
// count_suppliers_in_region: full SUPPLIER scan counting rows whose
// s_nationkey is in the given nation_set.  Used to cross-check the size of
// supplier_nation after build_q5_side_tables populates it.

static long count_suppliers_in_region(
    tpch::RocksDBBackend::Adapter<supplier_t>& supplier,
    const std::unordered_set<Integer>& nation_set)
{
   long count = 0;
   supplier.scan(
       supplier_t::Key{0},
       [&](const supplier_t::Key&, const supplier_t& s) -> bool {
          if (nation_set.count(s.s_nationkey)) count++;
          return true;
       },
       []() {});
   return count;
}

// ---------------------------------------------------------------------------
// run_assertions_for_region: build side tables and verify all invariants.

static bool run_assertions_for_region(
    const char*                                        region_name,
    Integer                                      expected_region_key,
    tpch::RocksDBBackend::Adapter<region_t>&    region,
    tpch::RocksDBBackend::Adapter<nation_t>&    nation,
    tpch::RocksDBBackend::Adapter<supplier_t>&  supplier)
{
   using namespace tpch::q5;

   Params params;
   params.region       = Varchar<25>(region_name);
   params.orderdate_lo = DATE_1994_01_01;

   Q5SideTables sides;
   build_q5_side_tables<tpch::RocksDBBackend>(region, nation, supplier, params, sides);

   bool all_ok = true;
   std::cout << "\n=== Region: " << region_name << " ===\n";

   // 1. region_key resolves correctly.
   bool rk_ok = (sides.region_key == expected_region_key);
   std::cout << pass(rk_ok)
             << " region_key: " << sides.region_key
             << "  (expected " << expected_region_key << ")\n";
   all_ok &= rk_ok;

   // 2. |nation_set| == 5 (TPC-H has exactly 5 nations per region).
   bool ns_ok = (static_cast<long>(sides.nation_set.size()) == 5L);
   std::cout << pass(ns_ok)
             << " |nation_set|: " << sides.nation_set.size()
             << "  (expected 5)\n";
   all_ok &= ns_ok;

   // 3. n_name_map has 5 entries and every key is in nation_set.
   bool nm_size_ok = (static_cast<long>(sides.n_name_map.size()) == 5L);
   bool nm_keys_ok = true;
   for (const auto& [nk, nm] : sides.n_name_map) {
      if (!sides.nation_set.count(nk)) { nm_keys_ok = false; break; }
   }
   std::cout << pass(nm_size_ok)
             << " |n_name_map|: " << sides.n_name_map.size()
             << "  (expected 5)\n";
   std::cout << pass(nm_keys_ok)
             << " n_name_map keys all in nation_set\n";
   all_ok &= (nm_size_ok && nm_keys_ok);

   // 4. |supplier_nation| matches the count of in-region suppliers and every
   //    stored nationkey is in nation_set.
   long expected_sn = count_suppliers_in_region(supplier, sides.nation_set);
   bool sn_size_ok  = (static_cast<long>(sides.supplier_nation.size()) == expected_sn);
   bool sn_keys_ok  = true;
   for (const auto& [sk, nk] : sides.supplier_nation) {
      if (!sides.nation_set.count(nk)) { sn_keys_ok = false; break; }
   }
   std::cout << pass(sn_size_ok)
             << " |supplier_nation|: " << sides.supplier_nation.size()
             << "  (expected " << expected_sn << " in-region suppliers)\n";
   std::cout << pass(sn_keys_ok)
             << " supplier_nation values all in nation_set\n";
   all_ok &= (sn_size_ok && sn_keys_ok);

   return all_ok;
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Q5 side-table builder unit test — RocksDB");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   RocksDB rocks_db(RocksDB::DB_TYPE::TransactionDB);
   using B = tpch::RocksDBBackend;

   // All 8 TPC-H base tables required by TPCHWorkload.
   B::Adapter<part_t>      part(rocks_db);
   B::Adapter<supplier_t>  supplier(rocks_db);
   B::Adapter<partsupp_t>  partsupp(rocks_db);
   B::Adapter<customerh_t> customer(rocks_db);
   B::Adapter<orders_t>    orders(rocks_db);
   B::Adapter<lineitem_t>  lineitem(rocks_db);
   B::Adapter<nation_t>    nation(rocks_db);
   B::Adapter<region_t>    region(rocks_db);

   rocks_db.open();

   RocksDBLogger logger(rocks_db);
   TPCHWorkload<B::Adapter> tpch(part, supplier, partsupp, customer,
                                  orders, lineitem, nation, region, logger);
   tpch.load();

   // TPC-H hardcoded region keys (tpch_tables.hpp::region_t::REGIONS):
   //   AFRICA=0, AMERICA=1, ASIA=2, EUROPE=3, MIDDLE EAST=4
   bool all_ok = true;
   all_ok &= run_assertions_for_region("ASIA",   2, region, nation, supplier);
   all_ok &= run_assertions_for_region("AFRICA", 0, region, nation, supplier);

   // Verify that an unknown region name throws.
   {
      tpch::q5::Params bad_params;
      bad_params.region       = Varchar<25>("NO_SUCH_REGION");
      bad_params.orderdate_lo = DATE_1994_01_01;
      tpch::q5::Q5SideTables sides;
      bool threw = false;
      try {
         tpch::q5::build_q5_side_tables<B>(region, nation, supplier, bad_params, sides);
      } catch (const std::runtime_error&) {
         threw = true;
      }
      std::cout << "\n";
      std::cout << pass(threw)
                << " bad region name throws std::runtime_error\n";
      all_ok &= threw;
   }

   std::cout << "\n" << (all_ok ? "[ALL OK]" : "[SOME FAILURES]") << "\n";
   return all_ok ? 0 : 1;
}
