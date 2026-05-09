#pragma once

// q5/side_tables.hpp — per-query in-process dimension-table structures for Q5.
//
// build_q5_side_tables() scans REGION, NATION, and SUPPLIER once before any
// COL walk or join chain begins.  All three fits in memory and are sub-ms at
// SF=1.  The caller passes `out` by reference and the function populates it.
//
// Call pattern (all four query_by_* paths):
//   Q5SideTables sides;
//   build_q5_side_tables<Backend>(region, nation, supplier, params, sides);
//   // then use sides.nation_set / sides.n_name_map / sides.supplier_nation

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "../tpch_tables.hpp"

namespace tpch::q5
{

// Forward declaration — full Params definition lives in workload.hpp.
// build_q5_side_tables is a function template; its body is instantiated at
// call sites (query.tpp), which already have Params complete.  Including
// workload.hpp here would create a cycle (workload.hpp → query.tpp →
// side_tables.hpp).
struct Params;

// ---------------------------------------------------------------------------
// Q5SideTables — three small in-process hashmaps resolved before each query.

struct Q5SideTables {
   Integer  region_key = -1;  // r_regionkey for the chosen region name

   // n_nationkey → presence: ~5 entries for the chosen region.
   std::unordered_set<Integer> nation_set;

   // n_nationkey → n_name: same ~5 entries as nation_set.
   std::unordered_map<Integer, std::string> n_name_map;

   // s_suppkey → s_nationkey: pre-filtered to suppliers in nation_set (~2K at SF=1).
   std::unordered_map<Integer, Integer> supplier_nation;
};

// ---------------------------------------------------------------------------
// build_q5_side_tables — populates `out` in three sequential adapter scans.
//
// Throws std::runtime_error if params.region does not match any REGION row
// (region name out of the TPC-H domain is a programming error, not a runtime
// condition — all five TPC-H regions are hardcoded in tpch_tables.hpp).

template <typename Backend>
void build_q5_side_tables(
    typename Backend::template Adapter<region_t>&   region,
    typename Backend::template Adapter<nation_t>&   nation,
    typename Backend::template Adapter<supplier_t>& supplier,
    const Params&                                    params,
    Q5SideTables&                                    out)
{
   out = Q5SideTables{};  // clear previous state

   // ------------------------------------------------------------------
   // Step 1: REGION scan — find the first row matching params.region.
   // REGION has only 5 rows, so a full scan is negligible.

   bool region_found = false;
   auto region_scan_cb = [&](const region_t::Key& k, const region_t& r) -> bool {
      auto rv  = std::string_view(r.r_name.data, r.r_name.length);
      auto pv  = std::string_view(params.region.data, params.region.length);
      if (rv == pv) {
         out.region_key = k.r_regionkey;
         region_found   = true;
         return false;  // stop scanning after first match
      }
      return true;  // continue
   };
   region.scan(region_t::Key{0}, region_scan_cb, []() {});

   if (!region_found) {
      throw std::runtime_error(
          std::string("Q5: region '")
          + std::string(params.region.data, params.region.length)
          + "' not found in REGION table");
   }

   // ------------------------------------------------------------------
   // Step 2: NATION scan — collect rows where n_regionkey == region_key.

   auto nation_scan_cb = [&](const nation_t::Key& k, const nation_t& n) -> bool {
      if (n.n_regionkey == out.region_key) {
         out.nation_set.insert(k.n_nationkey);
         out.n_name_map.emplace(k.n_nationkey,
                                std::string(n.n_name.data, n.n_name.length));
      }
      return true;  // always continue — all 25 nations must be checked
   };
   nation.scan(nation_t::Key{0}, nation_scan_cb, []() {});

   // ------------------------------------------------------------------
   // Step 3: SUPPLIER scan — insert (s_suppkey → s_nationkey) only for
   // suppliers whose nation is in the in-region set.  Pre-filtering here
   // keeps the map at ~2K entries instead of 10K.

   auto supplier_scan_cb = [&](const supplier_t::Key& k, const supplier_t& s) -> bool {
      if (out.nation_set.count(s.s_nationkey)) {
         out.supplier_nation.emplace(k.s_suppkey, s.s_nationkey);
      }
      return true;
   };
   supplier.scan(supplier_t::Key{0}, supplier_scan_cb, []() {});
}

}  // namespace tpch::q5
