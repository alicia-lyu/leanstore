#pragma once
#include <cstdint>
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"

template <template <typename> class AdapterType>
class TPCCJoinWorkload
{
   TPCCWorkload<AdapterType>* tpcc;
   AdapterType<joined_ols_t>& joined_ols;

  public:
   TPCCJoinWorkload(TPCCWorkload<AdapterType>* tpcc, AdapterType<joined_ols_t>& joined_ols) : tpcc(tpcc), joined_ols(joined_ols) {}

   void recentOrdersByCustomer(Integer w_id, Integer d_id, Timestamp since)
   {
      vector<joined_ols_t> results;
      joined_ols_t::Key start_key = {w_id, 0, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      uint64_t scanCardinality = 0;

      joined_ols.scan(
          start_key,
          [&](const joined_ols_t::Key& key, const joined_ols_t& rec) {
             scanCardinality++;
             if (rec.ol_delivery_d >= since) {
                results.push_back(rec);
             }
             return true;
          },
          []() { /* undo */ });

      // std::cout << "Scan cardinality: " << scanCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality = 184694
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<joined_ols_t> results;
      joined_ols_t::Key start_key = {w_id, i_id, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      uint64_t lookupCardinality = 0;

      joined_ols.scan(
          start_key,
          [&](const joined_ols_t::Key& key, const joined_ols_t& rec) {
             lookupCardinality++;
             if (key.i_id != i_id) {
                return false;
             }
             results.push_back(rec);
             return true;
          },
          []() {
             // This is executed after the scan completes
          });

      // std::cout << "Lookup cardinality: " << lookupCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality mostly 1 or 2. Can also be 3, etc.
   }

   int tx(Integer w_id)
   {
      u64 rnd = leanstore::utils::RandomGenerator::getRand(0, 4);
      if (rnd == 0) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Integer i_id = leanstore::utils::RandomGenerator::getRand(1, 100001);
         ordersByItemId(w_id, d_id, i_id);
         return 0;
      } else if (rnd == 1) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Timestamp since = 1;
         recentOrdersByCustomer(w_id, d_id, since);
         return 0;
      } else {
         return tpcc->tx(w_id);  // TODO: Change to write queries
      }
   }
};