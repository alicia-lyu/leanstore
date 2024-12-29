#pragma once
#include "Exceptions.hpp"
#include "Join.hpp"
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <numeric>
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "types.hpp"

template <template <typename> class AdapterType, int id_count>
class TPCCJoinWorkload : public TPCCBaseWorkload<AdapterType, id_count>
{
   using Base = TPCCBaseWorkload<AdapterType, id_count>;
   AdapterType<joined_t>& joined_ols;

  public:
   TPCCJoinWorkload(TPCCWorkload<AdapterType>* tpcc,
                    AdapterType<orderline_sec_t>* orderline_secondary,
                    AdapterType<stock_sec_t>* stock_secondary,
                    AdapterType<joined_t>& joined_ols)
       : Base(tpcc, orderline_secondary, stock_secondary), joined_ols(joined_ols)
   {
   }

   void scanJoin(typename joined_t::Key start_key, std::function<bool(const typename joined_selected_t::Key&, const joined_selected_t&)> cb)
   {
      // Must be able to compile with joined1_t and joined_selected_t, requires expand (implemented as UNREACHABLE)
      stock_sec_t::Key stock_key;
      stock_sec_t stock_payload;
      orderline_t::Key orderline_key;
      orderline_t orderline_payload;
      joined_ols.scan(
          start_key,
          [&](const joined_t::Key& key, const joined_t& payload) {
             PROCESS_PAYLOAD();
             return cb(key, selected_payload);
          },
          []() {});
   }

   // When this query can be realistic: Keep track of stock information for recent orders
   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      // vector<joined_t> results;
      typename joined_t::Key start_key = {w_id, 0, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      atomic<uint64_t> scanCardinality = 0;
      [[maybe_unused]] uint64_t resultsCardinality = 0;

      scanJoin(start_key, [&](const typename joined_selected_t::Key& key, const joined_selected_t& rec) {
         if (key.w_id != w_id) {
            return false;
         }
         scanCardinality++;
         if (rec.ol_delivery_d > since || key.ol_d_id == d_id) {
            resultsCardinality++;
         }
         return true;
      });
   }

   // When this query can be realistic: Find all orderlines and stock level for a specific item. Act on those orders according to the information.
   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      std::vector<joined_selected_t> results;
      typename joined_t::Key start_key = {w_id, i_id, FLAGS_locality_read ? 0 : d_id, 0, 0};

      [[maybe_unused]] uint64_t lookupCardinality = 0;

      scanJoin(start_key, [&](const joined_selected_t::Key& key, const joined_selected_t& rec) {
         if (key.w_id != w_id || key.i_id != i_id) {
            return false;
         }
         if (!FLAGS_locality_read && key.ol_d_id != d_id) {
            return false;
         }
         results.push_back(rec);
         lookupCardinality++;
         return true;
      });
   }

   void newOrderRnd(Integer w_id, Integer order_size = 5)
   {
      Base::newOrderRndCallback(
          w_id,
          [&](const stock_sec_t::Key& key, std::function<void(stock_sec_t&)> cb, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor,
              Integer qty) {
             if (Base::isSelected(key.s_i_id)) {
                this->tpcc->stock.update1(key, cb, update_descriptor);
             }
             if (!FLAGS_outer_join && !Base::isSelected(key.s_i_id)) {
                return;
             }
             // Updating stock causes join results to be updated
             if constexpr (std::is_same_v<joined_t, joined1_t>) {
                std::vector<joined1_t::Key> keys;
                joined_ols.scan(
                    {key.s_w_id, key.s_i_id, 0, 0, 0},
                    [&](const joined1_t::Key& joined_key, const joined1_t&) {
                       if (joined_key.w_id != key.s_w_id || joined_key.i_id != key.s_i_id) {
                          return false;
                       }
                       keys.push_back(joined_key);
                       return true;
                    },
                    [&]() { /* undo */ });
                UpdateDescriptorGenerator4(joined_ols_descriptor, joined1_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
                for (auto key : keys) {
                   joined_ols.update1(
                       key,
                       [&](joined1_t& rec) {
                          auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                          s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                          rec.s_remote_cnt += (key.w_id != w_id);
                          rec.s_order_cnt++;
                          rec.s_ytd += qty;
                       },
                       joined_ols_descriptor);
                }
             }
          },
          [&](const orderline_sec_t::Key& ol_key, const orderline_sec_t& ol_payload, const stock_sec_t::Key& stock_key,
              const stock_sec_t& stock_payload) {
             this->orderline_secondary->insert(ol_key, ol_payload);
             // Inserting orderline causes join results to be inserted
             if (stock_payload == stock_t{} && !FLAGS_outer_join) { // Out of stock
                return;
             }
             auto [joined_key, joined_payload] =
                 MergeJoin<orderline_sec_t, stock_sec_t, joined_t>::merge(ol_key, ol_payload, stock_key, stock_payload);
             joined_ols.insert(joined_key, joined_payload);
          },
          order_size);
   }

   void joinOrderlineAndStock(Integer w_id = std::numeric_limits<Integer>::max())
   {
      Base::joinOrderlineAndStock(
          [&](joined_t::Key& key, joined_t& rec) {
             if (key.w_id != w_id) {
                return false;
             }
             joined_ols.insert(key, rec);
             return true;
          },
          {w_id, 0, 0, 0, 0});
   }

   void verifyWarehouse(Integer w_id)
   {
      // for (Integer w_id = 1; w_id <= warehouseCount; w_id++) {
      std::cout << "Verifying warehouse " << w_id << std::endl;
      this->tpcc->warehouse.lookup1({w_id}, [&](const auto&) {});
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         this->tpcc->district.lookup1({w_id, d_id}, [&](const auto&) {});
         for (Integer c_id = 1; c_id <= this->tpcc->CUSTOMER_SCALE * this->tpcc->scale_factor / 10; c_id++) {
            this->tpcc->customer.lookup1({w_id, d_id, c_id}, [&](const auto&) {});
         }
      }
      for (Integer s_id = 1; s_id <= this->tpcc->ITEMS_NO * this->tpcc->scale_factor; s_id++) {
         bool ret = this->tpcc->stock.tryLookup({w_id, s_id}, [&](const auto&) {});
         if (!Base::isSelected(s_id)) {
            ensure(!ret);
         } else {
            ensure(ret);
         }
      }
      joined_ols.scan({w_id, 0, 0, 0, 0}, [&](const joined_t::Key& key, const auto&) { return key.w_id == w_id; }, []() {});
   }

   void addSizesToCsv(double core_size, uint64_t core_time, double ol_size, uint64_t ol_time, double join_size, uint64_t join_time)
   {
      std::ofstream csv_file(this->getCsvFile("join_size.csv"), std::ios::app);
      auto config = this->getConfigString();
      csv_file << "core," << config << "," << core_size << "," << core_time << std::endl;
      csv_file << "orderline_secondary," << config << "," << ol_size << "," << ol_time << std::endl;
      csv_file << "join_results," << config << "," << join_size << "," << join_time << std::endl;
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point t1,
                 std::chrono::steady_clock::time_point t2,
                 std::chrono::steady_clock::time_point t3,
                 leanstore::cr::CRManager& crm)
   {
      u64 core_page_count = 0;
      core_page_count = this->getCorePageCount(crm);
      auto core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

      auto orderline_secondary_page_count = 0;
      crm.scheduleJobSync(0, [&]() { orderline_secondary_page_count = this->orderline_secondary->estimatePages(); });
      auto orderline_secondary_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

      u64 joined_ols_page_count = 0;

      crm.scheduleJobSync(0, [&]() { joined_ols_page_count = joined_ols.estimatePages(); });
      auto joined_ols_time = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();

      std::cout << "core page count: " << core_page_count << " (" << Base::pageCountToGB(core_page_count)
                << " GB), orderline secondary page count: " << orderline_secondary_page_count << " ("
                << Base::pageCountToGB(orderline_secondary_page_count) << " GB), joined_ols page count: " << joined_ols_page_count << " ("
                << Base::pageCountToGB(joined_ols_page_count) << " GB)" << std::endl;

      addSizesToCsv(Base::pageCountToGB(core_page_count), core_time, Base::pageCountToGB(orderline_secondary_page_count), orderline_secondary_time,
                    Base::pageCountToGB(joined_ols_page_count), joined_ols_time);
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point t1,
                 std::chrono::steady_clock::time_point t2,
                 std::chrono::steady_clock::time_point t3,
                 RocksDB& map)
   {
      std::array<uint64_t, id_count> sizes = this->compactAndGetSizes(map);

      uint64_t core_size = std::accumulate(sizes.begin(), sizes.begin() + 11, 0);

      uint64_t ol_size = sizes.at(11);
      uint64_t join_size = sizes.at(12);

      auto core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
      auto ol_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
      auto join_time = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();

      addSizesToCsv(Base::byteToGB(core_size), core_time, Base::byteToGB(ol_size), ol_time, Base::byteToGB(join_size), join_time);
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      auto t1 = std::chrono::steady_clock::now();
      auto t2 = std::chrono::steady_clock::now();
      auto t3 = std::chrono::steady_clock::now();
      logSizes(t0, t1, t2, t3, crm);
   }

   void logSizes(RocksDB& map)
   {
      auto t0 = std::chrono::steady_clock::now();
      auto t1 = std::chrono::steady_clock::now();
      auto t2 = std::chrono::steady_clock::now();
      auto t3 = std::chrono::steady_clock::now();
      logSizes(t0, t1, t2, t3, map);
   }
};