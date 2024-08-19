#pragma once
#include "Exceptions.hpp"
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include "ExperimentHelper.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/utils/JumpMU.hpp"

template <template <typename> class AdapterType>
class TPCCJoinWorkload : public TPCCBaseWorkload<AdapterType>
{
   using Base = TPCCBaseWorkload<AdapterType>;
   using orderline_sec_t = typename Base::orderline_sec_t;
   using joined_t = typename Base::joined_t;
   AdapterType<joined_t>& joined_ols;

  public:
   TPCCJoinWorkload(TPCCWorkload<AdapterType>* tpcc, AdapterType<orderline_sec_t>* orderline_secondary, AdapterType<joined_t>& joined_ols)
       : TPCCBaseWorkload<AdapterType>(tpcc, orderline_secondary), joined_ols(joined_ols)
   {
   }

   void scanJoin(typename joined_t::Key start_key, std::function<bool(const typename joined_selected_t::Key&, const joined_selected_t&)> cb)
      requires (std::same_as<joined_t, joined1_t> || std::same_as<joined_t, joined_selected_t>)
   {
      joined_ols.scan(
          start_key,
          [&](const joined_t::Key& key, const joined_t& rec) {
             auto selected_rec = rec.toSelected();
             cb(key, selected_rec);
          },
          []() {});
   }

   void scanJoin(typename joined_t::Key start_key, std::function<void(const typename joined_selected_t::Key&, const joined_selected_t&)> cb)
      requires std::same_as<joined_t, joined0_t>
   {
      stock_t::Key stock_key;
      stock_t stock_payload;
      orderline_t::Key orderline_key;
      orderline_t orderline_payload;
      joined_ols.scan(
          start_key,
          [&](const joined_t::Key& key, const joined_t& rec) {
             if (stock_key.s_w_id != key.w_id || stock_key.s_i_id != key.i_id) {
                stock_key = {key.w_id, key.i_id};
                this->tpcc->stock.lookup1(stock_key, [&](const stock_t& rec) { stock_payload = rec; });
             }
             if (orderline_key.ol_w_id != key.w_id || orderline_key.ol_d_id != key.ol_d_id || orderline_key.ol_o_id != key.ol_o_id ||
                 orderline_key.ol_number != key.ol_number) {
                orderline_key = {key.w_id, key.ol_d_id, key.ol_o_id, key.ol_number};
                this->tpcc->orderline.lookup1(orderline_key, [&](const orderline_t& rec) { orderline_payload = rec; });
             }
             auto selected_payload = rec.expand(key, stock_payload, orderline_payload);
             cb(key, selected_payload);
          },
          []() {});
   }

      // When this query can be realistic: Keep track of stock information for recent orders
   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      // vector<joined_t> results;
      typename joined_t::Key start_key = {w_id, 0, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      atomic<uint64_t> scanCardinality = 0;
      uint64_t resultsCardinality = 0;

      scanJoin(start_key, [&](const typename joined_t::Key& key, const joined_t& rec) {
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

      uint64_t lookupCardinality = 0;

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
          [&](const stock_t::Key& key, std::function<void(stock_t&)> cb, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor, Integer qty) {
             this->tpcc->stock.update1(key, cb, update_descriptor);
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
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& payload) {
             this->orderline_secondary->insert(key, payload);
             // Inserting orderline causes join results to be inserted
             stock_t stock_rec;
             bool ret = this->tpcc->stock.tryLookup({key.ol_w_id, key.ol_i_id}, [&](const stock_t& rec) { stock_rec = rec; });
             if (ret) {
                if constexpr (std::is_same_v<joined_t, joined1_t>) {
                   ol_sec1_t expanded_payload = payload;
                   joined1_t joined_rec = {expanded_payload.ol_supply_w_id,
                                              expanded_payload.ol_delivery_d,
                                              expanded_payload.ol_quantity,
                                              expanded_payload.ol_amount,
                                              stock_rec.s_quantity,
                                              stock_rec.s_dist_01,
                                              stock_rec.s_dist_02,
                                              stock_rec.s_dist_03,
                                              stock_rec.s_dist_04,
                                              stock_rec.s_dist_05,
                                              stock_rec.s_dist_06,
                                              stock_rec.s_dist_07,
                                              stock_rec.s_dist_08,
                                              stock_rec.s_dist_09,
                                              stock_rec.s_dist_10,
                                              stock_rec.s_ytd,
                                              stock_rec.s_order_cnt,
                                              stock_rec.s_remote_cnt,
                                              stock_rec.s_data};
                   joined_ols.insert(key, joined_rec);
                } else {
                   joined_ols.insert(key, {});
                }
             }
          },
          order_size);
   }

   void joinOrderlineAndStock(Integer w_id = std::numeric_limits<Integer>::max())
   {
      Base::joinOrderlineAndStockOnTheFly(
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

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point t1,
                 std::chrono::steady_clock::time_point t2,
                 std::chrono::steady_clock::time_point t3,
                 leanstore::cr::CRManager& crm)
   {
      // return; // LATER
      std::ofstream csv_file(this->getCsvFile("join_size.csv"), std::ios::app);
      auto config = ExperimentHelper::getConfigString();
      u64 core_page_count = 0;
      core_page_count = this->getCorePageCount(crm);
      auto core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

      auto orderline_secondary_page_count = 0;
      crm.scheduleJobSync(0, [&]() { orderline_secondary_page_count = this->orderline_secondary->estimatePages(); });
      auto orderline_secondary_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

      u64 joined_ols_page_count = 0;
      u64 joined_ols_leaf_count = 0;
      u64 joined_ols_height = 0;

      crm.scheduleJobSync(0, [&]() {
         joined_ols_page_count = joined_ols.estimatePages();
         joined_ols_leaf_count = joined_ols.estimateLeafs();
      });
      auto joined_ols_time = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();

      std::cout << "joined_ols_page_count: " << joined_ols_page_count << ", joined_ols_leaf_count: " << joined_ols_leaf_count
                << ", joined_ols_height: " << joined_ols_height << std::endl;

      csv_file << "core," << config << "," << Base::pageCountToGB(core_page_count) << "," << core_time << std::endl;
      csv_file << "orderline_secondary," << config << "," << Base::pageCountToGB(orderline_secondary_page_count) << "," << orderline_secondary_time
               << std::endl;
      csv_file << "join_results," << config << "," << Base::pageCountToGB(joined_ols_page_count) << "," << joined_ols_time << std::endl;
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      logSizes(t0, t0, t0, t0, crm);
   }
};