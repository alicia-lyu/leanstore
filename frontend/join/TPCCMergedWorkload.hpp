#pragma once
#include <gflags/gflags.h>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <type_traits>
#include "../tpc-c/TPCCWorkload.hpp"
#include "ExperimentHelper.hpp"
#include "Join.hpp"
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"
#include "Units.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"

template <template <typename> class AdapterType, class MergedAdapterType>
class TPCCMergedWorkload : public TPCCBaseWorkload<AdapterType>
{
   using Base = TPCCBaseWorkload<AdapterType>;
   using orderline_sec_t = typename Base::orderline_sec_t;
   using joined_t = typename Base::joined_t;
   MergedAdapterType& merged;

   std::vector<std::pair<typename joined_t::Key, joined_t>> cartesianProducts(
       std::vector<std::pair<typename orderline_sec_t::Key, orderline_sec_t>>& cached_left,
       std::vector<std::pair<stock_t::Key, stock_t>>& cached_right)
   {
      std::vector<std::pair<typename joined_t::Key, joined_t>> results;
      results.reserve(cached_left.size() * cached_right.size());  // Reserve memory to avoid reallocations

      for (auto& left : cached_left) {
         for (auto& right : cached_right) {
            results.push_back(MergeJoin<orderline_sec_t, stock_t, joined_t>::merge_records(left.first, left.second, right.first, right.second));
         }
      }
      return results;
   }

  public:
   TPCCMergedWorkload(TPCCWorkload<AdapterType>* tpcc, MergedAdapterType& merged) : TPCCBaseWorkload<AdapterType>(tpcc), merged(merged) {}

   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      stock_t::Key start_key = {w_id, 0};  // Starting from the first item in the warehouse

      atomic<uint64_t> scanCardinality = 0;
      uint64_t produced = 0;

      stock_t::Key current_key = start_key;

      std::vector<std::pair<typename orderline_sec_t::Key, orderline_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      // cached_left.reserve(100);  // Multiple order lines can be associated with a single stock item
      // cached_right.reserve(2);

      // std::vector<std::pair<joined_t::Key, joined_t>> results; // Assuming we don't want the final results per se, but only scan it with
      // some predicate / task, similar to btree->scan

      merged.template scan<stock_t, orderline_sec_t>(
          start_key,
          [&](const stock_t::Key& key, const stock_t& rec) {
             ++scanCardinality;
             if (key.s_w_id != w_id) {
                return false;
             }
             if (key.s_i_id != current_key.s_i_id) {
                // A new join key discovered
                // Do a cartesian product of current cached rows
                // Calculating cartesian product could be done parallel to the scan. But I guess now it's fine too because the CPU is not saturated.
                auto cartesian_products = cartesianProducts(cached_left, cached_right);
                produced += cartesian_products.size();
                current_key = key;
                cached_left.clear();
                cached_right.clear();
             }
             cached_right.push_back({key, rec});
             return true;
          },
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& rec) {
             ++scanCardinality;
             if (key.ol_w_id != w_id) {
                return false;
             }
             if (key.ol_d_id != d_id) {
                return true;  // next item may still be in the same district
             }
             if (key.ol_i_id != current_key.s_i_id) {  // only happens when current i_id does not have any stock records
                return true;
             }
             if constexpr (std::is_same_v<orderline_sec_t, ol_join_sec_t>) {
                ol_join_sec_t expanded_rec = rec.expand();
                if (expanded_rec.ol_delivery_d < since) {
                   return true;  // Skip this record
                }
             } else {
                bool since_flag = true;
                this->tpcc->orderline.lookup1({w_id, d_id, key.ol_o_id, key.ol_number}, [&](const orderline_t& ol_rec) {
                   if (!(ol_rec.ol_delivery_d < since)) {
                      since_flag = false;
                   }
                });
                if (!since_flag)
                   return true;
             }

             cached_left.push_back({key, rec});
             return true;  // continue scan
          },
          []() { /* undo */ });

      // Final cartesian product for any remaining cached elements
      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      produced += final_cartesian_products.size();
      // results.insert(results.end(), final_cartesian_products.begin(), final_cartesian_products.end());

      // std::cout << "Scan cardinality: " << scanCardinality.load() << ", Produced: " << produced << std::endl;
      // All default configs, dram_gib = 8, cardinality = 385752
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<std::pair<typename joined_t::Key, joined_t>> results;

      std::vector<std::pair<typename orderline_sec_t::Key, orderline_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      uint64_t lookupCardinality = 0;

      if (!FLAGS_locality_read) {  // Search separately when there is an additional key to the join key
         merged.template scan<stock_t, orderline_sec_t>(
             stock_t::Key{w_id, i_id},
             [&](const stock_t::Key& key, const stock_t& rec) {
                ++lookupCardinality;
                if (key.s_w_id != w_id || key.s_i_id != i_id) {
                   return false;
                }
                cached_right.push_back({key, rec});
                return true;
             },
             [&](const orderline_sec_t::Key&, const orderline_sec_t&) {
                ++lookupCardinality;
                return false;
             },
             []() { /* undo */ });

         if (cached_right.empty()) {
            return;
         }

         merged.template scan<orderline_sec_t, stock_t>(
             typename orderline_sec_t::Key{w_id, i_id, d_id, 0, 0},
             [&](const orderline_sec_t::Key& key, const orderline_sec_t& rec) {
                ++lookupCardinality;
                if (key.ol_w_id != w_id || key.ol_i_id != i_id || key.ol_d_id != d_id) {
                   return false;
                }
                cached_left.push_back({key, rec});
                return true;
             },
             [&](const stock_t::Key&, const stock_t&) { return false; }, []() { /* undo */ });
      } else {  // Search continously when there is no additional key to the join key
         merged.template scan<stock_t, orderline_sec_t>(
             stock_t::Key{w_id, i_id},
             [&](const stock_t::Key& key, const stock_t& rec) {
                ++lookupCardinality;
                if (key.s_w_id != w_id || key.s_i_id != i_id) {
                   return false;
                }
                cached_right.push_back({key, rec});
                return true;
             },
             [&](const orderline_sec_t::Key& key, const orderline_sec_t& rec) {
                ++lookupCardinality;
                if (key.ol_w_id != w_id || key.ol_i_id != i_id) {
                   return false;
                }
                if (cached_right.empty()) {
                   return false;  // Matching stock record can only be found before the orderline record
                }
                cached_left.push_back({key, rec});
                return true;
             },
             []() { /* undo */ });
      }

      // Final cartesian product if the scan is complete without hitting the end condition
      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      results.insert(results.end(), final_cartesian_products.begin(), final_cartesian_products.end());

      // std::cerr << "Lookup cardinality: " << lookupCardinality << ", stock records: " << cached_right.size() << ", orderline records: " <<
      // cached_left.size() << ", Produced: " << results.size() << std::endl; All default configs, dram_gib = 8, cardinality = 2--8
   }

   void newOrderRnd(Integer w_id, Integer order_size = 5)
   {
      // this->tpcc->newOrderRnd(w_id);
      Integer d_id = this->tpcc->urand(1, 10);
      Integer c_id = this->tpcc->getCustomerID();
      Integer ol_cnt = this->tpcc->urand(order_size, order_size * 3);

      vector<Integer> lineNumbers;
      vector<Integer> supwares;
      vector<Integer> itemids;
      vector<Integer> qtys;

      lineNumbers.reserve(15);
      supwares.reserve(15);
      itemids.reserve(15);
      qtys.reserve(15);

      for (Integer i = 1; i <= ol_cnt; i++) {
         Integer supware = w_id;
         if (!this->tpcc->warehouse_affinity && this->tpcc->urand(1, 100) == 1)  // ATTN:remote transaction
            supware = this->tpcc->urandexcept(1, this->tpcc->warehouseCount, w_id);
         Integer itemid = this->tpcc->getItemID();
         if (false && (i == ol_cnt) && (this->tpcc->urand(1, 100) == 1))  // invalid item => random
            itemid = 0;
         lineNumbers.push_back(i);
         supwares.push_back(supware);
         itemids.push_back(itemid);
         qtys.push_back(this->tpcc->urand(1, 10));
      }

      Timestamp timestamp = this->tpcc->currentTimestamp();

      // this->tpcc->newOrder
      Numeric w_tax = this->tpcc->warehouse.lookupField({w_id}, &warehouse_t::w_tax);
      Numeric c_discount = this->tpcc->customer.lookupField({w_id, d_id, c_id}, &customer_t::c_discount);
      Numeric d_tax;
      Integer o_id;

      UpdateDescriptorGenerator1(district_update_descriptor, district_t, d_next_o_id);
      // UpdateDescriptorGenerator2(district_update_descriptor, district_t, d_next_o_id, d_ytd);
      this->tpcc->district.update1(
          {w_id, d_id},
          [&](district_t& rec) {
             d_tax = rec.d_tax;
             o_id = rec.d_next_o_id++;
          },
          district_update_descriptor);

      Numeric all_local = 1;
      for (Integer sw : supwares)
         if (sw != w_id)
            all_local = 0;
      Numeric cnt = lineNumbers.size();
      Integer carrier_id = 0; /*null*/
      this->tpcc->order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
      if (this->tpcc->order_wdc_index) {
         this->tpcc->order_wdc.insert({w_id, d_id, c_id, o_id}, {});
      }
      this->tpcc->neworder.insert({w_id, d_id, o_id}, {});

      // Batch update stock records
      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         Integer item_id = itemids[i];
         if (!Base::isSelected(item_id)) {
            continue;
         }
         // We don't need the primary index of stock_t at all, since all its info is in merged
         UpdateDescriptorGenerator4(stock_update_descriptor, stock_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
         merged.template update1<stock_t>(
             {supwares[i], itemids[i]},
             [&](stock_t& rec) {
                auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                rec.s_remote_cnt += (supwares[i] != w_id);
                rec.s_order_cnt++;
                rec.s_ytd += qty;
             },
             stock_update_descriptor);
      }

      // Batch insert orderline records
      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = this->tpcc->item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist = this->tpcc->template randomastring<24>(24, 24);
         merged.template tryLookup<stock_t>({w_id, itemid}, [&](const stock_t& rec) {
            switch (d_id) {
               case 1:
                  s_dist = rec.s_dist_01;
                  break;
               case 2:
                  s_dist = rec.s_dist_02;
                  break;
               case 3:
                  s_dist = rec.s_dist_03;
                  break;
               case 4:
                  s_dist = rec.s_dist_04;
                  break;
               case 5:
                  s_dist = rec.s_dist_05;
                  break;
               case 6:
                  s_dist = rec.s_dist_06;
                  break;
               case 7:
                  s_dist = rec.s_dist_07;
                  break;
               case 8:
                  s_dist = rec.s_dist_08;
                  break;
               case 9:
                  s_dist = rec.s_dist_09;
                  break;
               case 10:
                  s_dist = rec.s_dist_10;
                  break;
               default:
                  exit(1);
                  throw;
            }
         });
         Numeric ol_amount = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
         Timestamp ol_delivery_d = 0;  // NULL
         this->tpcc->orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
         // ********** Update Merged Index **********
         if constexpr (std::is_same_v<orderline_sec_t, ol_join_sec_t>) {
            merged.template insert<ol_join_sec_t>({w_id, itemid, d_id, o_id, lineNumber}, {supware, ol_delivery_d, qty, ol_amount, s_dist});
         } else {
            merged.template insert<ol_sec_key_only_t>({w_id, itemid, d_id, o_id, lineNumber}, {});
         }
      }
   }

   void loadStockToMerged(Integer w_id, Integer semijoin_selectivity = 100)  // TODO
   {
      std::cout << "Loading stock of warehouse " << w_id << " to merged" << std::endl;
      for (Integer i = 0; i < this->tpcc->ITEMS_NO * this->tpcc->scale_factor; i++) {
         if (!Base::isSelected(i + 1)) {
            continue;
         }
         Varchar<50> s_data = this->tpcc->template randomastring<50>(25, 50);
         if (this->tpcc->rnd(10) == 0) {
            s_data.length = this->tpcc->rnd(s_data.length - 8);
            s_data = s_data || Varchar<10>("ORIGINAL");
         }
         merged.template insert<stock_t>(
             typename stock_t::Key{w_id, i + 1},
             {this->tpcc->randomNumeric(10, 100), this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24), 0, 0, 0, s_data});
      }
   }

   void loadOrderlineSecondaryToMerged(Integer w_id = std::numeric_limits<Integer>::max())
   {
      std::cout << "Loading orderline secondary index to merged for warehouse " << w_id << std::endl;
      auto orderline_scanner = this->tpcc->orderline.getScanner();
      if (w_id != std::numeric_limits<Integer>::max()) {
         orderline_scanner->seek({w_id, 0, 0, 0});
      }
      while (true) {
         auto ret = orderline_scanner->next();
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         if (key.ol_w_id != w_id)
            break;
         typename orderline_sec_t::Key sec_key = {key.ol_w_id, payload.ol_i_id, key.ol_d_id, key.ol_o_id, key.ol_number};
         if constexpr (std::is_same_v<orderline_sec_t, ol_sec_key_only_t>) {
            merged.template insert<orderline_sec_t>(sec_key, {});
         } else {
            orderline_sec_t sec_payload = {payload.ol_supply_w_id, payload.ol_delivery_d, payload.ol_quantity, payload.ol_amount,
                                           payload.ol_dist_info};
            merged.template insert<orderline_sec_t>(sec_key, sec_payload);
         }
      }
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
         bool ret = merged.template tryLookup<stock_t>({w_id, s_id}, [&](const auto&) {});
         if (!Base::isSelected(s_id)) {
            ensure(!ret);
         } else {
            ensure(ret);
         }
      }

      merged.template scan<stock_t, orderline_sec_t>(
          {w_id, 0}, [&](const stock_t::Key& key, const stock_t&) { return key.s_w_id == w_id; },
          [&](const orderline_sec_t::Key& key, const orderline_sec_t&) { return key.ol_w_id == w_id; }, []() { /* undo */ });
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point t1,
                 std::chrono::steady_clock::time_point t2,
                 leanstore::cr::CRManager& crm)
   {
      std::ofstream csv_file(this->getCsvFile("merged_size.csv"), std::ios::app);

      auto config = ExperimentHelper::getConfigString();
      u64 core_page_count = 0;
      core_page_count = this->getCorePageCount(crm);
      auto core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
      u64 merged_page_count = 0;
      u64 merged_leaf_count = 0;
      u64 merged_height = 0;
      crm.scheduleJobSync(0, [&]() {
         merged_page_count = merged.btree->estimatePages();
         merged_leaf_count = merged.btree->estimateLeafs();
         merged_height = merged.btree->getHeight();
      });
      auto merged_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

      std::cout << "merged_page_count: " << merged_page_count << ", merged_leaf_count: " << merged_leaf_count << ", merged_height: " << merged_height
                << std::endl;

      csv_file << "core," << config << "," << (double)core_page_count * 4096 / 1024 / 1024 / 1024 << "," << core_time << std::endl;
      csv_file << "merged_index," << config << "," << (double)merged_page_count * 4096 / 1024 / 1024 / 1024 << "," << merged_time << std::endl;
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      logSizes(t0, t0, t0, crm);
   }
};
