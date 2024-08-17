#pragma once
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

   // When this query can be realistic: Keep track of stock information for recent orders
   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      // vector<joined_t> results;
      typename joined_t::Key start_key = {w_id, 0, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      atomic<uint64_t> scanCardinality = 0;
      uint64_t resultsCardinality = 0;

      joined_ols.scan(
          start_key,
          [&](const joined_t::Key& key, const joined_t& rec) {
             ++scanCardinality;
             if (key.w_id != w_id)
                return false;  // passed
             if (key.ol_d_id != d_id)
                return true;  // skip this
             if constexpr (std::is_same_v<joined_t, joined_ols_t>) {
                joined_ols_t joined_rec = rec.expand();
                if (joined_rec.ol_delivery_d >= since) {
                   //  results.push_back(rec);
                   resultsCardinality++;
                }
             } else {
                this->tpcc->orderline.lookup1({key.w_id, key.ol_d_id, key.ol_o_id, key.ol_number},
                                              [&](const orderline_t& orderline_rec) {  // ATTN: BTree operation in call back function
                                                 if (orderline_rec.ol_delivery_d >= since) {
                                                    this->tpcc->stock.lookup1({key.w_id, key.i_id},
                                                                              [&](const stock_t&) {  // ATTN: BTree operation in call back function
                                                                                 // Only emulating BTree operations, not concatenating stock_rec and
                                                                                 // joined_rec
                                                                                 //  results.push_back(rec);
                                                                                 resultsCardinality++;
                                                                              });
                                                 }
                                              });
             }
             return true;
          },
          []() { /* undo */ });

      // std::cout << "Scan cardinality: " << scanCardinality.load() << ", results cardinality: " << resultsCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality = 184694
   }

   // When this query can be realistic: Find all orderlines and stock level for a specific item. Act on those orders according to the information.
   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<joined_t> results;
      typename joined_t::Key start_key;  // Starting from the first item in the warehouse and district
      if (FLAGS_locality_read) {         // No additional key than the join key
         start_key = {w_id, i_id, 0, 0, 0};
      } else {
         start_key = {w_id, i_id, d_id, 0, 0};
      }

      uint64_t lookupCardinality = 0;

      joined_ols.scan(
          start_key,
          [&](const joined_t::Key& key, const joined_t& rec) {
             ++lookupCardinality;
             if (key.i_id != i_id || key.w_id != w_id) {  // passed
                return false;
             }
             if (!FLAGS_locality_read && key.ol_d_id != d_id) {
                return false;
             }
             if constexpr (std::is_same_v<joined_t, joined_ols_t>) {
                results.push_back(rec);
             } else {
                this->tpcc->stock.lookup1({key.w_id, key.i_id}, [&](const stock_t&) {});
                this->tpcc->orderline.lookup1({key.w_id, key.ol_d_id, key.ol_o_id, key.ol_number}, [&](const orderline_t&) {});
                results.push_back(rec);
             }
             return true;
          },
          []() {
             // This is executed after the scan completes
          });

      // std::cerr << "Lookup cardinality: " << lookupCardinality << ", results cardinality: " << results.size() << std::endl;
   }

   void newOrderRnd(Integer w_id, Integer order_size = 5)
   {
      // this->tpcc->newOrderRnd(w_id);
      Integer d_id = this->tpcc->urand(1, 10);
      Integer c_id = this->tpcc->getCustomerID();
      Integer ol_cnt = this->tpcc->urand(order_size, order_size * 3);

      vector<Integer> lineNumbers;
      lineNumbers.reserve(15);
      vector<Integer> supwares;
      supwares.reserve(15);
      vector<Integer> itemids;
      itemids.reserve(15);
      vector<Integer> qtys;
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

      // std::cout << "New order: w_id: " << w_id << ", d_id: " << d_id << ", o_id: " << o_id << ", c_id: " << c_id << ", timestamp: " << timestamp <<
      // std::endl;

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

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         Integer item_id = itemids[i];
         if (!Base::isSelected(item_id)) {
            continue;
         }
         UpdateDescriptorGenerator4(stock_update_descriptor, stock_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
         this->tpcc->stock.update1(
             {supwares[i], itemids[i]},
             [&](stock_t& rec) {
                auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                rec.s_remote_cnt += (supwares[i] != w_id);
                rec.s_order_cnt++;
                rec.s_ytd += qty;
             },
             stock_update_descriptor);
         // ********** Update to stock_t causes updates to join results **********
         // std::cout << "Line number #" << i << ": Updating stock_t causes updates to join results" << std::endl;
         if constexpr (std::is_same_v<joined_t, joined_ols_t>) {
            std::vector<joined_ols_t::Key> keys;
            joined_ols.scan(
                {supwares[i], itemids[i], 0, 0, 0},
                [&](const joined_ols_t::Key& key, const joined_ols_t&) {
                   if (key.w_id != supwares[i] || key.i_id != itemids[i]) {
                      return false;
                   }
                   keys.push_back(key);
                   return true;
                },
                [&]() { /* undo */ });
            // std::cout << "Line number #" << i << ": Updating join results" << std::endl;
            UpdateDescriptorGenerator4(joined_ols_descriptor, joined_ols_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
            for (auto key : keys) {
               // if (key.ol_o_id == o_id && key.ol_d_id == d_id && key.w_id == w_id) {
               //    std::cout << "newOrderRnd: duplicate key: " << key << std::endl;
               //    // throw std::runtime_error("newOrderRnd: duplicate key");
               //    jumpmu::jump();
               // }
               joined_ols.update1(
                   key,
                   [&](joined_ols_t& rec) {
                      auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                      s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                      rec.s_remote_cnt += (supwares[i] != w_id);
                      rec.s_order_cnt++;
                      rec.s_ytd += qty;
                   },
                   joined_ols_descriptor);
            }
         }
      }

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = this->tpcc->item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist = this->tpcc->template randomastring<24>(24, 24);
         stock_t stock_rec;
         bool ret = this->tpcc->stock.tryLookup({w_id, itemid}, [&](const stock_t& rec) {
            stock_rec = rec;
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
         // TODO: i_data, s_data
         // ********** Update Secondary Index **********
         // std::cout << "Line number #" << i << ": Updating secondary index" << std::endl;
         if constexpr (std::is_same_v<orderline_sec_t, ol_sec_key_only_t>) {
            this->orderline_secondary->insert({w_id, itemid, d_id, o_id, lineNumber}, {});
         } else {
            ol_join_sec_t payload = {supware, ol_delivery_d, qty, ol_amount, s_dist};
            this->orderline_secondary->insert({w_id, itemid, d_id, o_id, lineNumber}, payload);
         }
         // ********** Update Join Results **********
         // std::cout << "Line number #" << i << ": Updating join results" << std::endl;
         if (ret) {
            if constexpr (std::is_same_v<joined_t, joined_ols_t>) {
               joined_ols_t joined_rec = {supware,
                                          ol_delivery_d,
                                          qty,
                                          ol_amount,
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
               joined_ols.insert({w_id, itemid, d_id, o_id, lineNumber}, joined_rec);
            } else {
               joined_ols.insert({w_id, itemid, d_id, o_id, lineNumber}, {});
            }
         }
      }
   }

   void joinOrderlineAndStock(Integer w_id = std::numeric_limits<Integer>::max())
   {
      Base::joinOrderlineAndStockOnTheFly([&](joined_t::Key& key, joined_t& rec) {
         joined_ols.insert(key, rec);
      }, w_id);
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

      csv_file << "core," << config << "," << (double)core_page_count * 4098 / 1024 / 1024 / 1024 << "," << core_time << std::endl;
      csv_file << "orderline_secondary," << config << "," << (double)orderline_secondary_page_count * 4098 / 1024 / 1024 / 1024 << ","
               << orderline_secondary_time << std::endl;
      csv_file << "join_results," << config << "," << (double)joined_ols_page_count * 4098 / 1024 / 1024 / 1024 << "," << joined_ols_time
               << std::endl;
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      logSizes(t0, t0, t0, t0, crm);
   }
};