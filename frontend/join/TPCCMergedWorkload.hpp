#pragma once
#include <cstdint>
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"

template <template <typename> class AdapterType, class MergedAdapterType>
class TPCCMergedWorkload
{
   TPCCWorkload<AdapterType>* tpcc;
   MergedAdapterType& merged;

   std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> cartesianProducts(std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>>& cached_left,
                                                                             std::vector<std::pair<stock_t::Key, stock_t>>& cached_right)
   {
      std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;
      results.reserve(cached_left.size() * cached_right.size()); // Reserve memory to avoid reallocations

      for (auto& left : cached_left) {
         for (auto& right : cached_right) {
            joined_ols_t::Key joined_key = {left.first.ols_w_id, left.first.ols_i_id, left.first.ols_d_id, left.first.ols_o_id,
                                            left.first.ols_number};

            joined_ols_t joined_rec = {0,
                                       0,
                                       0,
                                       0,
                                       right.second.s_quantity,
                                       right.second.s_dist_01,
                                       right.second.s_dist_02,
                                       right.second.s_dist_03,
                                       right.second.s_dist_04,
                                       right.second.s_dist_05,
                                       right.second.s_dist_06,
                                       right.second.s_dist_07,
                                       right.second.s_dist_08,
                                       right.second.s_dist_09,
                                       right.second.s_dist_10,
                                       right.second.s_ytd,
                                       right.second.s_order_cnt,
                                       right.second.s_remote_cnt,
                                       right.second.s_data};

            tpcc->orderline.lookup1({left.first.ols_w_id, left.first.ols_d_id, left.first.ols_o_id, left.first.ols_number},
                                    [&](const orderline_t& orderline_rec) {
                                       joined_rec.ol_supply_w_id = orderline_rec.ol_supply_w_id;
                                       joined_rec.ol_delivery_d = orderline_rec.ol_delivery_d;
                                       joined_rec.ol_quantity = orderline_rec.ol_quantity;
                                       joined_rec.ol_amount = orderline_rec.ol_amount;
                                    });

            results.push_back({joined_key, joined_rec});
         }
      }
      return results;
   }

  public:
   TPCCMergedWorkload(TPCCWorkload<AdapterType>* tpcc, MergedAdapterType& merged) : tpcc(tpcc), merged(merged) {}

   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      stock_t::Key start_key = {w_id, 0};  // Starting from the first item in the warehouse

      uint64_t scanCardinality = 0;

      stock_t::Key current_key = start_key;

      std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      cached_left.reserve(100); // Multiple order lines can be associated with a single stock item
      cached_right.reserve(2);

      std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;

      merged.template scan<stock_t, ol_join_sec_t>(
          start_key,
          [&](const stock_t::Key& key, const stock_t& rec) {
             ++scanCardinality;
             if (key.s_w_id != current_key.s_w_id || key.s_i_id != current_key.s_i_id) {
                // A new join key discovered
                // Do a cartesian product of current cached rows
                auto cartesian_products = cartesianProducts(cached_left, cached_right);
                results.insert(results.end(), cartesian_products.begin(), cartesian_products.end());
                // Start a new group
                current_key = key;
                cached_left.clear();
                cached_right.clear();
             }
             cached_right.push_back({key, rec});
             return true;
          },
          [&](const ol_join_sec_t::Key& key, const ol_join_sec_t& rec) {
             ++scanCardinality;
             bool since_condition = false;
             tpcc->orderline.lookup1({key.ols_w_id, key.ols_d_id, key.ols_o_id, key.ols_number},
                                                [&](const orderline_t& orderline_rec) { since_condition = orderline_rec.ol_delivery_d >= since; });
             if (!since_condition || key.ols_d_id != d_id) {
                return true;  // continue scan
             }
             cached_left.push_back({key, rec});
             return true;  // continue scan
          },
          []() { /* undo */ });

      // Final cartesian product for any remaining cached elements
      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      results.insert(results.end(), final_cartesian_products.begin(), final_cartesian_products.end());

      // std::cout << "Scan cardinality: " << scanCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality = 385752
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;
      stock_t::Key start_key = {w_id, i_id};
      // Either type can be start key, as the prefix join key will be extracted in scan
      // But as soon as you choose a type for start_key, you must put the same type in the first cb

      std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      cached_left.reserve(100);
      cached_right.reserve(2);

      uint64_t lookupCardinality = 0;

      merged.template scan<stock_t, ol_join_sec_t>(
          start_key,
          [&](const stock_t::Key& key, const stock_t& rec) {
             ++lookupCardinality;
             if (key.s_w_id != w_id || key.s_i_id != i_id) {
                // Finish scanning, do a cartesian product
                auto cartesian_products = cartesianProducts(cached_left, cached_right);
                results.insert(results.end(), cartesian_products.begin(), cartesian_products.end());
                return false;
             }
             cached_right.push_back({key, rec});
             return true;
          },
          [&](const ol_join_sec_t::Key& key, const ol_join_sec_t& rec) {
             ++lookupCardinality;
             assert(key.ols_i_id == i_id && key.ols_w_id == w_id);
             // Change only occur at a stock entry
             if (key.ols_d_id != d_id) {
                return true;  // continue scan
             }
             cached_left.push_back({key, rec});
             return true;  // continue scan
          },
          []() { /* undo */ });

      // Final cartesian product if the scan is complete without hitting the end condition
      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      results.insert(results.end(), final_cartesian_products.begin(), final_cartesian_products.end());

      // std::cout << "Lookup cardinality: " << lookupCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality = 2--8
   }

   void newOrderRnd(Integer w_id)
   {
      // tpcc->newOrderRnd(w_id);
      Integer d_id = tpcc->urand(1, 10);
      Integer c_id = tpcc->getCustomerID();
      Integer ol_cnt = tpcc->urand(5, 15);

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
         if (!tpcc->warehouse_affinity && tpcc->urand(1, 100) == 1)  // ATTN:remote transaction
            supware = tpcc->urandexcept(1, tpcc->warehouseCount, w_id);
         Integer itemid = tpcc->getItemID();
         if (false && (i == ol_cnt) && (tpcc->urand(1, 100) == 1))  // invalid item => random
            itemid = 0;
         lineNumbers.push_back(i);
         supwares.push_back(supware);
         itemids.push_back(itemid);
         qtys.push_back(tpcc->urand(1, 10));
      }

      Timestamp timestamp = tpcc->currentTimestamp();

      // tpcc->newOrder
      Numeric w_tax = tpcc->warehouse.lookupField({w_id}, &warehouse_t::w_tax);
      Numeric c_discount = tpcc->customer.lookupField({w_id, d_id, c_id}, &customer_t::c_discount);
      Numeric d_tax;
      Integer o_id;

      UpdateDescriptorGenerator1(district_update_descriptor, district_t, d_next_o_id);
      // UpdateDescriptorGenerator2(district_update_descriptor, district_t, d_next_o_id, d_ytd);
      tpcc->district.update1(
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
      tpcc->order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
      if (tpcc->order_wdc_index) {
         tpcc->order_wdc.insert({w_id, d_id, c_id, o_id}, {});
      }
      tpcc->neworder.insert({w_id, d_id, o_id}, {});

      // Batch update stock records
      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
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

         Numeric i_price = tpcc->item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist;
         merged.template lookup1<stock_t>({w_id, itemid}, [&](const stock_t& rec) {
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
         tpcc->orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
         // ********** Update Merged Index **********
         merged.template insert<ol_join_sec_t>({w_id, itemid, d_id, o_id, lineNumber}, {});
      }
   }

   void loadStockToMerged(Integer w_id)
   {
      for (Integer i = 0; i < tpcc->ITEMS_NO * tpcc->scale_factor; i++) {
         Varchar<50> s_data = tpcc->template randomastring<50>(25, 50);
         if (tpcc->rnd(10) == 0) {
            s_data.length = tpcc->rnd(s_data.length - 8);
            s_data = s_data || Varchar<10>("ORIGINAL");
         }
         merged.template insert<stock_t>(typename stock_t::Key{w_id, i + 1},
                       {tpcc->randomNumeric(10, 100), tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24),
                        tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24),
                        tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24),
                        tpcc->template randomastring<24>(24, 24), tpcc->template randomastring<24>(24, 24), 0, 0, 0, s_data});
      }
   }

   void verifyWarehouse(Integer w_id)
   {
      // for (Integer w_id = 1; w_id <= warehouseCount; w_id++) {
      tpcc->warehouse.lookup1({w_id}, [&](const auto&) {});
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         for (Integer c_id = 1; c_id <= tpcc->CUSTOMER_SCALE * tpcc->scale_factor; c_id++) {
            tpcc->customer.lookup1({w_id, d_id, c_id}, [&](const auto&) {});
         }
      }
      for (Integer s_id = 1; s_id <= tpcc->ITEMS_NO * tpcc->scale_factor; s_id++) {
         merged.template lookup1<stock_t>({w_id, s_id}, [&](const auto&) {});
      }
   }

   int tx(Integer w_id, int read_percentage, int scan_percentage, int write_percentage)
   {
      u64 rnd = leanstore::utils::RandomGenerator::getRand(0, read_percentage + scan_percentage + write_percentage);
      if (rnd < read_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Integer i_id = leanstore::utils::RandomGenerator::getRand(1, 100001);
         ordersByItemId(w_id, d_id, i_id);
         return 0;
      } else if (rnd < read_percentage + scan_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Timestamp since = 1;
         recentOrdersStockInfo(w_id, d_id, since);
         return 0;
      } else {
         newOrderRnd(w_id);
         return 0;
      }
   }
};
