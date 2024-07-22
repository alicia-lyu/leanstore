#pragma once
#include <cstdint>
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "MergedAdapter.hpp"

template <template <typename> class AdapterType, template <typename, typename> class MergedAdapterType>
class TPCCMergedWorkload
{
   TPCCWorkload<AdapterType>* tpcc;
   MergedAdapterType<ol_join_sec_t, stock_t>& merged;
   using merged_key_type = RecordVariant<ol_join_sec_t, stock_t>::Key;
   using merged_value_type = RecordVariant<ol_join_sec_t, stock_t>::Value;
   using JoinKey = stock_t::Key;  // May be different if the join key is not the primary key

   std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> returnCartesianProducts(
       std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>>& cached_left,
       std::vector<std::pair<stock_t::Key, stock_t>>& cached_right)
   {
      std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;
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
   TPCCMergedWorkload(TPCCWorkload<AdapterType>* tpcc, MergedAdapterType<ol_join_sec_t, stock_t> merged) : tpcc(tpcc), merged(merged) {}

   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      JoinKey start_key = {w_id, 0};  // Starting from the first item in the warehouse

      uint64_t scanCardinality = 0;

      JoinKey current_key = {w_id, 0};
      std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      std::vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;

      merged.scan(
          start_key,
          [&](const merged_key_type& key, const merged_value_type& rec) {
             ++scanCardinality;
             return std::visit(
                 [&, this](auto&& key, auto&& rec) {
                    using KeyType = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<KeyType, typename stock_t::Key>) {
                       if (key.s_w_id != current_key.s_w_id || key.s_i_id != current_key.s_i_id) {
                          // A new join key discovered
                          // Do a cartesian product of current cached rows
                          auto cartesian_products = returnCartesianProducts(cached_left, cached_right);
                          results.insert(results.end(), cartesian_products.begin(), cartesian_products.end());
                          // Start a new group
                          current_key = key;
                          cached_left.clear();
                          cached_right.clear();
                       }
                       cached_right.push_back({key, rec});
                       return true;
                    } else {
                       if (rec.ol_delivery_d < since || key.ols_d_id != d_id) {
                          return true;  // continue scan
                       }
                       cached_left.push_back({key, rec});
                       return true;  // continue scan
                    }
                 },
                 key, rec);
          },
          []() { /* undo */ });

      std::cout << "Scan cardinality: " << scanCardinality << std::endl;
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<std::pair<joined_ols_t::Key, joined_ols_t>> results;
      JoinKey start_key = {w_id, i_id};

      std::vector<std::pair<ol_join_sec_t::Key, ol_join_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      uint64_t lookupCardinality = 0;

      merged.scan(
          start_key,
          [&](const merged_key_type& key, const merged_value_type& rec) {
             lookupCardinality++;
             return std::visit(
                 [&, this](auto&& key, auto&& rec) {
                    using KeyType = std::decay_t<decltype(key)>;
                    if constexpr (std::is_same_v<KeyType, typename stock_t::Key>) {
                       if (key.s_w_id != w_id || key.s_i_id != i_id) {
                          // Finish scanning, do a cartesian product
                          auto cartesian_products = returnCartesianProducts(cached_left, cached_right);
                          results.insert(results.end(), cartesian_products.begin(), cartesian_products.end());
                          return false;
                       }
                       cached_right.push_back({key, rec});
                       return true;
                    } else {
                       assert(key.ols_i_id == i_id && key.ols_w_id == w_id);
                       // Change only occur at a stock entry
                       if (key.ols_d_id != d_id) {
                          return true;  // continue scan
                       }
                       cached_left.push_back({key, rec});
                       return true;  // continue scan
                    }
                 },
                 key, rec);
          },
          []() { /* undo */});

      std::cout << "Lookup cardinality: " << lookupCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality mostly 1 or 2. Can also be 3, etc.
   }

   void newOrderRnd(Integer w_id)
   {
      // tpcc->newOrderRnd(w_id);
      Integer d_id = tpcc->urand(1, 10);
      Integer c_id = tpcc->getCustomerID();
      Integer ol_cnt = tpcc->urand(5, 15);

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

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         UpdateDescriptorGenerator4(stock_update_descriptor, stock_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
         tpcc->stock.update1(
             {supwares[i], itemids[i]},
             [&](stock_t& rec) {
                auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                rec.s_remote_cnt += (supwares[i] != w_id);
                rec.s_order_cnt++;
                rec.s_ytd += qty;
             },
             stock_update_descriptor);
         // TODO update merged---Do we need this primary index at all?
      }

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = tpcc->item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist;
         tpcc->stock.lookup1({w_id, itemid}, [&](const stock_t& rec) {
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
         // TODO: i_data, s_data
         // ********** Update Secondary Index **********
         merged.insert({w_id, itemid, d_id, o_id, lineNumber}, {});
      }
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
         recentOrdersStockInfo(w_id, d_id, since);
         return 0;
      } else if (rnd == 2) {
         newOrderRnd(w_id);
         return 0;
      } else {
         tpcc->deliveryRnd(w_id);
         return 0;
         // paymentRnd
         // orderStatusRnd
         // stockLevelRnd
      }
   }
};