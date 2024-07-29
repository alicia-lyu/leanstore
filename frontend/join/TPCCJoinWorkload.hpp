#pragma once
#include <gflags/gflags_declare.h>
#include <cstdint>
#include <stdexcept>
#include "../tpc-c/TPCCWorkload.hpp"
#include "JoinedSchema.hpp"
#include "Join.hpp"

template <template <typename> class AdapterType>
class TPCCJoinWorkload
{
   TPCCWorkload<AdapterType>* tpcc;
   AdapterType<ol_join_sec_t>& orderline_secondary;
   AdapterType<joined_ols_t>& joined_ols;

  public:
   TPCCJoinWorkload(TPCCWorkload<AdapterType>* tpcc, AdapterType<ol_join_sec_t>& orderline_secondary, AdapterType<joined_ols_t>& joined_ols)
       : tpcc(tpcc), orderline_secondary(orderline_secondary), joined_ols(joined_ols)
   {
   }

   // When this query can be realistic: Keep track of stock information for recent orders
   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      // vector<joined_ols_t> results;
      joined_ols_t::Key start_key = {w_id, 0, d_id, 0, 0};  // Starting from the first item in the warehouse and district

      uint64_t scanCardinality = 0;

      joined_ols.scan(
          start_key,
          [&](const joined_ols_t::Key&, const joined_ols_t& rec) {
             scanCardinality++;
             if (rec.ol_delivery_d >= since) {
               //  results.push_back(rec);
             }
             return true;
          },
          []() { /* undo */ });

      // std::cout << "Scan cardinality: " << scanCardinality << std::endl;
      // All default configs, dram_gib = 8, cardinality = 184694
   }

   // When this query can be realistic: Find all orderlines and stock level for a specific item. Act on those orders according to the information.
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
         // ********** Update to stock_t causes updates to join results **********
         // std::cout << "Line number #" << i << ": Updating stock_t causes updates to join results" << std::endl;
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
            if (key.ol_o_id == o_id && key.ol_d_id == d_id) {
               std::cout << "d_id: " << d_id << ", o_id: " << o_id << std::endl;
               throw std::runtime_error("newOrderRnd: duplicate key");
            }
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
         // std::cout << "Line number #" << i << ": Updating secondary index" << std::endl;
         orderline_secondary.insert({w_id, itemid, d_id, o_id, lineNumber}, {supware, ol_delivery_d, qty, ol_amount, s_dist});
         // ********** Update Join Results **********
         // std::cout << "Line number #" << i << ": Updating join results" << std::endl;
         stock_t stock_rec;
         tpcc->stock.lookup1({w_id, itemid}, [&](const stock_t& rec) { stock_rec = rec; });
         joined_ols.insert(
             {w_id, itemid, d_id, o_id, lineNumber},
             {
                 supware, ol_delivery_d, qty, ol_amount,  // the same info inserted into orderline
                 stock_rec.s_quantity, stock_rec.s_dist_01, stock_rec.s_dist_02, stock_rec.s_dist_03, stock_rec.s_dist_04, stock_rec.s_dist_05,
                 stock_rec.s_dist_06, stock_rec.s_dist_07, stock_rec.s_dist_08, stock_rec.s_dist_09, stock_rec.s_dist_10, stock_rec.s_ytd,
                 stock_rec.s_order_cnt, stock_rec.s_remote_cnt, stock_rec.s_data  // lookedup from stock
             });
      }
   }

   int tx(Integer w_id, int read_percentage, int scan_percentage, int write_percentage)
   {
      int rnd = (int) leanstore::utils::RandomGenerator::getRand(0, read_percentage + scan_percentage + write_percentage);
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

   void loadOrderlineSecondary()
   {
      auto orderline_scanner = tpcc->orderline.getScanner();
      while (true) {
         auto ret = orderline_scanner.next();
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         ol_join_sec_t::Key sec_key = {key.ol_w_id, payload.ol_i_id, key.ol_d_id, key.ol_o_id, key.ol_number};
         ol_join_sec_t sec_payload = {payload.ol_supply_w_id, payload.ol_delivery_d, payload.ol_quantity, payload.ol_amount, payload.ol_dist_info};
         orderline_secondary.insert(sec_key, sec_payload);
      }
   }

   void joinOrderlineAndStock()
   {
      auto orderline_scanner = orderline_secondary.getScanner();

      auto stock_scanner = tpcc->stock.getScanner();
      
      MergeJoin<ol_join_sec_t, stock_t, joined_ols_t> merge_join(&orderline_scanner, &stock_scanner);

      while (true) {
         auto ret = merge_join.next();
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         joined_ols.insert(key, payload);
      }
   }
};