#pragma once
#include <sys/types.h>
#include "../shared/Adapter.hpp"
#include "Exceptions.hpp"
#include "Schema.hpp"
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <vector>
using std::vector;

DEFINE_int32(semijoin_selectivity, 100, "\% of orderline to be joined with stock");
// Accomplished by only loading a subset of items. Semi-join selectivity of stock may be
// lower. Empirically 90+% items are present in some orderline, picking out those in stock.

template <template <typename> class AdapterType, int id_count = 14>
class TPCCBaseWorkload;
template <template <typename> class AdapterType, int id_count = 14>
class TPCCJoinWorkload;
template <template <typename> class AdapterType, class MergedAdapterType, int id_count = 14>
class TPCCMergedWorkload;
template <template <typename> class AdapterType>
class TPCHWorkload;
// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
class TPCCWorkload
{  
   template <template <typename> class, int>
   friend class TPCCBaseWorkload;

   template <template <typename> class, int>
   friend class TPCCJoinWorkload;

   template <template <typename> class, class, int>
   friend class TPCCMergedWorkload;

   template <template <typename> class>
   friend class TPCHWorkload;

  private:
   static constexpr INTEGER OL_I_ID_C = 7911;  // in range [0, 8191]
   static constexpr INTEGER C_ID_C = 259;      // in range [0, 1023]
   // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
   // be within [65, 119]
   static constexpr INTEGER C_LAST_LOAD_C = 157;  // in range [0, 255]
   static constexpr INTEGER C_LAST_RUN_C = 223;   // in range [0, 255]

   static constexpr INTEGER CUSTOMER_SCALE = 30000;
   static constexpr INTEGER ITEMS_NO = 100000;  // independent of warehouse count
   static constexpr INTEGER NO_SCALE = 9000;

   AdapterType<warehouse_t>& warehouse;
   AdapterType<district_t>& district;
   AdapterType<customer_t>& customer;
   AdapterType<customer_wdl_t>& customerwdl;
   AdapterType<history_t>& history;
   AdapterType<neworder_t>& neworder;
   AdapterType<order_t>& order;
   AdapterType<order_wdc_t>& order_wdc;
   AdapterType<orderline_t>& orderline;
   AdapterType<item_t>& item;
   AdapterType<stock_t>& stock;
   const bool order_wdc_index = true;
   const Integer warehouseCount;
   const Integer tpcc_remove;
   const bool manually_handle_isolation_anomalies;
   const bool warehouse_affinity;
   const double scale_factor;

   double calculate_scale_factor() const
   {
      // As defined in TPC-C: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf

      if (FLAGS_target_gib == 0) {
         std::cout << "WARNING: target_gib is 0, using scale factor 1.0" << std::endl;
         return 1.0;
      }

      uint64_t size = warehouse_t::rowSize() + district_t::rowSize() * 10 + customer_t::rowSize() * CUSTOMER_SCALE +
                      customer_wdl_t::rowSize() * CUSTOMER_SCALE + history_t::rowSize() * CUSTOMER_SCALE + order_t::rowSize() * CUSTOMER_SCALE +
                      neworder_t::rowSize() * NO_SCALE + orderline_t::rowSize() * CUSTOMER_SCALE * 10 + stock_t::rowSize() * ITEMS_NO;

      if (order_wdc_index) {
         size += order_wdc_t::rowSize() * CUSTOMER_SCALE;
      }

      size = size * warehouseCount + item_t::rowSize() * ITEMS_NO;

      double empirical_adjustment = 4;  // Observed that the actual size is 3x---Why?

      double scale = ((double)FLAGS_target_gib * 1024 * 1024 * 1024) / (size * empirical_adjustment);

      std::cout << "Size of " << warehouseCount << " warehouses: " << size << " bytes, scale factor: " << scale << std::endl;

      return scale;
   }
   // -------------------------------------------------------------------------------------
   Integer urandexcept(Integer low, Integer high, Integer v)
   {
      if (high <= low)
         return low;
      Integer r = rnd(high - low) + low;
      if (r >= v)
         return r + 1;
      else
         return r;
   }

   template <int maxLength>
   Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
   {
      assert(maxLenStr <= maxLength);
      Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
      Varchar<maxLength> result;
      for (Integer index = 0; index < len; index++) {
         Integer i = rnd(62);
         if (i < 10)
            result.append(48 + i);
         else if (i < 36)
            result.append(64 - 10 + i);
         else
            result.append(96 - 36 + i);
      }
      return result;
   }

   Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr)
   {
      Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
      Varchar<16> result;
      for (Integer i = 0; i < len; i++)
         result.append(48 + rnd(10));
      return result;
   }

   Varchar<16> namePart(Integer id)
   {
      assert(id < 10);
      Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
      return data[id];
   }

   Varchar<16> genName(Integer id) { return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10); }

   Numeric randomNumeric(Numeric min, Numeric max)
   {
      double range = (max - min);
      double div = RAND_MAX / range;
      return min + (leanstore::utils::RandomGenerator::getRandU64() / div);
   }

   Varchar<9> randomzip()
   {
      Integer id = rnd(10000);
      Varchar<9> result;
      result.append(48 + (id / 1000));
      result.append(48 + (id / 100) % 10);
      result.append(48 + (id / 10) % 10);
      result.append(48 + (id % 10));
      return result || Varchar<9>("11111");
   }

   Integer nurand(Integer a, Integer x, Integer y, Integer C = 42)
   {
      // TPC-C random is [a,b] inclusive
      // in standard: NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
      // return (((rnd(a + 1) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
      return (((urand(0, a) | urand(x, y)) + C) % (y - x + 1)) + x;
      // incorrect: return (((rnd(a) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
   }

   inline Integer getItemID()
   {
      // OL_I_ID_C
      return nurand(8191, 1, ITEMS_NO * scale_factor, OL_I_ID_C);
   }
   inline Integer getCustomerID()
   {
      // C_ID_C
      return nurand(1023, 1, CUSTOMER_SCALE * scale_factor / 10, C_ID_C);
      // return urand(1, 3000);
   }
   inline Integer getNonUniformRandomLastNameForRun()
   {
      // C_LAST_RUN_C
      return nurand(255, 0, 999, C_LAST_RUN_C);
   }
   inline Integer getNonUniformRandomLastNameForLoad()
   {
      // C_LAST_LOAD_C
      return nurand(255, 0, 999, C_LAST_LOAD_C);
   }
   // -------------------------------------------------------------------------------------
   Timestamp currentTimestamp() { return 1; }
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   // run
   void newOrder(Integer w_id,
                 Integer d_id,
                 Integer c_id,
                 const vector<Integer>& lineNumbers,
                 const vector<Integer>& supwares,
                 const vector<Integer>& itemids,
                 const vector<Integer>& qtys,
                 Timestamp timestamp)
   {
      Numeric w_tax = warehouse.lookupField({w_id}, &warehouse_t::w_tax);
      Numeric c_discount = customer.lookupField({w_id, d_id, c_id}, &customer_t::c_discount);
      Numeric d_tax;
      Integer o_id;

      UpdateDescriptorGenerator1(district_update_descriptor, district_t, d_next_o_id);
      // UpdateDescriptorGenerator2(district_update_descriptor, district_t, d_next_o_id, d_ytd);
      district.update1(
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
      order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
      if (order_wdc_index) {
         order_wdc.insert({w_id, d_id, c_id, o_id}, {});
      }
      neworder.insert({w_id, d_id, o_id}, {});

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         UpdateDescriptorGenerator4(stock_update_descriptor, stock_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
         stock.update1(
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

      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist;
         stock.lookup1({w_id, itemid}, [&](const stock_t& rec) {
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
         orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
         // TODO: i_data, s_data
      }
   }
   // -------------------------------------------------------------------------------------
   void newOrderRnd(Integer w_id)
   {
      Integer d_id = urand(1, 10);
      Integer c_id = getCustomerID();
      Integer ol_cnt = urand(5, 15);

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
         if (!warehouse_affinity && urand(1, 100) == 1)  // ATTN:remote transaction
            supware = urandexcept(1, warehouseCount, w_id);
         Integer itemid = getItemID();
         if (false && (i == ol_cnt) && (urand(1, 100) == 1))  // invalid item => random
            itemid = 0;
         lineNumbers.push_back(i);
         supwares.push_back(supware);
         itemids.push_back(itemid);
         qtys.push_back(urand(1, 10));
      }
      newOrder(w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, currentTimestamp());
   }
   // -------------------------------------------------------------------------------------
   void delivery(Integer w_id, Integer carrier_id, Timestamp datetime)
   {
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         Integer o_id = minInteger;
         neworder.scan(
             {w_id, d_id, minInteger},
             [&](const neworder_t::Key& key, const neworder_t&) {
                if (key.no_w_id == w_id && key.no_d_id == d_id) {
                   o_id = key.no_o_id;
                }
                return false;
             },
             [&]() { o_id = minInteger; });
         // -------------------------------------------------------------------------------------
         if (o_id == minInteger) {  // Should rarely happen
            cout << "WARNING: delivery tx skipped for warehouse = " << w_id << ", district = " << d_id << endl;
            continue;
         }
         // -------------------------------------------------------------------------------------
         if (tpcc_remove) {
            const auto ret = neworder.erase({w_id, d_id, o_id});
            ensure(ret || manually_handle_isolation_anomalies);
         }
         // -------------------------------------------------------------------------------------
         Integer ol_cnt = minInteger, c_id;
         if (manually_handle_isolation_anomalies) {
            order.scan(
                {w_id, d_id, o_id},
                [&](const order_t::Key&, const order_t& rec) {
                   ol_cnt = rec.o_ol_cnt;
                   c_id = rec.o_c_id;
                   return false;
                },
                [&]() {});
            if (ol_cnt == minInteger)
               continue;
         } else {
            order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
               ol_cnt = rec.o_ol_cnt;
               c_id = rec.o_c_id;
            });
         }
         // -------------------------------------------------------------------------------------
         if (manually_handle_isolation_anomalies) {
            bool is_safe_to_continue = false;
            order.scan(
                {w_id, d_id, o_id},
                [&](const order_t::Key& key, const order_t& rec) {
                   if (key.o_w_id == w_id && key.o_d_id == d_id && key.o_id == o_id) {
                      is_safe_to_continue = true;
                      ol_cnt = rec.o_ol_cnt;
                      c_id = rec.o_c_id;
                   } else {
                      is_safe_to_continue = false;
                   }
                   return false;
                },
                [&]() { is_safe_to_continue = false; });
            if (!is_safe_to_continue)
               continue;
         }
         // -------------------------------------------------------------------------------------
         UpdateDescriptorGenerator1(order_update_descriptor, order_t, o_carrier_id);
         order.update1({w_id, d_id, o_id}, [&](order_t& rec) { rec.o_carrier_id = carrier_id; }, order_update_descriptor);
         // -------------------------------------------------------------------------------------
         if (manually_handle_isolation_anomalies) {
            // First check if all orderlines have been inserted, a hack because of the missing transaction and concurrency control
            bool is_safe_to_continue = false;
            orderline.scan(
                {w_id, d_id, o_id, ol_cnt},
                [&](const orderline_t::Key& key, const orderline_t&) {
                   if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id && key.ol_number == ol_cnt) {
                      is_safe_to_continue = true;
                   } else {
                      is_safe_to_continue = false;
                   }
                   return false;
                },
                [&]() { is_safe_to_continue = false; });
            if (!is_safe_to_continue) {
               continue;
            }
         }
         // -------------------------------------------------------------------------------------
         Numeric ol_total = 0;
         for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++) {
            UpdateDescriptorGenerator1(orderline_update_descriptor, orderline_t, ol_delivery_d);
            orderline.update1(
                {w_id, d_id, o_id, ol_number},
                [&](orderline_t& rec) {
                   ol_total += rec.ol_amount;
                   rec.ol_delivery_d = datetime;
                },
                orderline_update_descriptor);
         }
         // UpdateDescriptorGenerator4(customer_update_descriptor, customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt);
         UpdateDescriptorGenerator2(customer_update_descriptor, customer_t, c_balance, c_delivery_cnt);
         customer.update1(
             {w_id, d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance += ol_total;
                rec.c_delivery_cnt++;
             },
             customer_update_descriptor);
      }
   }
   // -------------------------------------------------------------------------------------
   void deliveryRnd(Integer w_id)
   {
      Integer carrier_id = urand(1, 10);
      delivery(w_id, carrier_id, currentTimestamp());
   }
   // -------------------------------------------------------------------------------------
   void stockLevel(Integer w_id, Integer d_id, Integer threshold)
   {
      Integer o_id = district.lookupField({w_id, d_id}, &district_t::d_next_o_id);

      //"SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM orderline, stock WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND
      // S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"

      /*
       * http://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf P 116
       * EXEC SQL SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
    FROM order_line, stock
    WHERE ol_w_id=:w_id AND
    ol_d_id=:d_id AND ol_o_id<:o_id AND
    ol_o_id>=:o_id-20 AND s_w_id=:w_id AND
    s_i_id=ol_i_id AND s_quantity < :threshold;
       */
      vector<Integer> items;
      items.reserve(100);
      Integer min_ol_o_id = o_id - 20;
      orderline.scan(
          {w_id, d_id, min_ol_o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id < o_id && key.ol_o_id >= min_ol_o_id) {
                items.push_back(rec.ol_i_id);
                return true;
             }
             return false;
          },
          [&]() { items.clear(); });
      std::sort(items.begin(), items.end());
      std::unique(items.begin(), items.end());
      unsigned count = 0;
      for (Integer i_id : items) {
         if (!isSelected(i_id))
            continue;
         auto res_s_quantity = stock.lookupField({w_id, i_id}, &stock_t::s_quantity);
         count += res_s_quantity < threshold;
      }
   }
   // -------------------------------------------------------------------------------------
   void stockLevelRnd(Integer w_id) { stockLevel(w_id, urand(1, 10), urand(10, 20)); }
   // -------------------------------------------------------------------------------------
   void orderStatusId(Integer w_id, Integer d_id, Integer c_id)
   {
      Varchar<16> c_first;
      Varchar<2> c_middle;
      Varchar<16> c_last;
      [[maybe_unused]] Numeric c_balance;
      customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {
         c_first = rec.c_first;
         c_middle = rec.c_middle;
         c_last = rec.c_last;
         c_balance = rec.c_balance;
      });

      Integer o_id = -1;
      // -------------------------------------------------------------------------------------
      // latest order id desc
      if (order_wdc_index) {
         order_wdc.scanDesc(
             {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
             [&](const order_wdc_t::Key& key, const order_wdc_t&) {
                assert(key.o_w_id == w_id);
                assert(key.o_d_id == d_id);
                assert(key.o_c_id == c_id);
                o_id = key.o_id;
                return false;
             },
             [] {});
      } else {
         order.scanDesc(
             {w_id, d_id, std::numeric_limits<Integer>::max()},
             [&](const order_t::Key& key, const order_t& rec) {
                if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
                   o_id = key.o_id;
                   return false;
                }
                return true;
             },
             [&]() {});
      }
      ensure(o_id > -1);
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Timestamp o_entry_d;
      [[maybe_unused]] Integer o_carrier_id;

      order.lookup1({w_id, d_id, o_id}, [&](const order_t& rec) {
         o_entry_d = rec.o_entry_d;
         o_carrier_id = rec.o_carrier_id;
      });
      [[maybe_unused]] Integer ol_i_id;
      [[maybe_unused]] Integer ol_supply_w_id;
      [[maybe_unused]] Timestamp ol_delivery_d;
      [[maybe_unused]] Numeric ol_quantity;
      [[maybe_unused]] Numeric ol_amount;
      {
         // AAA: expensive
         orderline.scan(
             {w_id, d_id, o_id, minInteger},
             [&](const orderline_t::Key& key, const orderline_t& rec) {
                if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                   ol_i_id = rec.ol_i_id;
                   ol_supply_w_id = rec.ol_supply_w_id;
                   ol_delivery_d = rec.ol_delivery_d;
                   ol_quantity = rec.ol_quantity;
                   ol_amount = rec.ol_amount;
                   return true;
                }
                return false;
             },
             [&]() {
                // NOTHING
             });
      }
   }
   // -------------------------------------------------------------------------------------
   void orderStatusName(Integer w_id, Integer d_id, Varchar<16> c_last)
   {
      vector<Integer> ids;
      customerwdl.scan(
          {w_id, d_id, c_last, {}},
          [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
             if (key.c_w_id == w_id && key.c_d_id == d_id && key.c_last == c_last) {
                ids.push_back(rec.c_id);
                return true;
             }
             return false;
          },
          [&]() { ids.clear(); });
      unsigned c_count = ids.size();
      if (c_count == 0)
         return;  // TODO: rollback
      unsigned index = c_count / 2;
      if ((c_count % 2) == 0)
         index -= 1;
      Integer c_id = ids[index];

      Integer o_id = -1;
      // latest order id desc
      if (order_wdc_index) {
         order_wdc.scanDesc(
             {w_id, d_id, c_id, std::numeric_limits<Integer>::max()},
             [&](const order_wdc_t::Key& key, const order_wdc_t&) {
                assert(key.o_w_id == w_id);
                assert(key.o_d_id == d_id);
                assert(key.o_c_id == c_id);
                o_id = key.o_id;
                return false;
             },
             [] {});
      } else {
         order.scanDesc(
             {w_id, d_id, std::numeric_limits<Integer>::max()},
             [&](const order_t::Key& key, const order_t& rec) {
                if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
                   o_id = key.o_id;
                   return false;
                }
                return true;
             },
             [&]() {});
         ensure(o_id > -1);
      }
      // -------------------------------------------------------------------------------------
      [[maybe_unused]] Timestamp ol_delivery_d;
      orderline.scan(
          {w_id, d_id, o_id, minInteger},
          [&](const orderline_t::Key& key, const orderline_t& rec) {
             if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
                ol_delivery_d = rec.ol_delivery_d;
                return true;
             }
             return false;
          },
          []() {
             // NOTHING
          });
   }
   // -------------------------------------------------------------------------------------

   void orderStatusRnd(Integer w_id)
   {
      Integer d_id = urand(1, 10);
      if (urand(1, 100) <= 40) {
         orderStatusId(w_id, d_id, getCustomerID());
      } else {
         orderStatusName(w_id, d_id, genName(getNonUniformRandomLastNameForRun()));
      }
   }
   // -------------------------------------------------------------------------------------
   void paymentById(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Integer c_id, Timestamp h_date, Numeric h_amount, Timestamp datetime)
   {
      Varchar<10> w_name;
      Varchar<20> w_street_1;
      Varchar<20> w_street_2;
      Varchar<20> w_city;
      Varchar<2> w_state;
      Varchar<9> w_zip;
      [[maybe_unused]] Numeric w_ytd;
      warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
         w_name = rec.w_name;
         w_street_1 = rec.w_street_1;
         w_street_2 = rec.w_street_2;
         w_city = rec.w_city;
         w_state = rec.w_state;
         w_zip = rec.w_zip;
         w_ytd = rec.w_ytd;
      });
      // -------------------------------------------------------------------------------------
      UpdateDescriptorGenerator1(warehouse_update_descriptor, warehouse_t, w_ytd);
      warehouse.update1({w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, warehouse_update_descriptor);
      Varchar<10> d_name;
      Varchar<20> d_street_1;
      Varchar<20> d_street_2;
      Varchar<20> d_city;
      Varchar<2> d_state;
      Varchar<9> d_zip;
      [[maybe_unused]] Numeric d_ytd;
      district.lookup1({w_id, d_id}, [&](const district_t& rec) {
         d_name = rec.d_name;
         d_street_1 = rec.d_street_1;
         d_street_2 = rec.d_street_2;
         d_city = rec.d_city;
         d_state = rec.d_state;
         d_zip = rec.d_zip;
         d_ytd = rec.d_ytd;
      });
      // UpdateDescriptorGenerator2(district_update_descriptor, district_t, d_next_o_id, d_ytd);
      UpdateDescriptorGenerator1(district_update_descriptor, district_t, d_ytd);
      district.update1({w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, district_update_descriptor);

      Varchar<500> c_data;
      Varchar<2> c_credit;
      Numeric c_balance;
      Numeric c_ytd_payment;
      Numeric c_payment_cnt;
      customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
         c_data = rec.c_data;
         c_credit = rec.c_credit;
         c_balance = rec.c_balance;
         c_ytd_payment = rec.c_ytd_payment;
         c_payment_cnt = rec.c_payment_cnt;
      });
      Numeric c_new_balance = c_balance - h_amount;
      Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
      Numeric c_new_payment_cnt = c_payment_cnt + 1;

      if (c_credit == "BC") {
         Varchar<500> c_new_data;
         auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
                                  h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
         c_new_data.length = numChars;
         if (c_new_data.length > 500)
            c_new_data.length = 500;
         // -------------------------------------------------------------------------------------
         UpdateDescriptorGenerator4(customer_update_descriptor, customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt);
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_data = c_new_data;
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             },
             customer_update_descriptor);
      } else {
         // UpdateDescriptorGenerator4(customer_update_descriptor, customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt);
         UpdateDescriptorGenerator3(customer_update_descriptor, customer_t, c_balance, c_ytd_payment, c_payment_cnt);
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             },
             customer_update_descriptor);
      }

      Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
      Integer t_id = (Integer)leanstore::WorkerCounters::myCounters().t_id.load();
      Integer h_id = (Integer)leanstore::WorkerCounters::myCounters().variable_for_workload++;
      history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
   }
   // -------------------------------------------------------------------------------------
   void paymentByName(Integer w_id,
                      Integer d_id,
                      Integer c_w_id,
                      Integer c_d_id,
                      Varchar<16> c_last,
                      Timestamp h_date,
                      Numeric h_amount,
                      Timestamp datetime)
   {
      Varchar<10> w_name;
      Varchar<20> w_street_1;
      Varchar<20> w_street_2;
      Varchar<20> w_city;
      Varchar<2> w_state;
      Varchar<9> w_zip;
      [[maybe_unused]] Numeric w_ytd;
      warehouse.lookup1({w_id}, [&](const warehouse_t& rec) {
         w_name = rec.w_name;
         w_street_1 = rec.w_street_1;
         w_street_2 = rec.w_street_2;
         w_city = rec.w_city;
         w_state = rec.w_state;
         w_zip = rec.w_zip;
         w_ytd = rec.w_ytd;
      });
      // -------------------------------------------------------------------------------------
      UpdateDescriptorGenerator1(warehouse_update_descriptor, warehouse_t, w_ytd);
      warehouse.update1({w_id}, [&](warehouse_t& rec) { rec.w_ytd += h_amount; }, warehouse_update_descriptor);
      // -------------------------------------------------------------------------------------
      Varchar<10> d_name;
      Varchar<20> d_street_1;
      Varchar<20> d_street_2;
      Varchar<20> d_city;
      Varchar<2> d_state;
      Varchar<9> d_zip;
      [[maybe_unused]] Numeric d_ytd;
      district.lookup1({w_id, d_id}, [&](const district_t& rec) {
         d_name = rec.d_name;
         d_street_1 = rec.d_street_1;
         d_street_2 = rec.d_street_2;
         d_city = rec.d_city;
         d_state = rec.d_state;
         d_zip = rec.d_zip;
         d_ytd = rec.d_ytd;
      });
      UpdateDescriptorGenerator1(district_update_descriptor, district_t, d_ytd);
      // UpdateDescriptorGenerator2(district_update_descriptor, district_t, d_next_o_id, d_ytd);
      district.update1({w_id, d_id}, [&](district_t& rec) { rec.d_ytd += h_amount; }, district_update_descriptor);

      // Get customer id by name
      vector<Integer> ids;
      customerwdl.scan(
          {c_w_id, c_d_id, c_last, {}},
          [&](const customer_wdl_t::Key& key, const customer_wdl_t& rec) {
             if (key.c_w_id == c_w_id && key.c_d_id == c_d_id && key.c_last == c_last) {
                ids.push_back(rec.c_id);
                return true;
             }
             return false;
          },
          [&]() { ids.clear(); });
      unsigned c_count = ids.size();
      if (c_count == 0)
         return;  // TODO: rollback
      unsigned index = c_count / 2;
      if ((c_count % 2) == 0)
         index -= 1;
      Integer c_id = ids[index];

      Varchar<500> c_data;
      Varchar<2> c_credit;
      Numeric c_balance;
      Numeric c_ytd_payment;
      Numeric c_payment_cnt;
      customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {
         c_data = rec.c_data;
         c_credit = rec.c_credit;
         c_balance = rec.c_balance;
         c_ytd_payment = rec.c_ytd_payment;
         c_payment_cnt = rec.c_payment_cnt;
      });
      Numeric c_new_balance = c_balance - h_amount;
      Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
      Numeric c_new_payment_cnt = c_payment_cnt + 1;

      if (c_credit == "BC") {
         Varchar<500> c_new_data;
         auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount,
                                  h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
         c_new_data.length = numChars;
         if (c_new_data.length > 500)
            c_new_data.length = 500;
         // -------------------------------------------------------------------------------------
         UpdateDescriptorGenerator4(customer_update_descriptor, customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt);
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_data = c_new_data;
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             },
             customer_update_descriptor);
      } else {
         // UpdateDescriptorGenerator4(customer_update_descriptor, customer_t, c_data, c_balance, c_ytd_payment, c_payment_cnt);
         UpdateDescriptorGenerator3(customer_update_descriptor, customer_t, c_balance, c_ytd_payment, c_payment_cnt);
         customer.update1(
             {c_w_id, c_d_id, c_id},
             [&](customer_t& rec) {
                rec.c_balance = c_new_balance;
                rec.c_ytd_payment = c_new_ytd_payment;
                rec.c_payment_cnt = c_new_payment_cnt;
             },
             customer_update_descriptor);
      }

      Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
      Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
      Integer h_id = (Integer)leanstore::WorkerCounters::myCounters().variable_for_workload++;
      history.insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
   }
   // -------------------------------------------------------------------------------------
   void paymentRnd(Integer w_id)
   {
      Integer d_id = urand(1, 10);
      Integer c_w_id = w_id;
      Integer c_d_id = d_id;
      if (!warehouse_affinity && urand(1, 100) > 85) {  // ATTN: cross warehouses
         c_w_id = urandexcept(1, warehouseCount, w_id);
         c_d_id = urand(1, 10);
      }
      Numeric h_amount = randomNumeric(1.00, 5000.00);
      Timestamp h_date = currentTimestamp();

      if (urand(1, 100) <= 60) {
         paymentByName(w_id, d_id, c_w_id, c_d_id, genName(getNonUniformRandomLastNameForRun()), h_date, h_amount, currentTimestamp());
      } else {
         paymentById(w_id, d_id, c_w_id, c_d_id, getCustomerID(), h_date, h_amount, currentTimestamp());
      }
   }
   // -------------------------------------------------------------------------------------
  public:
   TPCCWorkload(AdapterType<warehouse_t>& w,
                AdapterType<district_t>& d,
                AdapterType<customer_t>& customer,
                AdapterType<customer_wdl_t>& customerwdl,
                AdapterType<history_t>& history,
                AdapterType<neworder_t>& neworder,
                AdapterType<order_t>& order,
                AdapterType<order_wdc_t>& order_wdc,
                AdapterType<orderline_t>& orderline,
                AdapterType<item_t>& item,
                AdapterType<stock_t>& stock,
                bool order_wdc_index,
                Integer warehouse_count,
                bool tpcc_remove,
                bool manually_handle_isolation_anomalies = true,
                bool warehouse_affinity = true)
       : warehouse(w),
         district(d),
         customer(customer),
         customerwdl(customerwdl),
         history(history),
         neworder(neworder),
         order(order),
         order_wdc(order_wdc),
         orderline(orderline),
         item(item),
         stock(stock),
         order_wdc_index(order_wdc_index),
         warehouseCount(warehouse_count),
         tpcc_remove(tpcc_remove),
         manually_handle_isolation_anomalies(manually_handle_isolation_anomalies),
         warehouse_affinity(warehouse_affinity),
         scale_factor(calculate_scale_factor())
   {
   }
   // -------------------------------------------------------------------------------------
   // [0, n)
   Integer rnd(Integer n) { return leanstore::utils::RandomGenerator::getRand(0, n); }
   // [fromId, toId]
   Integer randomId(Integer fromId, Integer toId) { return leanstore::utils::RandomGenerator::getRand(fromId, toId + 1); }
   // [low, high]
   Integer urand(Integer low, Integer high) { return rnd(high - low + 1) + low; }
   // -------------------------------------------------------------------------------------
   void prepare()
   {
      std::cout << "Preparing TPC-C" << std::endl;
      Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
      Integer h_id = 0;
      history.scanDesc(
          {t_id, std::numeric_limits<Integer>::max()},
          [&](const history_t::Key& key, const history_t&) {
             h_id = key.h_pk + 1;
             return false;
          },
          []() {});
      leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
   }
   // -------------------------------------------------------------------------------------
   static bool isSelected(Integer i_id)
   {
      int step = 100 / FLAGS_semijoin_selectivity;
      int rem = i_id % 100;
      return rem % step == 0 && rem / step <= FLAGS_semijoin_selectivity;
   }

   void loadStock(Integer w_id)
   {
      UNREACHABLE();
      std::cout << "Loading " << ITEMS_NO * scale_factor << " stock to warehouse " << w_id << ", with semi-join selectivity " << FLAGS_semijoin_selectivity << std::endl;
      int loaded = 0;
      for (Integer i = 0; i < ITEMS_NO * scale_factor; i++) {
         if (!isSelected(i + 1)) {
            continue;
         }
         Varchar<50> s_data = randomastring<50>(25, 50);
         if (rnd(10) == 0) {
            s_data.length = rnd(s_data.length - 8);
            s_data = s_data || Varchar<10>("ORIGINAL");
         }
         stock.insert({w_id, i + 1}, stock_t(randomNumeric(10, 100), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24),
                                      randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24),
                                      randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), 0, 0, 0, s_data));
         loaded++;
      }
      std::cout << "Loaded " << loaded << " stock to warehouse " << w_id << std::endl;
   }
   // -------------------------------------------------------------------------------------
   void loadDistrict(Integer w_id)
   {
      for (Integer i = 1; i < 11; i++) {
         district.insert({w_id, i}, {randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                     randomastring<2>(2, 2), randomzip(), randomNumeric(0.0000, 0.2000), 3000000,
                                     static_cast<Integer>(CUSTOMER_SCALE * scale_factor / 10) + 1});
      }
   }
   // -------------------------------------------------------------------------------------
   void loadCustomer(Integer w_id, Integer d_id)
   {
      std::cout << "Loading " << CUSTOMER_SCALE * scale_factor / 10 << " customers for district " << d_id << ", warehouse " << w_id << std::endl;
      Timestamp now = currentTimestamp();
      for (Integer i = 0; i < CUSTOMER_SCALE * scale_factor / 10; i++) {
         Varchar<16> c_last;
         if (i < 1000)
            c_last = genName(i);
         else
            c_last = genName(getNonUniformRandomLastNameForLoad());
         Varchar<16> c_first = randomastring<16>(8, 16);
         Varchar<2> c_credit(rnd(10) ? "GC" : "BC");
         customer.insert({w_id, d_id, i + 1}, {c_first, "OE", c_last, randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                               randomastring<2>(2, 2), randomzip(), randomnstring(16, 16), now, c_credit, 50000.00,
                                               randomNumeric(0.0000, 0.5000), -10.00, 1, 0, 0, randomastring<500>(300, 500)});
         customerwdl.insert({w_id, d_id, c_last, c_first}, {i + 1});
         Integer t_id = (Integer)leanstore::WorkerCounters::myCounters().t_id;
         Integer h_id = (Integer)leanstore::WorkerCounters::myCounters().variable_for_workload++;
         // Each history for each customer
         history.insert({t_id, h_id}, {i + 1, d_id, w_id, d_id, w_id, now, 10.00, randomastring<24>(12, 24)});
      }
   }
   // -------------------------------------------------------------------------------------
   void loadOrders(Integer w_id, Integer d_id)
   {
      std::cout << "Loading " << CUSTOMER_SCALE * scale_factor / 10 << " orders for district " << d_id << ", warehouse " << w_id << std::endl;
      Timestamp now = currentTimestamp();
      vector<Integer> c_ids;
      for (Integer i = 1; i <= CUSTOMER_SCALE * scale_factor / 10; i++)
         c_ids.push_back(i);
      random_shuffle(c_ids.begin(), c_ids.end());
      Integer o_id = 1;
      // Every order for each customer
      // Each order has 5-15 lines
      uint64_t ol_sum = 0;
      for (Integer o_c_id : c_ids) {
         Integer o_carrier_id = (o_id < 2101) ? rnd(10) + 1 : 0;
         Numeric o_ol_cnt = rnd(10) + 5;
         ol_sum += o_ol_cnt;

         order.insert({w_id, d_id, o_id}, {o_c_id, now, o_carrier_id, o_ol_cnt, 1});
         if (order_wdc_index) {
            order_wdc.insert({w_id, d_id, o_c_id, o_id}, {});
         }

         for (Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
            Timestamp ol_delivery_d = 0;
            if (o_id < 2101)
               ol_delivery_d = now;
            Numeric ol_amount = (o_id < 2101) ? 0 : randomNumeric(0.01, 9999.99);
            const Integer ol_i_id = rnd(ITEMS_NO * scale_factor) + 1;  // May not cover all items in stock
            // if (d_id > 10 || ol_number > 15) {
            //    std::cout << "TPCCWorkload::loadOrders: Invalid orderline key." << std::endl;
            //    exit(1);
            // }
            orderline.insert({w_id, d_id, o_id, ol_number},
                             {ol_i_id, w_id, ol_delivery_d, 5, ol_amount, randomastring<24>(24, 24)});  // All supplied by the same warehouse
         }
         o_id++;
      }

      std::cout << "Loaded " << ol_sum << " orderlines for district " << d_id << ", warehouse " << w_id << std::endl;

      for (Integer i = (ITEMS_NO - NO_SCALE) * scale_factor; i <= ITEMS_NO * scale_factor; i++)
         neworder.insert({w_id, d_id, i}, {});
   }
   // -------------------------------------------------------------------------------------
   void loadItem()
   {
      std::cout << "Loading " << ITEMS_NO * scale_factor << " items" << std::endl;
      for (Integer i = 1; i <= ITEMS_NO * scale_factor; i++) {
         Varchar<50> i_data = randomastring<50>(25, 50);
         if (rnd(10) == 0) {
            i_data.length = rnd(i_data.length - 8);
            i_data = i_data || Varchar<10>("ORIGINAL");
         }
         item.insert({i}, {randomId(1, ITEMS_NO * scale_factor), randomastring<24>(14, 24), randomNumeric(1.00, 100.00), i_data});
      }
   }
   // -------------------------------------------------------------------------------------
   void loadWarehouse()
   {
      for (Integer i = 0; i < warehouseCount; i++) {
         warehouse.insert({i + 1}, {randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20),
                                    randomastring<2>(2, 2), randomzip(), randomNumeric(0.1000, 0.2000), 3000000});
      }
   }
   // -------------------------------------------------------------------------------------
   void verifyWarehouse(Integer w_id)
   {
      std::cout << "Verifying warehouse " << w_id << std::endl;
      // for (Integer w_id = 1; w_id <= warehouseCount; w_id++) {
      warehouse.lookup1({w_id}, [&](const auto&) {});
      for (Integer d_id = 1; d_id <= 10; d_id++) {
         for (Integer c_id = 1; c_id <= CUSTOMER_SCALE * scale_factor / 10; c_id++) {
            customer.lookup1({w_id, d_id, c_id}, [&](const auto&) {});
         }
      }
      for (Integer s_id = 1; s_id <= ITEMS_NO * scale_factor; s_id++) {
         bool ret = stock.tryLookup({w_id, s_id}, [&](const auto&) {});
         if (!isSelected(s_id)) {
            ensure(!ret);
         } else {
            ensure(ret);
         }
      }
   }
   // -------------------------------------------------------------------------------------
   void verifyItems()
   {
      std::cout << "Verifying items" << std::endl;
      for (Integer i = 1; i <= ITEMS_NO * scale_factor; i++) {
         item.lookup1({i}, [&](const auto&) {});
      }
   }
   // -------------------------------------------------------------------------------------
   // <tx_num, read_only>
   std::tuple<s32, bool> getRandomTXInfo()
   {
      u64 rnd = leanstore::utils::RandomGenerator::getRand(0, 10000);
      if (rnd < 4300) {
         return {0, false};
      }
      rnd -= 4300;
      if (rnd < 400) {
         return {1, true};
      }
      rnd -= 400;
      if (rnd < 400) {
         return {2, false};
      }
      rnd -= 400;
      if (rnd < 400) {
         return {3, true};
      }
      rnd -= 400;
      return {4, false};
   }
   // -------------------------------------------------------------------------------------
   void execTX(Integer w_id, s32 tx_number)
   {
      if (tx_number == 0) {
         paymentRnd(w_id);
      } else if (tx_number == 1) {
         orderStatusRnd(w_id);
      } else if (tx_number == 2) {
         deliveryRnd(w_id);
      } else if (tx_number == 3) {
         stockLevelRnd(w_id);
      } else if (tx_number == 4) {
         newOrderRnd(w_id);
      }
   }
   // -------------------------------------------------------------------------------------
   int tx(Integer w_id)
   {
      // micro-optimized version of weighted distribution
      u64 rnd = leanstore::utils::RandomGenerator::getRand(0, 10000);
      if (rnd < 4300) {
         paymentRnd(w_id);
         return 0;
      }
      rnd -= 4300;
      if (rnd < 400) {
         orderStatusRnd(w_id);
         return 1;
      }
      rnd -= 400;
      if (rnd < 400) {
         deliveryRnd(w_id);
         return 2;
      }
      rnd -= 400;
      if (rnd < 400) {
         stockLevelRnd(w_id);
         return 3;
      }
      rnd -= 400;
      newOrderRnd(w_id);
      return 4;
   }

   int touch(Integer w_id)
   {
      u64 rnd = leanstore::utils::RandomGenerator::getRand(0, 8);
      Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 10);
      Integer c_id = leanstore::utils::RandomGenerator::getRand(1, (int) (CUSTOMER_SCALE * scale_factor / 10));
      Integer o_id = leanstore::utils::RandomGenerator::getRand(1, (int) (CUSTOMER_SCALE * scale_factor / 10));
      Integer i_id = leanstore::utils::RandomGenerator::getRand(1, (int) (ITEMS_NO * scale_factor));
      if (rnd == 0) { // touch warehouse
         warehouse.lookup1({w_id}, [&](const auto&) {});
      } else if (rnd == 1) { // touch district
         district.lookup1({w_id, d_id}, [&](const auto&) {});
      } else if (rnd == 2) { // touch customer
         customer.lookup1({w_id, d_id, c_id}, [&](const auto&) {});
      } else if (rnd == 3) { // touch neworder_t
         neworder.tryLookup({w_id, d_id, o_id}, [&](const auto&) {});
      } else if (rnd == 4) { // touch order_t
         order.lookup1({w_id, d_id, o_id}, [&](const auto&) {});
      } else if (rnd == 5) { // touch order_wdc_t
         order_wdc.tryLookup({w_id, d_id, c_id, o_id}, [&](const auto&) {});
      } else if (rnd == 6) { // touch item_t
         item.lookup1({i_id}, [&](const auto&) {});
      } else if (rnd == 7) { // touch orderline
         Integer ol_number = leanstore::utils::RandomGenerator::getRand(1, 15);
         orderline.tryLookup({w_id, d_id, o_id, ol_number}, [&](const auto&) {});
      } else if (rnd == 8) { // touch stock
         stock.tryLookup({w_id, i_id}, [&](const auto&) {});
      }
      // Omitted history, customerwdl
      return rnd;
   }
   // -------------------------------------------------------------------------------------
   template <typename Relation>
   void dummyScan(AdapterType<Relation>& adapter)
   {
      typename Relation::Key key;
      std::memset(&key, 0, sizeof(typename Relation::Key));
      adapter.scan(key, [&](const typename Relation::Key&, const Relation&) { return true; }, [&]() {});
   }
   // -------------------------------------------------------------------------------------
   void analyticalQuery(s32 query_no = 0)
   {
      // TODO: implement CH analytical queries
      Integer sum = 0, last_w = 0, last_i = 0;
      if (query_no == 0) {
         stock.scan(
             {1, 0},
             [&](const stock_t::Key& key, const stock_t&) {
                sum++;
                ensure(key.s_w_id >= last_w);
                last_w = key.s_w_id;
                last_i = key.s_i_id;
                return true;
             },
             [&]() {});
         if (sum != warehouseCount * 100000) {
            cout << "#stocks = " << sum << endl;
            cout << last_w << "," << last_i << endl;
            ensure(false);
         }
      } else if (query_no == 1) {
         warehouse.scan(
             {0},
             [&](const warehouse_t::Key& key, const warehouse_t&) {
                sum++;
                last_w = key.w_id;
                return true;
             },
             [&]() {});
         if (sum != warehouseCount) {
            cout << "#warehouse = " << sum << endl;
            cout << last_w << endl;
            ensure(false);
         }
      } else if (query_no == 2) {
         district.scan(
             {1, 0},
             [&](const district_t::Key& key, const district_t&) {
                sum++;
                last_w = key.d_w_id;
                last_i = key.d_id;
                return true;
             },
             [&]() {});
         if (sum != warehouseCount * 10) {
            cout << "#district = " << sum << endl;
            cout << last_w << "," << last_i << endl;
            ensure(false);
         }
      } else if (query_no == 3) {
         [[maybe_unused]] u64 olap_counter = 0;
         neworder_t::Key last_key;
         last_key.no_o_id = -1;
         neworder.scan(
             {0, 0, 0},
             [&](const neworder_t::Key& key, const neworder_t&) {
                COUNTERS_BLOCK()
                {
                   leanstore::WorkerCounters::myCounters().olap_scanned_tuples++;
                }
                if (last_key.no_o_id != -1) {
                   if (!(last_key.no_w_id != key.no_w_id || last_key.no_d_id != key.no_d_id || last_key.no_o_id + 1 == key.no_o_id)) {
                      cout << last_key.no_w_id << "," << key.no_w_id << endl;
                      cout << last_key.no_d_id << "," << key.no_d_id << endl;
                      cout << last_key.no_o_id << "," << key.no_o_id << endl;
                      ensure(false);
                   }
                }
                last_key = key;
                olap_counter++;
                return true;
             },
             [&]() { cout << "undo neworder scan" << endl; });
      } else if (query_no == 44) {
         // dummyScan(customer);
         // dummyScan(customerwdl);
         // dummyScan(item);
         // dummyScan(stock);
         dummyScan(district);
         dummyScan(warehouse);
         // -------------------------------------------------------------------------------------
         // dummyScan(neworder);
         // dummyScan(order);
         // dummyScan(order_wdc);
         // dummyScan(orderline);
      } else if (query_no == 99) {
         sleep(1);
      } else {
         UNREACHABLE();
      }
   }
};
