#pragma once
#include <gflags/gflags.h>
#include <cstdint>
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

   std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> cartesianProducts(
       std::vector<std::pair<ol_sec1_t::Key, ol_sec1_t>>& cached_left,
       std::vector<std::pair<stock_t::Key, stock_t>>& cached_right)
   {
      std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> results;
      results.reserve(cached_left.size() * cached_right.size());  // Reserve memory to avoid reallocations

      for (auto& left : cached_left) {
         for (auto& right : cached_right) {
            const auto [key, rec] = MergeJoin<ol_sec1_t, stock_t, joined1_t>::merge(left.first, left.second, right.first, right.second);
            results.push_back({key, rec.toSelected(key)});
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

      std::vector<std::pair<ol_sec1_t::Key, ol_sec1_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      merged.template scan<stock_t, orderline_sec_t>(
          start_key,
          [&](const stock_t::Key& key, const stock_t& rec) {
             ++scanCardinality;
             if (key.s_w_id != w_id) {
                return false;
             }
             if (key.s_i_id != current_key.s_i_id) {
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
             ol_sec1_t expanded_rec;
             if constexpr (std::is_same_v<orderline_sec_t, ol_sec1_t>) {
                expanded_rec = rec;
             } else {
                ol_sec1_t expanded_rec;
                this->tpcc->orderline.lookup1({w_id, d_id, key.ol_o_id, key.ol_number}, [&](const orderline_t& ol_rec) {
                   expanded_rec = {ol_rec.ol_supply_w_id, ol_rec.ol_delivery_d, rec.ol_quantity, rec.ol_amount, rec.ol_dist_info};
                });
             }
             if (expanded_rec.ol_delivery_d < since) {
                return true;  // Skip this record
             }
             cached_left.push_back({key, expanded_rec});
             return true;  // continue scan
          },
          []() { /* undo */ });

      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      produced += final_cartesian_products.size();
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      vector<std::pair<typename joined_t::Key, joined_t>> results;

      std::vector<std::pair<ol_sec1_t::Key, ol_sec1_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      uint64_t lookupCardinality = 0;

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
             if (!FLAGS_locality_read && key.ol_d_id != d_id) {
                return true;  // next item may still be in the same district
             }
             ol_sec1_t expanded_rec;
             if constexpr (std::is_same_v<orderline_sec_t, ol_sec1_t>) {
                expanded_rec = rec.expand();
             } else {
                this->tpcc->orderline.lookup1({w_id, d_id, key.ol_o_id, key.ol_number}, [&](const orderline_t& ol_rec) {
                   expanded_rec = {ol_rec.ol_supply_w_id, ol_rec.ol_delivery_d, rec.ol_quantity, rec.ol_amount, rec.ol_dist_info};
                });
             }
             cached_left.push_back({key, expanded_rec});
             return true;
          },
          []() { /* undo */ });

      auto final_cartesian_products = cartesianProducts(cached_left, cached_right);
      results.insert(results.end(), final_cartesian_products.begin(), final_cartesian_products.end());
   }

   void newOrderRnd(Integer w_id, Integer order_size = 5)
   {
      Base::newOrderRndCallback(
          w_id,
          [&](const stock_t::Key& key, std::function<void(stock_t&)> cb, leanstore::UpdateSameSizeInPlaceDescriptor& desc, Integer) {
             merged.template update1<stock_t>(key, cb, desc);
          },
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& rec) { merged.template insert<orderline_sec_t>(key, rec); }, order_size);
   }

   void loadStockToMerged(Integer w_id)
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
         if constexpr (std::is_same_v<orderline_sec_t, ol_sec0_t>) {
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
         merged_page_count = merged.estimatePages();
         merged_leaf_count = merged.estimateLeafs();
      });
      auto merged_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

      std::cout << "merged_page_count: " << merged_page_count << ", merged_leaf_count: " << merged_leaf_count << ", merged_height: " << merged_height
                << std::endl;

      csv_file << "core," << config << "," << Base::pageCountToGB(core_page_count) << "," << core_time << std::endl;
      csv_file << "merged_index," << config << "," << Base::pageCountToGB(merged_page_count) << "," << merged_time << std::endl;
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      logSizes(t0, t0, t0, crm);
   }
};
