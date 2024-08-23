#pragma once
#include <gflags/gflags.h>
#include <cstdint>
#include <fstream>
#include "../tpc-c/TPCCWorkload.hpp"
#include "Join.hpp"
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"
#include "Units.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"

template <template <typename> class AdapterType, class MergedAdapterType, int id_count>  // Default to 12
class TPCCMergedWorkload : public TPCCBaseWorkload<AdapterType, id_count>
{
   using Base = TPCCBaseWorkload<AdapterType, id_count>;
   MergedAdapterType& merged;

   // ol_sec1_t
   std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> cartesianProducts(
       std::vector<std::pair<ol_sec1_t::Key, ol_sec1_t>>& cached_left,
       std::vector<std::pair<stock_t::Key, stock_t>>& cached_right,
       std::function<bool(const ol_sec1_t&)> filter = [](const ol_sec1_t&) { return true; })
   {
      std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> results;
      results.reserve(cached_left.size() * cached_right.size());  // Reserve memory to avoid reallocations

      for (auto& left : cached_left) {
         for (auto& right : cached_right) {
            if (!filter(left.second))
               continue;
            const auto [key, rec] = MergeJoin<ol_sec1_t, stock_t, joined1_t>::merge(left.first, left.second, right.first, right.second);
            results.push_back({key, rec.toSelected(key)});
         }
      }
      return results;
   }

   // ol_sec0_t
   std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> cartesianProducts(
       std::vector<std::pair<ol_sec0_t::Key, ol_sec0_t>>& cached_left,
       std::vector<std::pair<stock_t::Key, stock_t>>& cached_right,
       std::function<bool(const ol_sec1_t&)> expand_filter = [](const ol_sec1_t&) { return true; })
   {
      std::vector<std::pair<joined_selected_t::Key, joined_selected_t>> results;
      results.reserve(cached_left.size() * cached_right.size());  // Reserve memory to avoid reallocations
      ol_sec1_t::Key curr_key;
      ol_sec1_t expanded_rec;
      for (auto& left : cached_left) {
         for (auto& right : cached_right) {
            auto& [left_key, left_rec] = left;
            if (left_key != curr_key) {
               ol_sec1_t expanded_rec;
               this->tpcc->orderline.lookup1(
                   {left_key.ol_w_id, left_key.ol_d_id, left_key.ol_o_id, left_key.ol_number}, [&](const orderline_t& ol_rec) {
                      expanded_rec = {ol_rec.ol_supply_w_id, ol_rec.ol_delivery_d, ol_rec.ol_quantity, ol_rec.ol_amount, ol_rec.ol_dist_info};
                   });
            }
            if (!expand_filter(expanded_rec))
               continue;
            const auto [key, rec] = MergeJoin<ol_sec1_t, stock_t, joined1_t>::merge(left_key, expanded_rec, right.first, right.second);
            results.push_back({key, rec.toSelected(key)});
         }
      }
      return results;
   }

  public:
   TPCCMergedWorkload(TPCCWorkload<AdapterType>* tpcc, MergedAdapterType& merged) : Base(tpcc), merged(merged) {}

   void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      stock_t::Key start_key = {w_id, 0};  // Starting from the first item in the warehouse

      atomic<uint64_t> scanCardinality = 0;
      uint64_t produced = 0;

      stock_t::Key current_key = start_key;

      std::vector<std::pair<orderline_sec_t::Key, orderline_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      auto filter = [&](const ol_sec1_t& rec) { return rec.ol_delivery_d >= since; };

      merged.template scan<stock_t, orderline_sec_t>(
          start_key,
          [&](const stock_t::Key& key, const stock_t& rec) {
             ++scanCardinality;
             if (key.s_w_id != w_id) {
                return false;
             }
             if (key.s_i_id != current_key.s_i_id) {
                auto cartesian_products = cartesianProducts(cached_left, cached_right, filter);
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
             if (key.ol_w_id != w_id)
                return false;  // End of range scan
             if (key.ol_d_id != d_id)
                return true;  // key filter
             if (key.ol_i_id != current_key.s_i_id)
                return true;  // No matching stock record
             cached_left.push_back({key, rec});
             return true;
          },
          []() { /* undo */ });

      auto final_cartesian_products = cartesianProducts(cached_left, cached_right, filter);
      produced += final_cartesian_products.size();
   }

   void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      std::vector<std::pair<orderline_sec_t::Key, orderline_sec_t>> cached_left;
      std::vector<std::pair<stock_t::Key, stock_t>> cached_right;

      merged.template scan<stock_t, orderline_sec_t>(
          stock_t::Key{w_id, i_id},
          [&](const stock_t::Key& key, const stock_t& rec) {
             if (key.s_w_id != w_id || key.s_i_id != i_id) {
                return false;
             }
             cached_right.push_back({key, rec});
             return true;
          },
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& rec) {
             // Passes lookup point
             if (key.ol_w_id != w_id || key.ol_i_id != i_id)
                return false;
             // No matching stock records
             if (cached_right.empty())
                return false;
             // Key filter
             if (!FLAGS_locality_read && key.ol_d_id != d_id)
                return true;
             cached_left.push_back({key, rec});
             return true;
          },
          []() { /* undo */ });

      auto results = cartesianProducts(cached_left, cached_right);  // No filter
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

   void loadOrderlineSecondaryToMerged(Integer w_id = 0)
   {
      this->loadOrderlineSecondaryCallback(
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& payload) { merged.template insert<orderline_sec_t>(key, payload); }, w_id);
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

   void addSizesToCsv(double core_size, uint64_t core_ms, double merged_size, uint64_t merged_ms)
   {
      std::string config = Base::getConfigString();
      std::ofstream csv_file(Base::getCsvFile("merged_size.csv"), std::ios::app);
      csv_file << "core," << config << "," << core_size << "," << core_ms << std::endl;
      csv_file << "merged_index," << config << "," << merged_size << "," << merged_ms << std::endl;
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point t1,
                 std::chrono::steady_clock::time_point t2,
                 leanstore::cr::CRManager& crm)
   {
      u64 core_page_count = 0;
      core_page_count = this->getCorePageCount(crm);
      auto core_time = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
      u64 merged_page_count = 0;
      crm.scheduleJobSync(0, [&]() { merged_page_count = merged.estimatePages(); });
      auto merged_time = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

      addSizesToCsv(Base::pageCountToGB(core_page_count), core_time, Base::pageCountToGB(merged_page_count), merged_time);
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      auto t1 = std::chrono::steady_clock::now();
      auto t2 = std::chrono::steady_clock::now();
      logSizes(t0, t1, t2, crm);
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point sec_start,
                 std::chrono::steady_clock::time_point sec_end,
                 RocksDB& map)
   {
      std::array<uint64_t, id_count> sizes = this->compactAndGetSizes(map);

      uint64_t core_size = std::accumulate(sizes.begin(), sizes.begin() + 11, 0);

      uint64_t merged_size = sizes.at(11);

      addSizesToCsv(Base::byteToGB(core_size), std::chrono::duration_cast<std::chrono::milliseconds>(sec_start - t0).count(),
                    Base::byteToGB(merged_size), std::chrono::duration_cast<std::chrono::milliseconds>(sec_end - sec_start).count());
   }

   void logSizes(RocksDB& map)
   {
      auto t0 = std::chrono::steady_clock::now();
      auto t1 = std::chrono::steady_clock::now();
      auto t2 = std::chrono::steady_clock::now();
      logSizes(t0, t1, t2, map);
   }
};
