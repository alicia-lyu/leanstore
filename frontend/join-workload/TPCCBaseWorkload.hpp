#pragma once
#include <array>
#include <filesystem>
#include <fstream>
#include "../shared/RocksDB.hpp"
#include "../shared/RocksDBAdapter.hpp"
#include "../tpc-c/TPCCWorkload.hpp"
#include "Exceptions.hpp"
#include "Join.hpp"
#include "JoinedSchema.hpp"
#include "Units.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "types.hpp"

#if INCLUDE_COLUMNS == 1 || INCLUDE_COLUMNS == 2
#define EXPAND_OR_PROJECT_JOIN_PAYLOAD() auto selected_payload = payload.toSelected();
#elif INCLUDE_COLUMNS == 0
#define EXPAND_OR_PROJECT_JOIN_PAYLOAD()                                                                                                   \
   if (stock_key.s_w_id != key.w_id || stock_key.s_i_id != key.i_id) {                                                      \
      stock_key = {key.w_id, key.i_id};                                                       stock_payload = stock_t();                              \
      this->tpcc->stock.tryLookup(stock_key, [&](const stock_t& rec) { stock_payload = rec; });                           \
   }                                                                                                                        \
   if (orderline_key.ol_w_id != key.w_id || orderline_key.ol_d_id != key.ol_d_id || orderline_key.ol_o_id != key.ol_o_id || \
       orderline_key.ol_number != key.ol_number) {                                                                          \
      orderline_key = {key.w_id, key.ol_d_id, key.ol_o_id, key.ol_number};                                                  \
      orderline_payload = orderline_t();                                                                                 \
      this->tpcc->orderline.tryLookup(orderline_key, [&](const orderline_t& rec) { orderline_payload = rec; });               \
   }                                                                                                                        \
   auto selected_payload = payload.expand(key, stock_payload, orderline_payload);  // Unique to joined0_t
#else
#error "Unsupported value for INCLUDE_COLUMNS"
#endif

#if INCLUDE_COLUMNS == 0
#define CONDITIONAL_CALL(custom_code) {}
#else
#define CONDITIONAL_CALL(custom_code) custom_code
#endif

#define PREPARE_MERGE_JOIN()                                                                                        \
   std::unique_ptr<Scanner<orderline_sec_t>> orderline_scanner = this->orderline_secondary->getScanner();           \
   auto stock_scanner = this->stock_secondary->getScanner();                                                        \
   orderline_scanner->seek({seek_key.w_id, seek_key.i_id, seek_key.ol_d_id, seek_key.ol_o_id, seek_key.ol_number}); \
   stock_scanner->seek({seek_key.w_id, seek_key.i_id});                                                             \
   MergeJoin<orderline_sec_t, stock_sec_t, joined_t> merge_join(orderline_scanner.get(), stock_scanner.get(), FLAGS_outer_join);

template <template <typename> class AdapterType, int id_count>  // Default to 14, defined in ../shared/TPCCWorkload.hpp
class TPCCBaseWorkload
{
  protected:
   TPCCWorkload<AdapterType>* tpcc;

   static constexpr bool ROCKSDB = std::is_same_v<AdapterType<int>, RocksDBAdapter<int>>;

   AdapterType<orderline_sec_t>* orderline_secondary;
   AdapterType<stock_sec_t>* stock_secondary;  // If included columns are the same, just a pointer to stock_t

  public:
   TPCCBaseWorkload(TPCCWorkload<AdapterType>* tpcc,
                    AdapterType<orderline_sec_t>* orderline_secondary = nullptr,
                    AdapterType<stock_sec_t>* stock_secondary = nullptr)
       : tpcc(tpcc), orderline_secondary(orderline_secondary), stock_secondary(stock_secondary)
   {
      if constexpr (INCLUDE_COLUMNS == 0) {
         std::cout << "Columns included: Key only" << std::endl;
      } else if constexpr (INCLUDE_COLUMNS == 1) {
         std::cout << "Columns included: All" << std::endl;
      } else if constexpr (INCLUDE_COLUMNS == 2) {
         std::cout << "Columns included: Selected" << std::endl;
      } else {
         throw std::runtime_error("Invalid INCLUDE_COLUMNS value");
      }
      if (FLAGS_locality_read) {
         std::cout << "Locality read: true" << std::endl;
      }
   }
   virtual ~TPCCBaseWorkload() { std::cout << "TPCCBaseWorkload::~TPCCBaseWorkload" << std::endl; }

   static bool isSelected(Integer i_id) { return TPCCWorkload<AdapterType>::isSelected(i_id); }

   void loadOrderlineSecondaryCallback(std::function<void(const orderline_sec_t::Key&, const orderline_sec_t&)> orderline_insert_cb, Integer w_id = 0)
   {
      std::cout << "Loading orderline secondary index for warehouse " << w_id << std::endl;
      auto orderline_scanner = this->tpcc->orderline.getScanner();
      orderline_scanner->seek({w_id, 0, 0, 0});
      while (true) {
         auto ret = orderline_scanner->next();
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         if (key.ol_w_id != w_id)
            break;

         typename orderline_sec_t::Key sec_key = {key.ol_w_id, payload.ol_i_id, key.ol_d_id, key.ol_o_id, key.ol_number};
         if constexpr (std::is_same_v<orderline_sec_t, ol_sec0_t>) {
            orderline_insert_cb(sec_key, {});
         } else if constexpr (std::is_same_v<orderline_sec_t, ol_sec1_t>) {
            orderline_sec_t sec_payload = {payload.ol_supply_w_id, payload.ol_delivery_d, payload.ol_quantity, payload.ol_amount,
                                           payload.ol_dist_info};
            orderline_insert_cb(sec_key, sec_payload);
         } else {
            UNREACHABLE();
         }
      }
   }

   virtual void loadOrderlineSecondary(Integer w_id = 0)
   {
      this->loadOrderlineSecondaryCallback(
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& payload) { this->orderline_secondary->insert(key, payload); }, w_id);
   }

   // Join with no extra column selection or expansion, intended for materialized join
   void joinOrderlineAndStock(std::function<bool(joined_t::Key&, joined_t&)> cb, joined_t::Key seek_key = {0, 0, 0, 0, 0})
   {
      std::cout << "TPCCBaseWorkload::joinOrderlineAndStock from" << seek_key << std::endl;

      PREPARE_MERGE_JOIN();

      [[maybe_unused]] u64 produced = 0;

      while (true) {
         auto ret = merge_join.next();
         produced++;
         // if (produced % 100000 == 0) {
         //    std::cout << "TPCCBaseWorkload::joinOrderlineAndStock -- Produced " << produced << " joined records" << std::endl;
         // }
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         // custom code to process key and payload
         if (!cb(key, payload))
            break;
      }
   }

   // joined0_t: Back join with base tables to get joined_selected_t to answer queries
   // joined1_t or joined_selected_t: Project only selected columns
   void joinOrderlineAndStockOnTheFly(std::function<bool(joined_selected_t::Key&, joined_selected_t&)> cb, joined_t::Key seek_key = {0, 0, 0, 0, 0})
   {
      PREPARE_MERGE_JOIN();

      // Only useful for joined0_t
      [[maybe_unused]] stock_t::Key stock_key;
      [[maybe_unused]] stock_t stock_payload;
      [[maybe_unused]] orderline_t::Key orderline_key;
      [[maybe_unused]] orderline_t orderline_payload;

      while (true) {
         auto ret = merge_join.next();
         if (!ret.has_value())
            break;
         auto [key, payload] = ret.value();
         EXPAND_OR_PROJECT_JOIN_PAYLOAD();
         if (!cb(key, selected_payload))
            break;
      }
   }

   virtual void recentOrdersStockInfo(Integer w_id, Integer d_id, Timestamp since)
   {
      std::vector<joined_selected_t> results;
      this->joinOrderlineAndStockOnTheFly(
          [&](joined_selected_t::Key& key, joined_selected_t& payload) {
             if (key.w_id != w_id)
                return false;
             if (key.ol_d_id != d_id)
                return true;
             if (payload.ol_delivery_d > since) {
                results.push_back(payload);
             }
             return true;
          },
          {w_id, 0, d_id, 0, 0});
   }

   virtual void ordersByItemId(Integer w_id, Integer d_id, Integer i_id)
   {
      std::vector<joined_selected_t> results;
      this->joinOrderlineAndStockOnTheFly(
          [&](joined_selected_t::Key& key, joined_selected_t& payload) {
             if (key.w_id != w_id)
                return false;
             if (!FLAGS_locality_read && key.ol_d_id != d_id)
                return false;
             if (key.i_id != i_id)
                return false;
             results.push_back(payload);
             return true;
          },
          {w_id, i_id, FLAGS_locality_read ? 0 : d_id, 0, 0});
   }

   void newOrderRndCallback(
       Integer w_id,
       std::function<void(const stock_sec_t::Key&, std::function<void(stock_sec_t&)>, leanstore::UpdateSameSizeInPlaceDescriptor&, Integer qty)>
           stock_sec_update_cb,
       std::function<void(const orderline_sec_t::Key&, const orderline_sec_t&, const stock_sec_t::Key&, const stock_sec_t&)> orderline_insert_cb,
       Integer order_size = 5)
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
      std::vector<stock_sec_t> stock_record_copies;
      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer qty = qtys[i];
         stock_t::Key key = {supwares[i], itemids[i]};
         UpdateDescriptorGenerator4(stock_update_descriptor, stock_t, s_remote_cnt, s_order_cnt, s_ytd, s_quantity);
         stock_sec_t stock_record_copy;
         if (!isSelected(key.s_i_id)) {
            stock_record_copies.push_back(stock_record_copy);
            continue;
         }
         // Update secondary index of stock if it includes changed columns
         CONDITIONAL_CALL(
            stock_sec_update_cb( \
                  key, \
                  [&](stock_sec_t& rec) { \
                     rec.s_quantity = (rec.s_quantity >= qty + 10) ? rec.s_quantity - qty : rec.s_quantity + 91 - qty; \
                     rec.s_remote_cnt += (supwares[i] != w_id); \
                     rec.s_order_cnt++; \
                     rec.s_ytd += qty; \
                     stock_record_copy = rec; \
                  }, \
                  stock_update_descriptor, qty);
         )
         // Update primary index if different
         if constexpr (!std::is_same_v<stock_sec_t, stock_t>) {
            this->tpcc->stock.update1(
               key,
               [&](stock_t& rec) {
                  auto& s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
                  s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
                  rec.s_remote_cnt += (supwares[i] != w_id);
                  rec.s_order_cnt++;
                  rec.s_ytd += qty;
                  stock_record_copy = rec;
               },
               stock_update_descriptor);
         }
         stock_record_copies.push_back(stock_record_copy);
      }

      // Batch insert orderline records
      for (unsigned i = 0; i < lineNumbers.size(); i++) {
         Integer lineNumber = lineNumbers[i];
         Integer supware = supwares[i];
         Integer itemid = itemids[i];
         Numeric qty = qtys[i];

         Numeric i_price = this->tpcc->item.lookupField({itemid}, &item_t::i_price);  // TODO: rollback on miss
         Varchar<24> s_dist = this->tpcc->template randomastring<24>(24, 24);
         Numeric ol_amount = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
         Timestamp ol_delivery_d = 0;  // NULL
         this->tpcc->orderline.insert({w_id, d_id, o_id, lineNumber}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
         // ********** Update Merged Index **********
         if constexpr (std::is_same_v<orderline_sec_t, ol_sec1_t>) {
            ol_sec1_t rec = {supware, ol_delivery_d, qty, ol_amount, s_dist};
            orderline_insert_cb(orderline_sec_t::Key{w_id, itemid, d_id, o_id, lineNumber}, orderline_sec_t(rec), stock_t::Key{supware, itemid}, stock_record_copies.at(i));
         } else if constexpr (std::is_same_v<orderline_sec_t, ol_sec0_t>) {
            orderline_insert_cb(orderline_sec_t::Key{w_id, itemid, d_id, o_id, lineNumber}, orderline_sec_t(), stock_t::Key{supware, itemid}, stock_record_copies.at(i));
         } else {
            UNREACHABLE();
         }
      }
   }

   virtual void newOrderRnd(Integer w_id, Integer order_size = 5)
   {
      this->newOrderRndCallback(
          w_id,
          [&](const stock_sec_t::Key& key, std::function<void(stock_sec_t&)> cb, leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor, Integer) {
             if (isSelected(key.s_i_id))
                this->stock_secondary->update1(key, cb, update_descriptor);
          },
          [&](const orderline_sec_t::Key& key, const orderline_sec_t& payload, const stock_sec_t::Key&, const stock_sec_t&) { this->orderline_secondary->insert(key, payload); }, order_size);
   }

   int tx(Integer w_id, int read_percentage, int scan_percentage, int write_percentage, Integer order_size = 5)
   {
      int rnd = (int)leanstore::utils::RandomGenerator::getRand(0, read_percentage + scan_percentage + write_percentage);
      if (rnd < read_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Integer i_id = leanstore::utils::RandomGenerator::getRand(1, (int)(this->tpcc->ITEMS_NO * this->tpcc->scale_factor) + 1);
         this->ordersByItemId(w_id, d_id, i_id);
         return 0;
      } else if (rnd < read_percentage + scan_percentage) {
         Integer d_id = leanstore::utils::RandomGenerator::getRand(1, 11);
         Timestamp since = 1;
         this->recentOrdersStockInfo(w_id, d_id, since);
         return 0;
      } else {
         this->newOrderRnd(w_id, order_size);
         return 0;
      }
   }

   virtual void verifyWarehouse(Integer w_id) { tpcc->verifyWarehouse(w_id); }

   void loadStockCallback(Integer w_id,
      std::function<void(const stock_sec_t::Key&, const stock_sec_t&)> stock_sec_insert_cb
   ) { 
      std::cout << "Loading " << this->tpcc->ITEMS_NO * this->tpcc->scale_factor << " stock of warehouse " << w_id << std::endl;
      int loaded = 0;
      for (Integer i = 0; i < this->tpcc->ITEMS_NO * this->tpcc->scale_factor; i++) {
         if (!isSelected(i + 1)) {
            continue;
         }
         Varchar<50> s_data = this->tpcc->template randomastring<50>(25, 50);
         if (this->tpcc->rnd(10) == 0) {
            s_data.length = this->tpcc->rnd(s_data.length - 8);
            s_data = s_data || Varchar<10>("ORIGINAL");
         }
         stock_t::Key key{w_id, i + 1};
         
         stock_t rec = stock_t(this->tpcc->randomNumeric(10, 100), this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24),
              this->tpcc->template randomastring<24>(24, 24), this->tpcc->template randomastring<24>(24, 24), 0, 0, 0, s_data);

         stock_sec_insert_cb(key, rec);

         // Also load to primary index if type is different
         if constexpr (!std::is_same_v<stock_sec_t, stock_t>) {
            this->tpcc->stock.insert(key, rec);
         }
         loaded++;
      }
      std::cout << "Loaded " << loaded << " stock of warehouse " << w_id << std::endl;
   }

   virtual void loadStock(Integer w_id)
   {
      this->loadStockCallback(w_id,
          [&](const stock_sec_t::Key& key, const stock_sec_t& payload) { this->stock_secondary->insert(key, payload); });
   }

   // -----------------------------------------------------------------
   // -------------------------- Logging ------------------------------
   // -----------------------------------------------------------------
   // Methods not marked virtual: All relevant calls must be made without the need of dynamic casting / pointer casting

   std::string getCsvFile(std::string)
   {
      std::string size_filename = "size";
      if (FLAGS_outer_join)
         size_filename += "_outer";
      size_filename += ".csv";
      std::filesystem::path csv_path = std::filesystem::path(FLAGS_csv_path).parent_path().parent_path() / size_filename;
      std::filesystem::create_directories(csv_path.parent_path());
      std::cout << "Logging size to " << csv_path << std::endl;
      std::ofstream csv_file(csv_path, std::ios::app);
      if (!std::filesystem::exists(csv_path) || std::filesystem::file_size(csv_path) == 0)
         csv_file << "table(s),config,size,time(ms)" << std::endl;
      return csv_path;
   }

   static double pageCountToGB(uint64_t page_count)
   {
      // std::cout << "Effective page size: " << leanstore::storage::EFFECTIVE_PAGE_SIZE << std::endl;
      return (double)page_count * leanstore::storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0;
   }

   static double byteToGB(uint64_t byte_count) { return (double)byte_count / 1024.0 / 1024.0 / 1024.0; }

   static std::string getConfigString()
   {
      std::stringstream config;
      config << FLAGS_dram_gib << "|" << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS << "|" << int(FLAGS_outer_join);
      return config.str();
   }

   std::array<uint64_t, id_count> compactAndGetSizes(RocksDB& map)
   {
      std::cout << "Compacting ";
      rocksdb::Range ranges[id_count];
      for (int i = 0; i < id_count; i++) {
         u8* start = new u8[sizeof(u32)];
         const u32 folded_key_len = fold(start, i);
         rocksdb::Slice start_slice((const char*)start, folded_key_len);

         u8* limit = new u8[sizeof(u32)];
         const u32 folded_limit_len = fold(limit, i + 1);
         rocksdb::Slice limit_slice((const char*)limit, folded_limit_len);

         auto options = rocksdb::CompactRangeOptions();
         options.change_level = true;
         std::cout << i << ", ";
         auto ret = map.db->CompactRange(options, &start_slice, &limit_slice);
         assert(ret.ok());
         ranges[i] = rocksdb::Range(start_slice, limit_slice);
      }

      std::cout << std::endl;

      auto configString = getConfigString();

      rocksdb::SizeApproximationOptions options;
      options.include_memtables = true;
      options.include_files = true;
      options.files_size_error_margin = 0.1;

      uint64_t sizes[id_count];
      map.db->GetApproximateSizes(options, map.db->DefaultColumnFamily(), ranges, id_count, sizes);
      std::cout << "Sizes:";
      for (u32 i = 0; i < id_count; i++) {
         std::cout << " " << sizes[i];
      }
      std::cout << std::endl;

      std::array<uint64_t, id_count> sizes_arr;
      std::copy(std::begin(sizes), std::end(sizes), std::begin(sizes_arr));

      for (int i = 0; i < id_count; i++) {
         delete[] ranges[i].start.data();
         delete[] ranges[i].limit.data();
      }

      return sizes_arr;
   }

   uint64_t getTotalSize(RocksDB& map)
   {
      uint64_t total_size = 0;
      map.db->GetIntProperty(rocksdb::DB::Properties::kEstimateLiveDataSize, &total_size);
      return total_size;
   }

   void logSizes(RocksDB& map)
   {
      auto t0 = std::chrono::steady_clock::now();
      this->logSizes(t0, t0, t0, map);
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point sec_start,
                 std::chrono::steady_clock::time_point sec_end,
                 RocksDB& map)
   {
      std::array<uint64_t, id_count> sizes = compactAndGetSizes(map);
      int orderline_secondary_id = 11;
      int stock_id;
      if (INCLUDE_COLUMNS == 1)  stock_id = 10;
      else stock_id = 13;

      uint64_t core_size = 0;
      for (u32 i = 0; i <= 10; i++) {
         if (i == orderline_secondary_id || i == stock_id)
            continue;
         core_size += sizes[i];
      }

      uint64_t merged_size = sizes[orderline_secondary_id] + sizes[stock_id];

      std::cout << "Stock size: " << sizes[stock_id] << ", orderline secondary size: " << sizes[orderline_secondary_id] << std::endl;

      addSizesToCsv(byteToGB(core_size), std::chrono::duration_cast<std::chrono::milliseconds>(sec_start - t0).count(), byteToGB(merged_size),
                    std::chrono::duration_cast<std::chrono::milliseconds>(sec_end - sec_start).count());
   }

   void addSizesToCsv(double core_size, uint64_t core_ms, double merged_size, uint64_t merged_ms)
   {
      std::string config = getConfigString();
      std::ofstream csv_file(getCsvFile("base_size.csv"), std::ios::app);
      csv_file << "core," << config << "," << core_size << "," << core_ms << std::endl;
      csv_file << "stock+orderline_secondary," << config << "," << merged_size << "," << merged_ms << std::endl;
   }

   void logSizes(std::chrono::steady_clock::time_point t0,
                 std::chrono::steady_clock::time_point sec_start,
                 std::chrono::steady_clock::time_point sec_end,
                 leanstore::cr::CRManager& crm)
   {
      auto core_page_count = getCorePageCount(crm, INCLUDE_COLUMNS!=1); // count stock if different

      uint64_t stock_page_count = 0;
      crm.scheduleJobSync(0, [&]() { stock_page_count = this->stock_secondary->estimatePages(); });
      uint64_t orderline_secondary_page_count = 0;
      crm.scheduleJobSync(0, [&]() { orderline_secondary_page_count = this->orderline_secondary->estimatePages(); });

      std::cout << "stock page count: " << stock_page_count << ", orderline secondary page count: " << orderline_secondary_page_count << std::endl;

      addSizesToCsv(pageCountToGB(core_page_count), std::chrono::duration_cast<std::chrono::milliseconds>(sec_start - t0).count(),
                    pageCountToGB(stock_page_count + orderline_secondary_page_count),
                    std::chrono::duration_cast<std::chrono::milliseconds>(sec_end - sec_start).count());
   }

   void logSizes(leanstore::cr::CRManager& crm)
   {
      auto t0 = std::chrono::steady_clock::now();
      this->logSizes(t0, t0, t0, crm);
   }

   u64 getCorePageCount(leanstore::cr::CRManager& crm, bool count_stock = true)
   {
      u64 core_page_count = 0;
      crm.scheduleJobSync(0, [&]() {
         core_page_count += this->tpcc->warehouse.estimatePages();
         core_page_count += this->tpcc->district.estimatePages();
         core_page_count += this->tpcc->customer.estimatePages();
         core_page_count += this->tpcc->customerwdl.estimatePages();
         core_page_count += this->tpcc->history.estimatePages();
         core_page_count += this->tpcc->neworder.estimatePages();
         core_page_count += this->tpcc->order.estimatePages();
         core_page_count += this->tpcc->order_wdc.estimatePages();
         core_page_count += this->tpcc->orderline.estimatePages();
         core_page_count += this->tpcc->item.estimatePages();
         if (count_stock)
            core_page_count += this->tpcc->stock.estimatePages();
      });
      return core_page_count;
   }
};