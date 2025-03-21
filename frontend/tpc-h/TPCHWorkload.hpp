#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <vector>
#include "Exceptions.hpp"
#include "Tables.hpp"

#include "Units.hpp"
#include "Views.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"

#include "Join.hpp"
#include "tabulate/table.hpp"

DEFINE_double(tpch_scale_factor, 1, "TPC-H scale factor");

template <template <typename> class AdapterType, class MergedAdapterType>
class TPCHWorkload
{
  private:
   leanstore::LeanStore& db;

   leanstore::profiling::BMTable bm_table;
   leanstore::profiling::DTTable dt_table;
   leanstore::profiling::CPUTable cpu_table;
   leanstore::profiling::CRTable cr_table;
   leanstore::profiling::ConfigsTable configs_table;
   std::vector<leanstore::profiling::ProfilingTable*> tables;
   // TODO: compatible with rocksdb
   AdapterType<part_t>& part;
   AdapterType<supplier_t>& supplier;
   AdapterType<partsupp_t>& partsupp;
   AdapterType<customerh_t>& customer;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;
   AdapterType<nation_t>& nation;
   AdapterType<region_t>& region;
   // TODO: Views
   AdapterType<joinedPPsL_t>& joinedPPsL;
   AdapterType<joinedPPs_t>& joinedPPs;
   AdapterType<merged_lineitem_t>& sortedLineitem;
   MergedAdapterType& mergedPPsL;

  public:
   TPCHWorkload(leanstore::LeanStore& db,
                AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r,
                MergedAdapterType& mbj,
                AdapterType<joinedPPsL_t>& jppsl,
                AdapterType<joinedPPs_t>& jpps,
                AdapterType<merged_lineitem_t>& sl)
       : db(db),
         part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         joinedPPsL(jppsl),
         joinedPPs(jpps),
         mergedPPsL(mbj),
         sortedLineitem(sl),
         bm_table(*db.buffer_manager.get()),
         dt_table(*db.buffer_manager.get()),
         tables({&bm_table, &dt_table, &cpu_table, &cr_table})
   {
      static fLS::clstring tpch_scale_factor_str = std::to_string(FLAGS_tpch_scale_factor);
      leanstore::LeanStore::addStringFlag("TPCH_SCALE", &tpch_scale_factor_str);
      for (auto& t : tables) {
         t->open();
         t->next();
      }
   }

  private:
   static constexpr Integer PART_SCALE = 200000;
   static constexpr Integer SUPPLIER_SCALE = 10000;
   static constexpr Integer CUSTOMER_SCALE = 150000;
   static constexpr Integer ORDERS_SCALE = 1500000;
   static constexpr Integer LINEITEM_SCALE = 6000000;
   static constexpr Integer PARTSUPP_SCALE = 800000;
   static constexpr Integer NATION_COUNT = 25;
   static constexpr Integer REGION_COUNT = 5;

   inline Integer getPartID() { return urand(1, PART_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getSupplierID() { return urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getCustomerID() { return urand(1, CUSTOMER_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getOrderID() { return urand(1, ORDERS_SCALE * FLAGS_tpch_scale_factor); }

   inline Integer getNationID() { return urand(1, NATION_COUNT); }

   inline Integer getRegionID() { return urand(1, REGION_COUNT); }

   void resetTables()
   {
      for (auto& t : tables) {
         t->next();
      }
   }

   std::pair<std::vector<variant<std::string, const char*, tabulate::Table>>, std::vector<variant<std::string, const char*, tabulate::Table>>>
   summarizeStats(long elapsed)
   {
      std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_header;
      std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_data;
      tx_console_header.reserve(20);
      tx_console_data.reserve(20);

      tx_console_header.push_back("Elapsed (us)");
      tx_console_data.push_back(std::to_string(elapsed));

      tx_console_header.push_back("W MiB");
      tx_console_data.push_back(bm_table.get("0", "w_mib"));

      tx_console_header.push_back("R MiB");
      tx_console_data.push_back(bm_table.get("0", "r_mib"));
      if (cpu_table.workers_agg_events.contains("instr")) {
         const double instr_cnt = cpu_table.workers_agg_events["instr"];
         tx_console_header.push_back("Instrs");
         tx_console_data.push_back(std::to_string(instr_cnt));
      }

      if (cpu_table.workers_agg_events.contains("cycle")) {
         const double cycles_cnt = cpu_table.workers_agg_events["cycle"];
         tx_console_header.push_back("Cycles");
         tx_console_data.push_back(std::to_string(cycles_cnt));
      }

      if (cpu_table.workers_agg_events.contains("CPU")) {
         tx_console_header.push_back("Utilized CPUs");
         tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["CPU"]));
      }

      if (cpu_table.workers_agg_events.contains("task")) {
         tx_console_header.push_back("CPUTime(ms)");
         tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["task"]));
      }

      if (cpu_table.workers_agg_events.contains("L1-miss")) {
         tx_console_header.push_back("L1-miss");
         tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["L1-miss"]));
      }

      if (cpu_table.workers_agg_events.contains("LLC-miss")) {
         tx_console_header.push_back("LLC-miss");
         tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["LLC-miss"]));
      }

      if (cpu_table.workers_agg_events.contains("GHz")) {
         tx_console_header.push_back("GHz");
         tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["GHz"]));
      }

      tx_console_header.push_back("WAL GiB/s");
      tx_console_data.push_back(cr_table.get("0", "wal_write_gib"));

      tx_console_header.push_back("GCT GiB/s");
      tx_console_data.push_back(cr_table.get("0", "gct_write_gib"));

      u64 dt_page_reads = dt_table.getSum("dt_page_reads");
      tx_console_header.push_back("SSDReads");
      u64 dt_page_writes = dt_table.getSum("dt_page_writes");
      tx_console_header.push_back("SSDWrites");

      tx_console_data.push_back(std::to_string(dt_page_reads));
      tx_console_data.push_back(std::to_string(dt_page_writes));

      return {tx_console_header, tx_console_data};
   }

   void logTables(long elapsed, std::string csv_dir)
   {
      u64 config_hash = configs_table.hash();
      std::vector<std::ofstream> csvs;
      std::string csv_dir_abs = FLAGS_csv_path + "/" + csv_dir;
      std::filesystem::create_directories(csv_dir_abs);
      for (u64 t_i = 0; t_i < tables.size(); t_i++) {
         csvs.emplace_back();
         auto& csv = csvs.back();
         csv.open(csv_dir_abs + "/" + tables[t_i]->getName() + ".csv", std::ios::app);
         csv << std::setprecision(2) << std::fixed;

         if (csv.tellp() == 0) {  // no header
            csv << "c_hash";
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.first;
            }
            csv << endl;
         }
         // assert(tables[t_i]->size() == 1);
         for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
            csv << config_hash;
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.second.values[r_i];
            }
            csv << endl;
         }
         csv.close();
      }

      auto [tx_console_header, tx_console_data] = summarizeStats(elapsed);
      std::ofstream csv_sum;
      csv_sum.open(csv_dir_abs + "sum.csv", std::ios::app);
      if (csv_sum.tellp() == 0) {  // no header
         for (auto& h : tx_console_header) {
            std::visit([&csv_sum](auto&& arg) { csv_sum << arg << ","; }, h);
         }
         csv_sum << endl;
      }
      for (auto& d : tx_console_data) {
         std::visit([&csv_sum](auto&& arg) { csv_sum << arg << ","; }, d);
      }
      csv_sum << endl;

      tabulate::Table table;
      table.add_row(tx_console_header);
      table.add_row(tx_console_data);
      table.format().width(10);
      printTable(table);
   }

   static void printTable(tabulate::Table& table)
   {
      std::stringstream ss;
      table.print(ss);
      string str = ss.str();
      for (u64 i = 0; i < str.size(); i++) {
         cout << str[i];
      }
      cout << std::endl;
   }

  public:
   // TXs: Measure end-to-end time
   void basicJoin()
   {
      // Enumrate materialized view
      resetTables();
      [[maybe_unused]] long produced = 0;
      auto inspect_produced = [&](const std::string& msg) {
         if (produced % 100 == 0) {
            std::cout << "\r" << msg << (double) produced / 1000 << "k------------------------------------";
         }
         produced++;
      };
      auto mtdv_start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan(
          {},
          [&](const auto&, const auto&) {
             inspect_produced("Enumerating materialized view: ");
             return true;
          },
          [&]() {});
      std::cout << std::endl;
      auto mtdv_end = std::chrono::high_resolution_clock::now();
      auto mtdv_t = std::chrono::duration_cast<std::chrono::microseconds>(mtdv_end - mtdv_start).count();
      logTables(mtdv_t, "mtdv");

      // Scan merged index + join on the fly
      resetTables();
      auto merged_start = std::chrono::high_resolution_clock::now();

      mergedPPsL.template scanJoin<PPsL_JK, joinedPPsL_t, merged_part_t, merged_partsupp_t, merged_lineitem_t>();
      
      auto merged_end = std::chrono::high_resolution_clock::now();
      auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
      logTables(merged_t, "merged");
   }

   void basicGroup();

   void basicJoinGroup();

   // ------------------------------------LOAD-------------------------------------------------

   void prepare()
   {
      std::cout << "Preparing TPC-H" << std::endl;
      [[maybe_unused]] Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
      Integer h_id = 0;
      leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
   }

   void printProgress(std::string msg, Integer i, Integer scale)
   {
      if (i % 1000 == 1 || i == scale) {
         double progress = (double)i / scale * 100;
         std::cout << "\rLoading " << msg << ": " << progress << "%------------------------------------";
      }
      if (i == scale) {
         std::cout << std::endl;
      }
   }

   void loadPart()
   {
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         // std::cout << "partkey: " << i << std::endl;
         part.insert(part_t::Key({i}), part_t::generateRandomRecord());
         printProgress("part", i, PART_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadSupplier()
   {
      for (Integer i = 1; i <= SUPPLIER_SCALE * FLAGS_tpch_scale_factor; i++) {
         supplier.insert(supplier_t::Key({i}), supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadPartsuppLineitem()
   {
      // Generate and shuffle lineitem keys
      std::vector<lineitem_t::Key> lineitems = {};
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2);
         for (Integer j = 1; j <= lineitem_cnt; j++) {
            lineitems.push_back(lineitem_t::Key({i, j}));
         }
      }
      std::random_shuffle(lineitems.begin(), lineitems.end());
      // Load partsupp and lineitem
      long l_global_cnt = 0;
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         // Randomly select suppliers for this part
         Integer supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2);
         std::vector<Integer> suppliers = {};
         while (true) {
            Integer supplier_id = urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
            if (std::find(suppliers.begin(), suppliers.end(), supplier_id) == suppliers.end()) {
               suppliers.push_back(supplier_id);
            }
            if (suppliers.size() == supplier_cnt) {
               break;
            }
         }
         for (auto& s : suppliers) {
            // load 1 partsupp
            partsupp.insert(partsupp_t::Key({i, s}), partsupp_t::generateRandomRecord());
            // load lineitems
            Integer lineitem_cnt = urand(0, LINEITEM_SCALE / PARTSUPP_SCALE * 2); // No reference integrity but mostly matched
            for (Integer l = 0; l < lineitem_cnt; l++) {
               auto rec = lineitem_t::generateRandomRecord([i]() { return i; }, [s]() { return s; });
               if (l_global_cnt >= lineitems.size()) {
                  std::cout << "Warning: lineitem table is not fully populated" << std::endl;
                  break;
               } else {
                  lineitem.insert(lineitems[l_global_cnt], rec);
                  l_global_cnt++;
               }
            }
         }
         printProgress("partsupp and lineitem", i, PART_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadCustomer()
   {
      for (Integer i = 1; i <= CUSTOMER_SCALE * FLAGS_tpch_scale_factor; i++) {
         customer.insert(customerh_t::Key({i}), customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, CUSTOMER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadOrders()
   {
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         orders.insert(orders_t::Key({i}), orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
         printProgress("orders", i, ORDERS_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadNation()
   {
      for (Integer i = 1; i <= NATION_COUNT; i++) {
         nation.insert(nation_t::Key({i}), nation_t::generateRandomRecord([this]() { return this->getRegionID(); }));
         printProgress("nation", i, NATION_COUNT);
      }
   }

   void loadRegion()
   {
      for (Integer i = 1; i <= REGION_COUNT; i++) {
         region.insert(region_t::Key({i}), region_t::generateRandomRecord());
         printProgress("region", i, REGION_COUNT);
      }
   }

   // ------------------------------------LOAD VIEWS-------------------------------------------------

   void loadSortedLineitem()
   {
      // sort lineitem
      this->lineitem.resetIterator();
      while (true) {
         auto kv = this->lineitem.next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         PPsL_JK jk{v.l_partkey, v.l_suppkey};
         merged_lineitem_t::Key k_new({jk, k});
         merged_lineitem_t v_new(v);
         this->sortedLineitem.insert(k_new, v_new);
      }
   }

   void loadBasicJoin()
   {
      std::cout << "Loading basic join" << std::endl;
      // first join
      {
         std::cout << "Joining part and partsupp" << std::endl;
         this->part.resetIterator();
         this->partsupp.resetIterator();
         Join<PPsL_JK, joinedPPs_t, part_t::Key, part_t, partsupp_t::Key, partsupp_t> join1(
             [](part_t::Key& k, part_t&) { return PPsL_JK{k.p_partkey, 0}; },
             [](partsupp_t::Key& k, partsupp_t&) { return PPsL_JK{k.ps_partkey, k.ps_suppkey}; },
             [](u8* in, u16) {
                part_t::Key k;
                part_t::unfoldKey(in, k);
                return k;
             },
             [](u8* in, u16) {
                partsupp_t::Key k;
                partsupp_t::unfoldKey(in, k);
                return k;
             },
             [this]() { return this->part.next(); }, [this]() { return this->partsupp.next(); });
         while (true) {
            auto kv = join1.next();
            if (kv == std::nullopt) {
               break;
            }
            auto& [k, v] = *kv;
            joinedPPs.insert(k, v);
         }
      }
      
      // second join
      {
         std::cout << "Joining joinedPPs and lineitem" << std::endl;
         assert(this->sortedLineitem.estimatePages() > 0);
         this->joinedPPs.resetIterator();
         this->sortedLineitem.resetIterator();
         Join<PPsL_JK, joinedPPsL_t, joinedPPs_t::Key, joinedPPs_t, merged_lineitem_t::Key, merged_lineitem_t> join2(
            [](joinedPPs_t::Key& k, joinedPPs_t&) { return k.jk; }, [](merged_lineitem_t::Key& k, merged_lineitem_t&) { return k.jk; },
            [](u8* in, u16) {
               joinedPPs_t::Key k;
               joinedPPs_t::unfoldKey(in, k);
               return k;
            },
            [](u8* in, u16) {
               merged_lineitem_t::Key k;
               merged_lineitem_t::unfoldKey(in, k);
               return k;
            },
            [this]() { return this->joinedPPs.next(); }, [this]() { return this->sortedLineitem.next(); });
         while (true) {
            auto kv = join2.next();
            if (kv == std::nullopt) {
               break;
            }
            auto& [k, v] = *kv;
            joinedPPsL.insert(k, v);
         }
      }
   };

   template <typename JK>
   struct HeapEntry {
      JK jk;
      std::vector<std::byte> k;
      std::vector<std::byte> v;
      u8 source;

      bool operator>(const HeapEntry& other) const { return jk > other.jk; }
   };

   template <typename JK>
   void heapMerge(std::vector<std::function<HeapEntry<JK>()>> sources, std::vector<std::function<void(HeapEntry<JK>&)>> consumes)
   {  
      std::priority_queue<HeapEntry<JK>, std::vector<HeapEntry<JK>>, std::greater<HeapEntry<JK>>> heap;
      for (auto& s: sources) {
         HeapEntry<JK> entry = s();
         heap.push(entry);
      }
      while (!heap.empty()) {
         HeapEntry<JK> entry = heap.top();
         heap.pop();
         consumes[entry.source](entry);
         HeapEntry<JK> next = sources[entry.source]();
         if (next.jk != JK::max()) {
            heap.push(next);
         }
      }
   }

   void loadMergedBasicJoin()
   {
      auto part_src = [this]() {
         auto kv = part.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 0};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));

         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.p_partkey, 0}, std::move(k_bytes), std::move(v_bytes), 0};
      };

      auto part_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_part_t::Key k_new({entry.jk, bytes_to_struct<part_t::Key>(entry.k)});
         merged_part_t v_new(bytes_to_struct<part_t>(entry.v));
         this->mergedPPsL.insert(k_new, v_new);
      };

      auto partsupp_src = [this]() {
         auto kv = partsupp.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 1};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));

         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.ps_partkey, k.ps_suppkey}, std::move(k_bytes), std::move(v_bytes), 1};
      };

      auto partsupp_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_partsupp_t::Key k_new({entry.jk, bytes_to_struct<partsupp_t::Key>(entry.k)});
         merged_partsupp_t v_new(bytes_to_struct<partsupp_t>(entry.v));
         this->mergedPPsL.insert(k_new, v_new);
      };

      auto lineitem_src = [this]() {
         auto kv = sortedLineitem.next();
         if (kv == std::nullopt) {
            return HeapEntry<PPsL_JK>{PPsL_JK::max(), {}, {}, 2};
         }
         auto& [k, v] = *kv;
         std::vector<std::byte> k_bytes(sizeof(k));
         std::memcpy(k_bytes.data(), &k, sizeof(k));
         std::vector<std::byte> v_bytes(sizeof(v));
         std::memcpy(v_bytes.data(), &v, sizeof(v));
         return HeapEntry<PPsL_JK>{PPsL_JK{k.jk.l_partkey, k.jk.l_partsuppkey}, std::move(k_bytes), std::move(v_bytes), 2};
      };

      auto lineitem_consume = [this](HeapEntry<PPsL_JK>& entry) {
         merged_lineitem_t::Key k_new = bytes_to_struct<merged_lineitem_t::Key>(entry.k);
         merged_lineitem_t v_new = bytes_to_struct<merged_lineitem_t>(entry.v);
         this->mergedPPsL.insert(k_new, v_new);
      };
      
      heapMerge<PPsL_JK>({part_src, partsupp_src, lineitem_src}, {part_consume, partsupp_consume, lineitem_consume});
   }

   void loadBasicGroup();

   void loadBasicJoinGroup();

   // Log size
   void logSize()
   {
      std::cout << "Logging size" << std::endl;
      std::ofstream size_csv;
      size_csv.open(FLAGS_csv_path + "/size.csv", std::ios::app);
      if (size_csv.tellp() == 0) {
         size_csv << "table,size (MiB)" << std::endl;
      }
      std::cout << "table,size" << std::endl;
      std::vector<std::ostream*> out = {&std::cout, &size_csv};
      for (std::ostream* o: out) {
         *o << "part," << part.size() << std::endl;
         *o << "supplier," << supplier.size() << std::endl;
         *o << "partsupp," << partsupp.size() << std::endl;
         *o << "customer," << customer.size() << std::endl;
         *o << "orders," << orders.size() << std::endl;
         *o << "lineitem," << lineitem.size() << std::endl;
         *o << "nation," << nation.size() << std::endl;
         *o << "region," << region.size() << std::endl;
         *o << "joinedPPsL," << joinedPPsL.size() << std::endl;
         *o << "joinedPPs," << joinedPPs.size() << std::endl;
         *o << "sortedLineitem," << sortedLineitem.size() << std::endl;
         *o << "mergedPPsL," << mergedPPsL.size() << std::endl;
      }
      size_csv.close();
   }
};
