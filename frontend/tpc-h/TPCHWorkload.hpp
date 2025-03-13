#pragma once
#include <gflags/gflags.h>
#include <chrono>
#include <fstream>
#include <functional>
#include <vector>
#include "Tables.hpp"

#include "Views.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"

#include "Join.hpp"
#include "tabulate/table.hpp"

DEFINE_int32(tpch_scale_factor, 1, "TPC-H scale factor");

template <template <typename> class AdapterType, class MergedAdapterType>
class TPCHWorkload
{
  private:
   std::unique_ptr<leanstore::storage::BufferManager> buffer_manager;
   std::unique_ptr<leanstore::cr::CRManager> cr_manager;
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
   TPCHWorkload(AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r)
       : part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         bm_table(*buffer_manager.get()),
         dt_table(*buffer_manager.get()),
         tables({&bm_table, &dt_table, &cpu_table, &cr_table})
   {
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

   void logTables(std::chrono::microseconds elapsed, std::string csv_dir)
   {
      u64 config_hash = configs_table.hash();
      std::vector<std::ofstream> csvs;
      std::ofstream::openmode open_flags;
      if (FLAGS_csv_truncate) {
         open_flags = std::ios::trunc;
      } else {
         open_flags = std::ios::app;
      }
      std::string csv_dir_abs = FLAGS_csv_path + "/" + csv_dir;
      for (u64 t_i = 0; t_i < tables.size() + 1; t_i++) {
         csvs.emplace_back();
         auto& csv = csvs.back();
         if (t_i < tables.size())
            csv.open(csv_dir_abs + "/" + tables[t_i]->getName() + ".csv", open_flags);
         else {
            csv.open(csv_dir_abs + "sum.csv", open_flags);  // summary
            continue;
         }
         csv.seekp(0, std::ios::end);
         csv << std::setprecision(2) << std::fixed;
         if (csv.tellp() == 0 && t_i < tables.size()) {  // summary is output below
            csv << "c_hash";
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.first;
            }
            csv << endl;
            csv << config_hash;
            assert(tables[t_i]->size() == 1);
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.second.values[0];
            }
            csv << endl;
         }
      }

      std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_header;
      std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_data;
      tx_console_header.reserve(20);
      tx_console_data.reserve(20);

      tx_console_header.push_back("Elapsed");
      tx_console_data.push_back(std::to_string(elapsed.count()));

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
   }

  public:
   // TXs: Measure end-to-end time
   void basicJoin()
   {
      // Enumrate materialized view
      resetTables();
      auto mtdv_start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan({}, [&](const auto&, const auto&) {});
      auto mtdv_end = std::chrono::high_resolution_clock::now();
      auto mtdv_t = std::chrono::duration_cast<std::chrono::microseconds>(mtdv_end - mtdv_start).count();
      logTables(mtdv_t, "mtdv");

      // Scan merged index + join on the fly
      resetTables();
      auto merged_start = std::chrono::high_resolution_clock::now();
      using JoinedRec = Joined<11, PPsL_JK, part_t, partsupp_t, lineitem_t>;
      using JoinedKey = JoinedRec::Key;
      PPsL_JK current_jk;
      std::vector<std::pair<merged_part_t::Key, merged_part_t>> cached_part;
      std::vector<std::pair<merged_partsupp_t::Key, merged_partsupp_t>> cached_partsupp;
      std::function<void(PPsL_JK)> comp_clear = [&](PPsL_JK jk) {
         if (current_jk != jk) {
            current_jk = jk;
            cached_part.clear();
            cached_partsupp.clear();
         }
      };
      u8 start_jk[PPsL_JK::maxFoldLength()];
      PPsL_JK::keyfold(start_jk, current_jk);

      auto part_descriptor = MergedAdapterType::template create<merged_part_t>([&](const merged_part_t::Key& k, const merged_part_t& v) {
         comp_clear(k.jk);
         cached_part.push_back({k, v});
         return false;
      });

      auto partsupp_descriptor =
          MergedAdapterType::template create<merged_partsupp_t>([&](const merged_partsupp_t::Key& k, const merged_partsupp_t& v) {
             comp_clear(k.jk);
             cached_partsupp.push_back({k, v});
             return false;
          });

      auto lineitem_descriptor =
          MergedAdapterType::template create<merged_lineitem_t>([&](const merged_lineitem_t::Key& k, const merged_lineitem_t& v) {
             comp_clear(k.jk);
             for (auto& [pk, pv] : cached_part) {
                for (auto& [psk, psv] : cached_partsupp) {
                   [[maybe_unused]]
                   JoinedKey joined_key =
                       Joined<11, PPsL_JK, part_t, partsupp_t, lineitem_t>::Key{{current_jk, std::make_tuple(pk.pk, psk.pk, k.pk)}};
                   [[maybe_unused]]
                   JoinedRec joined_rec = JoinedRec{std::make_tuple(pv.payload, psv.payload, v.payload)};
                   // Do something with the result
                }
             }
             return false;
          });

      mergedPPsL.scan(start_jk, PPsL_JK::maxFoldLength(),
                      std::vector<typename MergedAdapterType::ScanCallbackDescriptor>{part_descriptor, partsupp_descriptor, lineitem_descriptor},
                      [&]() {}  // undo
      );
      auto merged_end = std::chrono::high_resolution_clock::now();
      auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
      logTables(merged_t, "merged");
   }

   void basicGroup();

   void basicJoinGroup();

   // ------------------------------------LOAD-------------------------------------------------

   void prepare()
   {
      std::cout << "Preparing TPC-C" << std::endl;
      [[maybe_unused]] Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
      Integer h_id = 0;
      leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
   }

   void printProgress(std::string msg, Integer i, Integer scale)
   {
      if (i % 10000 == 1)
         std::cout << "\rLoading " << msg << ": " << i / scale * 100 << "%";
   }

   void loadPart()
   {
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         part.insert({i}, part_t::generateRandomRecord());
         printProgress("part", i, PART_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadSupplier()
   {
      for (Integer i = 1; i <= SUPPLIER_SCALE * FLAGS_tpch_scale_factor; i++) {
         supplier.insert({i}, supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadPartsupp()
   {
      for (Integer i = 1; i <= PARTSUPP_SCALE * FLAGS_tpch_scale_factor; i++) {
         partsupp.insert({i}, partsupp_t::generateRandomRecord());
         printProgress("partsupp", i, PARTSUPP_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadCustomer()
   {
      for (Integer i = 1; i <= CUSTOMER_SCALE * FLAGS_tpch_scale_factor; i++) {
         customer.insert({i}, customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, CUSTOMER_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadOrders()
   {
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         orders.insert({i}, orders_t::generateRandomRecord([this]() { return this->getCustomerID(); }));
         printProgress("orders", i, ORDERS_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadLineitem()
   {
      for (Integer i = 1; i <= LINEITEM_SCALE * FLAGS_tpch_scale_factor; i++) {
         lineitem.insert({i}, lineitem_t::generateRandomRecord([this]() { return this->getPartID(); }, [this]() { return this->getSupplierID(); }));
         printProgress("lineitem", i, LINEITEM_SCALE * FLAGS_tpch_scale_factor);
      }
   }

   void loadNation()
   {
      for (Integer i = 1; i <= NATION_COUNT; i++) {
         nation.insert({i}, nation_t::generateRandomRecord([this]() { return this->getRegionID(); }));
         printProgress("nation", i, NATION_COUNT);
      }
   }

   void loadRegion()
   {
      for (Integer i = 1; i <= REGION_COUNT; i++) {
         region.insert({i}, region_t::generateRandomRecord());
         printProgress("region", i, REGION_COUNT);
      }
   }

   // ------------------------------------LOAD VIEWS-------------------------------------------------

   void loadBasicJoin()
   {
      // first join
      this->part.resetIterator();
      this->partsupp.resetIterator();
      Join<joinedPPs_t::Key, joinedPPs_t, part_t::Key, part_t, partsupp_t::Key, partsupp_t> join1(
          [](part_t::Key& k, part_t&) { return PPsL_JK{k.p_partkey, 0}; },
          [](partsupp_t::Key& k, partsupp_t&) { return PPsL_JK{k.ps_partkey, k.ps_suppkey}; },
          [](u8* in, u16) {
             part_t::Key k;
             return part_t::unfoldKey(in, k);
          },
          [](u8* in, u16) {
             partsupp_t::Key k;
             return partsupp_t::unfoldKey(in, k);
          },
          [this]() { return this->part.next(); },
          [this]() { return this->partsupp.next(); }
      );
      while (true) {
         auto kv = join1.next();
         if (kv == std::nullopt) {
            break;
         }
         auto& [k, v] = *kv;
         joinedPPs.insert(k, v);
      }
      // sort lineitem
      this->lineitem.resetIterator();
      while (true) {
         auto kv = this->lineitem.next();
         if (kv == std::nullopt)
            break;
         auto& [k, v] = *kv;
         PPsL_JK jk{k.l_partkey, k.l_partsuppkey};
         merged_lineitem_t::Key k_new({jk, k});
         merged_lineitem_t v_new(v);
         this->sortedLineitem.insert(k_new, v_new);
      }
      // second join
      this->joinedPPs.resetIterator();
      this->sortedLineitem.resetIterator();
      Join<joinedPPsL_t::Key, joinedPPsL_t, joinedPPs_t::Key, joinedPPs_t, merged_lineitem_t::Key, merged_lineitem_t> join2(
          [](joinedPPs_t::Key& k, joinedPPs_t&) { return k.jk; },
          [](merged_lineitem_t::Key& k, merged_lineitem_t&) { return k.jk; },
          [](u8* in, u16) {
             joinedPPs_t::Key k;
             return joinedPPs_t::unfoldKey(in, k);
          },
          [](u8* in, u16) {
             merged_lineitem_t::Key k;
             return merged_lineitem_t::unfoldKey(in, k);
          },
          [this]() { return this->joinedPPs.next(); },
          [this]() { return this->sortedLineitem.next(); }
      );
      while (true) {
         auto kv = join2.next();
         if (kv == std::nullopt) {
            break;
         }
         auto& [k, v] = *kv;
         joinedPPsL.insert(k, v);
      }
   };

   void loadBasicGroup();

   void loadBasicJoinGroup();
};
