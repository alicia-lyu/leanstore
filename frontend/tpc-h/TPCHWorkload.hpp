#pragma once
#include <gflags/gflags.h>
#include <chrono>
#include <fstream>
#include <functional>
#include <optional>
#include <vector>
#include "Tables.hpp"

#include "Views.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
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
                AdapterType<region_t>& r,
                MergedAdapterType& mbj,
                AdapterType<joinedPPsL_t>& jppsl,
                AdapterType<joinedPPs_t>& jpps,
                AdapterType<merged_lineitem_t>& sl)
       : part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         mergedPPsL(mbj),
         joinedPPsL(jppsl),
         joinedPPs(jpps),
         sortedLineitem(sl),
         bm_table(*buffer_manager.get()),
         dt_table(*buffer_manager.get()),
         tables({&bm_table, &dt_table, &cpu_table, &cr_table})
   {
      static fLS::clstring tpch_scale_factor_str = std::to_string(FLAGS_tpch_scale_factor);
      leanstore::LeanStore::addStringFlag("TPCH_SCALE", &tpch_scale_factor_str);
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

      tx_console_header.push_back("Elapsed");
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
         csv << config_hash;
         assert(tables[t_i]->size() == 1);
         for (auto& c : tables[t_i]->getColumns()) {
            csv << "," << c.second.values[0];
         }
         csv << endl;
         csv.close();

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
      }
   }

  public:
   // TXs: Measure end-to-end time
   void basicJoin()
   {
      // Enumrate materialized view
      std::cout << "Enumerating materialized view" << std::endl;
      resetTables();
      [[maybe_unused]] int produced = 0;
      auto inspect_produced = [&](const std::string& msg) {
         produced++;
         if (produced % 100000 == 0) {
            std::cout << "\r" << msg << produced / 1000 << "k";
         }
      };
      auto mtdv_start = std::chrono::high_resolution_clock::now();
      joinedPPsL.scan(
          {},
          [&](const auto&, const auto&) {
             inspect_produced("Enumerating materialized view: ");
             return true;
          },
          [&]() {});
      auto mtdv_end = std::chrono::high_resolution_clock::now();
      auto mtdv_t = std::chrono::duration_cast<std::chrono::microseconds>(mtdv_end - mtdv_start).count();
      logTables(mtdv_t, "mtdv");

      // Scan merged index + join on the fly
      std::cout << "Scan merged index + join on the fly" << std::endl;
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
      produced = 0;

      auto part_descriptor =
          MergedAdapterType::ScanCallbackDescriptor::template create<merged_part_t>([&](const merged_part_t::Key& k, const merged_part_t& v) {
             inspect_produced("Scanning merged index (at a part_t record): ");
             comp_clear(k.jk);
             cached_part.push_back({k, v});
             return false;
          });

      auto partsupp_descriptor = MergedAdapterType::ScanCallbackDescriptor::template create<merged_partsupp_t>(
          [&](const merged_partsupp_t::Key& k, const merged_partsupp_t& v) {
             inspect_produced("Scanning merged index (at a partsupp_t record): ");
             comp_clear(k.jk);
             cached_partsupp.push_back({k, v});
             return false;
          });

      auto lineitem_descriptor = MergedAdapterType::ScanCallbackDescriptor::template create<merged_lineitem_t>([&](const merged_lineitem_t::Key& k,
                                                                                                                   const merged_lineitem_t& v) {
         inspect_produced("Scanning merged index (at a lineitem_t record): ");
         comp_clear(k.jk);
         for (auto& [pk, pv] : cached_part) {
            for (auto& [psk, psv] : cached_partsupp) {
               [[maybe_unused]]
               JoinedKey joined_key = Joined<11, PPsL_JK, part_t, partsupp_t, lineitem_t>::Key{{current_jk, std::make_tuple(pk.pk, psk.pk, k.pk)}};
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
      {
         double progress = (double) i / scale * 100;
         std::cout << "\rLoading " << msg << ": " << progress << "%";
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

   void loadPartsupp()
   {
      for (Integer i = 1; i <= PART_SCALE * FLAGS_tpch_scale_factor; i++) {
         int supplier_cnt = urand(1, PARTSUPP_SCALE / PART_SCALE * 2);
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
            partsupp.insert(partsupp_t::Key({i, s}), partsupp_t::generateRandomRecord());
            printProgress("partsupp", i * PARTSUPP_SCALE / PART_SCALE, PARTSUPP_SCALE * FLAGS_tpch_scale_factor);
         }
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

   void loadLineitem()
   {
      for (Integer i = 1; i <= ORDERS_SCALE * FLAGS_tpch_scale_factor; i++) {
         Integer lineitem_cnt = urand(1, LINEITEM_SCALE / ORDERS_SCALE * 2);
         std::vector<Integer> lineitems = {};
         while (true) {
            Integer lineitem_id = urand(1, LINEITEM_SCALE * FLAGS_tpch_scale_factor);
            if (std::find(lineitems.begin(), lineitems.end(), lineitem_id) == lineitems.end()) {
               lineitems.push_back(lineitem_id);
            }
            if (lineitems.size() == lineitem_cnt) {
               break;
            }
         }
         for (auto& l : lineitems) {
            lineitem.insert(lineitem_t::Key({i, l}), lineitem_t::generateRandomRecord([this]() { return this->getPartID(); }, [this]() { return this->getSupplierID(); }));
            printProgress("lineitem", i * LINEITEM_SCALE / ORDERS_SCALE, LINEITEM_SCALE * FLAGS_tpch_scale_factor);
         }
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
      // first join
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
      // second join
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
   };

   void loadMergedBasicJoin()
   {
      part.resetIterator();
      part_t::Key pk;
      part_t pv;
      PPsL_JK pjk;
      bool pkv = true;  // current pjk is valid
      partsupp.resetIterator();
      partsupp_t::Key psk;
      partsupp_t psv;
      PPsL_JK psjk;
      bool pskv = true;
      sortedLineitem.resetIterator();
      merged_lineitem_t::Key slk;
      merged_lineitem_t slv;
      PPsL_JK sljk;
      bool slkv = true;
      while (pkv || pskv || slkv) {
         if (pkv == true && pjk <= psjk && pjk <= sljk) {
            if (pjk != PPsL_JK{}) {
               merged_part_t::Key k({pjk, pk});
               merged_part_t v(pv);
               mergedPPsL.insert(k, v);
            }
            auto npkv = part.next();
            if (npkv != std::nullopt) {
               auto& [pk, pv] = *npkv;
               pjk = PPsL_JK{pk.p_partkey, 0};
            } else {
               pkv = false;
            }
         } else if (pskv == true && psjk <= pjk && psjk <= sljk) {
            if (psjk != PPsL_JK{}) {
               merged_partsupp_t::Key k({psjk, psk});
               merged_partsupp_t v(psv);
               mergedPPsL.insert(k, v);
            }
            auto npskv = partsupp.next();
            if (npskv != std::nullopt) {
               auto& [psk, psv] = *npskv;
               psjk = PPsL_JK{psk.ps_partkey, psk.ps_suppkey};
            } else {
               pskv = false;
            }
         } else if (slkv == true && sljk <= pjk && sljk <= psjk) {
            if (sljk != PPsL_JK{}) {
               mergedPPsL.insert(slk, slv);
            }
            auto nslkv = sortedLineitem.next();
            if (nslkv != std::nullopt) {
               auto& [slk, slv] = *nslkv;
               sljk = slk.jk;
            } else {
               slkv = false;
            }
         } else {
            break;
         }
      }
   }

   void loadBasicGroup();

   void loadBasicJoinGroup();

   // Log size
};
