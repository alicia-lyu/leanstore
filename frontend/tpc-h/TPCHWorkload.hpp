#pragma once
#include "Tables.hpp"
#include <gflags/gflags.h>
#include <chrono>
#include <functional>
#include <vector>
#include "Views.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/concurrency-recovery/HistoryTree.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "../join-workload/Join.hpp"
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
        : part(p), supplier(s), partsupp(ps), customer(c), orders(o), lineitem(l), nation(n), region(r), bm_table(*buffer_manager.get()), dt_table(*buffer_manager.get()), tables({&bm_table, &dt_table, &cpu_table, &cr_table})
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
        
        // [0, n)
        Integer rnd(Integer n) { return leanstore::utils::RandomGenerator::getRand(0, n); }
        // [low, high]
        Integer urand(Integer low, Integer high) { return rnd(high - low + 1) + low; }

        inline Integer getPartID()
        {
            return urand(1, PART_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getSupplierID()
        {
            return urand(1, SUPPLIER_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getCustomerID()
        {
            return urand(1, CUSTOMER_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getOrderID()
        {
            return urand(1, ORDERS_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getLineItemID()
        {
            return urand(1, LINEITEM_SCALE * FLAGS_tpch_scale_factor);
        }

        inline Integer getPartsuppID()
        {
            return urand(1, PARTSUPP_SCALE * FLAGS_tpch_scale_factor);
        }

        // TODO: Move profiling tables here
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
                open_flags = ios::trunc;
            } else {
                open_flags = ios::app;
            }
            std::string csv_dir_abs = FLAGS_csv_path + "/" + csv_dir;
            for (u64 t_i = 0; t_i < tables.size() + 1; t_i++) {
                csvs.emplace_back();
                auto& csv = csvs.back();
                if (t_i < tables.size())
                    csv.open(csv_dir_abs + "/" + tables[t_i]->getName() + ".csv", open_flags);
                else {
                    csv.open(csv_dir_abs + "sum.csv", open_flags); // summary
                    continue;
                }
                csv.seekp(0, ios::end);
                csv << std::setprecision(2) << std::fixed;
                if (csv.tellp() == 0 && t_i < tables.size()) { // summary is output below
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

            std::vector<variant<std::string, const char *, tabulate::Table>> tx_console_header;
            std::vector<variant<std::string, const char *, tabulate::Table>> tx_console_data;
            tx_console_header.reserve(20);
            tx_console_data.reserve(20);

            tx_console_header.push_back("Elapsed");
            tx_console_data.push_back(std::to_string(elapsed.count()));

            tx_console_header.push_back("W MiB");
            tx_console_data.push_back(bm_table.get("0", "w_mib"));

            tx_console_header.push_back("R MiB");
            tx_console_data.push_back(bm_table.get("0", "r_mib"));
            if (cpu_table.workers_agg_events.contains("instr"))
            {
                const double instr_cnt = cpu_table.workers_agg_events["instr"];
                tx_console_header.push_back("Instrs");
                tx_console_data.push_back(std::to_string(instr_cnt));
            }
            
            if (cpu_table.workers_agg_events.contains("cycle"))
            {
                const double cycles_cnt = cpu_table.workers_agg_events["cycle"];
                tx_console_header.push_back("Cycles");
                tx_console_data.push_back(std::to_string(cycles_cnt));
            }

            if (cpu_table.workers_agg_events.contains("CPU"))
            {
                tx_console_header.push_back("Utilized CPUs");
                tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["CPU"]));
            }

            if (cpu_table.workers_agg_events.contains("task"))
            {
                tx_console_header.push_back("CPUTime(ms)");
                tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["task"]));
            }

            if (cpu_table.workers_agg_events.contains("L1-miss"))
            {
                tx_console_header.push_back("L1-miss");
                tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["L1-miss"]));
            }

            if (cpu_table.workers_agg_events.contains("LLC-miss"))
            {
                tx_console_header.push_back("LLC-miss");
                tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["LLC-miss"]));
            }

            if (cpu_table.workers_agg_events.contains("GHz"))
            {
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
        void basicJoin() {
            // Enumrate materialized view
            auto mtdv_start = std::chrono::high_resolution_clock::now();
            joinedPPsL.scan(
                {},
                [&](const auto&, const auto&) {}
            );
            auto mtdv_end = std::chrono::high_resolution_clock::now();
            auto mtdv_t = std::chrono::duration_cast<std::chrono::microseconds>(mtdv_end - mtdv_start).count();

            // Scan merged index + join on the fly
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
            mergedPPsL.scan(
                {},
                [&](const merged_part_t::Key& k, const merged_part_t& v) {
                    comp_clear(k.jk);
                    cached_part.push_back({k, v});
                },
                [&](const merged_partsupp_t::Key& k, const merged_partsupp_t& v) {
                    comp_clear(k.jk);
                    cached_partsupp.push_back({k, v});
                },
                [&](const merged_lineitem_t::Key& k, const merged_lineitem_t& v) {
                    comp_clear(k.jk);
                    for (auto& [pk, pv] : cached_part) {
                        for (auto& [psk, psv] : cached_partsupp) {
                            [[maybe_unused]]
                            JoinedKey joined_key = Joined<11, PPsL_JK, part_t, partsupp_t, lineitem_t>::Key{current_jk, std::make_tuple(pk.pk, psk.pk, k.pk)};
                            [[maybe_unused]]
                            JoinedRec joined_rec = JoinedRec{std::make_tuple(pv.payload, psv.payload, v.payload)};
                            // Do something with the result
                        }
                    }
                },
                [&]() { } // undo
            );
            auto merged_end = std::chrono::high_resolution_clock::now();
            auto merged_t = std::chrono::duration_cast<std::chrono::microseconds>(merged_end - merged_start).count();
        }

        void basicGroup();

        void basicJoinGroup();

        // ------------------------------------LOAD-------------------------------------------------

        void prepare();

        void loadPart();

        void loadSupplier();

        void loadPartsupp();

        void loadCustomer();

        void loadOrders();

        void loadLineitem();

        void loadNation();

        void loadRegion();

        // ------------------------------------LOAD VIEWS-------------------------------------------------

        void loadBasicJoin();

        void loadBasicGroup();

        void loadBasicJoinGroup();


};







     

