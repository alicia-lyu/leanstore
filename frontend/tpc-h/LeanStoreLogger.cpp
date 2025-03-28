#include "LeanStoreLogger.hpp"
#include <filesystem>
#include "TPCHWorkload.hpp"
#include <gflags/gflags.h>

void LeanStoreLogger::reset()
{
   for (auto& t : tables) {
      t->next();
   }
}

void LeanStoreLogger::writeOutAll()
{
   std::cout << "Writing out all buffer frames... This may take a while." << std::endl;
   db.buffer_manager->writeAllBufferFrames();
}

std::pair<std::vector<variant<std::string, const char*, tabulate::Table>>, std::vector<variant<std::string, const char*, tabulate::Table>>>
LeanStoreLogger::summarizeStats(long elapsed = 0)
{
   std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_header;
   std::vector<variant<std::string, const char*, tabulate::Table>> tx_console_data;
   tx_console_header.reserve(20);
   tx_console_data.reserve(20);

   tx_console_header.push_back("Elapsed (ms)");
   tx_console_data.push_back(std::to_string((double) elapsed / 1000));

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

void LeanStoreLogger::log(long elapsed, std::string csv_dir)
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
   csv_sum.open(csv_dir_abs + ".csv", std::ios::app);
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

void LeanStoreLogger::logLoading()
{
   log(0, "load");
   auto w_mib = std::stod(bm_table.get("0", "w_mib"));
   if (w_mib != 0) {
      std::cout << "Out of memory workload, loading tables caused " << w_mib << " MiB write." << std::endl;
      writeOutAll();
   } else {
      std::cout << "In memory workload, loading tables caused no write." << std::endl;
   }
}

void LeanStoreLogger::prepare()
{
   std::cout << "Preparing TPC-H" << std::endl;
   [[maybe_unused]] Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
   Integer h_id = 0;
   leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
}