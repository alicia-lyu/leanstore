#include "leanstore_logger.hpp"
#include <gflags/gflags.h>
#include <filesystem>
#include <tabulate/table.hpp>

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

std::pair<std::vector<std::string>, std::vector<std::string>> LeanStoreLogger::summarizeStats(long elapsed = 0)
{
   std::vector<std::string> tx_console_header;
   std::vector<std::string> tx_console_data;
   tx_console_header.reserve(20);
   tx_console_data.reserve(20);

   tx_console_header.push_back("Elapsed (ms)");
   tx_console_data.push_back(to_fixed((double)elapsed / 1000));

   tx_console_header.push_back("W MiB");
   tx_console_data.push_back(bm_table.get("0", "w_mib"));

   tx_console_header.push_back("R MiB");
   tx_console_data.push_back(bm_table.get("0", "r_mib"));

   int worker_id = 0;
   for (auto& t : cpu_table.workers_events) {
      long long cycles = static_cast<long long>(t.at("cycle"));
      if (cycles == 0)
         continue;
      tx_console_header.push_back("Worker " + std::to_string(worker_id) + " Cycles");
      tx_console_data.push_back(std::to_string(cycles));
      double cpu_utilization = t.at("CPU");
      tx_console_header.push_back("Worker " + std::to_string(worker_id) + " CPU Utilization (%)");
      // 2 decimal places
      tx_console_data.push_back(to_fixed(cpu_utilization * 100));
      worker_id++;
   }

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
         csv_sum << h << ",";
      }
      csv_sum << endl;
   }
   for (auto& d : tx_console_data) {
      csv_sum << d << ",";
   }
   csv_sum << endl;

   std::vector<std::vector<std::string>> rows = {tx_console_header, tx_console_data};
   printTable(rows);
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