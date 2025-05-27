#include "leanstore_logger.hpp"
#include <gflags/gflags.h>
#include <filesystem>
#include <string>
#include <tabulate/table.hpp>
#include "logger.hpp"

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

std::pair<std::vector<std::string>, std::vector<std::string>> LeanStoreLogger::summarizeStats(long elapsed_or_tput,
                                                                                              ColumnName column_name,
                                                                                              int tx_count)
{
   std::vector<std::string> tx_console_header;
   std::vector<std::string> tx_console_data;
   tx_console_header.reserve(20);
   tx_console_data.reserve(20);

   tx_console_header.push_back(to_string(column_name));
   tx_console_data.push_back(std::to_string(elapsed_or_tput));

   switch (column_name) {
      case ColumnName::ELAPSED:
         tx_console_header.push_back("W MiB");
         tx_console_data.push_back(bm_table.get("0", "w_mib"));

         tx_console_header.push_back("R MiB");
         tx_console_data.push_back(bm_table.get("0", "r_mib"));

         for (auto& [t_name, worker_e] : cpu_table.workers_events) {
            long cycles = static_cast<long>(worker_e.at("cycle"));
            if (cycles == 0)
               continue;
            tx_console_header.push_back(t_name + " Cycles");
            tx_console_data.push_back(std::to_string(cycles));
         }
         break;
      case ColumnName::TPUT:
         tx_console_header.push_back("W MiB / TX");
         tx_console_data.push_back(tx_count > 0 ? to_fixed(stod(bm_table.get("0", "w_mib")) / tx_count) : "NaN");

         tx_console_header.push_back("R MiB / TX");
         tx_console_data.push_back(tx_count > 0 ? to_fixed(stod(bm_table.get("0", "r_mib")) / tx_count) : "NaN");

         for (auto& [t_name, worker_e] : cpu_table.workers_events) {
            long cycles = static_cast<long>(worker_e.at("cycle"));
            if (cycles == 0)
               continue;
            tx_console_header.push_back(t_name + " Cycles / TX");
            tx_console_data.push_back(tx_count > 0 ? std::to_string(cycles / tx_count) : "NaN");
         }
         break;
      default:
         break;
   }

   for (auto& [t_name, worker_e] : cpu_table.workers_events) {
      long cycles = static_cast<long>(worker_e.at("cycle"));
      if (cycles == 0)
         continue;
      double cpu_utilization = std::min(worker_e.at("CPU"), 1.0);
      tx_console_header.push_back(t_name + " CPU Util (%)");
      // 2 decimal places
      tx_console_data.push_back(to_fixed(cpu_utilization * 100));
   }

   return {tx_console_header, tx_console_data};
}

void LeanStoreLogger::log_template(std::string tx,
                                   std::string method,
                                   double size,
                                   ColumnName column_name,
                                   std::function<std::pair<std::vector<std::string>, std::vector<std::string>>()> main_stats_cb)
{
   u64 config_hash = configs_table.hash();
   std::vector<std::ofstream> csvs;
   std::filesystem::path csv_runtime(FLAGS_csv_path);
   std::filesystem::path csv_tx = csv_runtime / tx / method;
   std::filesystem::create_directories(csv_tx);
   for (u64 t_i = 0; t_i < tables.size(); t_i++) {
      csvs.emplace_back();
      auto& csv = csvs.back();
      csv.open(csv_tx / (tables[t_i]->getName() + ".csv"), std::ios::app);
      csv << std::setprecision(2) << std::fixed;  // TODO change precision by data type

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

   auto [tx_console_header, tx_console_data] = main_stats_cb();
   
   std::filesystem::path csv_db = csv_runtime.parent_path();
   std::filesystem::path csv_sum_path = csv_db / (tx + ".csv");
   bool csv_sum_exists = std::filesystem::exists(csv_sum_path);
   std::ofstream csv_sum;
   csv_sum.open(csv_sum_path, std::ios::app);
   if (!csv_sum_exists) {  // no header
      csv_sum << "method,tx,DRAM (GiB),scale,";
      for (auto& h : tx_console_header) {
         csv_sum << h << ",";
      }
      csv_sum << "size (MiB)" << endl;
   }
   csv_sum << method << "," << tx << "," << FLAGS_dram_gib << "," << FLAGS_tpch_scale_factor << ",";
   for (auto& d : tx_console_data) {
      csv_sum << d << ",";
   }
   csv_sum << size << endl;

   std::vector<std::vector<std::string>> rows = {tx_console_header, tx_console_data};
   printTable(rows);
}

void LeanStoreLogger::log(long tput, std::string tx, std::string method, double size, int tx_count)
{
   log_template(tx, method, size, ColumnName::TPUT, [&]() { return summarizeStats(tput, ColumnName::TPUT, tx_count); });
}

void LeanStoreLogger::log(long elapsed, std::string tx, std::string method, double size)
{
   log_template(tx, method, size, ColumnName::ELAPSED, [&]() { return summarizeStats(elapsed, ColumnName::ELAPSED, 1); });
}

void LeanStoreLogger::log_sizes(std::map<std::string, double> sizes)
{
   std::ofstream size_csv;
   std::filesystem::create_directories(FLAGS_csv_path);
   size_csv.open(FLAGS_csv_path + "/size.csv", std::ios::app);
   if (size_csv.tellp() == 0) {
      size_csv << "table,size (MiB)" << std::endl;
   }
   std::cout << "table,size (MiB)" << std::endl;
   std::vector<std::ostream*> out = {&std::cout, &size_csv};
   for (std::ostream* o : out) {
      for (auto& [table_name, size] : sizes) {
         *o << table_name << "," << size << std::endl;
      }
   }
   size_csv.close();
}

void LeanStoreLogger::logLoading()
{
   std::cout << "Loading" << std::endl;
   log(0, "load", "", 0);
}

void LeanStoreLogger::prepare()
{
   std::cout << "Preparing TPC-H" << std::endl;
   [[maybe_unused]] Integer t_id = Integer(leanstore::WorkerCounters::myCounters().t_id.load());
   Integer h_id = 0;
   leanstore::WorkerCounters::myCounters().variable_for_workload = h_id;
}