#include "logger.hpp"
#include <fstream>
#include <iostream>
#include <vector>
#include "workload.hpp"
#include "../shared/merge-join/premerged_join.hpp"

DEFINE_bool(log_progress, true, "Log progress of the workload execution");

void SumStats::log(std::ostream& csv_sum, bool csv_sum_exists)
{
   if (!csv_sum_exists) {  // no header
      csv_sum << "method,tx,DRAM (GiB),scale,tentative_skip_bytes,";
      for (auto& h : header) {
         csv_sum << h << ",";
      }
      csv_sum << "size (MiB)" << endl;
   }
   csv_sum << method << "," << tx << "," << FLAGS_dram_gib << "," << FLAGS_tpch_scale_factor << "," << FLAGS_tentative_skip_bytes << ",";
   for (auto& d : data) {
      csv_sum << d << ",";
   }
   csv_sum << size << endl;
   print();
}

void SumStats::print()
{
   std::vector<std::vector<std::string>> rows = {header, data};
   tabulate::Table table;
   for (auto& row : rows) {
      std::vector<variant<std::string, const char*, tabulate::Table>> cells;
      for (auto& cell : row) {
         cells.push_back(cell);
      }
      table.add_row(cells);
   }
   table.format().width(10);
   for (size_t c_id = 0; c_id < rows.at(0).size(); c_id++) {
      auto header = rows.at(0).at(c_id);
      table.column(c_id).format().width(header.length() + 2);
   }
   print(table);
}

std::string SumStats::elapsed_or_tput() const
{
   {
      if (column_name == ColumnName::ELAPSED) {
         return std::to_string(elapsed);
      } else {
         // 2 decimal places
         return Logger::to_fixed(tput);
      }
   }
}

void Logger::log_size()
{
   std::map<std::string, double> sizes = {{stats.method, stats.size}};
   log_sizes(sizes);
}

void Logger::log_sizes(std::map<std::string, double> sizes)
{
   std::ofstream size_csv;
   size_csv.open(csv_db / "size.csv", std::ios::app);
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

void Logger::summarize_shared_stats()
{
   auto& header = stats.header;
   auto& data = stats.data;
   header.push_back(to_string(stats.column_name));
   data.push_back(stats.elapsed_or_tput());

   switch (stats.column_name) {
      case ColumnName::ELAPSED:
         for (auto& [t_name, worker_e] : cpu_table.workers_events) {
            long cycles = static_cast<long>(worker_e.at("cycle"));
            if (cycles == 0)
               continue;
            header.push_back(t_name + " Cycles");
            data.push_back(std::to_string(cycles));
         }
         break;
      case ColumnName::TPUT:
         for (auto& [t_name, worker_e] : cpu_table.workers_events) {
            long cycles = static_cast<long>(worker_e.at("cycle"));
            if (cycles == 0)
               continue;
            header.push_back(t_name + " Cycles / TX");
            data.push_back(stats.tx_count > 0 ? std::to_string(cycles / stats.tx_count) : "NaN");
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
      header.push_back(t_name + " CPU Util (%)");
      // 2 decimal places
      data.push_back(to_fixed(cpu_utilization * 100));
   }
}

void Logger::log_summary()
{
   summarize_shared_stats();
   summarize_other_stats();
   std::filesystem::path csv_sum_path;
   csv_sum_path = csv_db / (to_short_string(stats.column_name) + ".csv");

   bool csv_sum_exists = std::filesystem::exists(csv_sum_path);
   std::ofstream csv_sum;
   csv_sum.open(csv_sum_path, std::ios::app);

   stats.log(csv_sum, csv_sum_exists);
}

void Logger::log_detail_table(leanstore::profiling::ProfilingTable& t)
{
   u64 config_hash = configs_table.hash();
   std::vector<std::ofstream> csvs;
   std::filesystem::path csv_tx = csv_runtime / stats.tx / stats.method;
   std::filesystem::create_directories(csv_tx);
   csvs.emplace_back();
   auto& csv = csvs.back();
   csv.open(csv_tx / (t.getName() + ".csv"), std::ios::app);
   csv << std::setprecision(2) << std::fixed;  // TODO change precision by stats.data type

   if (csv.tellp() == 0) {  // no header
      csv << "c_hash";
      for (auto& c : t.getColumns()) {
         csv << "," << c.first;
      }
      csv << endl;
   }
   // assert(t.size() == 1);
   for (u64 r_i = 0; r_i < t.size(); r_i++) {
      csv << config_hash;
      for (auto& c : t.getColumns()) {
         csv << "," << c.second.values[r_i];
      }
      csv << endl;
   }
   csv.close();
};