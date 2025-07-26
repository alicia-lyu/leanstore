#pragma once
#include <filesystem>
#include <map>
#include <string>
#include <gflags/gflags.h>
#include "Units.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "tabulate/table.hpp"

enum class ColumnName { ELAPSED, TPUT };

inline std::string to_string(ColumnName column)
{
   switch (column) {
      case ColumnName::ELAPSED:
         return "Elapsed (ms)";
      case ColumnName::TPUT:
         return "TPut (TX/s)";
      default:
         return "Unknown";
   }
}

inline std::string to_short_string(ColumnName column)
{
   switch (column) {
      case ColumnName::ELAPSED:
         return "Elapsed";
      case ColumnName::TPUT:
         return "TPut";
      default:
         return "Unknown";
   }
}

struct SumStats {
   double tput;
   long elapsed;
   int tx_count;
   std::string tx;
   std::string method;
   double size;
   ColumnName column_name;
   std::vector<std::string> header;
   std::vector<std::string> data;

   SumStats() = default;

   SumStats(std::string tx, std::string method, double size, ColumnName column_name)
       : tx(std::move(tx)), method(std::move(method)), size(size), column_name(column_name)
   {
      header.reserve(20);
      data.reserve(20);
   }

   void reset()
   {
      tx.clear();
      method.clear();
      size = 0;
      header.clear();
      data.clear();
   }

   void log(std::ostream& csv_sum, bool csv_sum_exists);

   static void print(tabulate::Table& table)
   {
      std::stringstream ss;
      table.print(ss);
      std::string str = ss.str();
      for (u64 i = 0; i < str.size(); i++) {
         std::cout << str[i];
      }
      std::cout << std::endl;
   }

   void print();

   void init(double tput, int tx_count, std::string tx, std::string method, double size)
   {
      elapsed = 0;
      this->tput = tput;
      this->tx_count = tx_count;
      this->tx = std::move(tx);
      this->method = std::move(method);
      this->size = size;
      column_name = ColumnName::TPUT;
   }

   void init(long elapsed, std::string tx, std::string method, double size)
   {
      this->elapsed = elapsed;
      this->tput = 0;
      this->tx_count = 1;
      this->tx = std::move(tx);
      this->method = std::move(method);
      this->size = size;
      column_name = ColumnName::ELAPSED;
   }

   std::string elapsed_or_tput() const;
};

// virtual class for logging
class Logger
{
  protected:
   leanstore::profiling::ConfigsTable configs_table;
   leanstore::profiling::CPUTable cpu_table;
   std::filesystem::path csv_runtime;
   std::filesystem::path csv_db;

   SumStats stats;

   virtual void summarize_other_stats() = 0;
   void summarize_shared_stats();

   virtual void log_details() = 0;

   void log_summary();

   void log_detail_table(leanstore::profiling::ProfilingTable& t);

  public:
   Logger() : csv_runtime(FLAGS_csv_path), csv_db(csv_runtime.parent_path()), stats()
   {
      std::filesystem::create_directories(csv_runtime);
      std::filesystem::create_directories(csv_db);
      cpu_table.open();
      configs_table.open();
      cpu_table.next();
      configs_table.next();
   }
   virtual void reset()
   {
      stats.reset();
      cpu_table.next();
      configs_table.next();
   }

   void log(double tput, int tx_count, std::string tx, std::string method, double size)
   {
      cpu_table.next();
      configs_table.next();
      stats.init(tput, tx_count, tx, method, size);
      log_summary();
      log_details();
      log_size();
   }
   void log(long elapsed, std::string tx, std::string method, double size)
   {
      cpu_table.next();
      configs_table.next();
      stats.init(elapsed, tx, method, size);
      log_summary();
      log_details();
      log_size();
   }
   void log_size();
   void log_sizes(std::map<std::string, double> sizes);

   virtual void prepare() = 0;

   void log_loading() { log(0, "load", "", 0); }

   static inline std::string to_fixed(double value)
   {
      std::ostringstream oss;
      if (value >= 0 && value <= 1) {
         oss << std::defaultfloat << std::setprecision(4) << value;
      } else {
         oss << std::fixed << std::setprecision(2) << value;
      }
      return oss.str();
   }
};