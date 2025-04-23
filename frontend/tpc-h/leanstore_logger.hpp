#pragma once
#include <string>
#include "logger.hpp"
#include "tpch_workload.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/ConfigsTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "tabulate/table.hpp"



class LeanStoreLogger : public Logger
{
   leanstore::LeanStore& db;

   leanstore::profiling::BMTable bm_table;
   leanstore::profiling::DTTable dt_table;
   leanstore::profiling::CPUTable cpu_table;
   leanstore::profiling::CRTable cr_table;
   leanstore::profiling::ConfigsTable configs_table;
   std::vector<leanstore::profiling::ProfilingTable*> tables;

  public:
   LeanStoreLogger(leanstore::LeanStore& db)
       : db(db), bm_table(*db.buffer_manager.get()), dt_table(*db.buffer_manager.get()), tables({&bm_table, &dt_table, &cpu_table, &cr_table})
   {
      static fLS::clstring tpch_scale_factor_str = std::to_string(FLAGS_tpch_scale_factor);
      leanstore::LeanStore::addStringFlag("TPCH_SCALE", &tpch_scale_factor_str);
      for (auto& t : tables) {
         t->open();
         t->next();
      }
   };

   ~LeanStoreLogger() { std::cout << "Logs written to " << FLAGS_csv_path << std::endl; }

   void writeOutAll();
   std::pair<std::vector<std::string>, std::vector<std::string>> summarizeStats(long elapsed_or_tput, ColumnName column_name);
   void reset();
   void log(long elapsed_or_tput, ColumnName column_name, std::string csv_dir);

   void log_sizes(std::map<std::string, double> sizes);

   void prepare();
   void logLoading();

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

   static void printTable(const std::vector<std::vector<std::string>>& rows)
   {
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
      printTable(table);
   }

   static inline std::string to_fixed(double value)
   {
      std::ostringstream oss;
      if (value >= 0 && value <=1) {
         oss << std::defaultfloat << std::setprecision(4) << value;
      } else {
         oss << std::fixed << std::setprecision(2) << value;
      }
      return oss.str();
   }
};