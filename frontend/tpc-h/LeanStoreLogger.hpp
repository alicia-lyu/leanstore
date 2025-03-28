#pragma once
#include "Logger.hpp"
#include "TPCHWorkload.hpp"
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
   void writeOutAll();
   std::pair<std::vector<variant<std::string, const char*, tabulate::Table>>, std::vector<variant<std::string, const char*, tabulate::Table>>>
   summarizeStats(long elapsed);
   void reset();
   void log(long elapsed, std::string csv_dir);
   void prepare();
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
};