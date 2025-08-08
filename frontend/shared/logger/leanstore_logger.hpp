#pragma once
#include <filesystem>
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "logger.hpp"
#include "../../geo/tpch_workload.hpp"

struct LeanStoreLogger : public Logger {
   leanstore::profiling::BMTable bm_table;
   leanstore::profiling::DTTable dt_table;
   leanstore::profiling::CRTable cr_table;

   std::vector<leanstore::profiling::ProfilingTable*> tables;

   LeanStoreLogger(leanstore::LeanStore& db)
       : bm_table(*db.buffer_manager.get()), dt_table(*db.buffer_manager.get()), tables({&bm_table, &dt_table, &cpu_table, &cr_table})
   {
      std::filesystem::create_directories(csv_runtime);
      static fLS::clstring tpch_scale_factor_str = std::to_string(FLAGS_tpch_scale_factor);
      leanstore::LeanStore::addStringFlag("TPCH_SCALE", &tpch_scale_factor_str);

      for (auto& t : tables) {
         t->open();
         t->next();
      }
   };

   ~LeanStoreLogger() { std::cout << "Logs written to " << csv_runtime << std::endl; }

   void summarize_other_stats() override;
   
   void reset() override
   {
      Logger::reset();
      for (auto& t : tables) {
         t->next();
      }
   }

   void log_details() override
   {
      for (auto& t : tables) {
         log_detail_table(*t);
      }
   }

   void prepare() override;
};