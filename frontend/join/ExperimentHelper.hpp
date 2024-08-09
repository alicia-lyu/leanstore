#pragma once

#include <filesystem>
#include <fstream>
#include <sstream>
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"

class ExperimentHelper
{
  protected:
   static std::string getConfigString()
   {
      std::stringstream config;
      config << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS;
      return config.str();
   }

  public:
   using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec_key_only_t, ol_join_sec_t>::type;
   using joined_t = typename std::conditional<INCLUDE_COLUMNS == 0, joined_ols_key_only_t, joined_ols_t>::type;
   void logSize(std::string table, std::chrono::steady_clock::time_point prev)
   {
      std::filesystem::path csv_path = std::filesystem::path(FLAGS_csv_path).parent_path().parent_path() / "join_size.csv";
      std::ofstream csv_file(csv_path, std::ios::app);
      if (std::filesystem::file_size(csv_path) == 0)
         csv_file << "table(s),config,size,time" << std::endl;
      std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
      auto config = getConfigString();
      auto size = 0;  // TODO
      auto time = std::chrono::duration_cast<std::chrono::milliseconds>(now - prev).count();
      csv_file << table << "," << config << "," << size << "," << time << std::endl;
   };
};