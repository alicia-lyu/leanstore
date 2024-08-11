#pragma once

#include <filesystem>
#include <fstream>
#include <sstream>
#include "JoinedSchema.hpp"
#include "TPCCBaseWorkload.hpp"
#include "leanstore/Config.hpp"

class ExperimentHelper
{
  public:
   using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec_key_only_t, ol_join_sec_t>::type;
   using joined_t = typename std::conditional<INCLUDE_COLUMNS == 0, joined_ols_key_only_t, joined_ols_t>::type;
   static std::string getConfigString()
   {
      std::stringstream config;
      config << FLAGS_dram_gib << "|" << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS;
      return config.str();
   }
};