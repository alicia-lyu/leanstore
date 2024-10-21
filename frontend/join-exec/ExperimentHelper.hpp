#pragma once

#include <filesystem>
#include <fstream>
#include <sstream>
#include "../join-workload/JoinedSchema.hpp"
#include "../join-workload/TPCCBaseWorkload.hpp"
#include "leanstore/Config.hpp"

class ExperimentHelper
{
  public:
   
   static std::string getConfigString()
   {
      std::stringstream config;
      config << FLAGS_dram_gib << "|" << FLAGS_target_gib << "|" << FLAGS_semijoin_selectivity << "|" << INCLUDE_COLUMNS;
      return config.str();
   }
};