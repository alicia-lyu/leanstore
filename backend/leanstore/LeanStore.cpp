#include "LeanStore.hpp"
#include "leanstore/utils/FVector.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DEFINE_bool(log_stdout, false, "");
// -------------------------------------------------------------------------------------
namespace leanstore {
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   // Set the default logger to file logger
   if ( !FLAGS_log_stdout ) {
      auto file_logger = spdlog::rotating_logger_mt("main_logger", "log.txt",
                                                    1024 * 1024 * 10, 3);
      spdlog::set_default_logger(file_logger);
   }
   BMC::global_bf = &buffer_manager;
}
// -------------------------------------------------------------------------------------
void LeanStore::persist()
{
   buffer_manager.persist();
   std::vector<string> btree_names(btrees.size());
   std::vector<u8> btree_objects(btree_size * btrees.size());
   u64 b_i = 0;
   for ( const auto &btree: btrees ) {
      btree_names.push_back(btree.first);
      std::memcpy(btree_objects.data() + (btree_size * b_i), btree.second.get(), btree_size);
      b_i++;
   }
   utils::writeBinary("leanstore_btree_names", btree_names);
   utils::writeBinary("leanstore_btree_objects", btree_objects);
}
// -------------------------------------------------------------------------------------
void LeanStore::restore()
{
   buffer_manager.restore();
   utils::FVector<std::string_view> btree_names("leanstore_btree_names");
   utils::FVector<u8> btree_objects("leanstore_btree_objects");
   for ( u64 b_i = 0; b_i < btree_names.size(); b_i++ ) {
      auto iter = btrees.emplace(btree_names[b_i], std::make_unique<u8[]>(btree_size));
      std::memcpy(iter.first->second.get(), btree_objects.data + (btree_size * b_i), btree_size);
   }
}
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
}
// -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------