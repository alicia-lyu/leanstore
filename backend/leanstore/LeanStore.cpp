#include "LeanStore.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/tables/BMTable.hpp"
#include "leanstore/profiling/tables/CPUTable.hpp"
#include "leanstore/profiling/tables/CRTable.hpp"
#include "leanstore/profiling/tables/DTTable.hpp"
#include "leanstore/profiling/tables/LatencyTable.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
#include "rapidjson/document.h"
#include "rapidjson/istreamwrapper.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "tabulate/table.hpp"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/resource.h>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
#include <filesystem>
#include <locale>
#include <sstream>
// -------------------------------------------------------------------------------------
using namespace tabulate;
// using leanstore::utils::threadlocal::sum;
namespace rs = rapidjson;
namespace leanstore
{
// -------------------------------------------------------------------------------------
LeanStore::LeanStore()
{
   // LeanStore::addStringFlag("ssd_path", &FLAGS_ssd_path);
   if (FLAGS_recover_file != "./leanstore.json" && std::filesystem::exists(FLAGS_recover_file)) {
      FLAGS_recover = true;
      assert(std::filesystem::exists(FLAGS_ssd_path) && !FLAGS_trunc);
   }
   if (FLAGS_persist_file != "./leanstore.json") {
      FLAGS_persist = true;
      std::cout << "Persist is enabled. Writing to: " << FLAGS_persist_file << std::endl;
   }
   if (FLAGS_recover) {
      deserializeFlags();
   }
   // -------------------------------------------------------------------------------------
   // Check if configurations make sense
   if ((FLAGS_vi) && !FLAGS_wal) {
      SetupFailed("You have to enable WAL");
   }
   if (FLAGS_isolation_level == "si" && (!FLAGS_mv | !FLAGS_vi)) {
      SetupFailed("You have to enable mv and vi (multi-versioning) for snapshot isolation.");
   }
   // -------------------------------------------------------------------------------------
   // Set the default logger to file logger
   // Init SSD pool
   int flags = O_RDWR | O_DIRECT;
   if (FLAGS_trunc) {
      flags |= O_TRUNC | O_CREAT;
   }
   ssd_fd = open(FLAGS_ssd_path.c_str(), flags, 0666);
   if (ssd_fd == -1) {
      perror("posix error");
      std::cout << "path: " << FLAGS_ssd_path << std::endl;
      SetupFailed("Could not open the file or the SSD block device");
   }
   if (FLAGS_falloc > 0) {
      const u64 gib_size = 1024ull * 1024ull * 1024ull;
      auto dummy_data = (u8*)aligned_alloc(512, gib_size);
      for (u64 i = 0; i < FLAGS_falloc; i++) {
         const int ret = pwrite(ssd_fd, dummy_data, gib_size, gib_size * i);
         posix_check(ret == gib_size);
      }
      free(dummy_data);
      fsync(ssd_fd);
   }
   ensure(fcntl(ssd_fd, F_GETFL) != -1);
   // -------------------------------------------------------------------------------------
   buffer_manager = make_unique<storage::BufferManager>(ssd_fd);
   BMC::global_bf = buffer_manager.get();
   // -------------------------------------------------------------------------------------
   DTRegistry::global_dt_registry.registerDatastructureType(0, storage::btree::BTreeLL::getMeta());
   DTRegistry::global_dt_registry.registerDatastructureType(2, storage::btree::BTreeVI::getMeta());
   // -------------------------------------------------------------------------------------
   if (FLAGS_recover) {
      deserializeState();
   }
   // -------------------------------------------------------------------------------------
   u64 end_of_block_device;
   if (FLAGS_wal_offset_gib == 0) {
      ioctl(ssd_fd, BLKGETSIZE64, &end_of_block_device);
   } else {
      end_of_block_device = FLAGS_wal_offset_gib * 1024 * 1024 * 1024;
   }
   // -------------------------------------------------------------------------------------
   history_tree = std::make_unique<cr::HistoryTree>();
   cr_manager = make_unique<cr::CRManager>(*history_tree.get(), ssd_fd, end_of_block_device);
   cr::CRManager::global = cr_manager.get();
   cr_manager->scheduleJobSync(0, [&]() {
      history_tree->update_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
      history_tree->remove_btrees = std::make_unique<leanstore::storage::btree::BTreeLL*[]>(FLAGS_worker_threads);
      for (u64 w_i = 0; w_i < FLAGS_worker_threads; w_i++) {
         std::string name = "_history_tree_" + std::to_string(w_i);
         history_tree->update_btrees[w_i] = &registerBTreeLL(name + "_updates", {.enable_wal = false, .use_bulk_insert = true});
         history_tree->remove_btrees[w_i] = &registerBTreeLL(name + "_removes", {.enable_wal = false, .use_bulk_insert = true});
      }
   });
   // -------------------------------------------------------------------------------------
   buffer_manager->startBackgroundThreads();
}

// -------------------------------------------------------------------------------------
void LeanStore::startProfilingThread()
{
   std::thread profiling_thread([&]() {
      utils::pinThisThread(((FLAGS_pin_threads) ? FLAGS_worker_threads : 0) + FLAGS_wal + FLAGS_pp_threads);
      if (FLAGS_root) {
         posix_check(setpriority(PRIO_PROCESS, 0, -20) == 0);
      }
      // -------------------------------------------------------------------------------------
      profiling::BMTable bm_table(*buffer_manager.get());
      profiling::DTTable dt_table(*buffer_manager.get());
      profiling::CPUTable cpu_table;
      profiling::CRTable cr_table;
      profiling::LatencyTable latency_table;
      std::vector<profiling::ProfilingTable*> tables = {&configs_table, &bm_table, &dt_table, &cpu_table, &cr_table};
      if (FLAGS_profile_latency) {
         tables.push_back(&latency_table);
      }
      // -------------------------------------------------------------------------------------
      std::vector<std::ofstream> csvs;
      std::ofstream::openmode open_flags;
      if (FLAGS_csv_truncate) {
         open_flags = ios::trunc;
      } else {
         open_flags = ios::app;
      }
      for (u64 t_i = 0; t_i < tables.size() + 1; t_i++) {
         if (t_i < tables.size()) {
            tables[t_i]->open();
            tables[t_i]->next(); // Clear previous values
         }
         // -------------------------------------------------------------------------------------
         csvs.emplace_back();
         auto& csv = csvs.back();
         if (t_i < tables.size())
            csv.open(FLAGS_csv_path + "_" + tables[t_i]->getName() + ".csv", open_flags);
         else
            csv.open(FLAGS_csv_path + "_sum.csv", open_flags); // summary
         csv.seekp(0, ios::end);
         csv << std::setprecision(2) << std::fixed;
         if (csv.tellp() == 0 && t_i < tables.size()) { // summary is output below
            csv << "t,c_hash";
            for (auto& c : tables[t_i]->getColumns()) {
               csv << "," << c.first;
            }
            csv << endl;
         }
      }
      // -------------------------------------------------------------------------------------
      config_hash = configs_table.hash();
      // -------------------------------------------------------------------------------------
      u64 seconds = 0;
      u64 dt_page_reads_acc = 0;
      u64 dt_page_writes_acc = 0;
      u64 cycles_prev = 0;
      u64 task_clock_acc = 0;
      while (bg_threads_keep_running) {
         for (u64 t_i = 0; t_i < tables.size(); t_i++) {
            tables[t_i]->next();
            if (tables[t_i]->size() == 0)
               continue;
            // -------------------------------------------------------------------------------------
            // CSV
            auto& csv = csvs[t_i];
            for (u64 r_i = 0; r_i < tables[t_i]->size(); r_i++) {
               csv << seconds << "," << config_hash;
               for (auto& c : tables[t_i]->getColumns()) {
                  csv << "," << c.second.values[r_i];
               }
               csv << endl;
            }
            // -------------------------------------------------------------------------------------
            // TODO: Websocket, CLI
         }
         // -------------------------------------------------------------------------------------
         std::vector<variant<std::string, const char *, Table>> tx_console_header;
         std::vector<variant<std::string, const char *, Table>> tx_console_data;
         tx_console_header.reserve(20);
         tx_console_data.reserve(20);
         tx_console_header.push_back("t");
         tx_console_data.push_back(std::to_string(seconds));

         const u64 tx = std::stoi(cr_table.get("0", "tx"));
         tx_console_header.push_back("OLTP TX");
         tx_console_data.push_back(std::to_string(tx));

         const double tx_abort = std::stoi(cr_table.get("0", "tx_abort"));
         const double tx_abort_pct = tx_abort * 100.0 / (tx_abort + tx);
         tx_console_header.push_back("Abort%");
         tx_console_data.push_back(std::to_string(tx_abort_pct));

         // const double rfa_pct = std::stod(cr_table.get("0", "rfa_committed_tx")) * 100.0 / tx;
         // const double remote_flushes_pct = 100.0 - rfa_pct;
         // tx_console_header.push_back("RF %");
         // tx_console_data.push_back(std::to_string(remote_flushes_pct));

         // const u64 olap_tx = std::stoi(cr_table.get("0", "olap_tx"));
         // tx_console_header.push_back("OLAP TX");
         // tx_console_data.push_back(std::to_string(olap_tx));

         tx_console_header.push_back("W MiB");
         tx_console_data.push_back(bm_table.get("0", "w_mib"));

         tx_console_header.push_back("R MiB");
         tx_console_data.push_back(bm_table.get("0", "r_mib"));

         // const double committed_gct_pct = std::stoi(cr_table.get("0", "gct_committed_tx")) * 100.0 / committed_tx;
         // Global Stats
         global_stats.accumulated_tx_counter += tx;
         // -------------------------------------------------------------------------------------
         // Console
         // -------------------------------------------------------------------------------------
         if (cpu_table.workers_agg_events.contains("instr"))
         {
            const double instr_per_tx = cpu_table.workers_agg_events["instr"] / tx;
            tx_console_header.push_back("Instrs/TX");
            tx_console_data.push_back(std::to_string(instr_per_tx));
         }
         
         if (cpu_table.workers_agg_events.contains("cycle"))
         {
            double cycles_per_tx;
            if (tx == 0) {
               cycles_prev += cpu_table.workers_agg_events["cycle"];
               cycles_per_tx = 0;
            } else {
               cycles_per_tx = (cpu_table.workers_agg_events["cycle"] + cycles_prev) / tx;
               cycles_prev = 0;
            }
            tx_console_header.push_back("Cycles/TX");
            tx_console_data.push_back(std::to_string(cycles_per_tx));
         }

         if (cpu_table.workers_agg_events.contains("CPU"))
         {
            tx_console_header.push_back("Utilized CPUs");
            tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["CPU"]));
         }

         if (cpu_table.workers_agg_events.contains("task"))
         {
            tx_console_header.push_back("CPUTime/TX (ms)");
            if (tx > 0) {
               tx_console_data.push_back(std::to_string(
               ((double) cpu_table.workers_agg_events["task"] + task_clock_acc) / tx * 1e-6));
               task_clock_acc = 0;
            } else {
               task_clock_acc += cpu_table.workers_agg_events["task"];
               tx_console_data.push_back("0");
            }
         }

         if (cpu_table.workers_agg_events.contains("L1-miss"))
         {
            const double l1_per_tx = cpu_table.workers_agg_events["L1-miss"] / tx;
            tx_console_header.push_back("L1/TX");
            tx_console_data.push_back(std::to_string(l1_per_tx));
         }

         if (cpu_table.workers_agg_events.contains("LLC-miss"))
         {
            const double llc_per_tx = cpu_table.workers_agg_events["LLC-miss"] / tx;
            tx_console_header.push_back("LLC/TX");
            tx_console_data.push_back(std::to_string(llc_per_tx));
         }

         if (cpu_table.workers_agg_events.contains("GHz"))
         {
            tx_console_header.push_back("GHz");
            tx_console_data.push_back(std::to_string(cpu_table.workers_agg_events["GHz"]));
         }

         tx_console_header.push_back("WAL GiB/s");
         tx_console_data.push_back(cr_table.get("0", "wal_write_gib"));

         tx_console_header.push_back("GCT GiB/s");
         tx_console_data.push_back(cr_table.get("0", "gct_write_gib"));
         
         u64 dt_page_reads = dt_table.getSum("dt_page_reads");
         tx_console_header.push_back("SSDReads/TX");
         u64 dt_page_writes = dt_table.getSum("dt_page_writes");
         tx_console_header.push_back("SSDWrites/TX");
         
         if (tx != 0) {
            tx_console_data.push_back(std::to_string((dt_page_reads + dt_page_reads_acc) / (double) tx));
            tx_console_data.push_back(std::to_string((dt_page_writes + dt_page_writes_acc) / (double) tx));
            dt_page_reads_acc = 0;
            dt_page_writes_acc = 0;
         } else {
            dt_page_reads_acc += dt_page_reads;
            dt_page_writes_acc += dt_page_writes;
            tx_console_data.push_back("0");
            tx_console_data.push_back("0");
         }

         auto& csv_sum = csvs.back();
         if (seconds == 0) {
            for (auto& h: tx_console_header) {
               std::visit([&csv_sum](auto&& arg) { csv_sum << arg << ","; }, h);
            }
            csv_sum << endl;
         }
         for (auto& d: tx_console_data) {
            std::visit([&csv_sum](auto&& arg) { csv_sum << arg << ","; }, d);
         }
         csv_sum << endl;

         // using RowType = std::vector<variant<std::string, const char*, Table>>;
         if (FLAGS_print_tx_console) {
            tabulate::Table table;
            table.add_row(tx_console_header);
            table.add_row(tx_console_data);
            // -------------------------------------------------------------------------------------
            table.format().width(10);
            table.column(0).format().width(5);
            table.column(1).format().width(12);
            // -------------------------------------------------------------------------------------
            auto print_table = [](tabulate::Table& table, std::function<bool(u64)> predicate) {
               std::stringstream ss;
               table.print(ss);
               string str = ss.str();
               u64 line_n = 0;
               for (u64 i = 0; i < str.size(); i++) {
                  if (str[i] == '\n') {
                     line_n++;
                  }
                  if (predicate(line_n)) {
                     cout << str[i];
                  }
               }
            };
            if (seconds == 0) {
               print_table(table, [](u64 line_n) { return (line_n < 3) || (line_n == 4); });
            } else {
               print_table(table, [](u64 line_n) { return line_n == 4; });
            }
            // -------------------------------------------------------------------------------------
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            seconds += 1;
            std::locale::global(std::locale::classic());
         }
      }
      bg_threads_counter--;
   });
   bg_threads_counter++;
   profiling_thread.detach();
}
// -------------------------------------------------------------------------------------
storage::btree::BTreeLL& LeanStore::registerBTreeLL(string name, storage::btree::BTreeGeneric::Config config)
{
   assert(btrees_ll.find(name) == btrees_ll.end());
   auto& btree = btrees_ll[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), name);
   btree.create(dtid, config);
   return btree;
}

// -------------------------------------------------------------------------------------
storage::btree::BTreeVI& LeanStore::registerBTreeVI(string name, storage::btree::BTreeLL::Config config)
{
   assert(btrees_vi.find(name) == btrees_vi.end());
   auto& btree = btrees_vi[name];
   DTID dtid = DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), name);
   auto& graveyard_btree = registerBTreeLL("_" + name + "_graveyard", {.enable_wal = false, .use_bulk_insert = false});
   btree.create(dtid, config, &graveyard_btree);
   return btree;
}
// -------------------------------------------------------------------------------------
u64 LeanStore::getConfigHash()
{
   return config_hash;
}
// -------------------------------------------------------------------------------------
LeanStore::GlobalStats LeanStore::getGlobalStats()
{
   return global_stats;
}
// -------------------------------------------------------------------------------------
void LeanStore::serializeState()
{
   // Serialize data structure instances
   std::ofstream json_file;
   json_file.open(FLAGS_persist_file, ios::trunc);
   rs::Document d;
   rs::Document::AllocatorType& allocator = d.GetAllocator();
   d.SetObject();
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialized_cr_map = cr_manager->serialize();
   rs::Value cr_serialized(rs::kObjectType);
   for (const auto& [key, value] : serialized_cr_map) {
      rs::Value k, v;
      k.SetString(key.c_str(), key.length(), allocator);
      v.SetString(value.c_str(), value.length(), allocator);
      cr_serialized.AddMember(k, v, allocator);
   }
   d.AddMember("cr_manager", cr_serialized, allocator);
   // -------------------------------------------------------------------------------------
   std::unordered_map<std::string, std::string> serialized_bm_map = buffer_manager->serialize();
   rs::Value bm_serialized(rs::kObjectType);
   for (const auto& [key, value] : serialized_bm_map) {
      rs::Value k, v;
      k.SetString(key.c_str(), key.length(), allocator);
      v.SetString(value.c_str(), value.length(), allocator);
      bm_serialized.AddMember(k, v, allocator);
   }
   d.AddMember("buffer_manager", bm_serialized, allocator);
   // -------------------------------------------------------------------------------------
   rs::Value dts(rs::kArrayType);
   for (auto& dt : DTRegistry::global_dt_registry.dt_instances_ht) {
      if (std::get<2>(dt.second).substr(0, 1) == "_") {
         continue;
      }
      rs::Value dt_json_object(rs::kObjectType);
      const DTID dt_id = dt.first;
      rs::Value name;
      name.SetString(std::get<2>(dt.second).c_str(), std::get<2>(dt.second).length(), allocator);
      dt_json_object.AddMember("name", name, allocator);
      dt_json_object.AddMember("type", rs::Value(std::get<0>(dt.second)), allocator);
      dt_json_object.AddMember("id", rs::Value(dt_id), allocator);
      // -------------------------------------------------------------------------------------
      std::unordered_map<std::string, std::string> serialized_dt_map = DTRegistry::global_dt_registry.serialize(dt_id);
      rs::Value dt_serialized(rs::kObjectType);
      for (const auto& [key, value] : serialized_dt_map) {
         rs::Value k, v;
         k.SetString(key.c_str(), key.length(), allocator);
         v.SetString(value.c_str(), value.length(), allocator);
         dt_serialized.AddMember(k, v, allocator);
      }
      dt_json_object.AddMember("serialized", dt_serialized, allocator);
      // -------------------------------------------------------------------------------------
      dts.PushBack(dt_json_object, allocator);
   }
   d.AddMember("registered_datastructures", dts, allocator);
   // -------------------------------------------------------------------------------------
   serializeFlags(d);
   rs::StringBuffer sb;
   rs::PrettyWriter<rs::StringBuffer> writer(sb);
   d.Accept(writer);
   json_file << sb.GetString();
}
// -------------------------------------------------------------------------------------
void LeanStore::serializeFlags(rs::Document& d)
{
   rs::Value flags_serialized(rs::kObjectType);
   rs::Document::AllocatorType& allocator = d.GetAllocator();
   for (auto flags : persisted_string_flags) {
      rs::Value name(std::get<0>(flags).c_str(), std::get<0>(flags).length(), allocator);
      rs::Value value;
      value.SetString((*std::get<1>(flags)).c_str(), (*std::get<1>(flags)).length(), allocator);
      flags_serialized.AddMember(name, value, allocator);
   }
   for (auto flags : persisted_s64_flags) {
      rs::Value name(std::get<0>(flags).c_str(), std::get<0>(flags).length(), allocator);
      string value_string = std::to_string(*std::get<1>(flags));
      rs::Value value;
      value.SetString(value_string.c_str(), value_string.length(), d.GetAllocator());
      flags_serialized.AddMember(name, value, allocator);
   }
   d.AddMember("flags", flags_serialized, allocator);
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeState()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& cr = d["cr_manager"];
   std::unordered_map<std::string, std::string> serialized_cr_map;
   for (rs::Value::ConstMemberIterator itr = cr.MemberBegin(); itr != cr.MemberEnd(); ++itr) {
      serialized_cr_map[itr->name.GetString()] = itr->value.GetString();
   }
   cr_manager->deserialize(serialized_cr_map);
   // -------------------------------------------------------------------------------------
   const rs::Value& bm = d["buffer_manager"];
   std::unordered_map<std::string, std::string> serialized_bm_map;
   for (rs::Value::ConstMemberIterator itr = bm.MemberBegin(); itr != bm.MemberEnd(); ++itr) {
      serialized_bm_map[itr->name.GetString()] = itr->value.GetString();
   }
   buffer_manager->deserialize(serialized_bm_map);
   // -------------------------------------------------------------------------------------
   const rs::Value& dts = d["registered_datastructures"];
   assert(dts.IsArray());
   for (auto& dt : dts.GetArray()) {
      assert(dt.IsObject());
      const DTID dt_id = dt["id"].GetInt();
      const DTType dt_type = dt["type"].GetInt();
      const std::string dt_name = dt["name"].GetString();
      std::unordered_map<std::string, std::string> serialized_dt_map;
      const rs::Value& serialized_object = dt["serialized"];
      for (rs::Value::ConstMemberIterator itr = serialized_object.MemberBegin(); itr != serialized_object.MemberEnd(); ++itr) {
         serialized_dt_map[itr->name.GetString()] = itr->value.GetString();
      }
      // -------------------------------------------------------------------------------------
      if (dt_type == 0) {
         auto& btree = btrees_ll[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(0, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else if (dt_type == 2) {
         auto& btree = btrees_vi[dt_name];
         DTRegistry::global_dt_registry.registerDatastructureInstance(2, reinterpret_cast<void*>(&btree), dt_name, dt_id);
      } else {
         UNREACHABLE();
      }
      DTRegistry::global_dt_registry.deserialize(dt_id, serialized_dt_map);
   }
}
// -------------------------------------------------------------------------------------
void LeanStore::deserializeFlags()
{
   std::ifstream json_file;
   json_file.open(FLAGS_recover_file);
   rs::IStreamWrapper isw(json_file);
   rs::Document d;
   d.ParseStream(isw);
   // -------------------------------------------------------------------------------------
   const rs::Value& flags = d["flags"];
   std::unordered_map<std::string, std::string> flags_serialized;
   for (rs::Value::ConstMemberIterator itr = flags.MemberBegin(); itr != flags.MemberEnd(); ++itr) {
      flags_serialized[itr->name.GetString()] = itr->value.GetString();
   }
   for (auto flags : persisted_string_flags) {
      *std::get<1>(flags) = flags_serialized[std::get<0>(flags)];
   }
   for (auto flags : persisted_s64_flags) {
      *std::get<1>(flags) = atoi(flags_serialized[std::get<0>(flags)].c_str());
   }
}
// -------------------------------------------------------------------------------------
LeanStore::~LeanStore()
{
   if (FLAGS_btree_print_height || FLAGS_btree_print_tuples_count) {
      cr_manager->joinAll();
      for (auto& iter : btrees_ll) {
         if (iter.first.substr(0, 1) == "_") {
            continue;
         }
         cout << "BTreeLL: " << iter.first << ", dt_id= " << iter.second.dt_id << ", height= " << iter.second.height;
         if (FLAGS_btree_print_tuples_count) {
            cr_manager->scheduleJobSync(0, [&]() { cout << ", #tuples= " << iter.second.countEntries() << endl; });
         } else {
            cout << endl;
         }
      }
      for (auto& iter : btrees_vi) {
         cout << "BTreeVI: " << iter.first << ", dt_id= " << iter.second.dt_id << ", height= " << iter.second.height;
         if (FLAGS_btree_print_tuples_count) {
            cr_manager->scheduleJobSync(0, [&]() { cout << ", #tuples= " << iter.second.countEntries() << endl; });
         } else {
            cout << endl;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   bg_threads_keep_running = false;
   while (bg_threads_counter) {
   }
   if (FLAGS_persist) {
      std::cout << "Writing all buffer frames to disk: " << FLAGS_ssd_path << "..." << std::endl;
      buffer_manager->writeAllBufferFrames();
      serializeState();
   }
   std::cout << "LeanStore::~LeanStore: Finished." << std::endl;
}
// -------------------------------------------------------------------------------------
// Static members
std::list<std::tuple<string, fLS::clstring*>> LeanStore::persisted_string_flags = {};
std::list<std::tuple<string, s64*>> LeanStore::persisted_s64_flags = {};
// -------------------------------------------------------------------------------------
}  // namespace leanstore
