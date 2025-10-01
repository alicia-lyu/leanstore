#include <filesystem>
#include <iostream>
#include "premerged_join.hpp"

PremergedJoinStats PremergedJoinLogger::last_stats; // cannot be inlined because PremergedJoinStats was an incomplete type in the header

// This static instance ensures flush() is called automatically at the end of the program
const static LoggerFlusher<PremergedJoinLogger> final_flusher;

// --- Implementation of PremergedJoinLogger methods ---

void PremergedJoinLogger::init()
{
   if (!is_initialized) {
      // You can get this path from your FLAGS or config
      std::filesystem::path log_path = std::filesystem::path(FLAGS_csv_path) / "premerged_join_stats.csv";
      log_file.open(log_path, std::ios::trunc);  // Use trunc to overwrite old logs

      // Write a clean CSV header
      log_file << "repeat_count,record_type_count,seek_cnt,right_next_cnt,"
               << "scan_filter_success,scan_filter_fail,emplace_cnt,avg_remaining_records_to_join,avg_produced\n";

      is_initialized = true;
   }
}

void PremergedJoinLogger::write_row()
{
   if (repeat_count > 0) {
      log_file << repeat_count << ',' << last_stats.record_type_count << ',' << last_stats.seek_cnt << ',' << last_stats.right_next_cnt << ','
               << last_stats.scan_filter_success << ',' << last_stats.scan_filter_fail << ',' << last_stats.emplace_cnt << ','
               << remaining_records_to_join_accumulated / repeat_count << ',' << produced_accumulated / repeat_count << std::endl;
   }
}

void PremergedJoinLogger::log(const PremergedJoinStats& current_stats, size_t remaining_records_to_join, long produced)
{
   std::lock_guard<std::mutex> lock(mtx);

   init();

   if (repeat_count == 0) {
      // First ever call
      last_stats = current_stats;
      repeat_count = 1;

   } else if (current_stats == last_stats) {
      // Stats are the same, just increment the counter
      repeat_count++;
   } else {
      // Stats have changed, write the previous run's data
      write_row();

      // And reset for the new run
      last_stats = current_stats;
      remaining_records_to_join_accumulated = 0;
      produced_accumulated = 0;
      repeat_count = 1;
   }
   remaining_records_to_join_accumulated += remaining_records_to_join;
   produced_accumulated += produced;
}

void PremergedJoinLogger::flush()
{
   std::lock_guard<std::mutex> lock(mtx);
   if (!is_initialized)
      return;

   // Write the final pending row
   write_row();

   // Reset state and close the file
   repeat_count = 0;
   if (log_file.is_open()) {
      log_file.close();
   }
   is_initialized = false;
}