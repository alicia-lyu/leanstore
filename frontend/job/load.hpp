#pragma once

#include <gflags/gflags.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <functional>
#include <stdexcept>
#include <cassert>

#include "tables.hpp" // Includes all table definitions from job/tables.hpp

// Define a flag to specify the location of the IMDb data files
DEFINE_string(data_dir, "imdb_data", "Directory containing IMDb .tsv files"); // REPLACE
DEFINE_bool(log_progress, true, "Log progress of data loading");

namespace imdb
{
/**
 * @brief Contains utility functions for parsing and loading the IMDb TSV data.
 */
namespace loader_utils
{

/**
 * @brief Parses a single line of a Tab-Separated-Value (TSV) file.
 * @param line The input string line.
 * @return A vector of strings, where each element is a column.
 */
inline std::vector<std::string> parse_tsv_line(const std::string& line)
{
   std::vector<std::string> columns;
   std::stringstream ss(line);
   std::string column;
   while (std::getline(ss, column, '\t')) {
      columns.push_back(std::move(column));
   }
   return columns;
}

/**
 * @brief Checks if a string field from the TSV represents NULL.
 * @param s The input string from a column.
 * @return true if the string is "\N", false otherwise.
 */
inline bool is_null(const std::string& s) { return s == "\\N"; }

/**
 * @brief Converts a string column to an Integer, handling "\N" as a default (null) Integer.
 */
inline Integer string_to_integer(const std::string& s)
{
   return is_null(s) ? Integer() : Integer(std::stoll(s));
}

/**
 * @brief Converts a string column to a Numeric, handling "\N" as a default (null) Numeric.
 */
inline Numeric string_to_numeric(const std::string& s)
{
   // Assuming Numeric has a constructor from double or long double
   return is_null(s) ? Numeric() : Numeric(std::stold(s));
}

/**
 * @brief Converts a string column to a Varchar of a specific size.
 */
template <int N>
inline Varchar<N> string_to_varchar(const std::string& s)
{
   return Varchar<N>(s.c_str());
}

/**
 * @brief Converts a string column to a tconst_t (Varchar<9>).
 */
inline tconst_t string_to_tconst(const std::string& s)
{
   return tconst_t(s.c_str());
}

/**
 * @brief Converts a string column to an nconst_t (Varchar<9>).
 */
inline nconst_t string_to_nconst(const std::string& s)
{
   return nconst_t(s.c_str());
}

/**
 * @brief A generic function to load a table from a TSV file.
 *
 * @tparam Adapter The adapter type (e.g., BwTreeAdapter<Record>).
 * @tparam Record The record type (e.g., title_basics_t).
 * @tparam Key The key type (e.g., title_basics_t::Key).
 * @param table The table adapter instance to load data into.
 * @param file_path The full path to the .tsv file.
 * @param parser_func A lambda function that takes a std::vector<std::string> (a parsed line)
 * and returns a std::pair<Key, Record> to be inserted.
 */
template <typename Adapter, typename Record, typename Key>
void load_table_from_file(Adapter& table,
                          const std::string& file_path,
                          std::function<std::pair<Key, Record>(const std::vector<std::string>&)> parser_func)
{
   std::ifstream file(file_path);
   if (!file.is_open()) {
      std::cerr << "Error: Could not open file: " << file_path << std::endl;
      throw std::runtime_error("Failed to open data file: " + file_path);
   }

   std::string line;
   // 1. Read and discard the header line
   if (!std::getline(file, line)) {
      std::cerr << "Warning: File is empty: " << file_path << std::endl;
      return;
   }

   long long record_count = 0;
   std::cout << "Loading table " << table.name << " from " << file_path << "..." << std::endl;

   // 2. Read and process data lines
   while (std::getline(file, line)) {
      if (line.empty())
         continue;

      std::vector<std::string> columns = parse_tsv_line(line);

      try {
         auto [key, record] = parser_func(columns);
         table.insert(key, record);
         record_count++;

         if (FLAGS_log_progress && record_count % 100000 == 0) {
            std::cout << "\rLoaded " << record_count << " records for " << table.name << "...";
         }
      } catch (const std::exception& e) {
         std::cerr << "\rError parsing line for table " << table.name << ": " << e.what() << "\nLine: " << line << std::endl;
      }
   }

   std::cout << "\rFinished loading " << table.name << ". Total records: " << record_count << "." << std::endl;
   file.close();
}

} // namespace loader_utils
} // namespace imdb