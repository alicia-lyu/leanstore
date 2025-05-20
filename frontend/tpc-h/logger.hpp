#pragma once
#include <map>
#include <string>
enum class ColumnName
{
   ELAPSED,
   TPUT
};

inline std::string to_string(ColumnName column)
{
   switch (column) {
      case ColumnName::ELAPSED:
         return "Elapsed (ms)";
      case ColumnName::TPUT:
         return "Tput (tx/s)";
      default:
         return "Unknown";
   }
}

// virtual class for logging
class Logger
{
  public:
   virtual void reset() = 0;
   virtual void log(long tput, std::string tx, std::string method, int tx_count) = 0;
   virtual void log(long elapsed, std::string tx, std::string method) = 0;
   virtual void log_sizes(std::map<std::string, double> sizes) = 0;
   virtual void prepare() = 0;
};