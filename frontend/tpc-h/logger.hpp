#pragma once
#include <string>
// virtual class for logging
class Logger
{
  public:
   virtual void reset() = 0;
   virtual void log(long elapsed, std::string csv_dir) = 0;
   virtual void prepare() = 0;
};