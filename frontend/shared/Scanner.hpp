#pragma once
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>

template <class Record>
class Scanner
{
  public:
  virtual void reset() = 0;

  virtual std::optional<std::pair<typename Record::Key, Record>> next() = 0;

  virtual std::optional<std::pair<typename Record::Key, Record>> current() = 0;

  virtual bool seek(typename Record::Key& key) = 0;
};