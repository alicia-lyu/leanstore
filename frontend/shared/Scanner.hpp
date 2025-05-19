#pragma once
#include <optional>

template <class Record>
class Scanner
{
  public:
  virtual ~Scanner() = default;
  virtual void reset() = 0;

  virtual std::optional<std::pair<typename Record::Key, Record>> next() = 0;

  virtual std::optional<std::pair<typename Record::Key, Record>> current() = 0;

  virtual void seek(const typename Record::Key& key) = 0;

  virtual void seekForPrev(const typename Record::Key& key) = 0;
};