#pragma once
#include <optional>

template <class Record>
class Scanner
{
  public:
   struct next_ret_t {
      typename Record::Key key;
      Record record;
   };

   Scanner() = default;
   virtual ~Scanner() = default;

   virtual bool seek(typename Record::Key key) = 0;
   virtual std::optional<next_ret_t> next() = 0;
};

template <class Record1, class Record2>
class ScannerSec : public Scanner<Record1>
{
   using Base = Scanner<Record1>;
   using typename Base::next_ret_t;

  public:
   ScannerSec() = default;
   virtual ~ScannerSec() = default;

   virtual bool seek(typename Record2::Key key) = 0;

   virtual std::optional<next_ret_t> next() = 0;
};