#pragma once
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>

template <class Record, class PayloadType = Record>
class Scanner
{
  public:
   uint64_t produced = 0;

   struct pair_t {
      typename Record::Key key;
      PayloadType record;
   };

   std::function<std::optional<pair_t>()> assemble;

   Scanner(std::function<std::optional<pair_t>()> assemble) : assemble(assemble) {}

   virtual ~Scanner() { 
      if (produced > 99999)
         std::cout << "Scanner::~Scanner: produced " << produced << " records" << std::endl;
   }

   virtual bool seek(typename Record::Key key) = 0;

   virtual std::optional<pair_t> next() = 0;
};