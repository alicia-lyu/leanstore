#pragma once
#include <optional>
#include <functional>

template <class Record, class PayloadType = Record>
class Scanner
{
  public:
   struct pair_t {
      typename Record::Key key;
      PayloadType record;
   };

   std::function<std::optional<pair_t>()> assemble;

   Scanner(std::function<std::optional<pair_t>()> assemble)
   : assemble(assemble) {}
   virtual ~Scanner() = default;

   virtual bool seek(typename Record::Key key) = 0;

   virtual std::optional<pair_t> next() = 0;
};