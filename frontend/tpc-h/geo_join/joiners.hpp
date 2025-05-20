// in geo_join_helpers.hpp
#pragma once

#include "../merge.hpp"
#include "views.hpp"

namespace geo_join
{
template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseJoiner {
   std::unique_ptr<ScannerType<nation2_t>> nation_scanner;
   std::unique_ptr<ScannerType<states_t>> states_scanner;
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;

   BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> joiner_ns;
   BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> joiner_nsc;
   BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t> final_joiner;

   BaseJoiner(AdapterType<nation2_t>& nation, AdapterType<states_t>& states, AdapterType<county_t>& county, AdapterType<city_t>& city)
       : nation_scanner(nation.getScanner()),
         states_scanner(states.getScanner()),
         county_scanner(county.getScanner()),
         city_scanner(city.getScanner()),
         joiner_ns([&] { return nation_scanner->next(); }, [&] { return states_scanner->next(); }),
         joiner_nsc([&] { return joiner_ns.next(); }, [&] { return county_scanner->next(); }),
         final_joiner([&] { return joiner_nsc.next(); }, [&] { return city_scanner->next(); })
   {
   }

   void run() { final_joiner.run(); }

   void next() { final_joiner.next(); }

   sort_key_t current_jk() const { return final_joiner.current_jk; }

   long produced() const { return final_joiner.produced; }

   void seek(const sort_key_t& sk)
   {
      nation_scanner->seekForPrev(nation2_t::Key{sk.nationkey});
      nation_scanner->prev();
      states_scanner->seekForPrev(states_t::Key{sk.nationkey, sk.statekey});
      states_scanner->prev();
      county_scanner->seekForPrev(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      county_scanner->prev();
      city_scanner->seekForPrev(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
      city_scanner->prev();
   }
};
template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoiner {
   std::unique_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> merged_scanner;
   PremergedJoin<sort_key_t, view_t, nation2_t, states_t, county_t, city_t> joiner;

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t>& merged) : merged_scanner(merged.getScanner()), joiner(*merged_scanner) {}

   void run() { joiner.run(); }

    void next() { joiner.next(); }

    sort_key_t current_jk() const { return joiner.current_jk; }

    long produced() const { return joiner.produced; }

    void seek(const sort_key_t& sk)
    {
        merged_scanner->template seekForPrev<nation2_t>(nation2_t::Key{sk.nationkey});
        merged_scanner->prev();
    }
};
}  // namespace geo_join