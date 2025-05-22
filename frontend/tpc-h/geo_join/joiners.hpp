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

   std::optional<BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t>> joiner_ns;
   std::optional<BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t>> joiner_nsc;
   std::optional<BinaryMergeJoin<sort_key_t, view_t, nsc_t, city_t>> final_joiner;

   BaseJoiner(AdapterType<nation2_t>& nation, AdapterType<states_t>& states, AdapterType<county_t>& county, AdapterType<city_t>& city, sort_key_t seek_key = sort_key_t::max())
       : nation_scanner(nation.getScanner()),
         states_scanner(states.getScanner()),
         county_scanner(county.getScanner()),
         city_scanner(city.getScanner())
   {
      if (seek_key != sort_key_t::max()) {
         seek(seek_key);
      }
      joiner_ns.emplace([&]() { return nation_scanner->next(); }, [&]() { return states_scanner->next(); });
      joiner_nsc.emplace([&]() { return joiner_ns->next(); }, [&]() { return county_scanner->next(); });
      final_joiner.emplace([&]() { return joiner_nsc->next(); }, [&]() { return city_scanner->next(); });
   }

   void run() { final_joiner->run(); }

   std::optional<std::pair<view_t::Key, view_t>> next() { return final_joiner->next(); }

   sort_key_t current_jk() const { return final_joiner->current_jk; }

   long produced() const { return final_joiner->produced; }

   void seek(const sort_key_t& sk)
   {
      nation_scanner->seek(nation2_t::Key{sk.nationkey});
      states_scanner->seek(states_t::Key{sk.nationkey, sk.statekey});
      county_scanner->seek(county_t::Key{sk.nationkey, sk.statekey, sk.countykey});
      city_scanner->seek(city_t::Key{sk.nationkey, sk.statekey, sk.countykey, sk.citykey});
   }
};
template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoiner {
   std::unique_ptr<MergedScannerType<nation2_t, states_t, county_t, city_t>> merged_scanner;
   std::optional<PremergedJoin<sort_key_t, view_t, nation2_t, states_t, county_t, city_t>> joiner;

   MergedJoiner(MergedAdapterType<nation2_t, states_t, county_t, city_t>& merged, sort_key_t seek_key = sort_key_t::max()) : merged_scanner(merged.getScanner()) {
      if (seek_key != sort_key_t::max()) {
         seek(seek_key);
      }
      joiner.emplace(*merged_scanner);
   }

   void run() { joiner->run(); }

   int next() { return joiner->next(); }

   sort_key_t current_jk() const { return joiner->current_jk; }

   long produced() const { return joiner->produced; }

   long consumed() const { return merged_scanner->produced; }

   void seek(const sort_key_t& sk) { merged_scanner->template seek<nation2_t>(nation2_t::Key{sk.nationkey}); }
};
}  // namespace geo_join