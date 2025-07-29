#pragma once

#include "../shared/merge-join/multi_table_merge_join.hpp"
#include "../shared/merge-join/premerged_join.hpp"
#include "views.hpp"
#include "workload.hpp"

namespace geo_join
{
template <template <typename> class AdapterType, template <typename> class ScannerType>
struct BaseJoinerCCC {
   std::unique_ptr<ScannerType<county_t>> county_scanner;
   std::unique_ptr<ScannerType<city_t>> city_scanner;
   std::unique_ptr<ScannerType<customer2_t>> customer2_scanner;

   std::optional<MergeJoin<sort_key_t, ccc_t, county_t, city_t, customer2_t>> joiner_ccc;

   BaseJoinerCCC(AdapterType<county_t>& county,
                 AdapterType<city_t>& city,
                 AdapterType<customer2_t>& customer2,
                 sort_key_t seek_key = sort_key_t::max())
       : county_scanner(county.getScanner()), city_scanner(city.getScanner()), customer2_scanner(customer2.getScanner())
   {
      if (seek_key != sort_key_t::max()) {
         county_scanner->seek(county_t::Key{seek_key});
         city_scanner->seek(city_t::Key{seek_key});
         customer2_scanner->seek(customer2_t::Key{seek_key});
      }
      joiner_ccc.emplace(*county_scanner, *city_scanner, *customer2_scanner);
   }

   void run() { joiner_ccc->run(); }
   std::optional<std::pair<ccc_t::Key, ccc_t>> next() { return joiner_ccc->next(); }
   sort_key_t current_jk() const { return joiner_ccc->jk_to_join(); }
   long produced() const { return joiner_ccc->produced(); }
};

template <template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedScannerCCC {
   std::unique_ptr<MergedScannerType<sort_key_t, view_t, nation2_t, states_t, county_t, city_t, customer2_t>> full_scanner;
   long long produced = 0;
   using K = std::variant<county_t::Key, city_t::Key, customer2_t::Key>;
   using V = std::variant<county_t, city_t, customer2_t>;

   MergedScannerCCC(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& full_merged, sort_key_t sk = sort_key_t::max())
       : full_scanner(full_merged.template getScanner<sort_key_t, view_t>())
   {
      if (sk != sort_key_t::max()) {
         full_scanner->template seek<city_t>(city_t::Key{sk});
      }
   }

   std::optional<std::pair<K, V>> next()
   {
      auto full_kv = full_scanner->next();
      if (!full_kv.has_value()) {
         return std::nullopt;
      }
      auto [k, structured_v] = *full_kv;  // 'v' is a structured binding, not a real variable
      auto& v = structured_v;             // Create a real reference variable that can be captured
      std::optional<std::pair<K, V>> ret = std::nullopt;
      std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {},
                            [&](const county_t::Key& ck) { ret = std::make_pair(K{ck}, V{std::get<county_t>(v)}); },
                            [&](const city_t::Key& ci) { ret = std::make_pair(K{ci}, V{std::get<city_t>(v)}); },
                            [&](const customer2_t::Key& cu) { ret = std::make_pair(K{cu}, V{std::get<customer2_t>(v)}); }},
                 k);
      if (!ret.has_value()) {
         return next();  // skip invalid keys
      } else {
         return ret;
      }
   }

   std::optional<std::pair<K, V>> last_in_page()
   {
      auto full_kv = full_scanner->last_in_page();
      if (!full_kv.has_value()) {
         return std::nullopt;
      }
      auto [k, structured_v] = *full_kv;  // 'v' is a structured binding, not a real variable
      auto& v = structured_v;             // Create a real reference variable that can be captured
      std::optional<std::pair<K, V>> ret = std::nullopt;
      std::visit(overloaded{[&](const nation2_t::Key&) {}, [&](const states_t::Key&) {},  // discard
                            [&](const county_t::Key& ck) { ret = std::make_pair(K{ck}, V{std::get<county_t>(v)}); },
                            [&](const city_t::Key& ci) { ret = std::make_pair(K{ci}, V{std::get<city_t>(v)}); },
                            [&](const customer2_t::Key& cu) { ret = std::make_pair(K{cu}, V{std::get<customer2_t>(v)}); }},
                 k);
      if (!ret.has_value()) {
         return std::nullopt;
      } else {
         return ret;
      }
   }

   int go_to_last_in_page() { return full_scanner->go_to_last_in_page(); }

   template <typename R>
   void seek(const typename R::Key& key)
      requires(std::is_same_v<R, county_t> || std::is_same_v<R, city_t> || std::is_same_v<R, customer2_t>)
   {
      full_scanner->template seek<R>(key);
   }
};

template <template <typename> class AdapterType, template <typename...> class MergedAdapterType, template <typename...> class MergedScannerType>
struct MergedJoinerCCC {
   MergedScannerCCC<MergedAdapterType, MergedScannerType> merged_scanner_ccc;
   PremergedJoin<MergedScannerCCC<MergedAdapterType, MergedScannerType>, sort_key_t, ccc_t, county_t, city_t, customer2_t> joiner_ccc;

   sort_key_t seek_key;
   const sort_key_t seek_max = sort_key_t::max();

   AdapterType<ccc_t>* ccc_view_ptr = nullptr;

   MergedJoinerCCC(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged, sort_key_t seek_key = sort_key_t::max())
       : merged_scanner_ccc(merged, seek_key), joiner_ccc(merged_scanner_ccc), seek_key(seek_key)
   {
   }

   MergedJoinerCCC(MergedAdapterType<nation2_t, states_t, county_t, city_t, customer2_t>& merged,
                   AdapterType<ccc_t>& ccc_view,
                   sort_key_t seek_key = sort_key_t::max())
       : merged_scanner_ccc(merged, seek_key),
         joiner_ccc(merged_scanner_ccc,
                    [this](const ccc_t::Key& k, const ccc_t& v) {
                       if (ccc_view_ptr) {
                          this->ccc_view_ptr->insert(k, v);
                       }
                    }),
         seek_key(seek_key),
         ccc_view_ptr(&ccc_view)
   {
   }

   void run() { joiner_ccc.run(); }
   std::optional<std::pair<ccc_t::Key, ccc_t>> next()
   {
      auto ret = joiner_ccc.next(seek_key);
      if (seek_key != seek_max) {
         seek_key = seek_max;  // reset seek_key after the first result
      }
      return ret;
   }

   sort_key_t current_jk() const { return joiner_ccc.current_jk(); }
   long produced() const { return joiner_ccc.produced(); }
   long consumed() const { return merged_scanner_ccc.produced; }
   bool went_past(const sort_key_t& sk) const { return joiner_ccc.went_past(sk); }
};

}  // namespace geo_join