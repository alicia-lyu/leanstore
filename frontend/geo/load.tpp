#pragma once
#include "views.hpp"
#include "workload.hpp"

template <typename Adapter, typename Record>
void write_table_to_stream(Adapter& table)
{
   std::filesystem::path dat_dir = std::filesystem::path(FLAGS_persist_file).parent_path() / std::filesystem::path(FLAGS_persist_file).stem();
   std::filesystem::create_directories(dat_dir);
   std::filesystem::path filename = dat_dir / (table.name + ".dat");
   std::ofstream out(filename);
   std::cout << "Writing to " << filename << "...";
   // 1. Write the number of records as a header for this table
   size_t record_count = 0;
   // Write a dummy record count at the beginning
   // out << record_count << "\n";

   table.scan(
       typename Record::Key{},
       [&](const auto& key, const auto& value) {
          out << key << value << "\n";
          record_count++;
          if (record_count % 1000 == 0)
             std::cout << "\rWriting to " << filename << " for " << table.name << ": " << record_count << " records...";
          return true;
       },
       [&]() {});
   // out.seekp(0);
   // out << record_count;
   out.close();
}

namespace geo_join
{
template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load()
{
   workload.load();
   load_state = LoadState(workload.last_customer_id, [this](int n, int s, int c, int ci, int cu) { load_1customer(n, s, c, ci, cu); });
   seq_load();
   load_state.advance_customers_to_hot_cities();
   // load geo view and join view
   auto nation_scanner_ptr = nation.getScanner();
   auto states_scanner_ptr = states.getScanner();
   auto county_scanner_ptr = county.getScanner();
   auto city_scanner_ptr = city.getScanner();
   auto customer_scanner_ptr = customer2.getScanner();
   BinaryMergeJoin<sort_key_t, ns_t, nation2_t, states_t> joiner_ns([nation_scanner = nation_scanner_ptr.get()]() { return nation_scanner->next(); },
                                                                    [states_scanner = states_scanner_ptr.get()]() { return states_scanner->next(); });
   BinaryMergeJoin<sort_key_t, nsc_t, ns_t, county_t> joiner_nsc([&joiner_ns]() { return joiner_ns.next(); },
                                                                 [county_scanner = county_scanner_ptr.get()]() { return county_scanner->next(); });
   BinaryMergeJoin<sort_key_t, nscci_t, nsc_t, city_t> joiner_nscci(
      [&joiner_nsc]() { return joiner_nsc.next(); },                    
      [city_scanner = city_scanner_ptr.get()]() { return city_scanner->next(); },                                                             
      [this](const nscci_t::Key& k, const nscci_t& v) { geo_view.insert(k, v); });
   BinaryMergeJoin<sort_key_t, view_t, nscci_t, customer2_t> final_joiner(
      [&joiner_nscci]() { return joiner_nscci.next(); },
      [customer_scanner = customer_scanner_ptr.get()]() { return customer_scanner->next(); },
      [this](const view_t::Key& k, const view_t& v) { join_view.insert(k, v); });
   final_joiner.run();

   // load cust_count_view
   customer_scanner_ptr->reset();
   customer_count_t::Key current_cuck{sort_key_t()};
   Integer current_count = 0;
   long long scanned_customers = 0;
   while (true) {
      auto kv = customer_scanner_ptr->next();
      if (!kv.has_value())
         break;
      scanned_customers++;
      auto [k, v] = *kv;
      sort_key_t sk = SKBuilder<sort_key_t>::create(k, v);
      customer_count_t::Key cuck{sk};
      if (current_cuck != cuck) {
         // new key
         if (current_count > 0) {
            customer_count_t cuc_v{current_count};
            cust_count_view.insert(current_cuck, cuc_v);
         }
         current_count = 0;
      }
      current_count++;
      current_cuck = cuck;
   }
   // insert the last key
   if (current_count > 0) {
      customer_count_t cuc_v{current_count};
      cust_count_view.insert(current_cuck, cuc_v);
   }
   std::cout << "Scanned " << scanned_customers << " customers to build customer_count_view." << std::endl;

   log_sizes();
   write_table_to_stream<AdapterType<nation2_t>, nation2_t>(nation);
   write_table_to_stream<AdapterType<states_t>, states_t>(states);
   write_table_to_stream<AdapterType<county_t>, county_t>(county);
   write_table_to_stream<AdapterType<city_t>, city_t>(city);
   write_table_to_stream<AdapterType<customer2_t>, customer2_t>(customer2);
   write_table_to_stream<AdapterType<customerh_t>, customerh_t>(workload.customer);
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::seq_load()
{
   for (int n = 1; n <= params.nation_count; n++) {
      // load_1nation(n);
      int state_cnt = params.get_state_cnt();
      nation2_t::Key nk{n};
      nation2_t nv = nation2_t::generateRandomRecord(state_cnt);
      nation.insert(nk, nv);
      merged.insert(nk, nv);
      if (!FLAGS_log_progress) {
         std::cout << "\rLoading nation " << n << " with " << state_cnt << " states...";
      }
      for (int s = 1; s <= state_cnt; s++) {
         if (FLAGS_log_progress)
            std::cout << "\rLoading nation " << n << "/" << params.nation_count << ", state " << s << "/" << state_cnt << "...";
         load_1state(n, s);
      }
   }
   std::cout << std::endl << "Loaded " << load_state.county_sum << " counties and " << load_state.city_sum << " cities." << std::endl;
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1state(int n, int s)
{
   int county_cnt = params.get_county_cnt();
   load_state.county_sum += county_cnt;
   auto sk = states_t::Key{n, s};
   auto sv = states_t::generateRandomRecord(county_cnt);
   states.insert(sk, sv);
   merged.insert(sk, sv);
   for (int c = 1; c <= county_cnt; c++) {
      load_1county(n, s, c);
   }
};

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1county(int n, int s, int c)
{
   int city_cnt = params.get_city_cnt();
   load_state.city_sum += city_cnt;
   auto ck = county_t::Key{n, s, c};
   auto cv = county_t::generateRandomRecord(city_cnt);
   county.insert(ck, cv);
   merged.insert(ck, cv);
   for (int ci = 1; ci <= city_cnt; ci++) {
      load_1city(n, s, c, ci);
   }
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1city(int n, int s, int c, int ci)
{
   auto cik = city_t::Key{n, s, c, ci};
   auto civ = city_t::generateRandomRecord();
   city.insert(cik, civ);
   merged.insert(cik, civ);
   int lottery = urand(1, 100);  // HARDCODED 1%
   if (lottery == 1) {
      load_state.hot_city_candidates.push_back(cik);
   }
   // insert customer2
   load_state.advance_customers_in_1city(params.get_customer_cnt(), n, s, c, ci);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::load_1customer(int n, int s, int c, int ci, int cu)
{
   customer2_t::Key cust_key{n, s, c, ci, cu};
   assert(s > 0);
   customer2_t cuv;
   workload.customer.lookup1(customerh_t::Key{cu}, [&](const customerh_t& v) { cuv = customer2_t{v}; });
   customer2.insert(cust_key, cuv);
   merged.insert(cust_key, cuv);
}

template <template <typename> class AdapterType,
          template <typename...> class MergedAdapterType,
          template <typename> class ScannerType,
          template <typename...> class MergedScannerType>
void GeoJoin<AdapterType, MergedAdapterType, ScannerType, MergedScannerType>::log_sizes()
{
   workload.log_sizes();
   double nation_size = nation.size();
   double states_size = states.size();
   double county_size = county.size();
   double city_size = city.size();
   double customer2_size = customer2.size();
   double indexes_size = nation_size + states_size + county_size + city_size + customer2_size;

   double join_view_size = join_view.size();
   // double city_count_per_county_size = city_count_per_county.size();
   double geo_view_size = geo_view.size();
   double cust_count_view_size = cust_count_view.size();
   double view_size = join_view_size + geo_view_size + cust_count_view_size;

   double merged_size = merged.size();

   std::map<std::string, double> sizes = {{"nation", nation_size},       {"states", states_size},       {"county", county_size},
                                          {"city", city_size},           {"customer2", customer2_size}, {"indexes", indexes_size},
                                          {"join_view", join_view_size}, {"geo_view", geo_view_size},   {"cust_count_view", cust_count_view_size},
                                          {"view", view_size},           {"merged", merged_size}};
   logger.log_sizes(sizes);
};
}  // namespace geo_join