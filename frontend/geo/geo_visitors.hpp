#pragma once
#include <set>
#include "geo_walk.tpp"
#include "views.hpp"

namespace geo_join
{

// GeoJoinCountVisitor — replaces PremergedJoin-driven cartesian count in
// range_query_by_merged. For geo's 1:N hierarchy, the cartesian cardinality
// equals the number of customer records under the prefix (exactly one
// matching nation/state/county/city per child), so counting customers is
// equivalent to counting full join tuples.
struct GeoJoinCountVisitor {
   long produced = 0;

   GeoAction on_nation  (const nation2_t::Key&,  const nation2_t&)   { return GeoAction::Continue; }
   GeoAction on_state   (const states_t::Key&,   const states_t&)    { return GeoAction::Continue; }
   GeoAction on_county  (const county_t::Key&,   const county_t&)    { return GeoAction::Continue; }
   GeoAction on_city    (const city_t::Key&,     const city_t&)      { return GeoAction::Continue; }
   GeoAction on_customer(const customer2_t::Key&, const customer2_t&) { produced++; return GeoAction::Continue; }
};

// GeoMixedSumVisitor — replaces MergedCounter/MergedScannerCounter for the
// `distinct=false` branch of range_mixed_query_by_merged. The legacy path
// synthesizes a customer_count_t per city and sums those; for geo this is
// algebraically identical to counting customers directly.
struct GeoMixedSumVisitor {
   long customer_sum = 0;

   GeoAction on_nation  (const nation2_t::Key&,  const nation2_t&)   { return GeoAction::Continue; }
   GeoAction on_state   (const states_t::Key&,   const states_t&)    { return GeoAction::Continue; }
   GeoAction on_county  (const county_t::Key&,   const county_t&)    { return GeoAction::Continue; }
   GeoAction on_city    (const city_t::Key&,     const city_t&)      { return GeoAction::Continue; }
   GeoAction on_customer(const customer2_t::Key&, const customer2_t&) { customer_sum++; return GeoAction::Continue; }
};

// GeoMixedDistinctVisitor — `distinct=true` branch: count distinct mktsegment
// values within each city, sum those per-city counts. Legacy path uses a
// std::vector + std::find inside MergedScannerCounter; std::set keeps the
// per-city memory bounded to mktsegment cardinality (5 in TPC-H), which is
// strictly smaller than per-customer storage.
struct GeoMixedDistinctVisitor {
   long distinct_sum = 0;
   std::set<Varchar<10>> seen_in_city;

   GeoAction on_nation  (const nation2_t::Key&,  const nation2_t&)   { return GeoAction::Continue; }
   GeoAction on_state   (const states_t::Key&,   const states_t&)    { return GeoAction::Continue; }
   GeoAction on_county  (const county_t::Key&,   const county_t&)    { return GeoAction::Continue; }
   GeoAction on_city    (const city_t::Key&,     const city_t&)      { seen_in_city.clear(); return GeoAction::Continue; }
   GeoAction on_customer(const customer2_t::Key&, const customer2_t& c)
   {
      seen_in_city.insert(c.c_mktsegment);
      return GeoAction::Continue;
   }
   void on_group_end(const sort_key_t&)
   {
      distinct_sum += static_cast<long>(seen_in_city.size());
      seen_in_city.clear();
   }
};

}  // namespace geo_join
