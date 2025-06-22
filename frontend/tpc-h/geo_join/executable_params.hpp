#pragma once
#include <functional>
#include <string>
#include <vector>

namespace geo_join
{
template <typename W>
struct ExeParams {
   W& workload;
   ExeParams(W& w) : workload(w) {}
   const std::vector<std::string> tput_prefixes = {
       // "join-ns",
       // "join-nsc",
       // "join-nscci",
       // "maintain",
       // "group-point",
       "mixed-point"};
   const std::vector<std::function<void()>> elapsed_cbs_base = {
       // std::bind(&GJ::query_by_base, &tpchGeoJoin),
       // std::bind(&GJ::agg_by_base, &tpchGeoJoin),
       std::bind(&W::mixed_query_by_base, &workload)};
   const std::vector<std::function<void()>> tput_cbs_base = {
       // std::bind(&GJ::ns_base, &tpchGeoJoin),
       // std::bind(&GJ::nsc_base, &tpchGeoJoin),
       // std::bind(&GJ::nscci_by_base, &tpchGeoJoin),
       // std::bind(&GJ::maintain_base, &tpchGeoJoin),
       // std::bind(&GJ::point_agg_by_base, &tpchGeoJoin),
       std::bind(&W::point_mixed_query_by_base, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_view = {
       // std::bind(&GJ::query_by_view, &tpchGeoJoin),
       // std::bind(&GJ::agg_in_view, &tpchGeoJoin),
       std::bind(&W::mixed_query_by_view, &workload)};
   const std::vector<std::function<void()>> tput_cbs_view = {
       // std::bind(&GJ::ns_view, &tpchGeoJoin),
       // std::bind(&GJ::nsc_view, &tpchGeoJoin),
       // std::bind(&GJ::nscci_by_view, &tpchGeoJoin),
       // std::bind(&GJ::maintain_view, &tpchGeoJoin),
       // std::bind(&GJ::point_agg_by_view, &tpchGeoJoin),
       std::bind(&W::point_mixed_query_by_view, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_merged = {
       // std::bind(&GJ::query_by_merged, &tpchGeoJoin),
       // std::bind(&GJ::agg_by_merged, &tpchGeoJoin),
       std::bind(&W::mixed_query_by_merged, &workload)};
   const std::vector<std::function<void()>> tput_cbs_merged = {
       // std::bind(&GJ::ns_merged, &tpchGeoJoin),
       // std::bind(&GJ::nsc_merged, &tpchGeoJoin),
       // std::bind(&GJ::nscci_by_merged, &tpchGeoJoin),
       // std::bind(&GJ::maintain_merged, &tpchGeoJoin),
       // std::bind(&GJ::point_agg_by_merged, &tpchGeoJoin),
       std::bind(&W::point_mixed_query_by_merged, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_2merged = {
       // std::bind(&GJ::query_by_2merged, &tpchGeoJoin),
       // std::bind(&GJ::agg_by_2merged, &tpchGeoJoin),
       std::bind(&W::mixed_query_by_2merged, &workload)};
   const std::vector<std::function<void()>> tput_cbs_2merged = {
       // std::bind(&GJ::ns_by_2merged, &tpchGeoJoin),
       // std::bind(&GJ::nsc_by_2merged, &tpchGeoJoin),
       // std::bind(&GJ::nscci_by_2merged, &tpchGeoJoin),
       // std::bind(&GJ::maintain_2merged, &tpchGeoJoin),
       // std::bind(&GJ::point_agg_by_2merged, &tpchGeoJoin),
       std::bind(&W::point_mixed_query_by_2merged, &workload)};
};
}  // namespace geo_join