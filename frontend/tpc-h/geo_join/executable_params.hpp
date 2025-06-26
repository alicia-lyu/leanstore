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
       "join-ns",
       "join-nsc",
       "join-nscci",
       "maintain",
       // "group-point",
       "mixed-point"};
   const std::vector<std::function<void()>> elapsed_cbs_base = {
       std::bind(&W::query_by_base, &workload),
    //    std::bind(&W::agg_by_base, &workload),
       std::bind(&W::mixed_query_by_base, &workload)};
   const std::vector<std::function<void()>> tput_cbs_base = {
       std::bind(&W::ns_base, &workload),
       std::bind(&W::nsc_base, &workload),
       std::bind(&W::nscci_by_base, &workload),
       std::bind(&W::maintain_base, &workload),
       // std::bind(&W::point_agg_by_base, &workload),
       std::bind(&W::point_mixed_query_by_base, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_view = {
       std::bind(&W::query_by_view, &workload),
    //    std::bind(&W::agg_in_view, &workload),
       std::bind(&W::mixed_query_by_view, &workload)};
   const std::vector<std::function<void()>> tput_cbs_view = {
       std::bind(&W::ns_view, &workload),
       std::bind(&W::nsc_view, &workload),
       std::bind(&W::nscci_by_view, &workload),
       std::bind(&W::maintain_view, &workload),
       // std::bind(&W::point_agg_by_view, &workload),
       std::bind(&W::point_mixed_query_by_view, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_merged = {
       std::bind(&W::query_by_merged, &workload),
    //    std::bind(&W::agg_by_merged, &workload),
       std::bind(&W::mixed_query_by_merged, &workload)};
   const std::vector<std::function<void()>> tput_cbs_merged = {
       std::bind(&W::ns_merged, &workload),
       std::bind(&W::nsc_merged, &workload),
       std::bind(&W::nscci_by_merged, &workload),
       std::bind(&W::maintain_merged, &workload),
       // std::bind(&W::point_agg_by_merged, &workload),
       std::bind(&W::point_mixed_query_by_merged, &workload)};
   const std::vector<std::function<void()>> elapsed_cbs_2merged = {
       std::bind(&W::query_by_2merged, &workload),
       // std::bind(&W::agg_by_2merged, &workload),
       std::bind(&W::mixed_query_by_2merged, &workload)};
   const std::vector<std::function<void()>> tput_cbs_2merged = {
       std::bind(&W::ns_by_2merged, &workload),
       std::bind(&W::nsc_by_2merged, &workload),
       std::bind(&W::nscci_by_2merged, &workload),
       std::bind(&W::maintain_2merged, &workload),
       // std::bind(&W::point_agg_by_2merged, &workload),
       std::bind(&W::point_mixed_query_by_2merged, &workload)};
};
}  // namespace geo_join