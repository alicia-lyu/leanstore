#pragma once
#include <functional>
#include <iostream>
#include <string>
#include <vector>

namespace geo_join
{
template <typename W>
struct ExeParams {
   W& workload;
   ExeParams(W& w) : workload(w) {
        std::cout << "******* Measuring elapsed times for: ";
        for (const auto& name : elapsed_names) {
            std::cout << name << ", ";
        }
        std::cout << "*******" << std::endl;
        std::cout << "******* Measuring throughput for: ";
        for (const auto& prefix : tput_prefixes) {
            std::cout << prefix << ", ";
        }
        std::cout << "*******" << std::endl;
   }
   const std::vector<std::string> elapsed_names = {
       "join",
       "mixed",
    };
   const std::vector<std::string> tput_prefixes = {
       "join-ns",
       "join-nsc",
       "join-nscci",
       "mixed-point",
       "maintain",
    };
   const std::vector<std::function<void()>> elapsed_cbs_base = {
       std::bind(&W::query_by_base, &workload),
       std::bind(&W::mixed_query_by_base, &workload)};
   const std::vector<std::function<void()>> tput_cbs_base = {
       std::bind(&W::ns_base, &workload),
       std::bind(&W::nsc_base, &workload),
       std::bind(&W::nscci_by_base, &workload),
       std::bind(&W::point_mixed_query_by_base, &workload),
       std::bind(&W::maintain_base, &workload),
    };
   const std::vector<std::function<void()>> elapsed_cbs_view = {
       std::bind(&W::query_by_view, &workload),
       std::bind(&W::mixed_query_by_view, &workload)};
   const std::vector<std::function<void()>> tput_cbs_view = {
       std::bind(&W::ns_view, &workload),
       std::bind(&W::nsc_view, &workload),
       std::bind(&W::nscci_by_view, &workload),
       std::bind(&W::point_mixed_query_by_view, &workload),
        std::bind(&W::maintain_view, &workload),
    };
   const std::vector<std::function<void()>> elapsed_cbs_merged = {
       std::bind(&W::query_by_merged, &workload),
       std::bind(&W::mixed_query_by_merged, &workload)};
   const std::vector<std::function<void()>> tput_cbs_merged = {
       std::bind(&W::ns_merged, &workload),
       std::bind(&W::nsc_merged, &workload),
       std::bind(&W::nscci_by_merged, &workload),
       std::bind(&W::point_mixed_query_by_merged, &workload),
       std::bind(&W::maintain_merged, &workload),
    };
   const std::vector<std::function<void()>> elapsed_cbs_2merged = {
       std::bind(&W::query_by_2merged, &workload),
       std::bind(&W::mixed_query_by_2merged, &workload)
    };
   const std::vector<std::function<void()>> tput_cbs_2merged = {
       std::bind(&W::ns_by_2merged, &workload),
       std::bind(&W::nsc_by_2merged, &workload),
       std::bind(&W::nscci_by_2merged, &workload),
       std::bind(&W::point_mixed_query_by_2merged, &workload),
       std::bind(&W::maintain_2merged, &workload),
    };
};
}  // namespace geo_join