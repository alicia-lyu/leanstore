#pragma once

#include "../shared/logger/logger.hpp"
#include "load.hpp"
#include "tables.hpp"

namespace imdb
{
/**
 * @brief Main workload class for the JOB (IMDb) benchmark.
 *
 * This class holds adapters for all base tables and orchestrates the
 * loading of data from TSV files. It does not handle view maintenance.
 *
 * @tparam AdapterType The storage adapter template (e.g., BwTreeAdapter).
 */
template <template <typename> class AdapterType>
class JOBWorkload
{
  public:
   // --- Table Adapters ---
   AdapterType<title_akas_t>& title_akas;
   AdapterType<title_basics_t>& title_basics;
   AdapterType<title_crew_t>& title_crew;
   AdapterType<title_episode_t>& title_episode;
   AdapterType<title_principals_t>& title_principals;
   AdapterType<title_ratings_t>& title_ratings;
   AdapterType<name_basics_t>& name_basics;

   Logger& logger;

   /**
    * @brief Constructs the JOBWorkload.
    * @param ta Adapter for title_akas
    * @param tb Adapter for title_basics
    * @param tc Adapter for title_crew
    * @param te Adapter for title_episode
    * @param tp Adapter for title_principals
    * @param tr Adapter for title_ratings
    * @param nb Adapter for name_basics
    * @param logger A reference to the logger.
    */
   JOBWorkload(AdapterType<title_akas_t>& ta,
               AdapterType<title_basics_t>& tb,
               AdapterType<title_crew_t>& tc,
               AdapterType<title_episode_t>& te,
               AdapterType<title_principals_t>& tp,
               AdapterType<title_ratings_t>& tr,
               AdapterType<name_basics_t>& nb,
               Logger& logger)
       : title_akas(ta),
         title_basics(tb),
         title_crew(tc),
         title_episode(te),
         title_principals(tp),
         title_ratings(tr),
         name_basics(nb),
         logger(logger)
   {
      std::cout << "JOBWorkload initialized. Data directory: " << FLAGS_data_dir << std::endl;
   }

   ~JOBWorkload() = default;

   /**
    * @brief Loads all base tables from their respective .tsv files.
    */
   void load();

   /**
    * @brief Logs the sizes (in bytes or records) of all loaded tables.
    */
   void log_sizes();

  private:
   // --- Private loader functions for each table ---
   void load_title_akas();
   void load_title_basics();
   void load_title_crew();
   void load_title_episode();
   void load_title_principals();
   void load_title_ratings();
   void load_name_basics();
};

} // namespace imdb

// Include the implementation file (Tpp)
#include "load.tpp" // IWYU pragma: keep