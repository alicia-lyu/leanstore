#pragma once

#include "workload.hpp"
#include "load.hpp"
#include <filesystem>
#include <map>

namespace imdb
{
// Use helper functions
using namespace imdb::loader_utils;

/**
 * @brief Main load function. Calls the specific loader for each table.
 */
template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load()
{
    std::cout << "Starting JOB data loading from directory: " << FLAGS_data_dir << std::endl;
   load_title_akas();
   load_title_basics();
   load_title_crew();
   load_title_episode();
   load_title_principals();
   load_title_ratings();
   load_name_basics();
   std::cout << "--------------------------------" << std::endl;
   log_sizes();
   std::cout << "--------------------------------" << std::endl;
}

/**
 * @brief Logs the size of all tables.
 */
template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::log_sizes()
{
   std::map<std::string, double> sizes = {
       {"title_akas", title_akas.size()},
       {"title_basics", title_basics.size()},
       {"title_crew", title_crew.size()},
       {"title_episode", title_episode.size()},
       {"title_principals", title_principals.size()},
       {"title_ratings", title_ratings.size()},
       {"name_basics", name_basics.size()},
   };
   logger.log_sizes(sizes);
}

// -------------------------------------------------------------------------------------
// Per-Table Loader Implementations
// -------------------------------------------------------------------------------------

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_akas()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.akas.tsv").string();
   load_table_from_file<AdapterType<title_akas_t>, title_akas_t, title_akas_t::Key>(
       title_akas, path, [](const std::vector<std::string>& cols) {
          // Schema: titleId, ordering, title, region, language, types, attributes, isOriginalTitle
          assert(cols.size() >= 8);
          title_akas_t::Key key{string_to_tconst(cols[0]), string_to_integer(cols[1])};
          title_akas_t rec{string_to_varchar<255>(cols[2]), string_to_varchar<8>(cols[3]),  string_to_varchar<8>(cols[4]),
                           string_to_varchar<128>(cols[5]), string_to_varchar<128>(cols[6]), string_to_integer(cols[7])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_basics()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.basics.tsv").string();
   load_table_from_file<AdapterType<title_basics_t>, title_basics_t, title_basics_t::Key>(
       title_basics, path, [](const std::vector<std::string>& cols) {
          // Schema: tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
          assert(cols.size() >= 9);
          title_basics_t::Key key{string_to_tconst(cols[0])};
          title_basics_t rec{string_to_varchar<16>(cols[1]), string_to_varchar<255>(cols[2]), string_to_varchar<255>(cols[3]),
                             string_to_integer(cols[4]),     string_to_integer(cols[5]),     string_to_integer(cols[6]),
                             string_to_integer(cols[7]),     string_to_varchar<64>(cols[8])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_crew()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.crew.tsv").string();
   load_table_from_file<AdapterType<title_crew_t>, title_crew_t, title_crew_t::Key>(
       title_crew, path, [](const std::vector<std::string>& cols) {
          // Schema: tconst, directors, writers
          assert(cols.size() >= 3);
          title_crew_t::Key key{string_to_tconst(cols[0])};
          title_crew_t rec{string_to_varchar<255>(cols[1]), string_to_varchar<255>(cols[2])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_episode()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.episode.tsv").string();
   load_table_from_file<AdapterType<title_episode_t>, title_episode_t, title_episode_t::Key>(
       title_episode, path, [](const std::vector<std::string>& cols) {
          // Schema: tconst, parentTconst, seasonNumber, episodeNumber
          assert(cols.size() >= 4);
          title_episode_t::Key key{string_to_tconst(cols[0])};
          title_episode_t rec{string_to_tconst(cols[1]), string_to_integer(cols[2]), string_to_integer(cols[3])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_principals()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.principals.tsv").string();
   load_table_from_file<AdapterType<title_principals_t>, title_principals_t, title_principals_t::Key>(
       title_principals, path, [](const std::vector<std::string>& cols) {
          // Schema: tconst, ordering, nconst, category, job, characters
          assert(cols.size() >= 6);
          title_principals_t::Key key{string_to_tconst(cols[0]), string_to_integer(cols[1])};
          title_principals_t rec{string_to_nconst(cols[2]), string_to_varchar<32>(cols[3]), string_to_varchar<128>(cols[4]),
                                 string_to_varchar<255>(cols[5])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_title_ratings()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "title.ratings.tsv").string();
   load_table_from_file<AdapterType<title_ratings_t>, title_ratings_t, title_ratings_t::Key>(
       title_ratings, path, [](const std::vector<std::string>& cols) {
          // Schema: tconst, averageRating, numVotes
          assert(cols.size() >= 3);
          title_ratings_t::Key key{string_to_tconst(cols[0])};
          title_ratings_t rec{string_to_numeric(cols[1]), string_to_integer(cols[2])};
          return std::make_pair(key, rec);
       });
}

template <template <typename> class AdapterType>
void JOBWorkload<AdapterType>::load_name_basics()
{
   std::string path = (std::filesystem::path(FLAGS_data_dir) / "name.basics.tsv").string();
   load_table_from_file<AdapterType<name_basics_t>, name_basics_t, name_basics_t::Key>(
       name_basics, path, [](const std::vector<std::string>& cols) {
          // Schema: nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
          assert(cols.size() >= 6);
          name_basics_t::Key key{string_to_nconst(cols[0])};
          name_basics_t rec{string_to_varchar<128>(cols[1]), string_to_integer(cols[2]),     string_to_integer(cols[3]),
                            string_to_varchar<128>(cols[4]), string_to_varchar<255>(cols[5])};
          return std::make_pair(key, rec);
       });
}

} // namespace imdb