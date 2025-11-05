#pragma once

#include <ostream> // For print()
#include "../shared/table_traits.hpp" // For ADD_KEY_TRAITS, ADD_RECORD_TRAITS
#include "../shared/Types.hpp"          // For Integer, Varchar, Numeric
#include "../shared/randutils.hpp"    // For randomastring, randomNumeric

// Use randutils functions for random record generation
using namespace randutils;

/**
 * @brief Defines table structures for the IMDb dataset (JOB benchmark).
 */
namespace imdb
{
// Define common ID types used throughout the IMDb schema
using tconst_t = Varchar<9>; // Alphanumeric title identifier (e.g., "tt0111161")
using nconst_t = Varchar<9>; // Alphanumeric name identifier (e.g., "nm0000136")

/// @brief from title.akas.tsv.gz
struct title_akas_t {
   static constexpr int id = 100;
   title_akas_t() = default;
   title_akas_t(const Varchar<255>& title, const Varchar<8>& region, const Varchar<8>& language,
                const Varchar<128>& types, const Varchar<128>& attributes, Integer isOriginalTitle)
       : title(title),
         region(region),
         language(language),
         types(types),
         attributes(attributes),
         isOriginalTitle(isOriginalTitle)
   {
   }

   struct Key {
      static constexpr int id = 100;
      tconst_t titleId;
      Integer ordering;
      ADD_KEY_TRAITS(&Key::titleId, &Key::ordering)

      Key() = default;
      Key(tconst_t titleId, Integer ordering) : titleId(titleId), ordering(ordering) {}

      Key get_pk() const { return *this; }
   };

   Varchar<255> title;
   Varchar<8> region;
   Varchar<8> language;
   Varchar<128> types;      // "array" stored as comma-separated string
   Varchar<128> attributes; // "array" stored as comma-separated string
   Integer isOriginalTitle;  // 0 or 1

   ADD_RECORD_TRAITS(title_akas_t)

   void print(std::ostream& os) const
   {
      os << title << "," << region << "," << language << "," << types << "," << attributes << "," << isOriginalTitle;
   }

   static title_akas_t generateRandomRecord()
   {
      return title_akas_t{randomastring<255>(10, 100), randomastring<8>(2, 4), randomastring<8>(2, 5),
                          randomastring<128>(0, 50), randomastring<128>(0, 50), urand(0, 1)};
   }
};

/// @brief from title.basics.tsv.gz
struct title_basics_t {
   static constexpr int id = 101;
   title_basics_t() = default;
   title_basics_t(const Varchar<16>& titleType, const Varchar<255>& primaryTitle,
                  const Varchar<255>& originalTitle, Integer isAdult, Integer startYear, Integer endYear,
                  Integer runtimeMinutes, const Varchar<64>& genres)
       : titleType(titleType),
         primaryTitle(primaryTitle),
         originalTitle(originalTitle),
         isAdult(isAdult),
         startYear(startYear),
         endYear(endYear),
         runtimeMinutes(runtimeMinutes),
         genres(genres)
   {
   }

   struct Key {
      static constexpr int id = 101;
      tconst_t tconst;
      ADD_KEY_TRAITS(&Key::tconst)

      Key() = default;
      Key(tconst_t tconst) : tconst(tconst) {}

      Key get_pk() const { return *this; }
   };

   Varchar<16> titleType;
   Varchar<255> primaryTitle;
   Varchar<255> originalTitle;
   Integer isAdult;
   Integer startYear;
   Integer endYear;        // '\N' represented by default-constructed Integer
   Integer runtimeMinutes;
   Varchar<64> genres;     // "string array" stored as comma-separated string

   ADD_RECORD_TRAITS(title_basics_t)

   void print(std::ostream& os) const
   {
      os << titleType << "," << primaryTitle << "," << originalTitle << "," << isAdult << "," << startYear << ","
         << endYear << "," << runtimeMinutes << "," << genres;
   }

   static title_basics_t generateRandomRecord()
   {
      // endYear is often null, represent with default-constructed Integer()
      return title_basics_t{randomastring<16>(5, 10),    randomastring<255>(10, 100),
                            randomastring<255>(10, 100), urand(0, 1),
                            urand(1900, 2025), Integer{}, // Default Integer for '\N'
                            urand(10, 240),    randomastring<64>(5, 30)};
   }
};

/// @brief from title.crew.tsv.gz
struct title_crew_t {
   static constexpr int id = 102;
   title_crew_t() = default;
   title_crew_t(const Varchar<255>& directors, const Varchar<255>& writers) : directors(directors), writers(writers) {}

   struct Key {
      static constexpr int id = 102;
      tconst_t tconst;
      ADD_KEY_TRAITS(&Key::tconst)

      Key() = default;
      Key(tconst_t tconst) : tconst(tconst) {}

      Key get_pk() const { return *this; }
   };

   Varchar<255> directors; // "array of nconsts" stored as comma-separated string
   Varchar<255> writers;   // "array of nconsts" stored as comma-separated string

   ADD_RECORD_TRAITS(title_crew_t)

   void print(std::ostream& os) const { os << directors << "," << writers; }

   static title_crew_t generateRandomRecord()
   {
      // Assuming comma-separated nconsts
      return title_crew_t{randomastring<255>(9, 100), randomastring<255>(9, 100)};
   }
};

/// @brief from title.episode.tsv.gz
struct title_episode_t {
   static constexpr int id = 103;
   title_episode_t() = default;
   title_episode_t(tconst_t parentTconst, Integer seasonNumber, Integer episodeNumber)
       : parentTconst(parentTconst), seasonNumber(seasonNumber), episodeNumber(episodeNumber)
   {
   }

   struct Key {
      static constexpr int id = 103;
      tconst_t tconst; // Episode's own ID
      ADD_KEY_TRAITS(&Key::tconst)

      Key() = default;
      Key(tconst_t tconst) : tconst(tconst) {}

      Key get_pk() const { return *this; }
   };

   tconst_t parentTconst; // ID of the parent TV Series
   Integer seasonNumber;
   Integer episodeNumber;

   ADD_RECORD_TRAITS(title_episode_t)

   void print(std::ostream& os) const { os << parentTconst << "," << seasonNumber << "," << episodeNumber; }

   static title_episode_t generateRandomRecord()
   {
      return title_episode_t{randomastring<9>(9, 9), // parentTconst
                             urand(1, 20), urand(1, 50)};
   }
};

/// @brief from title.principals.tsv.gz
struct title_principals_t {
   static constexpr int id = 104;
   title_principals_t() = default;
   title_principals_t(nconst_t nconst, const Varchar<32>& category, const Varchar<128>& job,
                      const Varchar<255>& characters)
       : nconst(nconst), category(category), job(job), characters(characters)
   {
   }

   struct Key {
      static constexpr int id = 104;
      tconst_t tconst;
      Integer ordering;
      ADD_KEY_TRAITS(&Key::tconst, &Key::ordering)

      Key() = default;
      Key(tconst_t tconst, Integer ordering) : tconst(tconst), ordering(ordering) {}

      Key get_pk() const { return *this; }
   };

   nconst_t nconst;
   Varchar<32> category;
   Varchar<128> job;
   Varchar<255> characters;

   ADD_RECORD_TRAITS(title_principals_t)

   void print(std::ostream& os) const { os << nconst << "," << category << "," << job << "," << characters; }

   static title_principals_t generateRandomRecord()
   {
      return title_principals_t{randomastring<9>(9, 9), randomastring<32>(5, 20), randomastring<128>(0, 50),
                                randomastring<255>(0, 100)};
   }
};

/// @brief from title.ratings.tsv.gz
struct title_ratings_t {
   static constexpr int id = 105;
   title_ratings_t() = default;
   title_ratings_t(Numeric averageRating, Integer numVotes) : averageRating(averageRating), numVotes(numVotes) {}

   struct Key {
      static constexpr int id = 105;
      tconst_t tconst;
      ADD_KEY_TRAITS(&Key::tconst)

      Key() = default;
      Key(tconst_t tconst) : tconst(tconst) {}

      Key get_pk() const { return *this; }
   };

   Numeric averageRating;
   Integer numVotes;

   ADD_RECORD_TRAITS(title_ratings_t)

   void print(std::ostream& os) const { os << averageRating << "," << numVotes; }

   static title_ratings_t generateRandomRecord()
   {
      return title_ratings_t{randomNumeric(1.0000, 10.0000), urand(1, 1000000)};
   }
};

/// @brief from name.basics.tsv.gz
struct name_basics_t {
   static constexpr int id = 106;
   name_basics_t() = default;
   name_basics_t(const Varchar<128>& primaryName, Integer birthYear, Integer deathYear,
                 const Varchar<128>& primaryProfession, const Varchar<255>& knownForTitles)
       : primaryName(primaryName),
         birthYear(birthYear),
         deathYear(deathYear),
         primaryProfession(primaryProfession),
         knownForTitles(knownForTitles)
   {
   }

   struct Key {
      static constexpr int id = 106;
      nconst_t nconst;
      ADD_KEY_TRAITS(&Key::nconst)

      Key() = default;
      Key(nconst_t nconst) : nconst(nconst) {}

      Key get_pk() const { return *this; }
   };

   Varchar<128> primaryName;
   Integer birthYear;
   Integer deathYear; // '\N' for living or unknown
   Varchar<128> primaryProfession;
   Varchar<255> knownForTitles;

   ADD_RECORD_TRAITS(name_basics_t)

   void print(std::ostream& os) const
   {
      os << primaryName << "," << birthYear << "," << deathYear << "," << primaryProfession << "," << knownForTitles;
   }

   static name_basics_t generateRandomRecord()
   {
      // deathYear is often null, represent with default-constructed Integer
      return name_basics_t{randomastring<128>(10, 50), urand(1900, 2010),
                           Integer{}, // Default Integer for '\N'
                           randomastring<128>(10, 50), randomastring<255>(10, 100)};
   }
};

} // namespace imdb

// Make tables available in the global scope for convenience, following geo/view.hpp
using namespace imdb;