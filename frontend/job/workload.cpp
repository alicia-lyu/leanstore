#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <iostream>
#include <tuple> // For std::tie

// Include the headers for the code we are testing
#include "workload.hpp"
#include "tables.hpp"
#include "load.hpp"
/**
 * @brief A mock in-memory adapter that satisfies the AdapterType template.
 * Uses std::map to store data.
 */
template <typename Record>
class MockAdapter
{
  public:
   using Key = typename Record::Key;

   /**
    * @brief Provides operator< for complex keys to be used in std::map.
    */
   struct KeyCompare {
      bool operator()(const Key& a, const Key& b) const
      {
         // Differentiate key structures based on record type
         if constexpr (std::is_same_v<Record, title_akas_t>) {
            // key: { titleId, ordering }
            return std::tie(a.titleId, a.ordering) < std::tie(b.titleId, b.ordering);
         } else if constexpr (std::is_same_v<Record, title_principals_t>) {
            // key: { tconst, ordering }
            return std::tie(a.tconst, a.ordering) < std::tie(b.tconst, b.ordering);
         } else if constexpr (std::is_same_v<Record, name_basics_t>) {
            // key: { nconst }
            return a.nconst < b.nconst;
         } else {
            // All other tables use a single `tconst` as the key
            return a.tconst < b.tconst;
         }
      }
   };

   std::map<Key, Record, KeyCompare> data;
   std::string name; // The loader_utils uses table.name

   MockAdapter(std::string n) : name(std::move(n)) {}

   void insert(Key key, Record record) { data.emplace(std::move(key), std::move(record)); }

   size_t size() const { return data.size(); }

   /**
    * @brief Mock scan for log_sizes() (which calls write_table_to_stream in geo/load.tpp, good to have)
    */
   template <typename ScanContFunc, typename ScanEndFunc>
   void scan(Key /* start_key */, ScanContFunc cont_op, ScanEndFunc end_op)
   {
      for (const auto& pair : data) {
         if (!cont_op(pair.first, pair.second)) {
            break; // Stop if callback returns false
         }
      }
      end_op();
   }

   /**
    * @brief Mock lookup for test verification.
    */
   bool lookup1(const Key& key, std::function<void(const Record&)> op)
   {
      auto it = data.find(key);
      if (it != data.end()) {
         op(it->second); // Apply operation
         return true;
      }
      return false;
   }
};

/**
 * @brief Test Fixture for JOBWorkload loading tests.
 *
 * This fixture creates a temporary directory and populates it with
 * mock .tsv files before each test. It tears down the directory after.
 */
class JobLoadTest : public ::testing::Test
{
  protected:
   std::string temp_dir_path;

   // Define mock adapters for all tables
   MockAdapter<title_akas_t> title_akas{"title_akas"};
   MockAdapter<title_basics_t> title_basics{"title_basics"};
   MockAdapter<title_crew_t> title_crew{"title_crew"};
   MockAdapter<title_episode_t> title_episode{"title_episode"};
   MockAdapter<title_principals_t> title_principals{"title_principals"};
   MockAdapter<title_ratings_t> title_ratings{"title_ratings"};
   MockAdapter<name_basics_t> name_basics{"name_basics"};

   // The class under test
   std::unique_ptr<imdb::JOBWorkload<MockAdapter>> workload;

   void SetUp() override
   {
      // 1. Create a unique temporary directory for our mock files
      temp_dir_path = (std::filesystem::temp_directory_path() / "job_load_test_").string() +
                      std::to_string(std::hash<std::string>{}(std::to_string(rand())));
      std::filesystem::create_directory(temp_dir_path);

      // 2. Point FLAGS_data_dir to our new temporary directory
      FLAGS_data_dir = temp_dir_path;

      // 3. Create mock data files in the temporary directory
      create_mock_files();

      // 4. Initialize the workload to be tested
      workload = std::make_unique<imdb::JOBWorkload<MockAdapter>>(
          title_akas, title_basics, title_crew, title_episode, title_principals, title_ratings, name_basics);
   }

   void TearDown() override
   {
      // Clean up the temporary directory
      std::filesystem::remove_all(temp_dir_path);
   }

   /**
    * @brief Creates small, sample .tsv files in the temp directory.
    */
   void create_mock_files()
   {
      // --- title.basics.tsv --- (2 records)
      std::ofstream tb_file(temp_dir_path + "/title.basics.tsv");
      tb_file << "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres\n";
      tb_file << "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short\n";
      tb_file << "tt0000002\tshort\tLe Clown\tLe Clown\t0\t1892\t\\N\t5\tAnimation,Short\n";
      tb_file.close();

      // --- name.basics.tsv --- (1 record)
      std::ofstream nb_file(temp_dir_path + "/name.basics.tsv");
      nb_file << "nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles\n";
      nb_file << "nm0000001\tFred Astaire\t1899\t1987\tactor,dancer\ttt0000001,tt0000002\n";
      nb_file.close();

      // --- title.akas.tsv --- (2 records)
      std::ofstream ta_file(temp_dir_path + "/title.akas.tsv");
      ta_file << "titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle\n";
      ta_file << "tt0000001\t1\tCarmencita (Spanish)\tES\tes\t\\N\t\\N\t0\n";
      ta_file << "tt0000001\t2\tCarmencita\tUS\t\\N\toriginal\t\\N\t1\n";
      ta_file.close();

      // --- title.ratings.tsv --- (1 record)
      std::ofstream tr_file(temp_dir_path + "/title.ratings.tsv");
      tr_file << "tconst\taverageRating\tnumVotes\n";
      tr_file << "tt0000001\t5.7\t1976\n";
      tr_file.close();

      // --- Create other files as empty (with headers) to prevent "file not found" errors ---
      std::ofstream(temp_dir_path + "/title.crew.tsv") << "tconst\tdirectors\twriters\n";
      std::ofstream(temp_dir_path + "/title.episode.tsv") << "tconst\tparentTconst\tseasonNumber\tepisodeNumber\n";
      std::ofstream(temp_dir_path + "/title.principals.tsv") << "tconst\tordering\tnconst\tcategory\tjob\tcharacters\n";
   }
};

// --- The Test Case ---

TEST_F(JobLoadTest, LoadTablesFromTSV)
{
   // 1. Act: Call the function we want to test
   ASSERT_NO_THROW(workload->load());

   // 2. Assert: Check if the mock adapters contain the correct data

   // --- Check record counts ---
   ASSERT_EQ(title_basics.size(), 2);
   ASSERT_EQ(name_basics.size(), 1);
   ASSERT_EQ(title_akas.size(), 2);
   ASSERT_EQ(title_ratings.size(), 1);
   ASSERT_EQ(title_crew.size(), 0);       // Empty file
   ASSERT_EQ(title_episode.size(), 0);    // Empty file
   ASSERT_EQ(title_principals.size(), 0); // Empty file

   // --- Check specific content from title.basics ---
   title_basics_t::Key tb_key1{imdb::string_to_tconst("tt0000001")};
   bool found_tb1 = title_basics.lookup1(tb_key1, [](const title_basics_t& rec) {
      ASSERT_EQ(rec.primaryTitle.toString(), "Carmencita");
      ASSERT_EQ(rec.startYear, 1894);
      ASSERT_EQ(rec.runtimeMinutes, 1);
      ASSERT_EQ(rec.genres.toString(), "Documentary,Short");
   });
   ASSERT_TRUE(found_tb1);

   // --- Check specific content from name.basics ---
   name_basics_t::Key nb_key1{imdb::string_to_nconst("nm0000001")};
   bool found_nb1 = name_basics.lookup1(nb_key1, [](const name_basics_t& rec) {
      ASSERT_EQ(rec.primaryName.toString(), "Fred Astaire");
      ASSERT_EQ(rec.birthYear, 1899);
      ASSERT_EQ(rec.deathYear, 1987);
      ASSERT_EQ(rec.primaryProfession.toString(), "actor,dancer");
   });
   ASSERT_TRUE(found_nb1);

   // --- Check specific content from title.akas (composite key) ---
   title_akas_t::Key ta_key1{imdb::string_to_tconst("tt0000001"), 1};
   bool found_ta1 = title_akas.lookup1(ta_key1, [](const title_akas_t& rec) {
      ASSERT_EQ(rec.title.toString(), "Carmencita (Spanish)");
      ASSERT_EQ(rec.region.toString(), "ES");
      ASSERT_EQ(rec.isOriginalTitle, 0);
   });
   ASSERT_TRUE(found_ta1);

   // --- Check specific content from title.ratings ---
   title_ratings_t::Key tr_key1{imdb::string_to_tconst("tt0000001")};
   bool found_tr1 = title_ratings.lookup1(tr_key1, [](const title_ratings_t& rec) {
      ASSERT_EQ(rec.averageRating, Numeric(5.7));
      ASSERT_EQ(rec.numVotes, 1976);
   });
   ASSERT_TRUE(found_tr1);
}

// --- Main function to run all tests ---
int main(int argc, char** argv)
{
   ::testing::InitGoogleTest(&argc, argv);
   // Parse gflags, which are used by job/load.hpp (FLAGS_data_dir)
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   return RUN_ALL_TESTS();
}