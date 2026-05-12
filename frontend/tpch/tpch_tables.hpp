#pragma once
#include <functional>
#include "../shared/randutils.hpp"
#include "../shared/table_traits.hpp"

using namespace randutils;

// id range: (0s) 0--7, 9

// -------------------------------------------------------------------------------------
// TPC-H date constants (days since 1970-01-01)
inline constexpr Timestamp TPCH_STARTDATE = 8035;      // 1992-01-01
inline constexpr Timestamp TPCH_CURRENTDATE = 9299;     // 1995-06-17
inline constexpr Timestamp TPCH_ENDDATE = 10591;         // 1998-12-31
inline constexpr Timestamp TPCH_ORDERS_ENDDATE = 10440;  // 1998-08-02 (ENDDATE - 151)
inline constexpr Timestamp DATE_1994_01_01 = 8766;       // 1994-01-01 (Q12 default receiptdate_lo)
inline constexpr Timestamp DATE_1995_01_01 = 9131;       // 1995-01-01 (Q12 default receiptdate_hi)
inline constexpr Timestamp DATE_1995_03_15 = 9204;       // 1995-03-15 (Q3/Q3I default orderdate/shipdate)

// P_NAME color words (92 values from TPC-H spec §4.2.3)
inline constexpr const char* TPCH_COLORS[] = {
    "almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
    "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
    "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
    "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
    "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
    "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
    "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
    "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna",
    "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet",
    "wheat", "white", "yellow"
};
inline constexpr int TPCH_NUM_COLORS = 92;

// Types: 3 syllables concatenated (150 combinations). For simplicity, generate at random.
inline constexpr const char* TPCH_TYPES_S1[] = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
inline constexpr const char* TPCH_TYPES_S2[] = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
inline constexpr const char* TPCH_TYPES_S3[] = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

// Containers: 2 syllables (40 combinations)
inline constexpr const char* TPCH_CONTAINERS_S1[] = {"SM", "LG", "MED", "JUMBO", "WRAP"};
inline constexpr const char* TPCH_CONTAINERS_S2[] = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};

inline constexpr const char* TPCH_SEGMENTS[] = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};
inline constexpr const char* TPCH_PRIORITIES[] = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};
inline constexpr const char* TPCH_MODES[] = {"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};
inline constexpr const char* TPCH_INSTRUCTIONS[] = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};

// -------------------------------------------------------------------------------------
// Pick a random entry from a null-terminated list of C-strings and return it as Varchar.
template <int maxLen>
Varchar<maxLen> randomFromList(const char* const* list, int count)
{
   return Varchar<maxLen>(list[randutils::urand(0, count - 1)]);
}

// -------------------------------------------------------------------------------------

struct part_t {
   static constexpr int id = 0;
   Varchar<55> p_name;
   Varchar<25> p_mfgr;
   Varchar<10> p_brand;
   Varchar<25> p_type;
   Integer p_size;
   Varchar<10> p_container;
   Numeric p_retailprice;
   Varchar<23> p_comment;

   struct Key {
      static constexpr int id = 0;
      Integer p_partkey;
      ADD_KEY_TRAITS(&Key::p_partkey)
   };

   ADD_RECORD_TRAITS(part_t)

   void print(std::ostream& os) const
   {
      os << p_name << "," << p_mfgr << "," << p_brand << "," << p_type << "," << p_size << "," << p_container << "," << p_retailprice << ","
         << p_comment;
   }

   static part_t generateRandomRecord(Integer partkey)
   {
      // p_name: 5 distinct random color words separated by spaces (critical for Q9 LIKE '%green%')
      int indices[5];
      for (int i = 0; i < 5; i++) {
         int idx;
         bool duplicate;
         do {
            idx = randutils::urand(0, TPCH_NUM_COLORS - 1);
            duplicate = false;
            for (int j = 0; j < i; j++) {
               if (indices[j] == idx) {
                  duplicate = true;
                  break;
               }
            }
         } while (duplicate);
         indices[i] = idx;
      }
      Varchar<55> p_name;
      for (int i = 0; i < 5; i++) {
         const char* color = TPCH_COLORS[indices[i]];
         for (int c = 0; color[c] != '\0'; c++) {
            p_name.append(color[c]);
         }
         if (i < 4) {
            p_name.append(' ');
         }
      }

      // p_mfgr: "Manufacturer#" + digit [1,5]
      char mfgr_buf[15];
      mfgr_buf[0] = 'M'; mfgr_buf[1] = 'a'; mfgr_buf[2] = 'n'; mfgr_buf[3] = 'u';
      mfgr_buf[4] = 'f'; mfgr_buf[5] = 'a'; mfgr_buf[6] = 'c'; mfgr_buf[7] = 't';
      mfgr_buf[8] = 'u'; mfgr_buf[9] = 'r'; mfgr_buf[10] = 'e'; mfgr_buf[11] = 'r';
      mfgr_buf[12] = '#';
      mfgr_buf[13] = static_cast<char>('0' + randutils::urand(1, 5));
      mfgr_buf[14] = '\0';
      Varchar<25> p_mfgr(mfgr_buf);

      // p_brand: "Brand#" + digit [1,5] + digit [1,5]
      char brand_buf[9];
      brand_buf[0] = 'B'; brand_buf[1] = 'r'; brand_buf[2] = 'a'; brand_buf[3] = 'n';
      brand_buf[4] = 'd'; brand_buf[5] = '#';
      brand_buf[6] = static_cast<char>('0' + randutils::urand(1, 5));
      brand_buf[7] = static_cast<char>('0' + randutils::urand(1, 5));
      brand_buf[8] = '\0';
      Varchar<10> p_brand(brand_buf);

      // p_type: S1 + " " + S2 + " " + S3
      Varchar<25> p_type;
      {
         const char* s1 = TPCH_TYPES_S1[randutils::urand(0, 5)];
         const char* s2 = TPCH_TYPES_S2[randutils::urand(0, 4)];
         const char* s3 = TPCH_TYPES_S3[randutils::urand(0, 4)];
         for (int c = 0; s1[c] != '\0'; c++) p_type.append(s1[c]);
         p_type.append(' ');
         for (int c = 0; s2[c] != '\0'; c++) p_type.append(s2[c]);
         p_type.append(' ');
         for (int c = 0; s3[c] != '\0'; c++) p_type.append(s3[c]);
      }

      // p_container: S1 + " " + S2
      Varchar<10> p_container;
      {
         const char* s1 = TPCH_CONTAINERS_S1[randutils::urand(0, 4)];
         const char* s2 = TPCH_CONTAINERS_S2[randutils::urand(0, 7)];
         for (int c = 0; s1[c] != '\0'; c++) p_container.append(s1[c]);
         p_container.append(' ');
         for (int c = 0; s2[c] != '\0'; c++) p_container.append(s2[c]);
      }

      return part_t{p_name, p_mfgr, p_brand, p_type,
                    randutils::urand(1, 50), p_container, computeRetailPrice(partkey), randomastring<23>(0, 23)};
   }

   // TPC-H §4.2.3 retailprice formula. Pure function of partkey so lineitem
   // generation can compute extendedprice without reading the part record back.
   static Numeric computeRetailPrice(Integer partkey)
   {
      return (90000 + ((partkey / 10) % 20001) + 100 * (partkey % 1000)) / 100.0;
   }
};

struct supplier_t {
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      Integer s_suppkey;
      ADD_KEY_TRAITS(&Key::s_suppkey)
   };

   Varchar<25> s_name;
   Varchar<40> s_address;
   Integer s_nationkey;
   Varchar<15> s_phone;
   Numeric s_acctbal;
   Varchar<101> s_comment;

   ADD_RECORD_TRAITS(supplier_t)

   void print(std::ostream& os) const
   {
      os << s_name << "," << s_address << "," << s_nationkey << "," << s_phone << "," << s_acctbal << "," << s_comment;
   }

   // Caller should pass generate_nationkey returning values in [0..24].
   // s_name format ("Supplier#XXXXXXXXX") requires the suppkey which isn't available here,
   // so we fall back to a random string — format isn't query-critical.
   static supplier_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return supplier_t{randomastring<25>(25, 25), randomastring<40>(0, 40),        generate_nationkey(),
                        randomastring<15>(15, 15), randomNumeric(0.0000, 100.0000), randomastring<101>(0, 101)};
   }
};

struct partsupp_t {
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      Integer ps_partkey;
      Integer ps_suppkey;
      ADD_KEY_TRAITS(&Key::ps_partkey, &Key::ps_suppkey)
   };

   Integer ps_availqty;
   Numeric ps_supplycost;
   Varchar<199> ps_comment;

   ADD_RECORD_TRAITS(partsupp_t)

   void print(std::ostream& os) const
   {
      os << ps_availqty << "," << ps_supplycost << "," << ps_comment;
   }

   static partsupp_t generateRandomRecord() { return partsupp_t{urand(1, 100000), randomNumeric(0.0000, 100.0000), randomastring<199>(0, 199)}; }
};

struct customerh_t {
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      Integer c_custkey;
      ADD_KEY_TRAITS(&Key::c_custkey)
   };

   Varchar<25> c_name;
   Varchar<40> c_address;
   Integer c_nationkey;
   Varchar<15> c_phone;
   Numeric c_acctbal;
   Varchar<10> c_mktsegment;
   Varchar<117> c_comment;

   ADD_RECORD_TRAITS(customerh_t)

   void print(std::ostream& os) const
   {
      os << c_name << "," << c_address << "," << c_nationkey << "," << c_phone << "," << c_acctbal << "," << c_mktsegment << "," << c_comment;
   }

   // Caller should pass generate_nationkey returning values in [0..24].
   static customerh_t generateRandomRecord(std::function<int()> generate_nationkey)
   {
      return customerh_t{randomastring<25>(0, 25),        randomastring<40>(0, 40),              generate_nationkey(),
                         randomastring<15>(15, 15),       randomNumeric(0.0000, 100.0000),
                         randomFromList<10>(TPCH_SEGMENTS, 5),                                   randomastring<117>(0, 117)};
   }
};

struct orders_t {
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      Integer o_orderkey;
      ADD_KEY_TRAITS(&Key::o_orderkey)
   };

   Integer o_custkey;
   Varchar<1> o_orderstatus;
   Numeric o_totalprice;
   Timestamp o_orderdate;
   Varchar<15> o_orderpriority;
   Varchar<15> o_clerk;
   Integer o_shippriority;
   Varchar<79> o_comment;

   ADD_RECORD_TRAITS(orders_t)

   void print(std::ostream& os) const
   {
      os << o_custkey << "," << o_orderstatus << "," << o_totalprice << "," << o_orderdate << "," << o_orderpriority << "," << o_clerk << ","
         << o_shippriority << "," << o_comment;
   }

   static orders_t generateRandomRecord(std::function<int()> generate_custkey)
   {
      return generateRandomRecord(generate_custkey,
                                  Timestamp(randutils::urand(TPCH_STARTDATE, TPCH_ORDERS_ENDDATE)),
                                  Varchar<1>("F"),  // status placeholder; finalize from lineitems
                                  Numeric(0));      // totalprice placeholder
   }

   // Per §4.2.3: o_orderstatus and o_totalprice are derived from the order's
   // lineitems. Loaders should accumulate over generated lineitems and pass
   // the finalized values via this overload.
   static orders_t generateRandomRecord(std::function<int()> generate_custkey,
                                        Timestamp o_orderdate,
                                        Varchar<1> o_orderstatus,
                                        Numeric o_totalprice)
   {
      return orders_t{generate_custkey(),
                      o_orderstatus,
                      o_totalprice,
                      o_orderdate,
                      randomFromList<15>(TPCH_PRIORITIES, 5),
                      randomastring<15>(15, 15), // o_clerk: format not query-critical
                      0,                         // o_shippriority: always 0 in TPC-H
                      randomastring<79>(0, 79)};
   }
};

struct lineitem_t {
   static constexpr int id = 5;
   struct Key {
      static constexpr int id = 5;
      Integer l_orderkey;
      Integer l_linenumber;
      ADD_KEY_TRAITS(&Key::l_orderkey, &Key::l_linenumber)
   };

   Integer l_partkey;
   Integer l_suppkey;

   Numeric l_quantity;
   Numeric l_extendedprice;
   Numeric l_discount;
   Numeric l_tax;
   Varchar<1> l_returnflag;
   Varchar<1> l_linestatus;
   Timestamp l_shipdate;
   Timestamp l_commitdate;
   Timestamp l_receiptdate;
   Varchar<25> l_shipinstruct;
   Varchar<10> l_shipmode;
   Varchar<44> l_comment;

   ADD_RECORD_TRAITS(lineitem_t)

   void print(std::ostream& os) const
   {
      os << l_partkey << "," << l_suppkey << "," << l_quantity << "," << l_extendedprice << "," << l_discount << "," << l_tax << ","
         << l_returnflag << "," << l_linestatus << "," << l_shipdate << "," << l_commitdate << "," << l_receiptdate << ","
         << l_shipinstruct << "," << l_shipmode << "," << l_comment;
   }

   static lineitem_t generateRandomRecord(std::function<int()> generate_partkey,
                                          std::function<int()> generate_suppkey,
                                          Timestamp o_orderdate)
   {
      Integer partkey = generate_partkey();
      // Per TPC-H §4.2.3: l_extendedprice = l_quantity * p_retailprice. Pull
      // retailprice from the deterministic partkey formula instead of reading
      // back the part record.
      return generateRandomRecord(partkey, generate_suppkey(), o_orderdate,
                                  part_t::computeRetailPrice(partkey));
   }

   static lineitem_t generateRandomRecord(Integer partkey,
                                          Integer suppkey,
                                          Timestamp o_orderdate,
                                          Numeric p_retailprice)
   {
      // Dates per §4.2.3: shipdate, commitdate, receiptdate are independent
      // offsets from orderdate; receiptdate is shipdate-relative.
      Timestamp l_shipdate    = o_orderdate + randutils::urand(1, 121);
      Timestamp l_commitdate  = o_orderdate + randutils::urand(30, 90);
      Timestamp l_receiptdate = l_shipdate  + randutils::urand(1, 30);

      Numeric l_quantity = Numeric(randutils::urand(1, 50));
      Numeric l_extendedprice = l_quantity * p_retailprice;
      Numeric l_discount = randomNumeric(0.00, 0.10);
      Numeric l_tax      = randomNumeric(0.00, 0.08);

      // l_returnflag: "R" or "A" if already received, "N" otherwise
      Varchar<1> l_returnflag(l_receiptdate <= TPCH_CURRENTDATE
                              ? (randutils::urand(0, 1) ? "R" : "A")
                              : "N");
      // l_linestatus: "O" if not yet shipped, "F" if shipped
      Varchar<1> l_linestatus(l_shipdate > TPCH_CURRENTDATE ? "O" : "F");

      return lineitem_t{partkey,
                        suppkey,
                        l_quantity,
                        l_extendedprice,
                        l_discount,
                        l_tax,
                        l_returnflag,
                        l_linestatus,
                        l_shipdate,
                        l_commitdate,
                        l_receiptdate,
                        randomFromList<25>(TPCH_INSTRUCTIONS, 4),
                        randomFromList<10>(TPCH_MODES, 7),
                        randomastring<44>(44, 44)};
   }

   // Backward-compatibility overload for geo/ code that doesn't supply an order date.
   static lineitem_t generateRandomRecord(std::function<int()> generate_partkey,
                                          std::function<int()> generate_suppkey)
   {
      return generateRandomRecord(generate_partkey, generate_suppkey,
                                  Timestamp(randutils::urand(TPCH_STARTDATE, TPCH_ORDERS_ENDDATE)));
   }
};

struct nation_t {
   static constexpr int id = 6;
   struct Key {
      static constexpr int id = 6;
      Integer n_nationkey;
      ADD_KEY_TRAITS(&Key::n_nationkey)
   };

   Integer n_regionkey;
   Varchar<25> n_name;
   Varchar<152> n_comment;

   ADD_RECORD_TRAITS(nation_t)

   void print(std::ostream& os) const
   {
      os << n_regionkey << "," << n_name << "," << n_comment;
   }

   // Hardcoded TPC-H nation table (25 rows, deterministic)
   struct NationData {
      Integer key;
      const char* name;
      Integer regionkey;
   };
   static constexpr NationData NATIONS[] = {
       {0,  "ALGERIA",        0}, {1,  "ARGENTINA",      1}, {2,  "BRAZIL",          1},
       {3,  "CANADA",         1}, {4,  "EGYPT",           4}, {5,  "ETHIOPIA",        0},
       {6,  "FRANCE",         3}, {7,  "GERMANY",         3}, {8,  "INDIA",           2},
       {9,  "INDONESIA",      2}, {10, "IRAN",            4}, {11, "IRAQ",            4},
       {12, "JAPAN",          2}, {13, "JORDAN",          4}, {14, "KENYA",           0},
       {15, "MOROCCO",        0}, {16, "MOZAMBIQUE",      0}, {17, "PERU",            1},
       {18, "CHINA",          2}, {19, "ROMANIA",         3}, {20, "SAUDI ARABIA",    4},
       {21, "VIETNAM",        2}, {22, "RUSSIA",          3}, {23, "UNITED KINGDOM",  3},
       {24, "UNITED STATES",  1}
   };
   static constexpr int NATION_COUNT = 25;

   static nation_t fromData(const NationData& d)
   {
      return nation_t{d.regionkey, Varchar<25>(d.name), Varchar<152>("")};
   }

   static nation_t generateRandomRecord(std::function<int()> generate_regionkey)
   {
      return nation_t{generate_regionkey(), randomastring<25>(1, 25), randomastring<152>(0, 152)};
   }
};

struct region_t {
   static constexpr int id = 7;
   struct Key {
      static constexpr int id = 7;
      Integer r_regionkey;
      ADD_KEY_TRAITS(&Key::r_regionkey)
   };

   Varchar<25> r_name;
   Varchar<152> r_comment;

   ADD_RECORD_TRAITS(region_t)

   void print(std::ostream& os) const
   {
      os << r_name << "," << r_comment;
   }

   // Hardcoded TPC-H region table (5 rows, deterministic)
   struct RegionData {
      Integer key;
      const char* name;
   };
   static constexpr RegionData REGIONS[] = {
       {0, "AFRICA"}, {1, "AMERICA"}, {2, "ASIA"}, {3, "EUROPE"}, {4, "MIDDLE EAST"}
   };
   static constexpr int REGION_COUNT = 5;

   static region_t fromData(const RegionData& d)
   {
      return region_t{Varchar<25>(d.name), Varchar<152>("")};
   }

   static region_t generateRandomRecord() { return region_t{randomastring<25>(1, 25), randomastring<152>(0, 152)}; }
};

