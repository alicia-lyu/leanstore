#include "../shared/Adapter.hpp"
#include "../shared/Types.hpp"
#include "leanstore/KVInterface.hpp"
#include <cassert>
#include <optional>
#include <vector>

template <typename Record1, typename Record2, typename JoinedRecord>
class MergeJoin {
  constexpr static unsigned join_key_length = JoinedRecord::joinKeyLength();

public:
  MergeJoin(Scanner<Record1>& left_scanner,
            Scanner<Record2>& right_scanner)
      : left_scanner(left_scanner),
        right_scanner(right_scanner),
        left_next_ret(leanstore::OP_RESULT::OK),
        right_next_ret(leanstore::OP_RESULT::OK)
  {
    advance_left();
    advance_right();
  }

  std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> next() {

    // Replace all left/right_record with both key and record

    while (!is_sentinel1(left_record)) {
      int cmp = compare_keys();
      if (cmp == 0) {
        auto joined_record = join_records();
        if (cached_right_records.empty()) {
          cached_right_key = right_key;
        }
        cached_right_records.push_back(right_record);
        advance_right();
        return joined_record;
      } else if (cmp < 0) {
        if (cached_right_records_iter == cached_right_records.end()) {
            // Next left record may join with the cached right records
            cached_right_records_iter = cached_right_records.begin();
            advance_left();
            continue;
        } else if (compare_keys(true) > 0) {
          // The cached right records will never be joined with left records beyond this point
          cached_right_records.clear();
          cached_right_records_iter = cached_right_records.begin();
          if (is_sentinel2(right_record))
            break; // The following left records has no cached rows or current row to join
          advance_left();
          continue;
        }
        assert(compare_keys(true) == 0); // Those cached records were joined with a left record smaller than or equal to the current one
        auto joined_record =
            join_records(true);
        ++cached_right_records_iter;
        return joined_record;
      } else {
        // Cached right records are even smaller
        cached_right_records.clear();
        cached_right_records_iter = cached_right_records.begin();
        advance_right();
      }
    }

    return std::nullopt;
  }

private:
  static const Record1 sentinel1;
  static const Record2 sentinel2;
  static bool is_sentinel1(Record1 record) {
    return std::memcmp(&record, &sentinel1, sizeof(Record1)) == 0;
  }
  static bool is_sentinel2(Record2 record) {
    return std::memcmp(&record, &sentinel2, sizeof(Record2)) == 0;
  }

  Scanner<Record1>& left_scanner;
  Scanner<Record2>& right_scanner;
  typename Record1::Key left_key;
  typename Record2::Key right_key;
  Record1 left_record;
  Record2 right_record;

  typename Record2::Key cached_right_key;
  std::vector<Record2> cached_right_records;
  typename std::vector<Record2>::iterator cached_right_records_iter;

  leanstore::OP_RESULT right_next_ret;
  leanstore::OP_RESULT left_next_ret;

  void advance_left() {
    if (left_next_ret == leanstore::OP_RESULT::OK) {
      auto ret = left_scanner.next(); // TODO: next ret changed
      left_key = ret.key;
      left_record = ret.record;
      left_next_ret = ret.res;
    } else {
      left_record = sentinel1;
    }
  }

  void advance_right() {
    if (right_next_ret == leanstore::OP_RESULT::OK) {
      auto ret = right_scanner.next();
      right_key = ret.key;
      right_record = ret.record;
      right_next_ret = ret.res;
    } else {
      right_record = sentinel2;
    }
  }

  int compare_keys(bool use_cached = false) const {
    // HARDCODED
    uint8_t left_joinkey[join_key_length];
    uint8_t right_joinkey[join_key_length];

    unsigned pos1 = 0;
    pos1 += fold(left_joinkey, left_key.ol_w_id);
    pos1 += fold(left_joinkey, left_record.ol_i_id);

    if (use_cached == false) {
      unsigned pos2 = 0;
      pos2 += fold(right_joinkey, right_key.s_w_id);
      pos2 += fold(right_joinkey, right_key.s_i_id);
    } else {
      unsigned pos2 = 0;
      pos2 += fold(right_joinkey, cached_right_key.s_w_id);
      pos2 += fold(right_joinkey, cached_right_key.s_i_id);
    }
    
    return std::memcmp(left_joinkey, right_joinkey,
                       join_key_length);
  }

  std::pair<typename JoinedRecord::Key, JoinedRecord> join_records(bool used_cached = false) const {
    // HARDCODED

    // Populate the joined record fields from left and right records

    const Record2 &right = used_cached ? *cached_right_records_iter : right_record;

    typename JoinedRecord::Key key {
      left_key.ol_w_id,
      left_record.ol_i_id,
      left_key.ol_d_id,
      left_key.ol_o_id,
      left_key.ol_number
    };

    JoinedRecord record {
      left_record.ol_i_id,
      left_record.ol_supply_w_id,
      left_record.ol_delivery_d,
      left_record.ol_quantity,
      left_record.ol_amount,
      left_record.ol_dist_info,
      right.s_quantity,
      right.s_dist_01,
      right.s_dist_02,
      right.s_dist_03,
      right.s_dist_04,
      right.s_dist_05,
      right.s_dist_06,
      right.s_dist_07,
      right.s_dist_08,
      right.s_dist_09,
      right.s_dist_10,
      right.s_ytd,
      right.s_order_cnt,
      right.s_remote_cnt,
      right.s_data
    };

    return {key, record};
  }
};

template <typename Record1, typename Record2, typename JoinedRecord>
const Record1 MergeJoin<Record1, Record2, JoinedRecord>::sentinel1 = []() {
  Record1 sentinel;
  std::memset(&sentinel, 255, sizeof(Record1));
  return sentinel;
}();

template <typename Record1, typename Record2, typename JoinedRecord>
const Record2 MergeJoin<Record1, Record2, JoinedRecord>::sentinel2 = []() {
  Record2 sentinel;
  std::memset(&sentinel, 255, sizeof(Record2));
  return sentinel;
}();