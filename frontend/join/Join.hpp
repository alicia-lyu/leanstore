#include "../shared/Adapter.hpp"
#include "../shared/Types.hpp"
#include "leanstore/KVInterface.hpp"
#include <algorithm>
#include <cassert>
#include <limits>
#include <optional>
#include <vector>

template <typename Record1, typename Record2, typename JoinedRecord>
class MergeJoin {
  constexpr static unsigned join_key_length = JoinedRecord::joinKeyLength();

public:
  MergeJoin(Scanner<Record1>& left_scanner,
            Scanner<Record2>& right_scanner)
      : left_scanner(std::move(left_scanner)),
        right_scanner(std::move(right_scanner))
  {
    std::tie(left_key, left_record, left_next_ret) = left_scanner.next();
    std::tie(right_key, right_record, right_next_ret) = right_scanner.next();
  }

  std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> next() {

    // Replace all left/right_record with both key and record

    while (left_key != std::numeric_limits<typename Record1::Key>::max()) {
      int cmp = compare_keys(left_key, right_record);
      if (cmp == 0) {
        auto joined_record = join_records(left_record, right_record);
        cached_right_records.push_back(right_record);
        advance_right();
        return joined_record;
      } else if (cmp < 0) {
        if (cached_right_records_iter == cached_right_records.end()) {
            // Next left record may join with the cached right records
            cached_right_records_iter = cached_right_records.begin();
            advance_left();
            continue;
        } else if (compare_keys(left_record, *cached_right_records_iter) > 0) {
          // The cached right records will never be joined with left records beyond this point
          cached_right_records.clear();
          cached_right_records_iter = cached_right_records.begin();
          if (right_record == std::numeric_limits<typename Record2::Key>::max()) 
            break; // The following left records has no cached rows or current row to join
          advance_left();
          continue;
        }
        assert(compare_keys(left_record, *cached_right_records_iter) == 0); // Those cached records were joined with a left record smaller than or equal to the current one
        auto joined_record =
            join_records(left_record, *cached_right_records_iter);
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
  Scanner<Record1>& left_scanner;
  Scanner<Record2>& right_scanner;
  Record1::Key left_key;
  Record2::Key right_key;
  Record1 &left_record;
  Record2 &right_record;
  std::vector<Record2> cached_right_records;
  typename std::vector<Record2>::iterator cached_right_records_iter;
  leanstore::OP_RESULT right_next_ret;
  leanstore::OP_RESULT left_next_ret;

  void advance_left() {
    if (left_next_ret == leanstore::OP_RESULT::OK) {
      std::tie(left_record, left_next_ret) = left_scanner.next();
    } else {
      left_record = std::numeric_limits<typename Record1::Key>::max();
    }
  }

  void advance_right() {
    if (right_next_ret == leanstore::OP_RESULT::OK) {
      std::tie(right_record, right_next_ret) = right_scanner.next();
    } else {
      right_record = std::numeric_limits<typename Record2::Key>::max();
    }
  }

  int compare_keys(const Record1 &left, const Record2 &right) const {
    // HARDCODED
    uint8_t left_joinkey[join_key_length];
    uint8_t right_joinkey[join_key_length];

    unsigned pos1 = 0;
    pos1 += fold(left_joinkey, left.left.ol_w_id);
    pos1 += fold(left_joinkey, left.left.ol_i_id);

    unsigned pos2 = 0;
    pos2 += fold(right_joinkey, right.right.s_w_id);
    pos2 += fold(right_joinkey, right.right.s_i_id);

    return std::memcmp(left_joinkey, right_joinkey,
                       join_key_length);
  }

  std::pair<typename JoinedRecord::Key, JoinedRecord> join_records(const Record1 &left, const Record2 &right) const {
    // HARDCODED

    // Populate the joined record fields from left and right records

    typename JoinedRecord::Key key {
      left.ol_w_id,
      left.ol_i_id,
      left.ol_d_id,
      left.ol_o_id,
      left.ol_number
    };

    JoinedRecord record {
      left.ol_i_id,
      left.ol_supply_w_id,
      left.ol_delivery_d,
      left.ol_quantity,
      left.ol_amount,
      left.ol_dist_info,
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