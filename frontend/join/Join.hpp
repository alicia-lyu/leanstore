#include "../shared/Adapter.hpp"
#include "../shared/Types.hpp"
#include "Exceptions.hpp"
#include <cassert>
#include <optional>
#include <vector>
#include <iostream>

template <typename Record1, typename Record2, typename JoinedRecord>
class MergeJoin {
  constexpr static unsigned join_key_length = JoinedRecord::joinKeyLength();

public:
  MergeJoin(Scanner<Record1>* left_scanner,
            Scanner<Record2>* right_scanner)
      : left_scanner(left_scanner),
        right_scanner(right_scanner),
        cached_right_key(std::nullopt),
        cached_right_records_iter(cached_right_records.begin())
  {
    advance_left();
    advance_right();
  }

  ~MergeJoin() {
    std::cout << "~MergeJoin(): Produced " << produced << ", consumed " << left_consumed << " (left) " << right_consumed << " (right)" << std::endl;
    std::cout << "Left semi-join selectivity: " << (double)left_matched / left_consumed << std::endl;
    std::cout << "Right semi-join selectivity: " << (double)right_matched / right_consumed << std::endl;
  }

  std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> next() {

    std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> joined_record = std::nullopt;

    while (!left_exhausted) {
      int cmp = compare_keys();
      if (cmp == 0 && !right_exhausted) {
        // Join current left with current right
        if (cached_right_records.empty()) {
          cached_right_key = right_key;
        }
        // Use of cached_right_records_iter is continuous---no sporadically insertions or deletions
        assert(cached_right_records_iter == cached_right_records.begin());
        cached_right_records.push_back(right_record);
        // std::cout << "Caching right: " << right_key << "; payload: " << right_record << "because it joined with left: " << left_key << std::endl;
        // Reset iterator after modifying cached_right_records
        cached_right_records_iter = cached_right_records.begin();

        joined_record = std::make_optional(join_records());
        break;
      } else if ((cmp < 0 || right_exhausted)) {
        // Join current left with cached right

        if (cached_right_records.empty()) {
          advance_left();
          continue;
        }
        
        if (cached_right_records_iter == cached_right_records.end()) {
            // Next left record may join with the cached right records
            cached_right_records_iter = cached_right_records.begin();
            advance_left();
            continue;
        } else if (compare_keys(true) > 0) {
          // The cached right records are too small, will never be joined with left records beyond this point
          cached_right_records.clear();
          cached_right_records_iter = cached_right_records.begin();
          if (right_exhausted)
            return std::nullopt; // The following left records has no cached rows or current row to join
          advance_left();
          continue;
        }
        assert(compare_keys(true) == 0); // Those cached records were joined with a left record smaller than or equal to the current one
        joined_record = std::make_optional(
            join_records(true));
        break;
      } else if (cmp > 0) {
        if (!right_exhausted) {
          // Current right record is too small. Cached right records are even smaller
          cached_right_records.clear();
          cached_right_records_iter = cached_right_records.begin();
          advance_right();
        } else {
          return std::nullopt;
        }
      } else {
        UNREACHABLE();
      }
    }

    if (joined_record.has_value() && produced % 10000 == 0) {
      std::cout << "Produced " << produced << ", consumed " << left_consumed << " (left) " << right_consumed << " (right)" << std::endl;
      std::cout << "Next joined record, key: " << joined_record.value().first << std::endl;
      // std::cout << "Payload: " << joined_record.value().second << std::endl;
    }
    if (joined_record.has_value()) produced++;
    
    return joined_record;
  }

private:
  Scanner<Record1>* left_scanner;
  Scanner<Record2>* right_scanner;
  size_t left_consumed = 0;
  size_t right_consumed = 0;
  size_t produced = 0;

  typename Record1::Key left_key;
  typename Record2::Key right_key;
  Record1 left_record;
  Record2 right_record;
  bool left_exhausted = false;
  bool right_exhausted = false;

  size_t left_matched = 0;
  size_t right_matched = 0;
  bool current_left_matched = false;
  bool current_right_matched = false;

  std::optional<typename Record2::Key> cached_right_key;
  std::vector<Record2> cached_right_records;
  typename std::vector<Record2>::iterator cached_right_records_iter;

  bool advance_left() {
    assert(!left_exhausted);
    if (current_left_matched) {
      left_matched++;
    }
    auto ret = left_scanner->next();
    if (!ret.has_value()) {
      left_exhausted = true;
      return false;
    }
    left_consumed++;
    left_key = ret.value().key;
    left_record = ret.value().record;
    current_left_matched = false;
    // std::cout << "Advance left: " << left_key.ol_w_id << " " << left_record.ol_i_id << std::endl;
    return true;
  }

  bool advance_right() {
    assert(!right_exhausted);
    if (current_right_matched) {
      right_matched++;
    }
    if (right_exhausted) return false;
    auto ret = right_scanner->next();
    if (!ret.has_value()) {
      right_exhausted = true;
      return false;
    }
    right_consumed++;
    right_key = ret.value().key;
    right_record = ret.value().record;
    current_right_matched = false;
    // std::cout << "Advance right: " << right_key.s_w_id << " " << right_key.s_i_id << std::endl;
    return true;
  }

  int compare_keys(bool use_cached = false) const {
    // HARDCODED
    if (use_cached == false) {
      s32 w_diff = left_key.ol_w_id - right_key.s_w_id;
      if (w_diff != 0) {
        return w_diff;
      }
      return left_record.ol_i_id - right_key.s_i_id;
    } else {
      s32 w_diff = left_key.ol_w_id - cached_right_key.value().s_w_id;
      if (w_diff != 0) {
        return w_diff;
      }
      return left_record.ol_i_id - cached_right_key.value().s_i_id;
    }
  }

  std::pair<typename JoinedRecord::Key, JoinedRecord> join_records(bool use_cached = false) {
    // HARDCODED

    const Record2 &right = use_cached ? *cached_right_records_iter : right_record;

    typename JoinedRecord::Key key {
      left_key.ol_w_id,
      left_record.ol_i_id,
      left_key.ol_d_id,
      left_key.ol_o_id,
      left_key.ol_number
    };

    JoinedRecord record {
      left_record.ol_supply_w_id,
      left_record.ol_delivery_d,
      left_record.ol_quantity,
      left_record.ol_amount,
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

    current_left_matched = true;
    if (use_cached) {
      ++cached_right_records_iter;
    } else {
      current_right_matched = true;
      bool ret = advance_right();
      if ((ret && compare_keys() != 0) || !ret) advance_left();
    }

    return {key, record};
  }
};
