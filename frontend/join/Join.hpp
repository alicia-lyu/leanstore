#include "../shared/Adapter.hpp"
#include "../shared/Types.hpp"
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
        right_scanner(right_scanner)
  {
    advance_left();
    advance_right();
  }

  std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> next() {

    std::optional<std::pair<typename JoinedRecord::Key, JoinedRecord>> joined_record = std::nullopt;

    while (!left_exhausted) {
      int cmp = compare_keys();
      if (cmp == 0 && !right_exhausted) {
        // Join current left with current right
        joined_record = join_records();
        if (cached_right_records.empty()) {
          cached_right_key = right_key;
        }
        cached_right_records.push_back(right_record);
        advance_right();
        break;
      } else if ((cmp < 0 || right_exhausted) && !cached_right_records.empty()) {
        // Join current left with cached right
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
        joined_record =
            join_records(true);
        ++cached_right_records_iter;
        break;
      } else if (!right_exhausted) {
        // Current right record is too small. Cached right records are even smaller
        cached_right_records.clear();
        cached_right_records_iter = cached_right_records.begin();
        advance_right();
      } else {
        return std::nullopt;
      }
    }
    assert(joined_record.has_value());
    if (produced % 1000 == 0) {
      std::cout << "Produced " << produced << ": " << left_consumed << " " << right_consumed << std::endl;
      std::cout << "Next joined record, key: " << joined_record.value().first << std::endl;
      std::cout << "Payload: " << joined_record.value().second << std::endl;
    }
    produced++;
    return joined_record.value();
  }

private:
  Scanner<Record1>& left_scanner;
  Scanner<Record2>& right_scanner;
  size_t left_consumed = 0;
  size_t right_consumed = 0;
  size_t produced = 0;

  typename Record1::Key left_key;
  typename Record2::Key right_key;
  Record1 left_record;
  Record2 right_record;
  bool left_exhausted = false;
  bool right_exhausted = false;

  typename Record2::Key cached_right_key;
  std::vector<Record2> cached_right_records;
  typename std::vector<Record2>::iterator cached_right_records_iter;

  void advance_left() {
    assert(!left_exhausted);
    auto ret = left_scanner.next();
    if (!ret.has_value()) {
      left_exhausted = true;
      return;
    }
    left_consumed++;
    left_key = ret.value().key;
    left_record = ret.value().record;
  }

  void advance_right() {
    // assert(!right_exhausted);
    if (right_exhausted) return;
    auto ret = right_scanner.next();
    if (!ret.has_value()) {
      right_exhausted = true;
      return;
    }
    right_consumed++;
    right_key = ret.value().key;
    right_record = ret.value().record;
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