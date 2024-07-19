#include "Adapter.hpp"
#include <algorithm>
#include <cassert>
#include <memory>
#include <vector>

template <typename Record1, typename Record2, typename JoinedRecord>
class MergeJoin {
  constexpr static unsigned join_key_length = Record1::joinKeyLength();

public:
  MergeJoin(Scanner<Record1> left_adapter,
            Scanner<Record2> right_adapter)
      : left_adapter(std::move(left_adapter)),
        right_adapter(std::move(right_adapter)) {
    assert(Record1::joinKeyLength() == Record2::joinKeyLength());
    assert(Record1::joinKeyLength() == JoinedRecord::joinKeyLength());
    left_record = left_adapter.next();
    right_record = right_adapter.next();
  }

  JoinedRecord &next() {
    if (!left_record || !right_record) {
      return nullptr;
    }

    while (left_record && right_record) {
      int cmp = compare_keys(left_record, right_record);
      if (cmp == 0) {
        auto joined_record = join_records(left_record, right_record);
        cached_right_records.push_back(right_record);
        right_record = right_adapter.next();
        return joined_record;
      } else if (cmp < 0) {
        if (cached_right_records_iter == cached_right_records.end()) {
            // Next left record may join with the cached right records
            cached_right_records_iter = cached_right_records.begin();
            left_record = left_adapter.next();
            continue;
        } else if (compare_keys(left_record, *cached_right_records_iter) > 0) {
          // The cached right records will never be joined with left records beyond this point
          cached_right_records.clear();
          cached_right_records_iter = cached_right_records.begin();
          left_record = left_adapter.next();
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
        right_record = right_adapter.next();
      }
    }

    return nullptr;
  }

private:
  Scanner<Record1> left_adapter;
  Scanner<Record2> right_adapter;
  Record1 &left_record;
  Record2 &right_record;
  std::vector<Record2 &> cached_right_records;
  std::vector<Record2 &>::iterator cached_right_records_iter;

  int compare_keys(const Record1 &left, const Record2 &right) const {
    uint8_t left_key[Record1::Key::maxFoldLength()];
    uint8_t right_key[Record2::Key::maxFoldLength()];

    Record1::foldKey(left_key, left.Key);
    Record2::foldKey(right_key, right.Key);

    return std::memcmp(left_key, right_key,
                       join_key_length);
  }

  JoinedRecord &join_records(const Record1 &left, const Record2 &right) const {
    auto joined_record = std::make_unique<JoinedRecord>();
    // HARDCODED

    // Populate the joined record fields from left and right records
    joined_record->w_id = left.ols_w_id;
    joined_record->i_id = left.ols_i_id;
    joined_record->ol_d_id = left.ols_d_id;
    joined_record->ol_o_id = left.ols_o_id;
    joined_record->ol_number = left.ols_number;

    joined_record->ol_i_id = left.ols_i_id;
    joined_record->ol_supply_w_id = left.ols_supply_w_id;
    joined_record->ol_delivery_d = left.ols_delivery_d;
    joined_record->ol_quantity = left.ols_quantity;
    joined_record->ol_amount = left.ols_amount;
    joined_record->ol_dist_info = left.ols_dist_info;

    joined_record->s_quantity = right.ols_quantity;
    joined_record->s_dist_01 = right.ols_dist_01;
    joined_record->s_dist_02 = right.ols_dist_02;
    joined_record->s_dist_03 = right.ols_dist_03;
    joined_record->s_dist_04 = right.ols_dist_04;
    joined_record->s_dist_05 = right.ols_dist_05;
    joined_record->s_dist_06 = right.ols_dist_06;
    joined_record->s_dist_07 = right.ols_dist_07;
    joined_record->s_dist_08 = right.ols_dist_08;
    joined_record->s_dist_09 = right.ols_dist_09;
    joined_record->s_dist_10 = right.ols_dist_10;
    joined_record->s_ytd = right.ols_ytd;
    joined_record->s_order_cnt = right.ols_order_cnt;
    joined_record->s_remote_cnt = right.ols_remote_cnt;
    joined_record->s_data = right.ols_data;

    return joined_record;
  }
};