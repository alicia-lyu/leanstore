#pragma once
#include <ostream>
#include "../shared/Types.hpp"

// TODO: Generate multiple schema with different included columns

// Joining order line and stock in the same warehouse
struct ol_join_sec_t {
  static constexpr int id = 11;
  struct Key {
    static constexpr int id = 11; // comes from table 8, i.e., orderline
    // join key
    Integer ol_w_id;
    Integer ol_i_id;
    // orderline_t::Key, included in Key to allow duplicate join keys from
    // orderline
    Integer ol_d_id;
    Integer ol_o_id;
    Integer ol_number;

    friend std::ostream& operator<<(std::ostream& os, const Key& key) {
      os << "w_id: " << key.ol_w_id << ", i_id: " << key.ol_i_id
         << ", d_id: " << key.ol_d_id << ", o_id: " << key.ol_o_id
         << ", number: " << key.ol_number;
      return os;
    }
  };
  // orderline_t::Value
  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Varchar<24> ol_dist_info;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ol_w_id);
    pos += fold(out + pos, key.ol_i_id);
    pos += fold(out + pos, Key::id);
    pos += fold(out + pos, key.ol_d_id);
    pos += fold(out + pos, key.ol_o_id);
    pos += fold(out + pos, key.ol_number);
    return pos;
  }

  template <class T> static unsigned foldPKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ol_w_id);
    pos += fold(out + pos, key.ol_d_id);
    pos += fold(out + pos, key.ol_o_id);
    pos += fold(out + pos, key.ol_number);
    return pos;
  }

  template <class T> static unsigned foldJKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ol_w_id);
    pos += fold(out + pos, key.ol_i_id);
    return pos;
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    unsigned pos = 0;
    pos += unfold(in + pos, key.ol_w_id);
    pos += unfold(in + pos, key.ol_i_id);
    int id;
    pos += unfold(in + pos, id);
    assert(id == Key::id);
    pos += unfold(in + pos, key.ol_d_id);
    pos += unfold(in + pos, key.ol_o_id);
    pos += unfold(in + pos, key.ol_number);
    return pos;
  }

  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_i_id) + sizeof(Key::id) +
           sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) +
           sizeof(Key::ol_number);
  };

  static constexpr unsigned joinKeyLength() {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_i_id);
  }

  static constexpr unsigned primaryKeyLength() {
    return 0 + sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
  }

  friend std::ostream& operator<<(std::ostream& os, const ol_join_sec_t& record) {
    os << ", supply_w_id: " << record.ol_supply_w_id
       << ", delivery_d: " << record.ol_delivery_d << ", quantity: " << record.ol_quantity << ", amount: " << record.ol_amount << ", dist_info: " << record.ol_dist_info.toString();
    return os;
  }

  ol_join_sec_t expand() const {
    return *this;
  }
};

struct ol_sec_key_only_t {
  using Key = ol_join_sec_t::Key;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    return ol_join_sec_t::foldKey(out, key);
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    return ol_join_sec_t::unfoldKey(in, key);
  }

  static constexpr unsigned maxFoldLength() {
    return ol_join_sec_t::maxFoldLength();
  };

  static constexpr unsigned joinKeyLength() {
    return ol_join_sec_t::joinKeyLength();
  }

  static constexpr unsigned primaryKeyLength() {
    return ol_join_sec_t::primaryKeyLength();
  }

  friend std::ostream& operator<<(std::ostream& os, const ol_sec_key_only_t& record) {
    os << "ol_sec_key_only";
  }

  ol_join_sec_t expand() const {
    return ol_join_sec_t {
      .ol_supply_w_id = 0,
      .ol_delivery_d = Timestamp(),
      .ol_quantity = Numeric(),
      .ol_amount = Numeric(),
      .ol_dist_info = Varchar<24>()
    };
  }
};

struct joined_ols_t {
  static constexpr int id = 12;
  struct Key {
    static constexpr int id = 12;
    // join key
    Integer w_id;
    Integer i_id;
    // orderline_t::Key, included in Key to allow duplicate join key
    Integer ol_d_id;
    Integer ol_o_id;
    Integer ol_number;
    // stock_t::Key, none other than join key

    friend std::ostream& operator<<(std::ostream& os, const Key& key) {
      os << "w_id: " << key.w_id << ", i_id: " << key.i_id
         << ", d_id: " << key.ol_d_id << ", o_id: " << key.ol_o_id
         << ", number: " << key.ol_number;
      return os;
    }
  };
  // from order line
  Integer ol_supply_w_id; // In inner join results, it must be the same as w_id, but not so in outer join results, where s info are NULL for unmatched records, as information from the actual supplying warehouse is not joined.
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  // Varchar<24> ol_dist_info; // Can be looked up by ol_d_id and s_dist_0x
  // From stock
  Numeric s_quantity;
  Varchar<24> s_dist_01;
  Varchar<24> s_dist_02;
  Varchar<24> s_dist_03;
  Varchar<24> s_dist_04;
  Varchar<24> s_dist_05;
  Varchar<24> s_dist_06;
  Varchar<24> s_dist_07;
  Varchar<24> s_dist_08;
  Varchar<24> s_dist_09;
  Varchar<24> s_dist_10;
  Numeric s_ytd;
  Numeric s_order_cnt;
  Numeric s_remote_cnt;
  Varchar<50> s_data;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.w_id);
    pos += fold(out + pos, key.i_id);
    pos += fold(out + pos, key.ol_d_id);
    pos += fold(out + pos, key.ol_o_id);
    pos += fold(out + pos, key.ol_number);
    return pos;
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    unsigned pos = 0;
    pos += unfold(in + pos, key.w_id);
    pos += unfold(in + pos, key.i_id);
    pos += unfold(in + pos, key.ol_d_id);
    pos += unfold(in + pos, key.ol_o_id);
    pos += unfold(in + pos, key.ol_number);
    return pos;
  }

  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::w_id) + sizeof(Key::i_id) + sizeof(Key::ol_d_id) +
           sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
  };

  static constexpr unsigned joinKeyLength() {
    return 0 + sizeof(Key::w_id) + sizeof(Key::i_id);
  }

  friend std::ostream& operator<<(std::ostream& os, const joined_ols_t& record) {
    os << ", supply_w_id: " << record.ol_supply_w_id
       << ", delivery_d: " << record.ol_delivery_d << ", quantity: " << record.ol_quantity << ", amount: " << record.ol_amount << ", s_quantity: " << record.s_quantity << ", s_dist_01: " << record.s_dist_01.toString() << ", s_dist_02: " << record.s_dist_02.toString() << ", s_dist_03: " << record.s_dist_03.toString() << ", s_dist_04: " << record.s_dist_04.toString() << ", s_dist_05: " << record.s_dist_05.toString() << ", s_dist_06: " << record.s_dist_06.toString() << ", s_dist_07: " << record.s_dist_07.toString() << ", s_dist_08: " << record.s_dist_08.toString() << ", s_dist_09: " << record.s_dist_09.toString() << ", s_dist_10: " << record.s_dist_10.toString() << ", s_ytd: " << record.s_ytd << ", s_order_cnt: " << record.s_order_cnt << ", s_remote_cnt: " << record.s_remote_cnt << ", s_data: " << record.s_data.toString();
    return os;
  }

  joined_ols_t expand() const {
    return *this;
  }
};

struct joined_ols_key_only_t {
  using Key = joined_ols_t::Key;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    return joined_ols_t::foldKey(out, key);
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    return joined_ols_t::unfoldKey(in, key);
  }

  static constexpr unsigned maxFoldLength() {
    return joined_ols_t::maxFoldLength();
  };

  static constexpr unsigned joinKeyLength() {
    return joined_ols_t::joinKeyLength();
  }

  friend std::ostream& operator<<(std::ostream& os, const joined_ols_key_only_t& record) {
    os << "joined_ols_key_only";
    return os;
  }

  joined_ols_t expand() const {
    return joined_ols_t {
      .ol_supply_w_id = 0,
      .ol_delivery_d = Timestamp(),
      .ol_quantity = Numeric(),
      .ol_amount = Numeric(),
      .s_quantity = Numeric(),
      .s_dist_01 = Varchar<24>(),
      .s_dist_02 = Varchar<24>(),
      .s_dist_03 = Varchar<24>(),
      .s_dist_04 = Varchar<24>(),
      .s_dist_05 = Varchar<24>(),
      .s_dist_06 = Varchar<24>(),
      .s_dist_07 = Varchar<24>(),
      .s_dist_08 = Varchar<24>(),
      .s_dist_09 = Varchar<24>(),
      .s_dist_10 = Varchar<24>(),
      .s_ytd = Numeric(),
      .s_order_cnt = Numeric(),
      .s_remote_cnt = Numeric(),
      .s_data = Varchar<50>()
    };
  }
};