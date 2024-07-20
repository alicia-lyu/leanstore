#pragma once
#include "../shared/Types.hpp"

// TODO: Generate multiple schema with different included columns

// Joining order line and stock
struct order_line_ols_t {
  static constexpr int id = 11;
  struct Key {
    static constexpr int id = 8; // comes from table 8, i.e., orderline
    // join key
    Integer ols_w_id;
    Integer ols_i_id;
    // orderline_t::Key, included in Key to allow duplicate join keys from
    // orderline
    Integer ols_d_id;
    Integer ols_o_id;
    Integer ols_number;
  };
  // orderline_t::Value
  Integer ols_i_id;
  Integer ols_supply_w_id;
  Timestamp ols_delivery_d;
  Numeric ols_quantity;
  Numeric ols_amount;
  Varchar<24> ols_dist_info;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ols_w_id);
    pos += fold(out + pos, key.ols_i_id);
    pos += fold(out + pos, Key::id);
    pos += fold(out + pos, key.ols_d_id);
    pos += fold(out + pos, key.ols_o_id);
    pos += fold(out + pos, key.ols_number);
    return pos;
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    unsigned pos = 0;
    pos += unfold(in + pos, key.ols_w_id);
    pos += unfold(in + pos, key.ols_i_id);
    int id;
    pos += unfold(in + pos, id);
    assert(id == Key::id);
    pos += unfold(in + pos, key.ols_d_id);
    pos += unfold(in + pos, key.ols_o_id);
    pos += unfold(in + pos, key.ols_number);
    return pos;
  }

  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::ols_w_id) + sizeof(Key::ols_i_id) + sizeof(Key::id) +
           sizeof(Key::ols_d_id) + sizeof(Key::ols_o_id) +
           sizeof(Key::ols_number);
  };

  static constexpr unsigned joinKeyLength() {
    return 0 + sizeof(Key::ols_w_id) + sizeof(Key::ols_i_id);
  }
};

struct stock_ols_t {
  static constexpr int id = 12;
  struct Key {
    // join key & stock_t::Key
    static constexpr int id = 10; // comes from table 10, i.e., stock
    Integer ols_w_id;
    Integer ols_i_id;
  };
  // stock_t::Value
  Numeric ols_quantity;
  Varchar<24> ols_dist_01;
  Varchar<24> ols_dist_02;
  Varchar<24> ols_dist_03;
  Varchar<24> ols_dist_04;
  Varchar<24> ols_dist_05;
  Varchar<24> ols_dist_06;
  Varchar<24> ols_dist_07;
  Varchar<24> ols_dist_08;
  Varchar<24> ols_dist_09;
  Varchar<24> ols_dist_10;
  Numeric ols_ytd;
  Numeric ols_order_cnt;
  Numeric ols_remote_cnt;
  Varchar<50> ols_data;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ols_w_id);
    pos += fold(out + pos, key.ols_i_id);
    pos += fold(out + pos, Key::id);
    return pos;
  }

  template <class T> static unsigned unfoldKey(const uint8_t *in, T &key) {
    unsigned pos = 0;
    pos += unfold(in + pos, key.ols_w_id);
    pos += unfold(in + pos, key.ols_i_id);
    int id;
    pos += unfold(in + pos, id);
    assert(id == Key::id);
    return pos;
  }

  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::ols_w_id) + sizeof(Key::ols_i_id) + sizeof(Key::id);
  };

  static constexpr unsigned joinKeyLength() {
    return 0 + sizeof(Key::ols_w_id) + sizeof(Key::ols_i_id);
  }
};

struct joined_ols_t {
  static constexpr int id = 11;
  struct Key {
    static constexpr int id = 11;
    // join key
    Integer w_id;
    Integer i_id;
    // orderline_t::Key, included in Key to allow duplicate join key
    Integer ol_d_id;
    Integer ol_o_id;
    Integer ol_number;
    // stock_t::Key, none other than join key
  };
  // from order line
  Integer ol_i_id;
  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Varchar<24> ol_dist_info;
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
};