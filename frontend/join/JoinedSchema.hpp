#pragma once
#include <ostream>
#include "../shared/Types.hpp"
#include "Exceptions.hpp"
#include "../tpc-c/Schema.hpp"

// Key is shared across different choices of included columns

struct orderline_secondary_base_t {
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
  orderline_secondary_base_t() = default;

  template <class T> static unsigned foldKey(uint8_t *out, const T &key) {
    unsigned pos = 0;
    pos += fold(out + pos, key.ol_w_id);
    pos += fold(out + pos, key.ol_i_id);
    pos += fold(out + pos, key.ol_d_id);
    pos += fold(out + pos, key.ol_o_id);
    pos += fold(out + pos, key.ol_number);
    return pos;
  }

  template <class T> static unsigned foldPKey(uint8_t *out, const T &key) { // Used when guiding primary index's scanner
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
    pos += unfold(in + pos, key.ol_d_id);
    pos += unfold(in + pos, key.ol_o_id);
    pos += unfold(in + pos, key.ol_number);
    return pos;
  }

  static constexpr unsigned maxFoldLength() {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_i_id) +
           sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) +
           sizeof(Key::ol_number);
  };

  static constexpr unsigned joinKeyLength() {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_i_id);
  }

  static constexpr unsigned primaryKeyLength() {
    return 0 + sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
  }
};

struct ol_sec0_t;

struct ol_sec1_t: public orderline_secondary_base_t {
  using orderline_secondary_base_t::id;
  using Key = orderline_secondary_base_t::Key;

  ol_sec1_t(Integer ol_supply_w_id, Timestamp ol_delivery_d, Numeric ol_quantity, Numeric ol_amount, Varchar<24> ol_dist_info)
      : ol_supply_w_id(ol_supply_w_id), ol_delivery_d(ol_delivery_d), ol_quantity(ol_quantity), ol_amount(ol_amount), ol_dist_info(ol_dist_info) {}

  ol_sec1_t() = default;

  ol_sec1_t(const ol_sec0_t&) {
    UNREACHABLE(); // To suppress compiler warning
  }

  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Varchar<24> ol_dist_info;

  friend std::ostream& operator<<(std::ostream& os, const ol_sec1_t& record) {
    os << ", supply_w_id: " << record.ol_supply_w_id
       << ", delivery_d: " << record.ol_delivery_d << ", quantity: " << record.ol_quantity << ", amount: " << record.ol_amount << ", dist_info: " << record.ol_dist_info.toString();
    return os;
  }
};

struct ol_sec0_t: public orderline_secondary_base_t {
  using orderline_secondary_base_t::id;
  using Key = orderline_secondary_base_t::Key;

  ol_sec0_t() = default;

  ol_sec0_t(const ol_sec1_t&) {}

  friend std::ostream& operator<<(std::ostream& os, const ol_sec0_t&) {
    os << "ol_sec_key_only";
    return os;
  }
};

struct joined_base_t {
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

    Key(Integer w_id, Integer i_id, Integer ol_d_id, Integer ol_o_id, Integer ol_number) : w_id(w_id), i_id(i_id), ol_d_id(ol_d_id), ol_o_id(ol_o_id), ol_number(ol_number) {}

    Key() = default;

    Key(const orderline_secondary_base_t::Key& ol_key) : w_id(ol_key.ol_w_id), i_id(ol_key.ol_i_id), ol_d_id(ol_key.ol_d_id), ol_o_id(ol_key.ol_o_id), ol_number(ol_key.ol_number) {}

    friend std::ostream& operator<<(std::ostream& os, const Key& key) {
      os << "w_id: " << key.w_id << ", i_id: " << key.i_id
         << ", d_id: " << key.ol_d_id << ", o_id: " << key.ol_o_id
         << ", number: " << key.ol_number;
      return os;
    }
  };

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

struct joined_selected_t;

struct joined1_t: public joined_base_t {
  using joined_base_t::id;
  using Key = joined_base_t::Key;

  joined1_t(Integer ol_supply_w_id, Timestamp ol_delivery_d, Numeric ol_quantity, Numeric ol_amount, Numeric s_quantity, Varchar<24> s_dist_01, Varchar<24> s_dist_02, Varchar<24> s_dist_03, Varchar<24> s_dist_04, Varchar<24> s_dist_05, Varchar<24> s_dist_06, Varchar<24> s_dist_07, Varchar<24> s_dist_08, Varchar<24> s_dist_09, Varchar<24> s_dist_10, Numeric s_ytd, Numeric s_order_cnt, Numeric s_remote_cnt, Varchar<50> s_data)
      : ol_supply_w_id(ol_supply_w_id), ol_delivery_d(ol_delivery_d), ol_quantity(ol_quantity), ol_amount(ol_amount), s_quantity(s_quantity), s_dist_01(s_dist_01), s_dist_02(s_dist_02), s_dist_03(s_dist_03), s_dist_04(s_dist_04), s_dist_05(s_dist_05), s_dist_06(s_dist_06), s_dist_07(s_dist_07), s_dist_08(s_dist_08), s_dist_09(s_dist_09), s_dist_10(s_dist_10), s_ytd(s_ytd), s_order_cnt(s_order_cnt), s_remote_cnt(s_remote_cnt), s_data(s_data) {}

  joined1_t() = default;

  // from order line
  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
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

  friend std::ostream& operator<<(std::ostream& os, const joined1_t& record) {
    os << "joined1_t: supply_w_id: " << record.ol_supply_w_id
       << ", delivery_d: " << record.ol_delivery_d << ", quantity: " << record.ol_quantity << ", amount: " << record.ol_amount << ", s_quantity: " << record.s_quantity << ", s_dist_01: " << record.s_dist_01.toString() << ", s_dist_02: " << record.s_dist_02.toString() << ", s_dist_03: " << record.s_dist_03.toString() << ", s_dist_04: " << record.s_dist_04.toString() << ", s_dist_05: " << record.s_dist_05.toString() << ", s_dist_06: " << record.s_dist_06.toString() << ", s_dist_07: " << record.s_dist_07.toString() << ", s_dist_08: " << record.s_dist_08.toString() << ", s_dist_09: " << record.s_dist_09.toString() << ", s_dist_10: " << record.s_dist_10.toString() << ", s_ytd: " << record.s_ytd << ", s_order_cnt: " << record.s_order_cnt << ", s_remote_cnt: " << record.s_remote_cnt << ", s_data: " << record.s_data.toString();
    return os;
  }

  joined_selected_t toSelected(const Key& key) const;

  joined_selected_t expand(const Key&, const stock_t&, const orderline_t&);
};

struct joined_selected_t: public joined_base_t {
  using joined_base_t::id;
  using Key = joined_base_t::Key;

  joined_selected_t(Integer ol_supply_w_id, Timestamp ol_delivery_d, Numeric ol_quantity, Numeric ol_amount, Numeric s_quantity, Varchar<24> s_dist, Numeric s_ytd, Numeric s_order_cnt, Numeric s_remote_cnt, Varchar<50> s_data)
      : ol_supply_w_id(ol_supply_w_id), ol_delivery_d(ol_delivery_d), ol_quantity(ol_quantity), ol_amount(ol_amount), s_quantity(s_quantity), s_dist(s_dist), s_ytd(s_ytd), s_order_cnt(s_order_cnt), s_remote_cnt(s_remote_cnt), s_data(s_data) {}

  joined_selected_t() = default;

  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Numeric s_quantity;
  Varchar<24> s_dist; // s_dist_0x, x = Key.ol_d_id
  Numeric s_ytd;
  Numeric s_order_cnt;
  Numeric s_remote_cnt;
  Varchar<50> s_data;

  friend std::ostream& operator<<(std::ostream& os, const joined_selected_t& rec) {
    os << "joined_selected: supply_w_id: " << rec.ol_supply_w_id << ", delivery_d: " << rec.ol_delivery_d << ", quantity: " << rec.ol_quantity << ", amount: " << rec.ol_amount << ", s_quantity: " << rec.s_quantity << ", s_dist: " << rec.s_dist.toString() << ", s_ytd: " << rec.s_ytd << ", s_order_cnt: " << rec.s_order_cnt << ", s_remote_cnt: " << rec.s_remote_cnt << ", s_data: " << rec.s_data.toString();
    return os;
  }

  joined_selected_t toSelected() const {
    return *this;
  }

  joined_selected_t expand(const Key&, const stock_t&, const orderline_t&) {
    UNREACHABLE(); // Only to suppress warning
  }
};

joined_selected_t joined1_t::toSelected(const Key& key) const {
  Varchar<24> s_dist;
  switch (key.ol_d_id) {
    case 1:
      s_dist = s_dist_01;
      break;
    case 2:
      s_dist = s_dist_02;
      break;
    case 3:
      s_dist = s_dist_03;
      break;
    case 4:
      s_dist = s_dist_04;
      break;
    case 5:
      s_dist = s_dist_05;
      break;
    case 6:
      s_dist = s_dist_06;
      break;
    case 7:
      s_dist = s_dist_07;
      break;
    case 8:
      s_dist = s_dist_08;
      break;
    case 9:
      s_dist = s_dist_09;
      break;
    case 10:
      s_dist = s_dist_10;
      break;
    default:
      throw std::runtime_error("Invalid ol_d_id " + std::to_string(key.ol_d_id));
  }
  return joined_selected_t(
    ol_supply_w_id,
    ol_delivery_d,
    ol_quantity,
    ol_amount,
    s_quantity,
    s_dist,
    s_ytd,
    s_order_cnt,
    s_remote_cnt,
    s_data
  );
};

joined_selected_t joined1_t::expand(const Key&, const stock_t&, const orderline_t&) {
  UNREACHABLE(); // Only to suppress warning
}

struct joined0_t: public joined_base_t {
  using joined_base_t::id;
  using Key = joined_base_t::Key;

  joined0_t() = default;

  joined0_t(const joined1_t&) {}

  friend std::ostream& operator<<(std::ostream& os, const joined0_t&) {
    os << "joined_ols_key_only";
    return os;
  }

  joined_selected_t toSelected(const joined0_t::Key&) const {
    UNREACHABLE(); // Only to suppress warning
    // If one really needs joined_selected_t, lookups into base tables are needed
  }

  joined_selected_t expand(const Key& key, const stock_t& stock, const orderline_t& orderline) {
    Varchar<24> s_dist;
    switch (key.ol_d_id) {
      case 1:
        s_dist = stock.s_dist_01;
        break;
      case 2:
        s_dist = stock.s_dist_02;
        break;
      case 3:
        s_dist = stock.s_dist_03;
        break;
      case 4:
        s_dist = stock.s_dist_04;
        break;
      case 5:
        s_dist = stock.s_dist_05;
        break;
      case 6:
        s_dist = stock.s_dist_06;
        break;
      case 7:
        s_dist = stock.s_dist_07;
        break;
      case 8:
        s_dist = stock.s_dist_08;
        break;
      case 9:
        s_dist = stock.s_dist_09;
        break;
      case 10:
        s_dist = stock.s_dist_10;
        break;
      default:
        throw std::runtime_error("Invalid ol_d_id " + std::to_string(key.ol_d_id));
    };
    return joined_selected_t(
      orderline.ol_supply_w_id,
      orderline.ol_delivery_d,
      orderline.ol_quantity,
      orderline.ol_amount,
      stock.s_quantity,
      s_dist,
      stock.s_ytd,
      stock.s_order_cnt,
      stock.s_remote_cnt,
      stock.s_data
    );
  }
};