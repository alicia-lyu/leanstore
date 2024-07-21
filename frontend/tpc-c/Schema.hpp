#pragma once
#include <ostream>
#include "../shared/Types.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
struct warehouse_t {
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      Integer w_id;
   };
   Varchar<10> w_name;
   Varchar<20> w_street_1;
   Varchar<20> w_street_2;
   Varchar<20> w_city;
   Varchar<2> w_state;
   Varchar<9> w_zip;
   Numeric w_tax;
   Numeric w_ytd;
   // -------------------------------------------------------------------------------------
   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.w_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.w_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::w_id); };
};

struct district_t {
   static constexpr int id = 1;
   struct Key {
      static constexpr int id = 1;
      Integer d_w_id;
      Integer d_id;
   };
   Varchar<10> d_name;
   Varchar<20> d_street_1;
   Varchar<20> d_street_2;
   Varchar<20> d_city;
   Varchar<2> d_state;
   Varchar<9> d_zip;
   Numeric d_tax;
   Numeric d_ytd;
   Integer d_next_o_id;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.d_w_id);
      pos += fold(out + pos, record.d_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.d_w_id);
      pos += unfold(in + pos, record.d_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::d_w_id) + sizeof(Key::d_id); };
};

struct customer_t { // 712B
   static constexpr int id = 2;
   struct Key {
      static constexpr int id = 2;
      Integer c_w_id;
      Integer c_d_id;
      Integer c_id;
   };
   Varchar<16> c_first;
   Varchar<2> c_middle;
   Varchar<16> c_last;
   Varchar<20> c_street_1;
   Varchar<20> c_street_2;
   Varchar<20> c_city;
   Varchar<2> c_state;
   Varchar<9> c_zip;
   Varchar<16> c_phone;
   Timestamp c_since;
   Varchar<2> c_credit;
   Numeric c_credit_lim;
   Numeric c_discount;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   Numeric c_delivery_cnt;
   Varchar<500> c_data;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.c_w_id);
      pos += fold(out + pos, record.c_d_id);
      pos += fold(out + pos, record.c_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.c_w_id);
      pos += unfold(in + pos, record.c_d_id);
      pos += unfold(in + pos, record.c_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::c_w_id) + sizeof(Key::c_d_id) + sizeof(Key::c_id); };
};

struct customer_wdl_t {
   static constexpr int id = 3;
   struct Key {
      static constexpr int id = 3;
      Integer c_w_id;
      Integer c_d_id;
      Varchar<16> c_last;
      Varchar<16> c_first;
   };
   Integer c_id;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.c_w_id);
      pos += fold(out + pos, record.c_d_id);
      pos += fold(out + pos, record.c_last);
      pos += fold(out + pos, record.c_first);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.c_w_id);
      pos += unfold(in + pos, record.c_d_id);
      pos += unfold(in + pos, record.c_last);
      pos += unfold(in + pos, record.c_first);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::c_w_id) + sizeof(Key::c_d_id) + sizeof(Key::c_last) + sizeof(Key::c_first); };
};

struct history_t {
   static constexpr int id = 4;
   struct Key {
      static constexpr int id = 4;
      Integer thread_id;
      Integer h_pk;
   };
   Integer h_c_id;
   Integer h_c_d_id;
   Integer h_c_w_id;
   Integer h_d_id;
   Integer h_w_id;
   Timestamp h_date;
   Numeric h_amount;
   Varchar<24> h_data;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.thread_id);
      pos += fold(out + pos, record.h_pk);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.thread_id);
      pos += unfold(in + pos, record.h_pk);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::thread_id) + sizeof(Key::h_pk); };
};

struct neworder_t {
   static constexpr int id = 5;
   struct Key {
      static constexpr int id = 5;
      Integer no_w_id;
      Integer no_d_id;
      Integer no_o_id;
   };

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.no_w_id);
      pos += fold(out + pos, record.no_d_id);
      pos += fold(out + pos, record.no_o_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.no_w_id);
      pos += unfold(in + pos, record.no_d_id);
      pos += unfold(in + pos, record.no_o_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::no_w_id) + sizeof(Key::no_d_id) + sizeof(Key::no_o_id); };
};

struct order_t {
   static constexpr int id = 6;
   struct Key {
      static constexpr int id = 6;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_id;
   };
   Integer o_c_id;
   Timestamp o_entry_d;
   Integer o_carrier_id;
   Numeric o_ol_cnt;
   Numeric o_all_local;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.o_w_id);
      pos += fold(out + pos, record.o_d_id);
      pos += fold(out + pos, record.o_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.o_w_id);
      pos += unfold(in + pos, record.o_d_id);
      pos += unfold(in + pos, record.o_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::o_w_id) + sizeof(Key::o_d_id) + sizeof(Key::o_id); };
};

struct order_wdc_t {
   static constexpr int id = 7;
   struct Key {
      static constexpr int id = 7;
      Integer o_w_id;
      Integer o_d_id;
      Integer o_c_id;
      Integer o_id;
   };

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.o_w_id);
      pos += fold(out + pos, record.o_d_id);
      pos += fold(out + pos, record.o_c_id);
      pos += fold(out + pos, record.o_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.o_w_id);
      pos += unfold(in + pos, record.o_d_id);
      pos += unfold(in + pos, record.o_c_id);
      pos += unfold(in + pos, record.o_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::o_w_id) + sizeof(Key::o_d_id) + sizeof(Key::o_c_id) + sizeof(Key::o_id); };
};

struct orderline_t {
   static constexpr int id = 8;
   struct Key {
      static constexpr int id = 8;
      Integer ol_w_id;
      Integer ol_d_id;
      Integer ol_o_id;
      Integer ol_number;

      friend std::ostream& operator<<(std::ostream& os, const Key& record)
      {
         os << "ol_w_id: " << record.ol_w_id << ", ol_d_id: " << record.ol_d_id << ", ol_o_id: " << record.ol_o_id << ", ol_number: " << record.ol_number;
         return os;
      }
   };
   Integer ol_i_id;
   Integer ol_supply_w_id;
   Timestamp ol_delivery_d;
   Numeric ol_quantity;
   Numeric ol_amount;
   Varchar<24> ol_dist_info;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.ol_w_id);
      pos += fold(out + pos, record.ol_d_id);
      pos += fold(out + pos, record.ol_o_id);
      pos += fold(out + pos, record.ol_number);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.ol_w_id);
      pos += unfold(in + pos, record.ol_d_id);
      pos += unfold(in + pos, record.ol_o_id);
      pos += unfold(in + pos, record.ol_number);
      return pos;
   }
   static constexpr unsigned maxFoldLength()
   {
      return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
   };

   friend bool operator==(const orderline_t& lhs, const orderline_t& rhs)
   {
      return lhs.ol_i_id == rhs.ol_i_id && lhs.ol_supply_w_id == rhs.ol_supply_w_id && lhs.ol_delivery_d == rhs.ol_delivery_d && lhs.ol_quantity == rhs.ol_quantity && lhs.ol_amount == rhs.ol_amount && lhs.ol_dist_info == rhs.ol_dist_info;
   }

   friend bool operator!=(const orderline_t& lhs, const orderline_t& rhs)
   {
      return !(lhs == rhs);
   }

   friend std::ostream& operator<<(std::ostream& os, const orderline_t& record)
   {
      os << "ol_i_id: " << record.ol_i_id << ", ol_supply_w_id: " << record.ol_supply_w_id << ", ol_delivery_d: " << record.ol_delivery_d << ", ol_quantity: " << record.ol_quantity << ", ol_amount: " << record.ol_amount << ", ol_dist_info: " << record.ol_dist_info.toString();
      return os;
   }
};

struct item_t {
   static constexpr int id = 9;
   struct Key {
      static constexpr int id = 9;
      Integer i_id;
   };
   Integer i_im_id;
   Varchar<24> i_name;
   Numeric i_price;
   Varchar<50> i_data;

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.i_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.i_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::i_id); };
};

struct stock_t {
   static constexpr int id = 10;
   struct Key {
      static constexpr int id = 10;
      Integer s_w_id;
      Integer s_i_id;

      friend std::ostream& operator<<(std::ostream& os, const Key& record)
      {
         os << "s_w_id: " << record.s_w_id << ", s_i_id: " << record.s_i_id;
         return os;
      }
   };
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

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& record)
   {
      unsigned pos = 0;
      pos += fold(out + pos, record.s_w_id);
      pos += fold(out + pos, record.s_i_id);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& record)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, record.s_w_id);
      pos += unfold(in + pos, record.s_i_id);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::s_w_id) + sizeof(Key::s_i_id); };

   friend bool operator==(const stock_t& lhs, const stock_t& rhs)
   {
      return lhs.s_quantity == rhs.s_quantity && lhs.s_dist_01 == rhs.s_dist_01 && lhs.s_dist_02 == rhs.s_dist_02 && lhs.s_dist_03 == rhs.s_dist_03 && lhs.s_dist_04 == rhs.s_dist_04 && lhs.s_dist_05 == rhs.s_dist_05 && lhs.s_dist_06 == rhs.s_dist_06 && lhs.s_dist_07 == rhs.s_dist_07 && lhs.s_dist_08 == rhs.s_dist_08 && lhs.s_dist_09 == rhs.s_dist_09 && lhs.s_dist_10 == rhs.s_dist_10 && lhs.s_ytd == rhs.s_ytd && lhs.s_order_cnt == rhs.s_order_cnt && lhs.s_remote_cnt == rhs.s_remote_cnt && lhs.s_data == rhs.s_data;
   }

   friend bool operator!=(const stock_t& lhs, const stock_t& rhs)
   {
      return !(lhs == rhs);
   }

   friend std::ostream& operator<<(std::ostream& os, const stock_t& record)
   {
      os << "s_quantity: " << record.s_quantity << ", s_dist_01: " << record.s_dist_01.toString() << ", s_dist_02: " << record.s_dist_02.toString() << ", s_dist_03: " << record.s_dist_03.toString() << ", s_dist_04: " << record.s_dist_04.toString() << ", s_dist_05: " << record.s_dist_05.toString() << ", s_dist_06: " << record.s_dist_06.toString() << ", s_dist_07: " << record.s_dist_07.toString() << ", s_dist_08: " << record.s_dist_08.toString() << ", s_dist_09: " << record.s_dist_09.toString() << ", s_dist_10: " << record.s_dist_10.toString() << ", s_ytd: " << record.s_ytd << ", s_order_cnt: " << record.s_order_cnt << ", s_remote_cnt: " << record.s_remote_cnt << ", s_data: " << record.s_data.toString();
      return os;
   }
};
