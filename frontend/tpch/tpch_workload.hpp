#pragma once
#include <gflags/gflags.h>
#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <unordered_map>
#include <vector>

#include "tpch_tables.hpp"

#include "../shared/logger/logger.hpp"

DECLARE_int32(tpch_scale_factor);

template <template <typename> class AdapterType>
struct TPCHWorkload {
   Logger& logger;
   AdapterType<part_t>& part;
   AdapterType<supplier_t>& supplier;
   AdapterType<partsupp_t>& partsupp;
   AdapterType<customerh_t>& customer;
   AdapterType<orders_t>& orders;
   AdapterType<lineitem_t>& lineitem;
   AdapterType<nation_t>& nation;
   AdapterType<region_t>& region;
   AdapterType<invoice_t>& invoice;

   // Per-spec §4.2.3: scale factors (counts in thousands, multiplied by SF at runtime)
   inline static Integer PART_SCALE = 200;      // 200K per SF
   inline static Integer SUPPLIER_SCALE = 10;    // 10K per SF
   inline static Integer CUSTOMER_SCALE = 150;   // 150K per SF
   inline static Integer ORDERS_SCALE = 1500;    // 1.5M per SF
   inline static Integer LINEITEM_SCALE = 6000;  // ~6M per SF (avg 4 lineitems per order)
   inline static Integer PARTSUPP_SCALE = 800;   // 800K per SF
   // Invoice scale: 2 × ORDERS_SCALE — ~3M per SF
   inline static Integer INVOICE_SCALE = 3000;
   inline static Integer NATION_COUNT = 25;
   inline static Integer REGION_COUNT = 5;

   Integer last_part_id;
   Integer last_supplier_id;
   Integer last_customer_id;
   Integer last_order_id;

   // Pre-populated by `prepopulate_order_dates` before lineitem loading so
   // lineitem dates can be generated relative to the correct orderdate
   // (§4.2.3). `loadOrders` then reads this map back when finalizing orders.
   std::unordered_map<Integer, Timestamp> order_dates;

   // Per-orderkey accumulator, populated as lineitems are generated and read
   // back by `loadOrders` to derive `o_totalprice` and `o_orderstatus`
   // (§4.2.3 derived fields).
   struct OrderAggregate {
      Numeric totalprice = 0;  // Σ l_extendedprice * (1 + l_tax) * (1 - l_discount)
      Integer line_count = 0;
      Integer ostatus_count = 0;  // count of l_linestatus == 'O'
   };
   std::unordered_map<Integer, OrderAggregate> order_aggregates;

   TPCHWorkload(AdapterType<part_t>& p,
                AdapterType<supplier_t>& s,
                AdapterType<partsupp_t>& ps,
                AdapterType<customerh_t>& c,
                AdapterType<orders_t>& o,
                AdapterType<lineitem_t>& l,
                AdapterType<nation_t>& n,
                AdapterType<region_t>& r,
                AdapterType<invoice_t>& inv,
                Logger& logger)
       : logger(logger),
         part(p),
         supplier(s),
         partsupp(ps),
         customer(c),
         orders(o),
         lineitem(l),
         nation(n),
         region(r),
         invoice(inv),
         last_part_id(0),
         last_supplier_id(0),
         last_customer_id(0),
         last_order_id(0)
   {
   }

   void load()
   {
      // Order matters per TPC-H §4.2.3:
      //  - `order_dates` must be populated before lineitem generation so each
      //    lineitem's dates derive from its order's orderdate.
      //  - Lineitems must be generated before `loadOrders` so that
      //    `order_aggregates` (totalprice, orderstatus) is finalized when the
      //    order record is materialized.
      //  - `last_customer_id` must be set (loadCustomer) before sampling
      //    custkeys for orders.
      loadPart();
      loadSupplier();
      loadCustomer();
      prepopulate_order_dates();
      loadPartsuppLineitem();
      loadOrders();
      loadInvoiceAndLinkLineitem();
      loadNation();
      loadRegion();
   }

   // Pre-fill `order_dates` for every orderkey that `loadOrders` will later
   // materialize. No order rows are inserted here — only the in-memory map.
   void prepopulate_order_dates(Integer start = 1,
                                Integer end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      for (Integer i = start; i <= end; i++) {
         Integer orderkey = orderkey_from_index(i);
         order_dates[orderkey] = Timestamp(urand(TPCH_STARTDATE, TPCH_ORDERS_ENDDATE));
      }
      last_order_id = orderkey_from_index(end);
   }

   // Fold one generated lineitem into the per-order accumulator.
   // Called inline from every lineitem insert site.
   void accumulate_for_order(Integer orderkey, const lineitem_t& l)
   {
      auto& agg = order_aggregates[orderkey];
      agg.line_count += 1;
      // §4.2.3: o_totalprice = Σ l_extendedprice * (1 + l_tax) * (1 - l_discount)
      agg.totalprice += l.l_extendedprice * (1.0 + l.l_tax) * (1.0 - l.l_discount);
      if (l.l_linestatus.length == 1 && l.l_linestatus.data[0] == 'O') agg.ostatus_count += 1;
   }

   void recover_last_ids()
   {
      std::cout << "Recovering last ids..." << std::endl;
      part.scanDesc(
          part_t::Key{std::numeric_limits<Integer>::max()},
          [&](const part_t::Key& k, const part_t&) {
             last_part_id = k.p_partkey;
             return false;
          },
          []() {});
      supplier.scanDesc(
          supplier_t::Key{std::numeric_limits<Integer>::max()},
          [&](const supplier_t::Key& k, const supplier_t&) {
             last_supplier_id = k.s_suppkey;
             return false;
          },
          []() {});
      customer.scanDesc(
          customerh_t::Key{std::numeric_limits<Integer>::max()},
          [&](const customerh_t::Key& k, const customerh_t&) {
             last_customer_id = k.c_custkey;
             return false;
          },
          []() {});
      orders.scanDesc(
          orders_t::Key{std::numeric_limits<Integer>::max()},
          [&](const orders_t::Key& k, const orders_t&) {
             last_order_id = k.o_orderkey;
             return false;
          },
          []() {});
      std::cout << "last_part_id: " << last_part_id << ", last_supplier_id: " << last_supplier_id
                << ", last_customer_id: " << last_customer_id << ", last_order_id: " << last_order_id
                << std::endl;
   }

   inline Integer getPartID() { return urand(1, last_part_id); }

   inline Integer getSupplierID() { return urand(1, last_supplier_id); }

   inline Integer getCustomerID() { return urand(1, last_customer_id); }

   inline Integer getOrderID() { return urand(1, last_order_id); }

   // Per spec §4.2.3: nation keys are 0-indexed (0..N_COUNT-1).
   inline Integer getNationID() { return urand(0, NATION_COUNT - 1); }

   inline Integer getRegionID() { return urand(0, REGION_COUNT - 1); }

   // ------------------------------------LOAD-------------------------------------------------

   void prepare() { logger.prepare(); }

   static void printProgress(std::string msg, Integer i, Integer start, Integer end)
   {
      auto scale = end - start + 1;
      if (scale < 100)
         return;
      if (i % 1000 == start % 1000 && FLAGS_log_progress) {
         double progress = (double)(i - start + 1) / scale * 100;
         std::cout << "\rLoading " << scale << " " << msg << ": " << progress
                   << "%------------------------------------";
      }
      if (i == end && scale > 100) {
         std::cout << "Loaded " << scale << " " << msg << " records." << std::endl;
      } else if (i == end && FLAGS_log_progress) {
         std::cout << std::endl;
      }
   }

   // Per spec §4.2.3: every third customer (ck % 3 == 0) is not assigned any order.
   // This lambda resamples until we get a valid custkey.
   auto generate_custkey_for_orders()
   {
      return [this]() {
         Integer ck;
         do {
            ck = urand(1, last_customer_id);
         } while (ck % 3 == 0);
         return ck;
      };
   }

   // Per spec §4.2.3: O_ORDERKEY is sparse — only the first 8 of each group of 32 keys are
   // populated initially. Sequential index i (1-based) maps to a sparse orderkey.
   static Integer orderkey_from_index(Integer i)
   {
      Integer group = (i - 1) / 8;
      Integer offset = (i - 1) % 8;
      return group * 32 + offset + 1;
   }

   void loadPart(std::function<void(const part_t::Key&, const part_t&)> insert_func,
                 Integer start,
                 Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(part_t::Key{i}, part_t::generateRandomRecord(i));
         printProgress("part", i, start, end);
      }
      last_part_id = end;
   }

   void loadPart(Integer start = 1, Integer end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPart([this](const part_t::Key& k, const part_t& v) { this->part.insert(k, v); }, start,
               end);
   }

   void loadSupplier(std::function<void(const supplier_t::Key&, const supplier_t&)> insert_func,
                     Integer start,
                     Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(supplier_t::Key{i},
                     supplier_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("supplier", i, start, end);
      }
      last_supplier_id = end;
   }

   void loadSupplier(Integer start = 1, Integer end = SUPPLIER_SCALE * FLAGS_tpch_scale_factor)
   {
      loadSupplier(
          [this](const supplier_t::Key& k, const supplier_t& v) { this->supplier.insert(k, v); },
          start, end);
   }

   void loadPartsuppLineitem(
       std::function<void(const partsupp_t::Key&, const partsupp_t&)> ps_insert_func,
       std::function<void(const lineitem_t::Key&, const lineitem_t&)> l_insert_func,
       Integer part_start,
       Integer part_end,
       Integer order_start,
       Integer order_end)
   {
      if (order_end > order_start)
         std::cout << "Generating and shuffling order keys..." << std::endl;
      std::vector<Integer> order_keys(order_end - order_start + 1);
      std::iota(order_keys.begin(), order_keys.end(), order_start);
      std::shuffle(order_keys.begin(), order_keys.end(), std::mt19937{std::random_device{}()});

      // Per spec §4.2.3: exactly 4 partsupp rows per part; avg 4 lineitems per order.
      const Integer partsupp_size = (PARTSUPP_SCALE / PART_SCALE) * (part_end - part_start + 1);
      const Integer lineitem_size = (LINEITEM_SCALE / ORDERS_SCALE) * (order_end - order_start + 1);
      auto current_order_key = order_keys.begin();
      // Per spec: each order has a random number of lineitems within [1..7].
      auto lineitem_cnt_in_order = urand(1, 7);
      int lineitem_number = 1;

      for (Integer i = part_start; i <= part_end; i++) {
         printProgress("parts of partsupp and lineitem", i, part_start, part_end);
         // Per spec §4.2.3: exactly 4 PARTSUPP rows per PART.
         const size_t supplier_cnt = 4;
         std::set<Integer> suppliers = {};
         while (suppliers.size() < supplier_cnt) {
            Integer supplier_id = urand(1, last_supplier_id);
            suppliers.insert(supplier_id);
         }
         for (auto& s : suppliers) {
            ps_insert_func(partsupp_t::Key{i, s}, partsupp_t::generateRandomRecord());
            Integer lineitem_cnt_ps = urand(0, lineitem_size / partsupp_size * 2);
            for (Integer l = 0; l < lineitem_cnt_ps; l++) {
               Integer okey = orderkey_from_index(*current_order_key);
               // Look up the order date so lineitem dates are generated relative to it.
               Timestamp o_orderdate = 0;
               auto it = order_dates.find(okey);
               if (it != order_dates.end())
                  o_orderdate = it->second;
               auto rec = lineitem_t::generateRandomRecord(i, s, o_orderdate,
                                                          part_t::computeRetailPrice(i));
               accumulate_for_order(okey, rec);
               l_insert_func(lineitem_t::Key{okey, lineitem_number}, rec);
               lineitem_number++;
               if (lineitem_number > lineitem_cnt_in_order) {
                  lineitem_number = 1;
                  current_order_key++;
                  lineitem_cnt_in_order = urand(1, 7);
                  if (current_order_key == order_keys.end()) {
                     current_order_key = order_keys.begin();
                  }
               }
            }
         }
      }

      // Fill any orders that have not yet received any lineitems.
      if (lineitem_number > 1 && current_order_key < order_keys.end()) {
         lineitem_number = 1;
         current_order_key++;
         lineitem_cnt_in_order = urand(1, 7);
      }
      auto rem_order_cnt = order_keys.end() - current_order_key;
      if (rem_order_cnt > 0)
         std::cout << rem_order_cnt << " orders left to fill out lineitems" << std::endl;
      auto orders_rem_start = current_order_key;
      for (; current_order_key < order_keys.end(); current_order_key++) {
         printProgress("orders of lineitems", current_order_key - orders_rem_start, 0,
                       order_keys.end() - orders_rem_start);
         load_lineitems_1order(l_insert_func, orderkey_from_index(*current_order_key));
      }
   }

   void loadPartsuppLineitem(Integer part_start = 1,
                             Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor,
                             Integer order_start = 1,
                             Integer order_end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadPartsuppLineitem(
          [this](const partsupp_t::Key& k, const partsupp_t& v) { this->partsupp.insert(k, v); },
          [this](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); },
          part_start, part_end, order_start, order_end);
   }

   void loadPartsupp(std::function<void(const partsupp_t::Key&, const partsupp_t&)> insert_func,
                     Integer part_start = 1,
                     Integer part_end = PART_SCALE * FLAGS_tpch_scale_factor)
   {
      // Pass an empty range for orders so no lineitems are generated.
      loadPartsuppLineitem(insert_func, [](const lineitem_t::Key&, const lineitem_t&) {},
                           part_start, part_end, last_order_id, last_order_id - 1);
   }

   void loadLineitem(std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_func,
                     Integer order_start,
                     Integer order_end)
   {
      for (Integer i = order_start; i <= order_end; i++) {
         load_lineitems_1order(insert_func, orderkey_from_index(i));
      }
   }

   int load_lineitems_1order(
       std::function<void(const lineitem_t::Key&, const lineitem_t&)> insert_func,
       Integer orderkey)
   {
      // Look up the order date for this orderkey.
      Timestamp o_orderdate = 0;
      auto it = order_dates.find(orderkey);
      if (it != order_dates.end())
         o_orderdate = it->second;

      // Per spec §4.2.3: each order has [1..7] lineitems.
      Integer lineitem_cnt = urand(1, 7);
      for (Integer j = 1; j <= lineitem_cnt; j++) {
         auto p = urand(1, last_part_id);
         auto s = urand(1, last_supplier_id);
         auto start_key = partsupp_t::Key{p, s};
         bool found = false;
         partsupp.scan(
             start_key,
             [&](const partsupp_t::Key& k, const partsupp_t&) {
                p = k.ps_partkey;
                s = k.ps_suppkey;
                found = true;
                return false;
             },
             []() {});
         if (!found) {
            partsupp.scanDesc(
                start_key,
                [&](const partsupp_t::Key& k, const partsupp_t&) {
                   p = k.ps_partkey;
                   s = k.ps_suppkey;
                   found = true;
                   return false;
                },
                []() {});
         }
         assert(found);
         auto rec = lineitem_t::generateRandomRecord(p, s, o_orderdate,
                                                     part_t::computeRetailPrice(p));
         accumulate_for_order(orderkey, rec);
         insert_func(lineitem_t::Key{orderkey, j}, rec);
      }
      return lineitem_cnt;
   }

   void loadLineitem(Integer order_start = 1,
                     Integer order_end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadLineitem(
          [&](const lineitem_t::Key& k, const lineitem_t& v) { this->lineitem.insert(k, v); },
          order_start, order_end);
   }

   void loadCustomer(std::function<void(const customerh_t::Key&, const customerh_t&)> insert_func,
                     Integer start,
                     Integer end)
   {
      for (Integer i = start; i <= end; i++) {
         insert_func(customerh_t::Key{i},
                     customerh_t::generateRandomRecord([this]() { return this->getNationID(); }));
         printProgress("customer", i, start, end);
      }
      last_customer_id = end;
   }

   void loadCustomer(Integer start = 1, Integer end = CUSTOMER_SCALE * FLAGS_tpch_scale_factor)
   {
      loadCustomer(
          [this](const customerh_t::Key& k, const customerh_t& v) { this->customer.insert(k, v); },
          start, end);
   }

   // Per spec §4.2.3: O_ORDERKEY is sparse — only 8 of each 32 consecutive keys are populated.
   // Sequential index i (1-based) is mapped to a sparse orderkey via orderkey_from_index().
   // Per spec: every third customer (custkey % 3 == 0) receives no orders.
   void loadOrders(std::function<void(const orders_t::Key&, const orders_t&)> insert_func,
                   Integer start,
                   Integer end)
   {
      auto custkey_gen = generate_custkey_for_orders();
      for (Integer i = start; i <= end; i++) {
         Integer orderkey = orderkey_from_index(i);
         // Orderdate was pre-populated by `prepopulate_order_dates`; aggregates
         // were populated by `loadPartsuppLineitem` / `loadLineitem` while
         // generating the order's lineitems.
         Timestamp orderdate = order_dates[orderkey];
         auto agg_it = order_aggregates.find(orderkey);
         Numeric totalprice = (agg_it != order_aggregates.end()) ? agg_it->second.totalprice : 0;
         Varchar<1> orderstatus = derive_orderstatus(agg_it);
         orders_t rec = orders_t::generateRandomRecord(custkey_gen, orderdate, orderstatus, totalprice);
         insert_func(orders_t::Key{orderkey}, rec);
         printProgress("orders", i, start, end);
      }
      last_order_id = orderkey_from_index(end);
   }

   // §4.2.3: o_orderstatus = 'O' if all lineitems are 'O', 'F' if all are 'F',
   // 'P' otherwise. Orders with no lineitems (legitimate per spec) default to 'F'.
   Varchar<1> derive_orderstatus(typename std::unordered_map<Integer, OrderAggregate>::const_iterator agg_it) const
   {
      if (agg_it == order_aggregates.end() || agg_it->second.line_count == 0)
         return Varchar<1>("F");
      Integer total = agg_it->second.line_count;
      Integer o    = agg_it->second.ostatus_count;
      if (o == total) return Varchar<1>("O");
      if (o == 0)    return Varchar<1>("F");
      return Varchar<1>("P");
   }

   void loadOrders(Integer start = 1, Integer end = ORDERS_SCALE * FLAGS_tpch_scale_factor)
   {
      loadOrders(
          [this](const orders_t::Key& k, const orders_t& v) { this->orders.insert(k, v); }, start,
          end);
   }

   // Per spec: nation rows are fixed, not randomly generated. nation_t::NATIONS[] holds them.
   void loadNation(std::function<void(const nation_t::Key&, const nation_t&)> insert_func)
   {
      for (int i = 0; i < nation_t::NATION_COUNT; i++) {
         auto& d = nation_t::NATIONS[i];
         insert_func(nation_t::Key{d.key}, nation_t::fromData(d));
      }
   }

   void loadNation()
   {
      loadNation([this](const nation_t::Key& k, const nation_t& v) { this->nation.insert(k, v); });
   }

   // Per spec: region rows are fixed, not randomly generated. region_t::REGIONS[] holds them.
   void loadRegion(std::function<void(const region_t::Key&, const region_t&)> insert_func)
   {
      for (int i = 0; i < region_t::REGION_COUNT; i++) {
         auto& d = region_t::REGIONS[i];
         insert_func(region_t::Key{d.key}, region_t::fromData(d));
      }
   }

   void loadRegion()
   {
      loadRegion([this](const region_t::Key& k, const region_t& v) { this->region.insert(k, v); });
   }

   // Generate invoice records and rewrite each lineitem with its assigned
   // `l_invoicekey`.  Must be called after `loadOrders` and
   // `loadPartsuppLineitem` have both committed, since we read back
   // `order_aggregates` (for per-lineitem revenue) and the lineitem adapter
   // (for the rewrite pass).
   //
   // Algorithm (plan §4):
   //   1. Scan orders and group by custkey, recording (orderdate, orderkey).
   //   2. Scan lineitems grouped by orderkey (natural key order), accumulate
   //      per-order into a per-custkey list of (orderkey, linenumber, lineitem).
   //   3. For each customer: allocate 2 * N invoice slots.
   //      Walk lineitems in (orderdate, orderkey, linenumber) order, assigning
   //      each to the current invoice; close and advance after
   //      total_lineitems / num_invoices lineitems.
   //   4. Insert invoice rows; erase+reinsert each lineitem with its invoicekey.
   //
   // Secondary indexes for Structure 1 (custkey-sorted Invoice and custkey-
   // sorted Orders for sibling merge join) are a TODO: no secondary-index
   // adapter pattern exists yet in this codebase.  When added, populate them
   // here after inserting each invoice / order record.
   void loadInvoiceAndLinkLineitem()
   {
      std::cout << "Building customer→orders and customer→lineitems maps..." << std::endl;

      // Per-order metadata we need when sorting and closing invoices.
      struct OrderMeta {
         Timestamp orderdate;
         Integer   orderkey;
      };
      // Per-lineitem snapshot stored in memory for the rewrite pass.
      struct LineitemEntry {
         Integer     orderkey;
         Integer     linenumber;
         Timestamp   orderdate;  // copied from parent order for sort key
         lineitem_t  rec;
      };

      // custkey → list of orders, sorted later by orderdate
      std::unordered_map<Integer, std::vector<OrderMeta>> cust_orders;
      // custkey → list of lineitems (unsorted; we sort inside the loop)
      std::unordered_map<Integer, std::vector<LineitemEntry>> cust_lineitems;

      // --- Pass 1: scan orders to build cust_orders map ---
      orders.scan(
          orders_t::Key{std::numeric_limits<Integer>::min()},
          [&](const orders_t::Key& ok, const orders_t& o) {
             cust_orders[o.o_custkey].push_back({o.o_orderdate, ok.o_orderkey});
             return true;
          },
          []() {});

      // --- Pass 2: scan lineitems, joining to order date via cust_orders ---
      // Build a temporary orderkey→custkey map for O(1) lookup per lineitem.
      std::unordered_map<Integer, Integer> order_to_cust;
      order_to_cust.reserve(cust_orders.size());
      for (auto& [ck, oms] : cust_orders) {
         for (auto& om : oms)
            order_to_cust[om.orderkey] = ck;
      }

      lineitem.scan(
          lineitem_t::Key{std::numeric_limits<Integer>::min(),
                          std::numeric_limits<Integer>::min()},
          [&](const lineitem_t::Key& lk, const lineitem_t& l) {
             auto it = order_to_cust.find(lk.l_orderkey);
             if (it == order_to_cust.end())
                return true;  // orphan lineitem — skip
             Integer ck = it->second;
             // Look up orderdate for this orderkey (needed for sort key).
             Timestamp odate = 0;
             auto dt = order_dates.find(lk.l_orderkey);
             if (dt != order_dates.end())
                odate = dt->second;
             cust_lineitems[ck].push_back(
                 {lk.l_orderkey, lk.l_linenumber, odate, l});
             return true;
          },
          []() {});

      // --- Pass 3: per-customer invoice generation ---
      // Global invoice key counter, 1-based.
      Integer next_invoicekey = 1;

      long customers_processed = 0;
      long invoices_inserted   = 0;
      long lineitems_rewritten = 0;

      for (auto& [custkey, order_metas] : cust_orders) {
         auto& items = cust_lineitems[custkey];  // may be empty
         Integer N = static_cast<Integer>(order_metas.size());
         // 2 invoices per order
         Integer num_invoices = 2 * N;

         // Sort lineitems by (orderdate, orderkey, linenumber) so that
         // lineitems from the same order cluster together and earlier orders
         // appear first — matching the plan §4 traversal order.
         std::sort(items.begin(), items.end(),
                   [](const LineitemEntry& a, const LineitemEntry& b) {
                      if (a.orderdate != b.orderdate) return a.orderdate < b.orderdate;
                      if (a.orderkey  != b.orderkey)  return a.orderkey  < b.orderkey;
                      return a.linenumber < b.linenumber;
                   });

         Integer total_lineitems = static_cast<Integer>(items.size());
         // Target lineitems per invoice (≥ 1 to avoid division by zero).
         Integer target_per_invoice =
             (num_invoices > 0 && total_lineitems > 0)
                 ? std::max(Integer(1), total_lineitems / num_invoices)
                 : 1;

         // State for the current open invoice.
         Integer first_invoice_of_cust = next_invoicekey;
         Integer inv_idx    = 0;       // which invoice slot we are filling
         Integer inv_count  = 0;       // lineitems assigned to current invoice
         Integer invoicekey = next_invoicekey;
         next_invoicekey   += num_invoices;

         // Collect per-invoice metadata for final insert.
         struct InvoiceDraft {
            Integer   invoicekey;
            Timestamp max_orderdate;
            Numeric   totaldue;
         };
         std::vector<InvoiceDraft> drafts;
         drafts.reserve(static_cast<size_t>(num_invoices));
         // Initialise first draft.
         drafts.push_back({invoicekey, 0, 0.0});

         // Assign lineitems to invoices; rewrite each with its invoicekey.
         for (auto& entry : items) {
            // Close current invoice and open next if threshold reached.
            if (inv_count >= target_per_invoice && inv_idx + 1 < num_invoices) {
               ++inv_idx;
               invoicekey = first_invoice_of_cust + inv_idx;
               drafts.push_back({invoicekey, 0, 0.0});
               inv_count     = 0;
            }

            auto& draft = drafts.back();
            // Accumulate invoice totals.
            draft.totaldue +=
                entry.rec.l_extendedprice
                * (1.0 - static_cast<double>(entry.rec.l_discount))
                * (1.0 + static_cast<double>(entry.rec.l_tax));
            if (entry.orderdate > draft.max_orderdate)
               draft.max_orderdate = entry.orderdate;

            // Rewrite lineitem: erase old, insert with assigned invoicekey.
            lineitem_t updated = entry.rec;
            updated.l_invoicekey = invoicekey;
            lineitem_t::Key lk{entry.orderkey, entry.linenumber};
            lineitem.erase(lk);
            lineitem.insert(lk, updated);
            ++inv_count;
            ++lineitems_rewritten;
         }

         // Insert invoice records for all drafts allocated to this customer.
         // Any trailing invoice slots with no lineitems get zero totaldue and
         // max_orderdate = 0 (edge case: customer has more invoice slots than
         // lineitems).  Emit them to satisfy the "every invoicekey reachable"
         // invariant checked by the load-test.
         for (Integer slot = 0; slot < num_invoices; ++slot) {
            Integer ikey = first_invoice_of_cust + slot;
            Timestamp idate = 0;
            Numeric   idue  = 0.0;
            if (slot < static_cast<Integer>(drafts.size())) {
               idate = drafts[static_cast<size_t>(slot)].max_orderdate;
               idue  = drafts[static_cast<size_t>(slot)].totaldue;
            }
            // i_invoicedate = max(orderdate of bundled orders) + uniform(7,60)
            idate += urand(7, 60);

            // i_status weighted random: 'P' 70%, 'O' 25%, 'L' 5%
            Integer roll = urand(1, 100);
            Varchar<1> status(roll <= 70 ? "P" : (roll <= 95 ? "O" : "L"));

            invoice_t rec{custkey, idate, idue, status,
                          randomastring<25>(0, 25),
                          randomastring<79>(0, 79)};
            invoice.insert(invoice_t::Key{ikey}, rec);
            ++invoices_inserted;

            // TODO(secondary-index): when a custkey-sorted secondary index on
            // Invoice is added (needed for Structure 1 sibling merge join),
            // insert into it here alongside the primary insert.
         }

         // TODO(secondary-index): when a custkey-sorted secondary index on
         // Orders is added (needed for Structure 1 sibling merge join), scan
         // this customer's orders and insert into the secondary index here.

         inspect_produced("customers with invoices", customers_processed);
      }

      std::cout << "\nInserted " << invoices_inserted << " invoices, rewrote "
                << lineitems_rewritten << " lineitems." << std::endl;
   }

   void log_sizes()
   {
      std::map<std::string, double> sizes = {{"part", part.size()},
                                             {"supplier", supplier.size()},
                                             {"partsupp", partsupp.size()},
                                             {"customer", customer.size()},
                                             {"orders", orders.size()},
                                             {"lineitem", lineitem.size()},
                                             {"nation", nation.size()},
                                             {"region", region.size()},
                                             {"invoice", invoice.size()}};
      logger.log_sizes(sizes);
   }

   static void inspect_produced(const std::string& msg, long& produced)
   {
      if (produced % 1000 == 0 && FLAGS_log_progress) {
         std::cout << "\r" << msg << (double)produced / 1000
                   << "k------------------------------------";
      }
      produced++;
   }
};
