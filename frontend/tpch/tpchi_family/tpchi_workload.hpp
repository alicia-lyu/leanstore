#pragma once

// Invoice-extended TPC-H workload.
//
// TPCHIWorkload<AdapterType> extends TPCHWorkload with:
//   - An AdapterType<invoice_t> member for the INVOICE base table.
//   - A lineitem adapter typed on lineitem_i_t (FK-bearing variant).
//   - An overriding load() that calls TPCHWorkload::load() then
//     loadInvoiceAndLinkLineitem() to assign each lineitem its invoicekey.
//
// The lineitem adapter stores lineitem_i_t rows with l_invoicekey=0 after the
// initial base load; loadInvoiceAndLinkLineitem() does an erase+reinsert pass
// to write the real invoice FK into each row.
//
// Vanilla code (Q12, Q3, Q9) includes only tpch_workload.hpp and never sees
// invoice_t or l_invoicekey.  Invoice-extended code (Q3I, Q5I, Q10I) includes
// this header.

#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "tpchi_tables.hpp"
#include "../tpch_workload.hpp"

// TPCHIWorkload is a type alias for TPCHWorkload<AdapterType, lineitem_i_t>
// plus an invoice adapter and a derived load().  We use inheritance to add the
// invoice member and override load() without re-declaring all the base methods.

template <template <typename> class AdapterType>
struct TPCHIWorkload : public TPCHWorkload<AdapterType, lineitem_i_t> {
   using Base = TPCHWorkload<AdapterType, lineitem_i_t>;

   AdapterType<invoice_t>& invoice;

   // Invoice scale: 2 × ORDERS_SCALE — ~3M per SF.
   inline static Integer INVOICE_SCALE = 3000;

   TPCHIWorkload(AdapterType<part_t>& p,
                 AdapterType<supplier_t>& s,
                 AdapterType<partsupp_t>& ps,
                 AdapterType<customerh_t>& c,
                 AdapterType<orders_t>& o,
                 AdapterType<lineitem_i_t>& l,
                 AdapterType<nation_t>& n,
                 AdapterType<region_t>& r,
                 AdapterType<invoice_t>& inv,
                 Logger& logger)
       : Base(p, s, ps, c, o, l, n, r, logger), invoice(inv)
   {
   }

   // Override load() to add the invoice linking pass after the base load.
   void load()
   {
      Base::load();
      loadInvoiceAndLinkLineitem();
   }

   void log_sizes()
   {
      Base::log_sizes();
      // Base::log_sizes() already logged the 8 base tables; we add invoice here.
      // Note: this prints a separate line, not inside the base map — acceptable
      // since log_sizes() is for diagnostics only.
      std::cout << "invoice: " << invoice.size() << " MiB" << std::endl;
   }

   // Generate invoice records and rewrite each lineitem with its assigned
   // l_invoicekey.  Must be called after Base::load() has committed all orders
   // and lineitems.
   //
   // Algorithm:
   //   1. Scan orders and group by custkey, recording (orderdate, orderkey).
   //   2. Scan lineitems (now lineitem_i_t with l_invoicekey=0) grouped by
   //      orderkey; accumulate per-custkey lists.
   //   3. For each customer: allocate 2 * N invoice slots.  Walk lineitems in
   //      (orderdate, orderkey, linenumber) order, assigning each to the current
   //      invoice; close and advance after total_lineitems / num_invoices.
   //   4. Insert invoice rows; erase+reinsert each lineitem_i_t with its real
   //      invoicekey.
   void loadInvoiceAndLinkLineitem()
   {
      std::cout << "Building customer→orders and customer→lineitems maps..." << std::endl;

      struct OrderMeta {
         Timestamp orderdate;
         Integer   orderkey;
      };
      // Snapshot the base lineitem_t fields we need for invoice total computation.
      // We store the full lineitem_i_t (which inherits lineitem_t) for convenience.
      struct LineitemEntry {
         Integer      orderkey;
         Integer      linenumber;
         Timestamp    orderdate;  // copied from parent order for sort key
         lineitem_i_t rec;
      };

      std::unordered_map<Integer, std::vector<OrderMeta>>    cust_orders;
      std::unordered_map<Integer, std::vector<LineitemEntry>> cust_lineitems;

      // --- Pass 1: scan orders to build cust_orders map ---
      Base::orders.scan(
          orders_t::Key{std::numeric_limits<Integer>::min()},
          [&](const orders_t::Key& ok, const orders_t& o) {
             cust_orders[o.o_custkey].push_back({o.o_orderdate, ok.o_orderkey});
             return true;
          },
          []() {});

      // --- Pass 2: scan lineitem_i_t rows (l_invoicekey still 0) ---
      std::unordered_map<Integer, Integer> order_to_cust;
      order_to_cust.reserve(cust_orders.size());
      for (auto& [ck, oms] : cust_orders) {
         for (auto& om : oms)
            order_to_cust[om.orderkey] = ck;
      }

      Base::lineitem.scan(
          lineitem_i_t::Key{std::numeric_limits<Integer>::min(),
                            std::numeric_limits<Integer>::min()},
          [&](const lineitem_i_t::Key& lk, const lineitem_i_t& l) {
             auto it = order_to_cust.find(lk.l_orderkey);
             if (it == order_to_cust.end())
                return true;  // orphan lineitem — skip
             Integer ck = it->second;
             Timestamp odate = 0;
             auto dt = Base::order_dates.find(lk.l_orderkey);
             if (dt != Base::order_dates.end())
                odate = dt->second;
             cust_lineitems[ck].push_back({lk.l_orderkey, lk.l_linenumber, odate, l});
             return true;
          },
          []() {});

      // --- Pass 3: per-customer invoice generation ---
      Integer next_invoicekey  = 1;
      long customers_processed = 0;
      long invoices_inserted   = 0;
      long lineitems_rewritten = 0;

      for (auto& [custkey, order_metas] : cust_orders) {
         auto& items      = cust_lineitems[custkey];
         Integer N          = static_cast<Integer>(order_metas.size());
         Integer num_invoices = 2 * N;

         std::sort(items.begin(), items.end(),
                   [](const LineitemEntry& a, const LineitemEntry& b) {
                      if (a.orderdate != b.orderdate) return a.orderdate < b.orderdate;
                      if (a.orderkey  != b.orderkey)  return a.orderkey  < b.orderkey;
                      return a.linenumber < b.linenumber;
                   });

         Integer total_lineitems = static_cast<Integer>(items.size());
         Integer target_per_invoice =
             (num_invoices > 0 && total_lineitems > 0)
                 ? std::max(Integer(1), total_lineitems / num_invoices)
                 : 1;

         Integer first_invoice_of_cust = next_invoicekey;
         Integer inv_idx  = 0;
         Integer inv_count = 0;
         Integer invoicekey = next_invoicekey;
         next_invoicekey += num_invoices;

         struct InvoiceDraft {
            Integer   invoicekey;
            Timestamp max_orderdate;
            Numeric   totaldue;
         };
         std::vector<InvoiceDraft> drafts;
         drafts.reserve(static_cast<size_t>(num_invoices));
         drafts.push_back({invoicekey, 0, 0.0});

         for (auto& entry : items) {
            if (inv_count >= target_per_invoice && inv_idx + 1 < num_invoices) {
               ++inv_idx;
               invoicekey = first_invoice_of_cust + inv_idx;
               drafts.push_back({invoicekey, 0, 0.0});
               inv_count = 0;
            }

            auto& draft = drafts.back();
            draft.totaldue +=
                entry.rec.l_extendedprice
                * (1.0 - static_cast<double>(entry.rec.l_discount))
                * (1.0 + static_cast<double>(entry.rec.l_tax));
            if (entry.orderdate > draft.max_orderdate)
               draft.max_orderdate = entry.orderdate;

            // Rewrite the lineitem_i_t row with the assigned invoicekey.
            lineitem_i_t updated(static_cast<const lineitem_t&>(entry.rec), invoicekey);
            lineitem_i_t::Key lk{entry.orderkey, entry.linenumber};
            Base::lineitem.erase(lk);
            Base::lineitem.insert(lk, updated);
            ++inv_count;
            ++lineitems_rewritten;
         }

         // Insert invoice records for all draft slots allocated to this customer.
         for (Integer slot = 0; slot < num_invoices; ++slot) {
            Integer ikey  = first_invoice_of_cust + slot;
            Timestamp idate = 0;
            Numeric   idue  = 0.0;
            if (slot < static_cast<Integer>(drafts.size())) {
               idate = drafts[static_cast<size_t>(slot)].max_orderdate;
               idue  = drafts[static_cast<size_t>(slot)].totaldue;
            }
            idate += urand(7, 60);

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

         Base::inspect_produced("customers with invoices", customers_processed);
      }

      std::cout << "\nInserted " << invoices_inserted << " invoices, rewrote "
                << lineitems_rewritten << " lineitems." << std::endl;
   }
};
