#pragma once

// Invoice-extended TPC-H schema.
//
// This header adds the invoice extension on top of vanilla tpch_tables.hpp:
//   - invoice_t   — the INVOICE base table (id=9).
//   - lineitem_i_t — lineitem_t + l_invoicekey payload (the FK linking each
//                    lineitem to its invoice). Used by invoice-extended queries
//                    (Q3I, Q5I, Q10I) and by the COLI / aCOLI merged-index
//                    pipelines that encode l_invoicekey in the key.
//
// Vanilla code (Q12, Q3, Q9, geo) includes only tpch_tables.hpp and never
// sees invoice_t or l_invoicekey.  Invoice-extended code includes this header.

#include "../tpch_tables.hpp"

// -------------------------------------------------------------------------------------
// invoice_t — INVOICE base table.
//
// id=9; keyed by i_invoicekey.
// i_custkey is stored in the payload for easy COLI MI rekey without a map lookup.

struct invoice_t {
   static constexpr int id = 9;
   struct Key {
      static constexpr int id = 9;
      Integer i_invoicekey;
      ADD_KEY_TRAITS(&Key::i_invoicekey)
   };

   Integer    i_custkey;      // FK → customerh_t (the MI join key)
   Timestamp  i_invoicedate;
   Numeric    i_totaldue;     // Σ l_extendedprice*(1-discount)*(1+tax) over bundled lineitems
   Varchar<1>  i_status;      // 'P' paid, 'O' open, 'L' late
   Varchar<25> i_paymentterm;
   Varchar<79> i_comment;

   ADD_RECORD_TRAITS(invoice_t)

   void print(std::ostream& os) const
   {
      os << i_custkey << "," << i_invoicedate << "," << i_totaldue << ","
         << i_status << "," << i_paymentterm << "," << i_comment;
   }

   // Caller should pass generate_custkey returning values in [1..num_customers].
   // i_totaldue is accumulated externally during loadInvoiceAndLinkLineitem.
   static invoice_t generateRandomRecord(std::function<int()> generate_custkey,
                                         Timestamp i_invoicedate,
                                         Numeric   i_totaldue,
                                         Varchar<1> i_status)
   {
      return invoice_t{generate_custkey(),
                       i_invoicedate,
                       i_totaldue,
                       i_status,
                       randomastring<25>(0, 25),
                       randomastring<79>(0, 79)};
   }
};

// -------------------------------------------------------------------------------------
// lineitem_i_t — invoice-extended lineitem.
//
// Inherits all lineitem_t fields and adds l_invoicekey (FK → invoice_t).
// The base lineitem_t no longer carries this field; only invoice-extended
// workloads (TPCHIWorkload) and COLI/aCOLI pipelines use lineitem_i_t.
//
// Key shape: identical to lineitem_t::Key — (l_orderkey, l_linenumber).
// id=55 (distinct from lineitem_t id=5 to avoid adapter collisions).
//
// Constructor: lineitem_i_t(const lineitem_t& base, Integer invoicekey)
//   — used by loadInvoiceAndLinkLineitem to upgrade a vanilla lineitem after
//     invoice assignment.

struct lineitem_i_t : lineitem_t {
   static constexpr int id = 55;

   struct Key {
      static constexpr int id = 55;
      Integer l_orderkey;
      Integer l_linenumber;
      ADD_KEY_TRAITS(&Key::l_orderkey, &Key::l_linenumber)
   };

   Integer l_invoicekey;  // FK → invoice_t; assigned by loadInvoiceAndLinkLineitem

   ADD_RECORD_TRAITS(lineitem_i_t)

   void print(std::ostream& os) const
   {
      lineitem_t::print(os);
      os << "," << l_invoicekey;
   }

   // Upgrade constructor: wraps an existing lineitem_t with an assigned invoicekey.
   lineitem_i_t(const lineitem_t& base, Integer invoicekey)
       : lineitem_t(base), l_invoicekey(invoicekey)
   {
   }

   // Default constructor: zero-initializes l_invoicekey (unassigned).
   // Used by the initial lineitem load pass before invoice linking.
   lineitem_i_t() : lineitem_t(), l_invoicekey(0) {}

   // Generate a random record (delegates to lineitem_t, sets l_invoicekey=0).
   // Callers that need the invoice key should use the upgrade constructor after
   // calling loadInvoiceAndLinkLineitem.
   static lineitem_i_t generateRandomRecord(std::function<int()> generate_partkey,
                                            std::function<int()> generate_suppkey,
                                            Timestamp o_orderdate)
   {
      return lineitem_i_t(
          lineitem_t::generateRandomRecord(generate_partkey, generate_suppkey, o_orderdate),
          0);
   }

   static lineitem_i_t generateRandomRecord(std::function<int()> generate_partkey,
                                            std::function<int()> generate_suppkey)
   {
      return lineitem_i_t(
          lineitem_t::generateRandomRecord(generate_partkey, generate_suppkey),
          0);
   }
};

// Size note: sizeof(lineitem_i_t) == sizeof(lineitem_t) + sizeof(Integer) + padding.
// The compiler may insert alignment padding before l_invoicekey depending on the
// trailing alignment of lineitem_t.  This is expected and harmless — lineitem_i_t
// is never stored in a packed array alongside lineitem_t; adapters are typed and
// store only one record type per column family.
