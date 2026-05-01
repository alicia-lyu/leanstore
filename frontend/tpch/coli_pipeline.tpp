// Template method bodies for CustomerOrdersLineitemInvoicePipeline<Backend>.
//
// Key discrimination in merged_coli:
//   toType<Records...> matches each stored entry by
//     (k.size() == Records::maxFoldLength()) && (v.size() == sizeof(Records))
//
//   customerh_t, orders_t, and invoice_t all have 4-byte native keys, so
//   storing them under their own keys would cause three-way collisions.
//   Instead, all four record types are stored under the 20-byte
//   coli_sort_key_t key (5 × Integer).  Discrimination then falls back to
//   sizeof alone, which works because the four payload sizes are distinct:
//     customerh_t ≈ 224 B, orders_t ≈ 138 B, lineitem_t ≈ 154 B, invoice_t ≈ 128 B.
//
//   Four proxy structs below inherit each base record's payload but expose
//   coli_sort_key_t as their Key.  The merged adapter's insert<Proxy>(sk, val)
//   folds sk under coli_sort_key_t::keyfold and stores sizeof(Base) bytes.
//   The scanner's toType sees the 20-byte key and matches by payload size.
//
// Lineitem rekeying:
//   lineitem_t::Key = {l_orderkey, l_linenumber} carries no custkey.
//   populate_merged scans orders first to build an orderkey → custkey map,
//   then re-inserts each lineitem under (custkey, 1, orderkey, l_invoicekey,
//   linenumber).  Memory cost at SF=1: ~1.5 M entries × 8 bytes ≈ 12 MiB.

#pragma once

#include <unordered_map>

#include "views_coli.hpp"

namespace tpch
{

// ---------------------------------------------------------------------------
// Proxy record types: identical payload layout to the base record but with
// Key = coli_sort_key_t so the merged adapter folds a 20-byte sort key.
//
// ADD_RECORD_TRAITS(ProxyType) supplies foldKey/unfoldKey by delegating to
// Key::keyfold/keyunfold, which are inherited from coli_sort_key_t via the
// Key struct below.  The value stored is sizeof(BaseRecord) bytes and is
// read back by reinterpret_cast<const BaseRecord*>(value.data()).

struct coli_proxy_key_t : coli_sort_key_t {
   // Re-expose the static keyfold/keyunfold/maxFoldLength under the name
   // "Key" so that ADD_RECORD_TRAITS (which calls Key::keyfold) finds them.
   using coli_sort_key_t::keyfold;
   using coli_sort_key_t::keyunfold;
   using coli_sort_key_t::maxFoldLength;

   coli_proxy_key_t() = default;
   explicit coli_proxy_key_t(const coli_sort_key_t& sk) : coli_sort_key_t(sk) {}
};

struct customerh_coli_t : customerh_t {
   static constexpr int id = customerh_t::id;
   using Key = coli_proxy_key_t;
   ADD_RECORD_TRAITS(customerh_coli_t)
};

struct orders_coli_t : orders_t {
   static constexpr int id = orders_t::id;
   using Key = coli_proxy_key_t;
   ADD_RECORD_TRAITS(orders_coli_t)
};

struct lineitem_coli_t : lineitem_t {
   static constexpr int id = lineitem_t::id;
   using Key = coli_proxy_key_t;
   ADD_RECORD_TRAITS(lineitem_coli_t)
};

struct invoice_coli_t : invoice_t {
   static constexpr int id = invoice_t::id;
   using Key = coli_proxy_key_t;
   ADD_RECORD_TRAITS(invoice_coli_t)
};

// ---------------------------------------------------------------------------
// Helper: reinterpret a base record as its COLI proxy (safe — same layout).

template <typename Proxy, typename Base>
static const Proxy& as_proxy(const Base& b)
{
   static_assert(sizeof(Proxy) == sizeof(Base));
   return *reinterpret_cast<const Proxy*>(&b);
}

// ---------------------------------------------------------------------------
// Constructor

template <typename Backend>
CustomerOrdersLineitemInvoicePipeline<Backend>::CustomerOrdersLineitemInvoicePipeline(
    typename Backend::template Adapter<customerh_t>& customer,
    typename Backend::template Adapter<orders_t>& orders,
    typename Backend::template Adapter<lineitem_t>& lineitem,
    typename Backend::template Adapter<invoice_t>& invoice,
    typename Backend::template MergedAdapter<customerh_t, orders_t, lineitem_t, invoice_t>&
        merged_coli)
    : customer(customer), orders(orders), lineitem(lineitem), invoice(invoice),
      merged_coli(merged_coli)
{
}

// ---------------------------------------------------------------------------
// populate_merged: five-pass dual-write replay.
//
// Pass 1 scans orders to build the orderkey → custkey map (needed for
// lineitem rekeying in pass 4).  Passes 2–5 insert each table under its
// coli_sort_key_t-shaped proxy key so all entries sort by custkey.

template <typename Backend>
void CustomerOrdersLineitemInvoicePipeline<Backend>::populate_merged()
{
   // --- Pass 1: build orderkey → custkey map ---
   std::unordered_map<Integer, Integer> orderkey_to_custkey;
   {
      auto scanner = orders.getScanner();
      while (auto kv = scanner->next()) {
         orderkey_to_custkey[kv->first.o_orderkey] = kv->second.o_custkey;
      }
   }

   // --- Pass 2: Customer — sort key (custkey, 0, 0, 0, 0) ---
   {
      auto scanner = customer.getScanner();
      while (auto kv = scanner->next()) {
         coli_proxy_key_t pk{SKBuilder<coli_sort_key_t>::create(kv->first, kv->second)};
         merged_coli.template insert<customerh_coli_t>(pk, as_proxy<customerh_coli_t>(kv->second));
      }
   }

   // --- Pass 3: Orders — sort key (custkey, 1, orderkey, 0, 0) ---
   {
      auto scanner = orders.getScanner();
      while (auto kv = scanner->next()) {
         coli_proxy_key_t pk{SKBuilder<coli_sort_key_t>::create(kv->first, kv->second)};
         merged_coli.template insert<orders_coli_t>(pk, as_proxy<orders_coli_t>(kv->second));
      }
   }

   // --- Pass 4: Lineitem — sort key (custkey, 1, orderkey, l_invoicekey, linenumber) ---
   {
      auto scanner = lineitem.getScanner();
      while (auto kv = scanner->next()) {
         auto it = orderkey_to_custkey.find(kv->first.l_orderkey);
         if (it == orderkey_to_custkey.end()) {
            // Orphaned lineitem (should not happen after loadOrders fix).
            continue;
         }
         coli_proxy_key_t pk{
             SKBuilder<coli_sort_key_t>::create_lineitem(it->second, kv->first, kv->second)};
         merged_coli.template insert<lineitem_coli_t>(pk, as_proxy<lineitem_coli_t>(kv->second));
      }
   }

   // --- Pass 5: Invoice — sort key (custkey, 2, 0, invoicekey, 0) ---
   {
      auto scanner = invoice.getScanner();
      while (auto kv = scanner->next()) {
         coli_proxy_key_t pk{SKBuilder<coli_sort_key_t>::create(kv->first, kv->second)};
         merged_coli.template insert<invoice_coli_t>(pk, as_proxy<invoice_coli_t>(kv->second));
      }
   }
}

// ---------------------------------------------------------------------------
// get_merged_size: delegates to the adapter's CF-level size estimate.

template <typename Backend>
double CustomerOrdersLineitemInvoicePipeline<Backend>::get_merged_size() const
{
   return merged_coli.size();
}

}  // namespace tpch
