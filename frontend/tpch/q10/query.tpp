// Template method bodies for Q10Workload<Backend> query methods,
// Params::defaults(), q10_pipeline_view_t::print(), q10_agg_row_t::print(),
// and predicate implementations.
// Operator-translation reference: see ../OPERATORS.md §3.
//
// Phase 1: all query_by_* return empty results (XOR digest = 0x0).
// Real bodies land in Phase 4 §7.1 (S3), §7.2 (S1), §7.3 (S2), §7.5 (S4).

#pragma once

#include <gflags/gflags.h>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "../tpch_tables.hpp"
#include "../tpch_family/col_pipeline.hpp"
#include "../q10_family/admit.hpp"
#include "../q10_family/visitor.hpp"
#include "../q10_family/out_class.hpp"

DECLARE_int32(storage_structure);

namespace tpch::q10
{

// ---------------------------------------------------------------------------
// Q10 SUBSTITUTION-PARAMETER rotation table (Decision E4 — 24 entries).
//
// TPC-H §2.4.10 domain for `:d`:
//   first day of a month between 1993-02-01 and 1995-01-01 inclusive
//   (24 valid month starts).  Validation default = 1993-10-01.
//
// Days since 1970-01-01.  DATE_1994_01_01 = 8766 and DATE_1995_01_01 = 9131
// ship in tpch_tables.hpp.  1993-01-01 = DATE_1994_01_01 − 365.  Year 1993,
// 1994, 1995 are all non-leap, so month offsets follow the standard
// Jan(31)/Feb(28)/Mar(31)/Apr(30)/May(31)/Jun(30)/Jul(31)/Aug(31)/
// Sep(30)/Oct(31)/Nov(30)/Dec(31) cumulative pattern.
//
// Validation default first so iter=0 is always spec-valid.  The PLAYBOOK
// rationale (§3.6): a fixed-param loop hid Q3I's pre_revenue bug for weeks;
// domain-full rotation surfaces param-bake regressions within one helper.run().

static constexpr Timestamp Q10_DATE_1993_01_01 = DATE_1994_01_01 - 365;  // 8401

static constexpr Timestamp PARAM_TABLE[] = {
    Q10_DATE_1993_01_01 + 273,  // 1993-10-01 = 8674  [validation default]
    Q10_DATE_1993_01_01 +  31,  // 1993-02-01
    Q10_DATE_1993_01_01 +  59,  // 1993-03-01
    Q10_DATE_1993_01_01 +  90,  // 1993-04-01
    Q10_DATE_1993_01_01 + 120,  // 1993-05-01
    Q10_DATE_1993_01_01 + 151,  // 1993-06-01
    Q10_DATE_1993_01_01 + 181,  // 1993-07-01
    Q10_DATE_1993_01_01 + 212,  // 1993-08-01
    Q10_DATE_1993_01_01 + 243,  // 1993-09-01
    Q10_DATE_1993_01_01 + 304,  // 1993-11-01
    Q10_DATE_1993_01_01 + 334,  // 1993-12-01
    DATE_1994_01_01,            // 1994-01-01 = 8766
    DATE_1994_01_01 +  31,      // 1994-02-01
    DATE_1994_01_01 +  59,      // 1994-03-01
    DATE_1994_01_01 +  90,      // 1994-04-01
    DATE_1994_01_01 + 120,      // 1994-05-01
    DATE_1994_01_01 + 151,      // 1994-06-01
    DATE_1994_01_01 + 181,      // 1994-07-01
    DATE_1994_01_01 + 212,      // 1994-08-01
    DATE_1994_01_01 + 243,      // 1994-09-01
    DATE_1994_01_01 + 273,      // 1994-10-01
    DATE_1994_01_01 + 304,      // 1994-11-01
    DATE_1994_01_01 + 334,      // 1994-12-01
    DATE_1995_01_01,            // 1995-01-01 = 9131
};

static constexpr long PARAM_TABLE_SIZE =
    static_cast<long>(sizeof(PARAM_TABLE) / sizeof(PARAM_TABLE[0]));
static_assert(PARAM_TABLE_SIZE == 24,
              "Q10 PARAM_TABLE must cover all 24 valid month starts in [1993-02-01, 1995-01-01]");

// ---------------------------------------------------------------------------

inline Params Params::defaults()
{
   return Params{PARAM_TABLE[0]};
}

// Rotate through the substitution-parameter table so each TX iteration
// exercises a distinct date.  Surfaces param-bake regressions that a
// fixed-default loop would miss (PLAYBOOK §3.6 rationale).
template <typename Backend>
void Q10Workload<Backend>::set_params_for_iter(long iter)
{
   params.date_lo = PARAM_TABLE[iter % PARAM_TABLE_SIZE];
}

// ---------------------------------------------------------------------------
// Predicate implementations.

inline bool q10_predicate_orders(const orders_t& o, const Params& p)
{
   // o_orderdate ∈ [date_lo, date_lo + 3 months).
   // 3 months ≈ 90 days for the spec's monthly grid; consistent with how
   // Q3 / Q5 lower their date-window predicates.
   return o.o_orderdate >= p.date_lo
       && o.o_orderdate <  p.date_lo + 90;
}

inline bool q10_predicate_lineitem(const lineitem_t& l, const Params& /*p*/)
{
   // l_returnflag = 'R' (spec-hardcoded; kept live for view reusability).
   return l.l_returnflag.data[0] == 'R';
}

// ---------------------------------------------------------------------------
// Output formatting.

inline void q10_pipeline_view_t::print(std::ostream& os) const
{
   os << l_extendedprice << '\t' << l_discount << '\t'
      << l_returnflag    << '\t' << o_orderdate << '\t'
      << c_name          << '\t' << c_nationkey << '\n';
}

inline void q10_agg_row_t::print(std::ostream& os) const
{
   os << c_custkey << '\t' << c_name      << '\t' << revenue   << '\t'
      << c_acctbal << '\t' << n_name      << '\t' << c_address << '\t'
      << c_phone   << '\t' << c_comment   << '\n';
}

// ---------------------------------------------------------------------------
// Stub query bodies — Phase 1 commit 1.
// Real bodies land in Phase 4 §7.1/§7.2/§7.3/§7.5.

template <typename Backend>
long Q10Workload<Backend>::query_by_base(std::vector<q10_agg_row_t>& out)
{
   // S1: traditional indexes over the custkey-sorted COL split secondaries
   // (D7 — reused verbatim from Q5). The plan calls for a 2-BMJ chain;
   // we implement the equivalent shape as a nested forward walk: outer
   // scan over CUSTOMER (custkey-sorted by PK), per-customer seek into
   // split_orders[custkey, ...] applying the orderdate window, per-order
   // seek into split_lineitem[custkey, orderkey, ...] applying the
   // returnflag filter. Each surviving lineitem feeds the per-customer
   // aggregator via q10_admit_lineitem_from_join; NATION INL fires per
   // surviving customer at finalize (Decision D6). Functionally identical
   // to BMJ for correctness purposes — same access pattern, same emit
   // cardinality, same XOR digest as S3 by construction.
   out.clear();

   Q10QuerySink sink(stats);
   Q10PerCustomerAggregator agg{};
   agg.stats = stats;
   std::unordered_map<Integer, Varchar<25>> nation_cache;

   auto cust_sc = customer.getScanner();
   auto ord_sc  = col.split_orders().getScanner();
   auto lin_sc  = col.split_lineitem().getScanner();

   while (auto ckv = cust_sc->next()) {
      if (stats) stats->customers_scanned++;
      const Integer       custkey = ckv->first.c_custkey;
      const customerh_t&  c       = ckv->second;

      ord_sc->seek(orders_coli_t::Key{custkey, 0});
      while (auto okv = ord_sc->next()) {
         if (okv->first.custkey != custkey) break;
         if (stats) stats->orders_scanned++;
         if (okv->second.o_orderdate < params.date_lo
             || okv->second.o_orderdate >= params.date_lo + 90) {
            continue;
         }
         if (stats) stats->orders_passing_date++;
         const Integer orderkey = okv->first.orderkey;

         lin_sc->seek(lineitem_col_t::Key{custkey, orderkey, 0});
         while (auto lkv = lin_sc->next()) {
            if (lkv->first.custkey != custkey
                || lkv->first.orderkey != orderkey) break;
            if (stats) stats->lineitems_scanned++;
            if (lkv->second.l_returnflag.data[0] != 'R') continue;
            if (stats) stats->lineitems_passing_returnflag++;
            q10_admit_lineitem_from_join(custkey, c,
                                         lkv->second.l_extendedprice,
                                         lkv->second.l_discount,
                                         agg, stats);
         }
      }
   }

   q10_finalize_aggregator(agg, nation, nation_cache, sink, stats);
   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_view(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

template <typename Backend>
long Q10Workload<Backend>::query_by_merged(std::vector<q10_agg_row_t>& out)
{
   // S3: col_group_walk over the 3-table COL MI using the bespoke
   // Q10GroupWalkVisitor (Decision D1; user-memory
   // `[[feedback_col_walk_is_shared_util]]` — NOT a Q3FamilyVisitor
   // subclass). Per-customer accumulator finalises at on_group_end
   // (D2); NATION INL on PK resolves n_name inline (D6); the
   // assembled row is offered to a bounded TopN(20) sink
   // (CONVENTIONS §Post-pipeline OutClass).
   out.clear();

   using NationAdapterT = typename Backend::template Adapter<nation_t>;
   Q10QuerySink sink(stats);
   std::unordered_map<Integer, Varchar<25>> nation_cache;  // ≤25 entries

   Q10GroupWalkVisitor<NationAdapterT, Q10QuerySink, Q10FilterMode::Query>
       visitor{params, nation, sink, nation_cache, stats};
   ::tpch::col_group_walk<Backend>(col.merged_adapter(), visitor);

   sink.finalize(out);
   return static_cast<long>(out.size());
}

template <typename Backend>
long Q10Workload<Backend>::query_by_hash(std::vector<q10_agg_row_t>& out)
{
   (void)out;
   return 0;
}

}  // namespace tpch::q10
