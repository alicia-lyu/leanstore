// DBToaster refresh_sales baseline harness.
//
// Maintains the Q3 + Q5 pipeline views (QUERY_1 / QUERY_2 in refresh_sales.sql)
// incrementally under a TPC-H RF1 (insert) / RF2 (delete) stream, and measures
// per-update maintenance throughput + resident memory — a generic-IVM baseline
// for LeanStore's S2 materialized pipeline view.
//
// Copy-adapted from geodb-dbtoaster/main.cpp. DBToaster auto-generates ALL view
// maintenance from the two SQL SELECTs; this harness only fires events and
// measures them.
//
//   * Inserts (base + RF1) stream from the CSV files via Program::run() and are
//     measured in the process_stream_event override (geodb pattern). Base rows
//     (the first `warmup_orders` / `warmup_lineitems` per relation, = line count
//     of the un-concatenated base .tbl) are warmup; the appended RF1 tail is the
//     measured set.
//   * Deletes (RF2) cannot come from a CSV file (DBToaster file streams are
//     insert-only: csv_adaptor "deletions"="false"), so we fire them after the
//     stream drains by calling the generated typed triggers data.on_delete_*().
//     DBToaster deletes are multiset deltas with no PK: only the query-referenced
//     columns affect the view delta, so unreferenced columns are passed as
//     dummies. The referenced column values are captured from the base insert
//     events during warmup (no separate .tbl parse, no date-string parsing —
//     the values arrive already typed in the event args).
//
// Generated API used (see refresh_sales.hpp / /opt/dbtoaster/lib/dbt_c++):
//   dbtoaster::Program            : init(), run(), process_stream_event(event_t),
//                                   get_relation_id(name); protected `data_t data`
//   data_t                        : public on_delete_ORDERS / on_delete_LINEITEM
//                                   typed triggers; QUERY_1_COUNT / QUERY_2_COUNT
//                                   MultiHashMaps with .count()
//   event_t{type,id,event_order,data}; event_type{delete_tuple=0,insert_tuple}
//   event_args_t = vector<shared_ptr<void>>  (field i = *reinterpret_cast<T*>(data[i].get()))
//   date = int, DOUBLE_TYPE = double, STRING_TYPE = PString (const char* ctor)

#include "refresh_sales.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace dbtoaster;
using clk = std::chrono::steady_clock;
static double us_since(clk::time_point t) {
  return std::chrono::duration<double, std::micro>(clk::now() - t).count();
}

// VmRSS (KB) from /proc/self/status — verbatim from geodb-dbtoaster/main.cpp.
static long get_memory_usage_linux() {
  long rss = 0;
  std::ifstream ifs("/proc/self/status");
  std::string line;
  while (std::getline(ifs, line)) {
    if (line.rfind("VmRSS:", 0) == 0) {
      size_t pos = line.find_first_of("0123456789");
      if (pos != std::string::npos) rss = std::stol(line.substr(pos));
      break;
    }
  }
  return rss;
}

static long count_lines(const std::string& path) {
  std::ifstream ifs(path);
  long n = 0;
  std::string line;
  while (std::getline(ifs, line))
    if (!line.empty()) ++n;
  return n;
}

namespace {
// Referenced columns retained for the RF2 delete replay (the rest are dummies).
struct OrderDel { long custkey; date orderdate; long shippriority; };
struct LineDel  { long suppkey; long linenumber; double extprice; double discount; date shipdate; };
}  // namespace

namespace dbtoaster {

class RefreshHarness : public Program {
  // --- config (derived from files in ./data_files) ---
  long warmup_orders_ = 0;     // base ORDERS line count -> warmup boundary
  long warmup_lineitems_ = 0;  // base LINEITEM line count -> warmup boundary
  int orders_id_ = -1, lineitem_id_ = -1;
  std::unordered_set<long> delete_keys_;  // RF2 orderkeys (from delete.1)
  std::vector<long> delete_order_;        // deterministic RF2 iteration order

  // --- captured base tuples for the RF2 delete slice ---
  std::unordered_map<long, OrderDel> cap_orders_;
  std::unordered_map<long, std::vector<LineDel>> cap_lines_;

  // --- counters / timing ---
  long orders_seen_ = 0, lineitems_seen_ = 0;
  long rf1_orders_ = 0, rf1_lineitems_ = 0;
  double rf1_us_ = 0.0;          // RF1 insert maintenance time (orders + lineitems)
  long warmup_rss_kb_ = 0;       // RSS at the warmup->RF1 boundary (~working set)
  bool warmup_rss_taken_ = false;

  static long as_long(const event_t& ev, size_t i)  { return *reinterpret_cast<long*>(ev.data[i].get()); }
  static date as_date(const event_t& ev, size_t i)  { return *reinterpret_cast<date*>(ev.data[i].get()); }
  static double as_dbl(const event_t& ev, size_t i)  { return *reinterpret_cast<double*>(ev.data[i].get()); }

  void mark_warmup_rss() {
    if (!warmup_rss_taken_) { warmup_rss_kb_ = get_memory_usage_linux(); warmup_rss_taken_ = true; }
  }

 public:
  RefreshHarness(int argc = 0, char* argv[] = 0) : Program(argc, argv) {
    orders_id_ = get_relation_id("ORDERS");
    lineitem_id_ = get_relation_id("LINEITEM");
    warmup_orders_ = count_lines("./data_files/orders.tbl");
    warmup_lineitems_ = count_lines("./data_files/lineitem.tbl");
    std::ifstream del("./data_files/delete.1");
    std::string line;
    while (std::getline(del, line)) {
      if (line.empty()) continue;
      long k = std::stol(line);  // first integer = orderkey
      if (delete_keys_.insert(k).second) delete_order_.push_back(k);
    }
    std::cout << "warmup_orders=" << warmup_orders_
              << " warmup_lineitems=" << warmup_lineitems_
              << " rf2_delete_keys=" << delete_keys_.size() << std::endl;
  }

  // Inserts arrive here from run() (base + appended RF1 tail). Time the RF1 tail;
  // capture the RF2 delete slice from base events as they pass.
  void process_stream_event(const event_t& ev) override {
    if (ev.type == insert_tuple && ev.id == orders_id_) {
      long ok = as_long(ev, 0);
      if (++orders_seen_ > warmup_orders_) {           // RF1 (measured)
        mark_warmup_rss();
        auto t0 = clk::now();
        Program::process_stream_event(ev);
        rf1_us_ += us_since(t0);
        ++rf1_orders_;
        return;
      }
      if (delete_keys_.count(ok))                        // base row in RF2 slice
        cap_orders_[ok] = OrderDel{as_long(ev, 1), as_date(ev, 4), as_long(ev, 7)};
      Program::process_stream_event(ev);
      return;
    }
    if (ev.type == insert_tuple && ev.id == lineitem_id_) {
      long ok = as_long(ev, 0);
      if (++lineitems_seen_ > warmup_lineitems_) {       // RF1 (measured)
        mark_warmup_rss();
        auto t0 = clk::now();
        Program::process_stream_event(ev);
        rf1_us_ += us_since(t0);
        ++rf1_lineitems_;
        return;
      }
      if (delete_keys_.count(ok))                        // base row in RF2 slice
        cap_lines_[ok].push_back(
            LineDel{as_long(ev, 2), as_long(ev, 3), as_dbl(ev, 5), as_dbl(ev, 6), as_date(ev, 10)});
      Program::process_stream_event(ev);
      return;
    }
    Program::process_stream_event(ev);  // CUSTOMER inserts (all warmup)
  }

  // RF2: fire deletes for the captured base orders + their lineitems. Only the
  // query-referenced columns carry real values; the rest are dummies (they are
  // absent from both pipeline views, so they don't affect the view delta).
  void run_rf2() {
    const STRING_TYPE E("");
    const date D0 = 0;
    long rf2_orders = 0, rf2_lineitems = 0;
    auto t0 = clk::now();
    for (long ok : delete_order_) {
      auto lit = cap_lines_.find(ok);
      if (lit != cap_lines_.end())
        for (const LineDel& l : lit->second) {
          // schema: orderkey,partkey,suppkey,linenumber,quantity,extprice,
          //         discount,tax,returnflag,linestatus,shipdate,commitdate,
          //         receiptdate,shipinstruct,shipmode,comment
          data.on_delete_LINEITEM(ok, 0, l.suppkey, l.linenumber, 0.0, l.extprice,
                                  l.discount, 0.0, E, E, l.shipdate, D0, D0, E, E, E);
          ++rf2_lineitems;
        }
      auto oit = cap_orders_.find(ok);
      if (oit != cap_orders_.end()) {
        // schema: orderkey,custkey,orderstatus,totalprice,orderdate,
        //         orderpriority,clerk,shippriority,comment
        data.on_delete_ORDERS(ok, oit->second.custkey, E, 0.0, oit->second.orderdate,
                              E, E, oit->second.shippriority, E);
        ++rf2_orders;
      }
    }
    double rf2_us = us_since(t0);
    report(rf2_orders, rf2_lineitems, rf2_us);
  }

  void report(long rf2_orders, long rf2_lineitems, double rf2_us) {
    long final_rss = get_memory_usage_linux();
    if (!warmup_rss_taken_) warmup_rss_kb_ = final_rss;  // no RF1 tail (tiny SF)

    double rf1_rate = rf1_orders_  > 0 ? rf1_orders_  / (rf1_us_ / 1e6) : 0.0;
    double rf2_rate = rf2_orders   > 0 ? rf2_orders   / (rf2_us  / 1e6) : 0.0;
    double pair_rate = (rf1_orders_ + rf2_orders) > 0
                           ? (rf1_orders_ + rf2_orders) / ((rf1_us_ + rf2_us) / 1e6) : 0.0;
    double elapsed_s = (rf1_us_ + rf2_us) / 1e6;

    std::cout << "\n--- DBToaster refresh_sales (Q3+Q5 pipeline views) ---\n"
              << "QUERY_1 (Q3 view) rows: " << data.get_QUERY_1_COUNT().count() << "\n"
              << "QUERY_2 (Q5 view) rows: " << data.get_QUERY_2_COUNT().count() << "\n"
              << "RF1: " << rf1_orders_ << " orders / " << rf1_lineitems_
              << " lineitems in " << rf1_us_ / 1e6 << " s -> " << rf1_rate << " orders/s\n"
              << "RF2: " << rf2_orders << " orders / " << rf2_lineitems
              << " lineitems in " << rf2_us / 1e6 << " s -> " << rf2_rate << " orders/s\n"
              << "pair orders/s (aggregate): " << pair_rate << "\n"
              << "VmRSS after warmup: " << warmup_rss_kb_ << " KB; final: " << final_rss << " KB"
              << std::endl;

    bool have = std::ifstream("./results/RefreshTPut.dbtoaster.csv").good();
    std::ofstream csv("./results/RefreshTPut.dbtoaster.csv", std::ios::app);
    if (!have)
      csv << "elapsed_s,rf1_orders_per_s,rf2_orders_per_s,pair_orders_per_s,vmrss_kb\n";
    csv << elapsed_s << "," << rf1_rate << "," << rf2_rate << "," << pair_rate << ","
        << warmup_rss_kb_ << "\n";
  }
};

}  // namespace dbtoaster

int main(int argc, char* argv[]) {
  dbtoaster::RefreshHarness p(argc, argv);
  std::cout << "Loading static tables (NATION, REGION)..." << std::endl;
  p.init();  // static tables + system-ready
  std::cout << "Streaming base + RF1 inserts (warmup + measured tail)..." << std::endl;
  p.run();   // synchronous: pumps CUSTOMER/ORDERS/LINEITEM through process_stream_event
  std::cout << "Firing RF2 deletes..." << std::endl;
  p.run_rf2();
  return 0;
}
