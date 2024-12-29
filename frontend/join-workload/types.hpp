#pragma once
#include "gflags/gflags.h"
#include <gflags/gflags_declare.h>
#include <type_traits>
#include "JoinedSchema.hpp"

DEFINE_int64(tpcc_warehouse_count, 1, "");
DEFINE_int32(tpcc_abort_pct, 0, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_verify, false, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_remove, false, "");
DEFINE_bool(order_wdc_index, true, "");
DEFINE_uint32(tpcc_threads, 0, "");
DEFINE_uint32(read_percentage, 0, "");
DEFINE_uint32(scan_percentage, 0, "");
DEFINE_uint32(write_percentage, 100, "");
DEFINE_uint32(order_size, 5, "Number of lines in a new order");
DEFINE_bool(locality_read, false, "Lookup key in the read transactions are the same or smaller than the join key.");
DEFINE_bool(outer_join, false, "Outer join in the join transactions.");

#if !defined(INCLUDE_COLUMNS)
#define INCLUDE_COLUMNS \
   1  // All columns, unless defined. Now included columns in orderline secondary and joined results are the same. Maybe should be different.
#endif

using orderline_sec_t = typename std::conditional<INCLUDE_COLUMNS == 0, ol_sec0_t, ol_sec1_t>::type;  // INCLUDE_COLUMNS == 2 will still select all
                                                                                                      // columns from orderline
using joined_t = typename std::
    conditional<INCLUDE_COLUMNS == 0, joined0_t, typename std::conditional<INCLUDE_COLUMNS == 1, joined1_t, joined_selected_t>::type>::type;

using stock_sec_t = typename std::
    conditional<INCLUDE_COLUMNS == 0, stock_0_t, typename std::conditional<INCLUDE_COLUMNS == 1, stock_t, stock_selected_t>::type>::type;