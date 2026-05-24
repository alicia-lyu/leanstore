#!/bin/bash
# Run the DBToaster refresh_sales baseline on UNLIMITED memory and report the
# peak resident set, which is a loose lower bound on the RAM DBToaster needs.
#
# DBToaster is pure in-memory: the maintained Q3+Q5 pipeline views (the full
# join multisets) plus the delta-maintenance index maps must all stay resident
# — there is no eviction or SSD spill. So the memory it requires is essentially
# its working set; constraining below that just OOMs rather than degrading.
# This is the headline contrast with LeanStore S2, which serves a 5 GiB
# SSD-backed secondary from a 0.4-1.0 GiB DRAM buffer pool by spilling. We
# therefore do not sweep `ulimit -v`; we report the unlimited-memory footprint:
#   * VmRSS after warmup ~= the maintained-view working set
#   * peak RSS (time -v "Maximum resident set size") ~= run high-water mark
# both of which the harness/`time` print and which bound the RAM requirement.

BIN=${BIN:-./build/refresh_sales}

if command -v /usr/bin/time >/dev/null 2>&1; then
  /usr/bin/time -v "$BIN" "$@"
else
  "$BIN" "$@"   # harness still prints VmRSS after warmup + final
fi
