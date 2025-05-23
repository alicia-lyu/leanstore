/*

Copyright (c) 2018 Viktor Leis

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

 */

#pragma once

#if defined(__linux__)

#include <sys/types.h>
#include <cstdint>
#include <limits>
#include <asm/unistd.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

struct PerfEvent {
  struct event {
    struct read_format {
      uint64_t value;
      uint64_t time_enabled;
      uint64_t time_running;
      uint64_t id;
    };

    perf_event_attr pe;
    int fd;
    read_format prev;
    read_format data;

    double readCounter()
    {
      double multiplexingCorrection = static_cast<double>(data.time_enabled - prev.time_enabled) / (data.time_running - prev.time_running);
      return (data.value - prev.value) * multiplexingCorrection;
    }
  };

  std::vector<event> events;
  std::vector<std::string> names;
  timespec startTime;
  timespec stopTime;
  std::map<std::string, std::string> params;
  const bool inherit;
  bool printHeader;

  bool isIntel()
  {
#ifdef __x86_64__
    // EAX=0: Highest Function Parameter and Manufacturer ID
    // ecx is often an input as well as an output
    unsigned eax = 0, ebx, ecx = 0, edx;
    asm volatile("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "0"(0), "2"(ecx));
    return ecx == 0x6c65746e;
#else
    return false;
#endif
  }

  PerfEvent(bool inherit = true) : inherit(inherit), printHeader(true)
  {
    registerCounter("cycle", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, false, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_CPU_CLOCK);
    registerCounter("cycle-kernel", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES, true); // no fall back to software
    registerCounter("instr", PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS); // no fall back to software
    registerCounter("L1-miss", PERF_TYPE_HW_CACHE,
                    PERF_COUNT_HW_CACHE_L1D | (PERF_COUNT_HW_CACHE_OP_READ << 8) | (PERF_COUNT_HW_CACHE_RESULT_MISS << 16)); // no fall back to software
    if (isIntel())
      registerCounter("LLC-miss", PERF_TYPE_HARDWARE, PERF_COUNT_HW_CACHE_MISSES); // no fall back to software
    registerCounter("br-miss", PERF_TYPE_HARDWARE, PERF_COUNT_HW_BRANCH_MISSES); // no fall back to software
    registerCounter("task", PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK, false, PERF_TYPE_SOFTWARE, PERF_COUNT_SW_TASK_CLOCK);
    // additional counters can be found in linux/perf_event.h
  }

  void registerCounter(const std::string& name, uint64_t type, uint64_t eventID, const bool exclude_user = false, uint64_t fallback_type = std::numeric_limits<uint64_t>::max(), uint64_t fallback_eventID = std::numeric_limits<uint64_t>::max())
  {
    names.push_back(name);
    events.push_back(event());
    auto& event = events.back();
    auto& pe = event.pe;
    memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = type;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = eventID;
    pe.disabled = true;
    pe.inherit = inherit;
    pe.inherit_stat = 0;
    pe.exclude_user = exclude_user;
    pe.exclude_kernel = false;
    pe.exclude_hv = false;
    pe.read_format = PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING;

    event.fd = syscall(__NR_perf_event_open, &event.pe, 0, -1, -1, 0);
    if (event.fd < 0) {
      if (fallback_type != std::numeric_limits<uint64_t>::max() && fallback_eventID != std::numeric_limits<uint64_t>::max()) {
        event.pe.type = fallback_type;
        event.pe.config = fallback_eventID;
        event.fd = syscall(__NR_perf_event_open, &event.pe, 0, -1, -1, 0);
        if (event.fd >= 0) {
        // fall back works
          std::cout << "PerfEvent::PerfEvent(): Fallback counter " << name << " is used" << std::endl;
          return;
        }
      }
      std::cout << "PerfEvent::PerfEvent() Warning: Proceed without counter " << name << std::endl;
      events.pop_back();
      names.pop_back();
      return;
    }
    std::cout << "PerfEvent::PerfEvent(): Counter " << name << " is registered" << std::endl;
  }

  void startCounters()
  {
    for (unsigned i = 0; i < events.size(); i++) {
      auto& event = events[i];
      ioctl(event.fd, PERF_EVENT_IOC_RESET, 0);
      ioctl(event.fd, PERF_EVENT_IOC_ENABLE, 0);
      if (read(event.fd, &event.prev, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
        std::cerr << "Error reading counter " << names[i] << std::endl;
    }
    clock_gettime(CLOCK_MONOTONIC_RAW, &startTime);
  }

  ~PerfEvent()
  {
    for (auto& event : events) {
      close(event.fd);
    }
  }

  void stopCounters()
  {
    clock_gettime(CLOCK_MONOTONIC_RAW, &stopTime);
    for (unsigned i = 0; i < events.size(); i++) {
      auto& event = events[i];
      if (read(event.fd, &event.data, sizeof(uint64_t) * 3) != sizeof(uint64_t) * 3)
        std::cerr << "Error reading counter " << names[i] << std::endl;
      ioctl(event.fd, PERF_EVENT_IOC_DISABLE, 0);
    }
  }

  long getDuration() const {
    return (stopTime.tv_sec - startTime.tv_sec) * 1e9 + (stopTime.tv_nsec - startTime.tv_nsec);
  }

  bool IPCAvailable() { return std::find(names.begin(), names.end(), "instr") != names.end() && std::find(names.begin(), names.end(), "cycle") != names.end(); }
  double getIPC() { return (double) getCounter("instr") / getCounter("cycle"); }

  bool CPUsAvailable() { return std::find(names.begin(), names.end(), "task") != names.end(); }
  double getCPUs() { return (double) getCounter("task") / getDuration(); }

  bool GHzAvailable() { return std::find(names.begin(), names.end(), "cycle") != names.end() && std::find(names.begin(), names.end(), "task") != names.end(); }
  double getGHz() { return (double) getCounter("cycle") / getCounter("task"); }

  long getCounter(const std::string& name)
  {
    for (unsigned i = 0; i < events.size(); i++)
      if (names[i] == name)
        return events[i].readCounter();
    return -1;
  }

  static void printCounter(std::ostream& headerOut, std::ostream& dataOut, std::string name, std::string counterValue, bool addComma = true)
  {
    auto width = std::max(name.length(), counterValue.length());
    headerOut << std::setw(width) << name << (addComma ? "," : "") << " ";
    dataOut << std::setw(width) << counterValue << (addComma ? "," : "") << " ";
  }

  template <typename T>
  static void printCounter(std::ostream& headerOut, std::ostream& dataOut, std::string name, T counterValue, bool addComma = true)
  {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(2) << counterValue;
    PerfEvent::printCounter(headerOut, dataOut, name, stream.str(), addComma);
  }

  void printReport(std::ostream& out, uint64_t normalizationConstant)
  {
    std::stringstream header;
    std::stringstream data;
    printReport(header, data, normalizationConstant);
    out << header.str() << std::endl;
    out << data.str() << std::endl;
  }

  void printReport(std::ostream& headerOut, std::ostream& dataOut, uint64_t normalizationConstant)
  {
    if (!events.size())
      return;

    // print all metrics
    for (unsigned i = 0; i < events.size(); i++) {
      printCounter(headerOut, dataOut, names[i], events[i].readCounter() / normalizationConstant);
    }

    printCounter(headerOut, dataOut, "scale", normalizationConstant);

    // derived metrics
    if (IPCAvailable())
      printCounter(headerOut, dataOut, "IPC", getIPC());

    if (CPUsAvailable())
      printCounter(headerOut, dataOut, "CPU", getCPUs());
    
    if (GHzAvailable())
      printCounter(headerOut, dataOut, "GHz", getGHz(), false);
  }
  // -------------------------------------------------------------------------------------
  void printCSVHeaders(std::ostream& headerOut)
  {
    if (!events.size())
      return;

    // print all metrics
    for (unsigned i = 0; i < events.size(); i++) {
      headerOut << "," << names[i];
    }

    // derived metrics
    if (IPCAvailable())
      headerOut << "," << "IPC";

    if (CPUsAvailable())
      headerOut << "," << "CPU";

    if (GHzAvailable())
      headerOut << "," << "GHz";
  }
  // -------------------------------------------------------------------------------------
  void printCSVData(std::ostream& dataOut, uint64_t normalizationConstant)
  {
    if (!events.size())
      return;
    // print all metrics
    for (unsigned i = 0; i < events.size(); i++) {
      dataOut << "," << events[i].readCounter() / normalizationConstant;
    }
    // derived metrics
    if (IPCAvailable())
      dataOut << "," << getIPC();
    if (CPUsAvailable())
      dataOut << "," << getCPUs();
    if (GHzAvailable())
      dataOut << "," << getGHz();
  }
  // -------------------------------------------------------------------------------------
  std::vector<std::string> getEventsName()
  {
    std::vector<std::string> extendedNames;
    for (auto& name : names) {
      extendedNames.push_back(name);
    }
    if (IPCAvailable())
      extendedNames.push_back("IPC");
    if (CPUsAvailable())
      extendedNames.push_back("CPU");
    if (GHzAvailable())
      extendedNames.push_back("GHz");
    return extendedNames;
  }
  std::unordered_map<std::string, double> getCountersMap()
  {
    std::unordered_map<std::string, double> res;
    // print all metrics
    for (unsigned i = 0; i < events.size(); i++) {
      res[names[i]] = events[i].readCounter();
    }
    // derived metrics
    if (IPCAvailable())
      res["IPC"] = getIPC();
    if (CPUsAvailable())
      res["CPU"] = getCPUs();
    if (GHzAvailable())
      res["GHz"] = getGHz();
    return res;
  }
  // -------------------------------------------------------------------------------------
  void setParam(const std::string& name, const std::string& value) { params[name] = value; }

  void setParam(const std::string& name, const char* value) { params[name] = value; }

  template <typename T>
  void setParam(const std::string& name, T value)
  {
    setParam(name, std::to_string(value));
  }

  void printParams(std::ostream& header, std::ostream& data)
  {
    for (auto& p : params) {
      printCounter(header, data, p.first, p.second);
    }
  }
};

struct PerfEventBlock {
  PerfEvent& e;
  uint64_t scale;
  bool print_in_destructor = true;
  PerfEventBlock(PerfEvent& e, uint64_t scale = 1) : e(e), scale(scale) { e.startCounters(); }

  void printCounters()
  {
    std::stringstream header;
    std::stringstream data;
    e.printParams(header, data);
    PerfEvent::printCounter(header, data, "time", e.getDuration());
    e.printReport(header, data, scale);
    if (e.printHeader) {
      std::cout << header.str() << std::endl;
      e.printHeader = false;
    }
    std::cout << data.str() << std::endl;
  }

  ~PerfEventBlock()
  {
    if (print_in_destructor) {
      e.stopCounters();
      printCounters();
    }
  }
};

#else
#include <ostream>
struct PerfEvent {
  void startCounters() {}
  void stopCounters() {}
  void printReport(std::ostream&, uint64_t) {}
};
#endif
