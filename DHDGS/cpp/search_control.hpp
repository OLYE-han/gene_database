#pragma once
#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <time.h>

namespace dhsearch {
inline int64_t now_ns() {
  timespec t{}; clock_gettime(CLOCK_MONOTONIC, &t);
  return int64_t(t.tv_sec)*1000000000+t.tv_nsec;
}
struct Cancelled : std::runtime_error { using std::runtime_error::runtime_error; };
class Control {
  std::atomic<double> lease_, deadline_;
  std::atomic<bool> stopped_{false};
public:
  Control(double lease, double deadline):lease_(lease),deadline_(deadline){}
  void renew(double lease) { lease_.store(lease, std::memory_order_relaxed); }
  void cancel() { stopped_.store(true, std::memory_order_relaxed); }
  bool cancelled() const {
    const double now=now_ns()/1e9;
    return stopped_.load(std::memory_order_relaxed) ||
      now>=lease_.load(std::memory_order_relaxed) || now>=deadline_.load(std::memory_order_relaxed);
  }
  void check() const { if(cancelled()) throw Cancelled("Query cancelled, lease or deadline expired"); }
};
}
