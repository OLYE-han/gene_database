#pragma once
#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <time.h>
#include <memory>
#include <limits>

namespace dhsearch {
inline int64_t now_ns() {
  timespec t{}; clock_gettime(CLOCK_MONOTONIC, &t);
  return int64_t(t.tv_sec)*1000000000+t.tv_nsec;
}
struct Cancelled : std::runtime_error { using std::runtime_error::runtime_error; };
// Fixed, query-budget-bounded exact set. No resize and no blocking lock on
// compute threads. CAS publishes the complete uint64 ID atomically; UINT64_MAX
// has its own bit so every user ID remains representable. Linear probing is
// bounded by capacity (at most half full for a valid credit tree).
class ExpansionClaims {
  size_t size_=1;
  std::unique_ptr<std::atomic<uint64_t>[]> slots_;
  std::atomic<bool> maximum_{false};
public:
  explicit ExpansionClaims(size_t budget){
    if(budget>size_t(1)<<28)throw std::invalid_argument("Expansion claim budget too large");
    while(size_<2*budget)size_*=2;
    slots_=std::make_unique<std::atomic<uint64_t>[]>(size_);
    for(size_t i=0;i<size_;++i)slots_[i].store(UINT64_MAX,std::memory_order_relaxed);
  }
  bool claim(uint64_t id){
    if(id==UINT64_MAX)return !maximum_.exchange(true,std::memory_order_relaxed);
    uint64_t h=id;h=(h^(h>>30))*0xbf58476d1ce4e5b9ULL;h=(h^(h>>27))*0x94d049bb133111ebULL;h^=h>>31;
    for(size_t i=0;i<size_;++i){auto& slot=slots_[(h+i)&(size_-1)];uint64_t expected=UINT64_MAX;
      if(slot.compare_exchange_strong(expected,id,std::memory_order_relaxed))return true;
      if(expected==id)return false;
    }
    throw std::runtime_error("Expansion claims exceed conserved query budget");
  }
};
class Control {
  std::atomic<double> lease_, deadline_;
  std::atomic<bool> stopped_{false};
  std::shared_ptr<ExpansionClaims> claims_;

public:
  Control(double lease, double deadline):lease_(lease),deadline_(deadline){}
  void claims(std::shared_ptr<ExpansionClaims> c){claims_=std::move(c);} // before pool publication
  bool claim(uint64_t id){return !claims_||claims_->claim(id);}
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
