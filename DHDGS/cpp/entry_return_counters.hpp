#pragma once
#include "value_utils.hpp"
#include <atomic>
#include <memory>
#include <thread>

namespace dhruntime {
// One packed atomic is an internally consistent (remaining sum, return count).
// Writers never acquire a mutex, allocate, wait for a reader, or compute means.
// Stripes spread independent merger threads across cache lines. Overflow is
// reported explicitly; wrapping must never silently reopen a boundary portal.
class EntryReturnCounters {
  struct alignas(64) Stripe {
    std::atomic<uint64_t> packed{0}, completed{0}, saturated{0};
  };
  size_t stripes_;
  std::unique_ptr<Stripe[]> values_;
public:
  static_assert(std::atomic<uint64_t>::is_always_lock_free,
                "Entry telemetry requires lock-free 64-bit atomics");
  explicit EntryReturnCounters(size_t stripes):stripes_(stripes),values_(new Stripe[stripes]) {}
  void record(bool returned, uint64_t remaining) noexcept {
    const auto lane=std::hash<std::thread::id>{}(std::this_thread::get_id())%stripes_;
    auto& s=values_[lane];s.completed.fetch_add(1,std::memory_order_relaxed);
    if(!returned)return;
    auto before=s.packed.load(std::memory_order_relaxed);
    for(;;){
      uint64_t count=before&0xffffffffULL, sum=before>>32;
      if(count==0xffffffffULL || remaining>0xffffffffULL-sum){
        s.saturated.fetch_add(1,std::memory_order_relaxed);return;
      }
      auto after=((sum+remaining)<<32)|(count+1);
      if(s.packed.compare_exchange_weak(before,after,std::memory_order_relaxed))return;
    }
  }
  dhwire::Value snapshot()const {
    uint64_t count=0,sum=0,completed=0,saturated=0;
    for(size_t i=0;i<stripes_;++i){
      auto pair=values_[i].packed.load(std::memory_order_relaxed);
      count+=pair&0xffffffffULL;sum+=pair>>32;
      completed+=values_[i].completed.load(std::memory_order_relaxed);
      saturated+=values_[i].saturated.load(std::memory_order_relaxed);
    }
    return dhvalue::M{{"returns",count},{"remaining_sum",sum},
                      {"completed",completed},{"saturated",saturated}};
  }
};
}
