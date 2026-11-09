#pragma once
#include "value_utils.hpp"
#include "native_affinity.hpp"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <time.h>

namespace dhruntime {
// Bounded, prestarted workers. Tasks and callbacks always run outside mu_.
class Pool {
  mutable std::mutex mu_;
  std::condition_variable cv_, idle_;
  std::deque<std::function<void()>> queue_;
  std::deque<std::function<void()>> tail_queue_;
  size_t tail_capacity_=0,tail_active_=0,tail_peak_=0;
  double tail_fraction_=0,credit_ns_=0,credit_limit_ns_=0;
  uint64_t normal_cpu_ns_=0,tail_cpu_ns_=0;
  std::vector<std::thread> threads_;
  size_t capacity_, active_ = 0, ready_ = 0, peak_ = 0, rejected_ = 0,
                    errors_ = 0;
  bool stopping_ = false;
  std::function<void()> idle_flush_;
  void run() {
    {
      std::lock_guard<std::mutex> l(mu_);
      ++ready_;
      idle_.notify_all();
    }
    for (;;) {
      std::function<void()> fn;
      bool tail=false,shared=false;
      {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [&] { return stopping_ || !queue_.empty() || !tail_queue_.empty(); });
        if (queue_.empty() && tail_queue_.empty())
          return;
        shared=tail_capacity_>0;
        tail=!tail_queue_.empty()&&(queue_.empty()||credit_ns_>=0);
        auto& q=tail?tail_queue_:queue_;
        fn = std::move(q.front());q.pop_front();
        if(tail)++tail_active_;
        ++active_;
      }
      timespec begin{},end{};if(shared)clock_gettime(CLOCK_THREAD_CPUTIME_ID,&begin);
      try {
        fn();
      } catch (...) {
        std::lock_guard<std::mutex> l(mu_);
        ++errors_;
      }
      bool flush=false;
      if(shared)clock_gettime(CLOCK_THREAD_CPUTIME_ID,&end);
      {
        std::lock_guard<std::mutex> l(mu_);
        --active_;
        if(tail)--tail_active_;
        if(shared){const auto cpu=uint64_t((end.tv_sec-begin.tv_sec)*1000000000LL+end.tv_nsec-begin.tv_nsec);
          (tail?tail_cpu_ns_:normal_cpu_ns_)+=cpu;
          credit_ns_=std::clamp(credit_ns_+cpu*(tail?tail_fraction_-1:tail_fraction_),-credit_limit_ns_,credit_limit_ns_);}
        flush=queue_.empty()&&tail_queue_.empty();
        if (!active_ && flush)
          idle_.notify_all();
      }
      if(flush&&idle_flush_)idle_flush_();
    }
  }

public:
  Pool(size_t workers, size_t capacity, const std::vector<int>& cpus = {}, bool individual_cpu = true,
       std::function<void()> idle_flush = {}, const std::string& role = {}) : capacity_(capacity),idle_flush_(std::move(idle_flush)) {
    if (!workers || !capacity)
      throw std::invalid_argument("Invalid native worker pool limits");
    try {
      for (size_t i = 0; i < workers; ++i) {
        threads_.emplace_back([this, role] {
          if(!role.empty())pthread_setname_np(pthread_self(),role.substr(0,15).c_str());
          run();
        });
        if(!cpus.empty())bind_thread(threads_.back(),individual_cpu ? std::vector<int>{cpus[i%cpus.size()]} : cpus);
      }
      std::unique_lock<std::mutex> l(mu_);
      idle_.wait(l, [&] { return ready_ == workers; });
    } catch (...) {
      close();
      throw;
    }
  }
  ~Pool() { close(); }
  // Called during runtime construction, before accepting external requests.
  // The original workers and the additional workers can execute either queue.
  void share_tail(size_t workers,size_t capacity,const std::vector<int>& cpus,bool pin,double fraction,double limit_ms) {
    if(!capacity||!(fraction>0&&fraction<1)||!(limit_ms>0))throw std::invalid_argument("Invalid shared compute capacity");
    {std::lock_guard<std::mutex> l(mu_);if(tail_capacity_)throw std::logic_error("Shared tail already configured");
      tail_capacity_=capacity;tail_fraction_=fraction;credit_limit_ns_=limit_ms*1e6;}
    for(size_t i=0;i<workers;++i){threads_.emplace_back([this]{run();});if(!cpus.empty())bind_thread(threads_.back(),pin?std::vector<int>{cpus[i%cpus.size()]}:cpus);}
    std::unique_lock<std::mutex> l(mu_);idle_.wait(l,[&]{return ready_==threads_.size();});
  }
  bool submit_tail(std::function<void()> fn) {
    {std::lock_guard<std::mutex> l(mu_);if(stopping_||!tail_capacity_||tail_queue_.size()>=tail_capacity_){++rejected_;return false;}
      tail_queue_.push_back(std::move(fn));tail_peak_=std::max(tail_peak_,tail_queue_.size());peak_=std::max(peak_,queue_.size()+tail_queue_.size());}
    cv_.notify_one();return true;
  }
  bool submit(std::function<void()> fn) {
    {
      std::lock_guard<std::mutex> l(mu_);
      if (stopping_ || queue_.size() >= capacity_) {
        ++rejected_;
        return false;
      }
      queue_.push_back(std::move(fn));
      peak_ = std::max(peak_, queue_.size());
    }
    cv_.notify_one();
    return true;
  }
  // Opportunistic work must not consume the slots reserved for regular work.
  // Check pressure and enqueue under the same mutex; a metrics snapshot alone
  // would let concurrent reply callbacks over-admit helpers.
  bool submit_if_idle(std::function<void()> fn, size_t reserve, size_t max_queued) {
    {
      std::lock_guard<std::mutex> l(mu_);
      if (stopping_ || reserve >= threads_.size() || queue_.size() > max_queued ||
          queue_.size() >= capacity_ || active_ + queue_.size() >= threads_.size() - reserve)
        return false;
      queue_.push_back(std::move(fn));
      peak_ = std::max(peak_, queue_.size());
    }
    cv_.notify_one();
    return true;
  }
  void drain() {
    std::unique_lock<std::mutex> l(mu_);
    idle_.wait(l, [&] { return queue_.empty() && tail_queue_.empty() && !active_; });
  }
  void close() {
    {
      std::lock_guard<std::mutex> l(mu_);
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto &t : threads_)
      if (t.joinable())
        t.join();
  }
  dhwire::Value metrics() const {
    std::lock_guard<std::mutex> l(mu_);
    return dhwire::Value::Map{{"workers", uint64_t(threads_.size())},
                              {"queued", uint64_t(queue_.size()+tail_queue_.size())},
                              {"normal_queued",uint64_t(queue_.size())},{"tail_queued",uint64_t(tail_queue_.size())},
                              {"tail_active",uint64_t(tail_active_)},{"tail_queue_peak",uint64_t(tail_peak_)},
                              {"shared_compute",uint64_t(tail_capacity_>0)},{"tail_fraction",tail_fraction_},
                              {"normal_cpu_ns",normal_cpu_ns_},{"tail_cpu_ns",tail_cpu_ns_},
                              {"active", uint64_t(active_)},
                              {"queue_capacity", uint64_t(capacity_)},
                              {"peak_queued", uint64_t(peak_)},
                              {"capacity_rejections", uint64_t(rejected_)},
                              {"callback_errors", uint64_t(errors_)}};
  }
};
} // namespace dhruntime
