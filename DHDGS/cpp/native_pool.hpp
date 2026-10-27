#pragma once
#include "value_utils.hpp"
#include "native_affinity.hpp"
#include <algorithm>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace dhruntime {
// Bounded, prestarted workers. Tasks and callbacks always run outside mu_.
class Pool {
  mutable std::mutex mu_;
  std::condition_variable cv_, idle_;
  std::deque<std::function<void()>> queue_;
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
      {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [&] { return stopping_ || !queue_.empty(); });
        if (queue_.empty())
          return;
        fn = std::move(queue_.front());
        queue_.pop_front();
        ++active_;
      }
      try {
        fn();
      } catch (...) {
        std::lock_guard<std::mutex> l(mu_);
        ++errors_;
      }
      bool flush=false;
      {
        std::lock_guard<std::mutex> l(mu_);
        --active_;
        flush=queue_.empty();
        if (!active_ && queue_.empty())
          idle_.notify_all();
      }
      if(flush&&idle_flush_)idle_flush_();
    }
  }

public:
  Pool(size_t workers, size_t capacity, const std::vector<int>& cpus = {}, bool individual_cpu = true,
       std::function<void()> idle_flush = {}) : capacity_(capacity),idle_flush_(std::move(idle_flush)) {
    if (!workers || !capacity)
      throw std::invalid_argument("Invalid native worker pool limits");
    try {
      for (size_t i = 0; i < workers; ++i) {
        threads_.emplace_back([this] { run(); });
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
  void drain() {
    std::unique_lock<std::mutex> l(mu_);
    idle_.wait(l, [&] { return queue_.empty() && !active_; });
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
                              {"queued", uint64_t(queue_.size())},
                              {"active", uint64_t(active_)},
                              {"queue_capacity", uint64_t(capacity_)},
                              {"peak_queued", uint64_t(peak_)},
                              {"capacity_rejections", uint64_t(rejected_)},
                              {"callback_errors", uint64_t(errors_)}};
  }
};
} // namespace dhruntime
