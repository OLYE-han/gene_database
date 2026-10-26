#pragma once
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace py = pybind11;

// Immutable schema and atomic values: no shard-wide statistics mutex. These
// short Python calls retain the GIL, avoiding a release/reacquire per update.
// This removes an application mutex, not the interpreter's GIL.
class ExpansionCounters {
  std::vector<std::string> names_;
  std::unordered_map<std::string, size_t> positions_;
  struct alignas(64) Sum { std::atomic<double> value{0}; };
  size_t stripes_;
  std::unique_ptr<Sum[]> sums_;
  std::unique_ptr<std::atomic<double>[]> dense_;
  std::atomic<double>& cell(size_t index) const {
    return stripes_ == 1 ? dense_[index] : sums_[index].value;
  }
  inline static std::atomic<size_t> next_thread_{0};
  size_t stripe() const {
    // A process-wide thread identity avoids stale per-object TLS pointers when
    // an index is closed and a new counter object reuses the same address.
    static thread_local const size_t id = next_thread_.fetch_add(1, std::memory_order_relaxed);
    return id % stripes_;
  }
  std::atomic<uint64_t> requests_{0}, completed_{0}, failed_{0}, active_{0}, peak_{0};

public:
  void finish_native(bool success, const std::unordered_map<std::string, double>& stats) {
    if (success) {
      for (const auto& [name, value] : stats) {
        const auto found = positions_.find(name);
        if (found == positions_.end() || !std::isfinite(value) || value < 0)
          throw std::invalid_argument("Invalid native expansion metric: " + name);
      }
      const size_t base = stripe() * names_.size();
      for (const auto& [name, value] : stats) {
        auto& counter = cell(base + positions_.at(name));
        auto previous = counter.load(std::memory_order_relaxed);
        while (!counter.compare_exchange_weak(previous, previous + value, std::memory_order_relaxed)) {}
      }
    }
    (success ? completed_ : failed_).fetch_add(1, std::memory_order_relaxed);
    active_.fetch_sub(1, std::memory_order_relaxed);
  }

  explicit ExpansionCounters(const std::vector<std::string>& names, size_t stripes = 1)
      : names_(names), stripes_(stripes) {
    if (!stripes || stripes > 4096)
      throw std::invalid_argument("Invalid expansion counter stripe count");
    // Preserve the original dense layout when the experiment switch is off.
    if(stripes == 1) {
      dense_.reset(new std::atomic<double>[names.size()]);
      for(size_t i=0;i<names.size();++i)dense_[i].store(0,std::memory_order_relaxed);
    } else sums_.reset(new Sum[names.size() * stripes]);
    for (size_t i = 0; i < names.size(); ++i) {
      if (!positions_.emplace(names[i], i).second || names[i] == "requests" ||
          names[i] == "completed" || names[i] == "failed" ||
          names[i] == "active" || names[i] == "peak_active")
        throw std::invalid_argument("Duplicate or reserved expansion metric");
    }
  }

  void begin() {
    requests_.fetch_add(1, std::memory_order_relaxed);
    const auto current = active_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto previous = peak_.load(std::memory_order_relaxed);
    while (previous < current && !peak_.compare_exchange_weak(
        previous, current, std::memory_order_relaxed)) {}
  }

  void finish(bool success, const py::dict& stats) {
    // Validate before mutation: the caller can account for invalid input using
    // finish(false, {}) exactly once, without partially adding successful work.
    std::vector<std::pair<size_t, double>> updates;
    if (success) {
      updates.reserve(stats.size());
      for (auto item : stats) {
        if (!py::isinstance<py::int_>(item.second) &&
            !py::isinstance<py::float_>(item.second)) continue;
        const auto name = py::cast<std::string>(item.first);
        const auto found = positions_.find(name);
        if (found == positions_.end())
          throw std::invalid_argument("Unknown expansion metric: " + name);
        const double value = py::cast<double>(item.second);
        if (!std::isfinite(value) || value < 0)
          throw std::invalid_argument("Invalid expansion metric: " + name);
        updates.emplace_back(found->second, value);
      }
    }
    if (!active_.load(std::memory_order_relaxed))
      throw std::logic_error("Expansion finished without begin");
    const size_t base = stripe() * names_.size();
    for (auto [index, value] : updates) {
      auto& sum = cell(base + index);
      auto previous = sum.load(std::memory_order_relaxed);
      while (!sum.compare_exchange_weak(
          previous, previous + value, std::memory_order_relaxed)) {}
    }
    (success ? completed_ : failed_).fetch_add(1, std::memory_order_relaxed);
    active_.fetch_sub(1, std::memory_order_relaxed);
  }

  py::dict snapshot() const {
    py::dict out;
    out["requests"] = requests_.load(std::memory_order_relaxed);
    out["completed"] = completed_.load(std::memory_order_relaxed);
    out["failed"] = failed_.load(std::memory_order_relaxed);
    out["active"] = active_.load(std::memory_order_relaxed);
    out["peak_active"] = peak_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < names_.size(); ++i) {
      double sum = 0;
      for (size_t s = 0; s < stripes_; ++s)
        sum += cell(s * names_.size() + i).load(std::memory_order_relaxed);
      out[py::str(names_[i])] = sum;
    }
    return out;
  }

  bool is_lock_free() const {
    if (!requests_.is_lock_free()) return false;
    for (size_t i = 0; i < names_.size() * stripes_; ++i)
      if (!cell(i).is_lock_free()) return false;
    return true;
  }
};

