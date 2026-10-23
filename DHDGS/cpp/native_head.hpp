#pragma once
#include "native_runtime.hpp"

namespace dhruntime {
// Route function returns the same initial state used by the Python entry.
using RouteFunction = std::function<V(const std::vector<double> &, const V &)>;
class HeadRuntime {
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  HeadRuntime(V config, V manifest, RouteFunction);
  ~HeadRuntime();
  // Internal native completion callback: short, nonthrowing, no file I/O.
  // Invoked after releasing head/query locks and the active-query slot.
  using Completion = std::function<void(V)>;
  std::string submit(std::vector<double>, V query_id, std::string request_id = {},
                     Completion completion = {});
  V completed(size_t maximum, uint64_t timeout_ms);
  V metrics() const;
  void close();
};
// Also exposed for protocol/adversarial tests without running searches.
bool validate_manifests(const V &reports, const std::string &root, size_t node,
                        size_t budget, size_t slots, size_t node_count);
V aggregate_native(const V &config, const V &reports, const V &transfer,
                   const V &involved);
} // namespace dhruntime
