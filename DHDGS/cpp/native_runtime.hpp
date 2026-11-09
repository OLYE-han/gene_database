#pragma once
#include "native_control.hpp"
#include "native_server.hpp"
#include "search_control.hpp"
#include "one_sided.hpp"
#include <functional>

namespace dhruntime {
using V = dhwire::Value;
using SearchFunction = std::function<void(
    const V &, const std::vector<double> &, std::shared_ptr<dhsearch::Control>,
    const std::function<V(V)> &, std::shared_ptr<dhcontrol::Ticket>, std::shared_ptr<void>,
    const std::function<V(int,const std::vector<uint64_t>&)>&)>;
using SearchPrepare = std::function<std::shared_ptr<void>(const V&)>;
using RecordRead = std::function<V(const std::vector<uint64_t>&,size_t)>;
// No Python objects, callbacks or APIs are used by this runtime.
class DataRuntime {
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  DataRuntime(V config, V shard_metadata, std::string signature,
              SearchFunction, SearchPrepare = {}, RecordRead = {}, dhonesided::Export = {});
  ~DataRuntime();
  dhwire::Reply handle(V request, uint64_t received_ns, size_t rx_bytes);
  V metrics() const;
  V network_metrics() const;
  void close();
};
std::string identifier();
bool trace_selected(const V &config, const V &query_id, bool lifecycle = false);
} // namespace dhruntime
