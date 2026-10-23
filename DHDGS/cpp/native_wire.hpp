#pragma once
#include "native_transport.hpp"
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>
#include <sys/socket.h>
namespace pybind11 { class module_; class handle; class object; }
namespace dhwire {
struct Value {
  using Bytes = std::vector<uint8_t>;
  using Array = std::vector<Value>;
  using Map = std::map<std::string, Value>;
  using Data = std::variant<std::monostate, bool, int64_t, uint64_t, double,
                            std::string, Bytes, Array, Map>;
  Data data;
  Value() = default;
  Value(bool x): data(x) {}
  Value(int64_t x): data(x) {}
  Value(uint64_t x): data(x) {}
  Value(double x): data(x) {}
  Value(const char* x): data(std::string(x)) {}
  Value(std::string x): data(std::move(x)) {}
  Value(Bytes x): data(std::move(x)) {}
  Value(Array x): data(std::move(x)) {}
  Value(Map x): data(std::move(x)) {}
  const Map& map() const { return std::get<Map>(data); }
  Map& map() { return std::get<Map>(data); }
};
std::vector<uint8_t> pack(const Value&, size_t max_bytes = 67108864);
Value unpack(const uint8_t*, size_t, size_t max_depth = 64);
// Resolve once during configuration. Never call DNS on reactor/compute threads.
struct Endpoint {
  std::string key;
  sockaddr_storage address{};
  socklen_t address_length = 0;
  int family = AF_UNSPEC;
  Endpoint(const std::string& host, uint16_t port);
};
struct Config {
  std::vector<int> cpu_ids;
  std::string transport = "tcp";
  uint64_t rdma_poll_interval_us = 50;
  size_t max_pending = 4096;
  size_t max_connections_per_endpoint = 2; // 0: no application-level count limit
  uint64_t connection_idle_ms = 0; // 0 disables trimming excess idle connections
  size_t min_connections_per_endpoint = 0; // 0 preserves on-demand connection setup
  size_t max_connecting = 4; // per reactor, across endpoints
  bool async_connect = false; // bounded connector pool; reactor retains framing ownership
  bool async_connect_progress = true; // also progress unpublished CM/handshake events
  size_t max_message_bytes = 67108864;
  size_t max_endpoints = 64;
  uint64_t timeout_ms = 5000;
};
struct Result {
  bool ok = false; // successful framing + decoding; application ok inspected upstream
  std::string error;
  Value response;
  uint64_t request_bytes = 0, response_bytes = 0;
  // send_start = connection slot acquisition, before connect if a new socket.
  // Queue = send_start-queued; send phase = send_done-send_start, INCLUDES
  // connect. Do not add connect duration a second time when partitioning wall time.
  // done is after frame decoding; response wait = done-send_done includes decode.
  uint64_t queued_ns = 0, send_start_ns = 0, send_done_ns = 0, done_ns = 0;
  uint64_t connect_start_ns = 0, connect_done_ns = 0;
  bool connection_created = false;
};
class Client {
 public:
  using Callback = std::function<void(Result)>;
  explicit Client(Config config = {});
  ~Client();
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  // false = capacity/closed rejection, callback not retained or invoked.
  // Invalid payload throws before accepting. Callback must be short/nonblocking;
  // it may request close(), but must not destroy this Client on its reactor.
  bool submit(const Endpoint&, const Value&, Callback);
  bool prepare(const Endpoint&); // connection-only warmup, never sends a query
  Value metrics() const;
  size_t queued() const;
  size_t ready_connections() const;
  void close(); // cancel queued/inflight; callbacks invoked without internal locks
  size_t pending() const;
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
void bind_native_wire(pybind11::module_&);
Value from_python(pybind11::handle);
pybind11::object to_python(const Value&);
} // namespace dhwire
