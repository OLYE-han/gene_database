#pragma once
#include "native_wire.hpp"
#include <functional>
#include <atomic>
#include <memory>

namespace dhwire {
// Local instrumentation only: never accepted from a client-supplied payload.
struct HandlerTiming {
  uint64_t worker_enter_ns=0,decode_done_ns=0,handler_enter_ns=0;
  uint64_t gate_enter_ns=0,gate_acquired_ns=0;
};
inline thread_local HandlerTiming handler_timing;
struct ServerConfig {
  std::vector<int> cpu_ids;
  std::string thread_role = "dhdgs-net";
  std::string transport = "tcp";
  uint64_t rdma_poll_interval_us = 50;
  bool external_accept = false;
  std::string host = "127.0.0.1";
  uint16_t port = 0;
  size_t workers = 4, queue_capacity = 4096, max_connections = 4096;
  size_t max_message_bytes = 67108864, listen_backlog = 128;
  // 0 preserves legacy drain-all behavior. A positive batch interleaves
  // response encoding with socket progress instead of delaying early replies.
  size_t response_batch_size = 0;
  uint64_t idle_timeout_ms = 30000;
  bool reuse_port=false;
  std::shared_ptr<std::atomic<size_t>> connection_budget;
};
struct Reply {
  Value message;

  // Runs on the I/O reactor after complete response transmission. No Python,
  // blocking network, or long computation is permitted in this callback.
  std::function<void(uint64_t, size_t)> sent;
  std::function<void(std::function<void(Reply)>)> deferred;
  // Set only by the local API adapter; never inferred from request payloads.
  bool trace_response = false;
};
class Server {
public:
  using Handler = std::function<Reply(Value, uint64_t, size_t)>;
  Server(ServerConfig, Handler data, Handler admin = {});
  ~Server();
  Server(const Server &) = delete;
  bool adopt(int fd); // true transfers descriptor ownership to this reactor
  bool adopt_transferred(int fd); // already holds the shared connection-budget credit
  void migrate_idle(std::function<bool(int)> target, size_t maximum);
  void stop_accepting(); // drain persistent connections without dropping requests
  void close();
  uint16_t port() const;
  Value metrics() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
} // namespace dhwire
