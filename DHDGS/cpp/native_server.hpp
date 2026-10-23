#pragma once
#include "native_wire.hpp"
#include <functional>
#include <atomic>
#include <memory>

namespace dhwire {
struct ServerConfig {
  std::vector<int> cpu_ids;
  std::string transport = "tcp";
  uint64_t rdma_poll_interval_us = 50;
  bool external_accept = false;
  std::string host = "127.0.0.1";
  uint16_t port = 0;
  size_t workers = 4, queue_capacity = 4096, max_connections = 4096;
  size_t max_message_bytes = 67108864, listen_backlog = 128;
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
