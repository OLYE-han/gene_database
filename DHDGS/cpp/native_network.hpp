#pragma once
#include "native_server.hpp"
namespace dhnet {
// Shared policy and implementation for all inter-node RPC traffic. Application
// state, retries and graph expansion decisions stay above this module.
struct Policy {
  std::vector<int> cpu_ids;
  std::string transport="tcp";
  uint64_t rdma_poll_interval_us=50;
  size_t warm_connections=0,max_connecting=4;
  bool async_connect=false;
  bool async_connect_progress=true;
  bool queued_pressure=false,rebalance_connections=false;
  uint64_t rebalance_interval_ms=20;
  size_t rebalance_batch=8;
  double scale_busy_fraction=0.5; // Grow only when existing lanes are substantially busy.
  bool enabled=false;
  size_t tx_min=1,tx_max=1,rx_min=1,rx_max=1,handlers=4;
  size_t connections=8,send_capacity=4096,receive_capacity=4096,max_connections=4096;
  size_t tx_high=8,rx_high=4,high_samples=2;
  uint64_t sample_ms=20,cooldown_ms=100,idle_ms=1000,idle_probe_ms=1000;
  explicit Policy(const dhwire::Value& config);
};
class Client {
  struct Core;std::shared_ptr<Core> core_;
public:
  using Callback=dhwire::Client::Callback;
  Client(dhwire::Config,const dhwire::Value&);
  ~Client();
  bool submit(const dhwire::Endpoint&,const dhwire::Value&,Callback);
  size_t pending()const;
  dhwire::Value metrics()const;
  void close();
};
class Server {
  struct Core;std::shared_ptr<Core> core_;
public:
  using Handler=dhwire::Server::Handler;
  Server(dhwire::ServerConfig,Handler,Handler,const dhwire::Value&);
  ~Server();
  uint16_t port()const;
  dhwire::Value metrics()const;
  void close();
};
void bind_native_network(pybind11::module_&);
}
