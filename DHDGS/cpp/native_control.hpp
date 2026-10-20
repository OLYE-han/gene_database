#pragma once
#include "native_wire.hpp"
#include <memory>
#include <string>
namespace pybind11 { class dict; }
namespace dhsearch { class Control; }
namespace dhcontrol {
struct Engine;
struct Branch;
class Ticket {
  std::weak_ptr<Engine> engine_;
  std::shared_ptr<Branch> branch_;
  friend class Controller;
public:
  Ticket(std::weak_ptr<Engine>, std::shared_ptr<Branch>);
  void start(double admission_ms, double executor_start_ms, double ready_ms);
  void child(dhwire::Value descriptor);
  void transfer_done(dhwire::Value peer_metrics, std::string error = "");
  void complete(dhwire::Value report);
  void fail(std::string error);
  void renew(double lease);
  void cancel();
};
class Controller {
  std::shared_ptr<Engine> engine_;
public:
  explicit Controller(pybind11::dict config);
  ~Controller();
  std::shared_ptr<Ticket> register_branch(pybind11::dict metadata,
                                        std::shared_ptr<dhsearch::Control> control);
  dhwire::Value snapshot(bool include_reports = false) const;
  dhwire::Value take_updates();
  void close();
};
void register_native_control(pybind11::module_&);
} // namespace dhcontrol
