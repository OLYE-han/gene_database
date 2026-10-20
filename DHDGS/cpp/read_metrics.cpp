#include "read_metrics.hpp"

void register_read_metrics(py::module_& m) {
  py::class_<ExpansionCounters>(m, "ExpansionCounters")
      .def(py::init<const std::vector<std::string>&>())
      .def("begin", &ExpansionCounters::begin)
      .def("finish", &ExpansionCounters::finish)
      .def("snapshot", &ExpansionCounters::snapshot)
      .def_property_readonly("is_lock_free", &ExpansionCounters::is_lock_free);
}
