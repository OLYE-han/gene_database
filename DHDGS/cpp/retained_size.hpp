#pragma once
#include "native_wire.hpp"
namespace dhruntime {
  // Conservative retained-object accounting, not a process RSS measurement.
  // Charge capacities, map nodes, string capacities (even SSO), plus allocator
  // slack. The Python capture layer separately reserves per-record metadata.
inline uint64_t retained_extra_bytes(const dhwire::Value &v) {
    if (auto p = std::get_if<std::string>(&v.data)) return p->capacity() + 65;
    if (auto p = std::get_if<dhwire::Value::Bytes>(&v.data)) return p->capacity() + 64;
    if (auto p = std::get_if<dhwire::Value::Array>(&v.data)) {
      uint64_t total = p->capacity() * sizeof(dhwire::Value) + 64;
      for (const auto &child : *p) total += retained_extra_bytes(child);
      return total;
    }
    if (auto p = std::get_if<dhwire::Value::Map>(&v.data)) {
      uint64_t total = 64;
      for (const auto &entry : *p)
        total += sizeof(dhwire::Value::Map::value_type) + 64 + entry.first.capacity() + 65
                 + retained_extra_bytes(entry.second);
      return total;
    }
    return 0;
  }
}
