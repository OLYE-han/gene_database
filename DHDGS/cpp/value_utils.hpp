#pragma once
#include "native_wire.hpp"
#include <cmath>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace dhvalue {
using V = dhwire::Value;
using M = V::Map;
using A = V::Array;
inline const V &at(const V &v, const std::string &key) {
  return v.map().at(key);
}
inline bool has(const V &v, const std::string &key) {
  return v.map().count(key) != 0;
}
inline bool null(const V &v) {
  return std::holds_alternative<std::monostate>(v.data);
}
inline const A &array(const V &v) { return std::get<A>(v.data); }
inline const std::string &str(const V &v) {
  return std::get<std::string>(v.data);
}
inline double number(const V &v) {
  if (auto p = std::get_if<double>(&v.data))
    return *p;
  if (auto p = std::get_if<uint64_t>(&v.data))
    return double(*p);
  if (auto p = std::get_if<int64_t>(&v.data))
    return double(*p);
  throw std::invalid_argument("Expected numeric value");
}
template <class T> T cast(const V &v) {
  if constexpr (std::is_same_v<T, bool>)
    return std::get<bool>(v.data);
  else if constexpr (std::is_same_v<T, std::string>)
    return str(v);
  else if constexpr (std::is_floating_point_v<T>)
    return T(number(v));
  else if constexpr (std::is_integral_v<T>) {
    if (auto p = std::get_if<uint64_t>(&v.data)) {
      if (*p > uint64_t(std::numeric_limits<T>::max()))
        throw std::invalid_argument("Integer out of range");
      return T(*p);
    }
    if (auto p = std::get_if<int64_t>(&v.data)) {
      if constexpr (std::is_unsigned_v<T>) {
        if (*p < 0 || uint64_t(*p) > std::numeric_limits<T>::max())
          throw std::invalid_argument("Integer out of range");
      } else if (*p < std::numeric_limits<T>::min() ||
                 *p > std::numeric_limits<T>::max())
        throw std::invalid_argument("Integer out of range");
      return T(*p);
    }
    throw std::invalid_argument("Expected integer value");
  } else {
    T result;
    for (const auto &item : array(v))
      result.push_back(cast<typename T::value_type>(item));
    return result;
  }
}
template <class T> T get(const V &v, const std::string &key) {
  return cast<T>(at(v, key));
}
template <class T> T optional(const V &v, const std::string &key, T fallback) {
  return has(v, key) ? get<T>(v, key) : fallback;
}
template <class T> V vector(const std::vector<T> &values) {
  A out;
  out.reserve(values.size());
  for (auto x : values) {
    if constexpr (std::is_integral_v<T> && std::is_unsigned_v<T>)
      out.emplace_back(uint64_t(x));
    else if constexpr (std::is_integral_v<T>)
      out.emplace_back(int64_t(x));
    else
      out.emplace_back(x);
  }
  return out;
}
inline void add(M &m, const std::string &key, double value) {
  auto p = m.find(key);
  m[key] = (p == m.end() ? 0. : number(p->second)) + value;
}
} // namespace dhvalue
