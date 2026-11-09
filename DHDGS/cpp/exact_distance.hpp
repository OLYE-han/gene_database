#pragma once
#include <cmath>
#include <cstddef>
#include <cstring>
#include <stdexcept>

namespace dhsearch {
// Disk records need not be float-aligned. Keep double accumulation and IEEE
// NaN/Inf propagation; never compile this kernel with finite/fast-math flags.
// A finite sum of nonnegative squares proves every input component was finite.
// Checking once after the reduction lets the compiler vectorize the hot loop.
inline double exact_squared_l2(const float* query, const void* record, size_t dim) {
  const auto* bytes = static_cast<const unsigned char*>(record);
  double sum = 0.;
  #pragma omp simd reduction(+:sum)
  for (size_t j = 0; j < dim; ++j) {
    float value;
    std::memcpy(&value, bytes + j * sizeof(float), sizeof(value));
    const double delta = double(query[j]) - double(value);
    sum += delta * delta;
  }
  if (!std::isfinite(sum)) throw std::invalid_argument("Nonfinite exact distance input");
  return sum;
}
}
