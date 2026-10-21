#pragma once
// NumPy-compatible integer SeedSequence and PCG64 XSL-RR. This is a small C++
// adaptation of NumPy v2.2.0 random/bit_generator.pyx and src/pcg64/pcg64.h.
// MIT License: Copyright (c) 2014, 2015 Melissa E. O'Neill;
// Copyright (c) 2015 Robert Kern; Copyright (c) 2019 NumPy Developers.
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions: The above copyright
// notice and this permission notice shall be included in all copies or
// substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS",
// WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
// TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
// THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace dhruntime {
class PCG64 {
  using U128 = __uint128_t;
  U128 state_ = 0, inc_ = 0;
  static constexpr U128 multiplier =
      (U128(2549297995355413924ULL) << 64) | 4865540595714422341ULL;
  static uint32_t mix(uint32_t x, uint32_t y) {
    uint32_t r = 0xca01f9ddU * x - 0x4973f715U * y;
    return r ^ (r >> 16);
  }
  static uint32_t hash(uint32_t v, uint32_t &c) {
    v ^= c;
    c *= 0x931e8875U;
    v *= c;
    return v ^ (v >> 16);
  }

public:
  explicit PCG64(uint64_t seed) {
    uint32_t pool[4], constant = 0x43b0d7e5U;
    for (int i = 0; i < 4; ++i)
      pool[i] = hash(i == 0   ? uint32_t(seed)
                     : i == 1 ? uint32_t(seed >> 32)
                              : 0,
                     constant);
    for (int i = 0; i < 4; ++i)
      for (int j = 0; j < 4; ++j)
        if (i != j)
          pool[j] = mix(pool[j], hash(pool[i], constant));
    uint32_t words[8];
    constant = 0x8b51f9ddU;
    for (int i = 0; i < 8; ++i) {
      uint32_t x = pool[i % 4] ^ constant;
      constant *= 0x58f38dedU;
      x *= constant;
      words[i] = x ^ (x >> 16);
    }
    uint64_t s[4];
    for (int i = 0; i < 4; ++i)
      s[i] = uint64_t(words[2 * i]) | (uint64_t(words[2 * i + 1]) << 32);
    inc_ = (((U128(s[2]) << 64) | s[3]) << 1) | 1;
    state_ = inc_;
    state_ += (U128(s[0]) << 64) | s[1];
    state_ = state_ * multiplier + inc_;
  }
  uint64_t raw() {
    state_ = state_ * multiplier + inc_;
    uint64_t x = uint64_t(state_) ^ uint64_t(state_ >> 64);
    unsigned r = unsigned(state_ >> 122);
    return (x >> r) | (x << ((-r) & 63));
  }
  double random() { return (raw() >> 11) * (1.0 / 9007199254740992.0); }
  uint32_t child_seed() { return uint32_t(raw()); }
  std::vector<double> draws(size_t n) {
    std::vector<double> out;
    out.reserve(n);
    while (n--)
      out.push_back(random());
    return out;
  }
};
} // namespace dhruntime
