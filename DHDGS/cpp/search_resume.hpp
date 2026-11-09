#pragma once
#include "native_wire.hpp"
#include "one_sided.hpp"
#include "demand_growth.hpp"
#include <functional>
#include <vector>

namespace dhsearch {
// Internal cooperative yield, deliberately not a std::exception: ordinary
// data-read failures fall back to graph handoff, suspension must not do so.
struct PullRequest {
  std::vector<uint64_t> ids;
  std::function<void(dhwire::Value)> resume;
  dhonesided::CodeLookup code_lookup;
  std::string mode="full";
  std::function<void(dhgrowth::Read,std::function<void(dhwire::Value)>)> demand_start;
};
// Cooperative control-plane suspension. A parked context owns no worker.
struct CandidatePause {
  dhwire::Value request;
  std::function<void(dhwire::Value)> resume;
};
}
