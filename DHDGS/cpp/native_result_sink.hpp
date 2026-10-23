#pragma once
#include "native_head.hpp"
namespace dhruntime {
// Native retrieval captures results in RAM; only after all retrievals finish
// do parallel writers encode/write/sync. No Python or GIL in either phase.
V run_disk_queries(HeadRuntime&, const double* vectors, const uint64_t* ids,
                   size_t count, size_t dimension, size_t concurrency,
                   const std::string& directory, const V& settings, size_t max_message);
}
