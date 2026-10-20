#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <rabitqlib/core/api.hpp>
#include <rabitqlib/core/rotator.hpp>
#include <rabitqlib/third/hnswlib/hnswlib.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <stdexcept>
#include <unordered_set>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>

namespace py = pybind11;
namespace api = rabitqlib::quant::v1;
template <typename T>
using Array = py::array_t<T, py::array::c_style | py::array::forcecast>;

void require(bool condition, const char *message) {
  if (!condition)
    throw std::invalid_argument(message);
}

void finite(const float *x, size_t n) {
  for (size_t i = 0; i < n; ++i)
    require(std::isfinite(x[i]), "Non-finite native input");
}

// Storage-neutral upstream RaBitQ API. Persist codes/factors and actual
// rotation separately.
class Quantizer {
  size_t dim_, padded_, bits_, centers_count_;
  std::unique_ptr<rabitqlib::Rotator<float>> rotation_;
  std::vector<float> centers_;
  api::Parameters parameters_;
  api::Requirements sizes_;

public:
  Quantizer(Array<float> centers, size_t bits, uint32_t seed, py::bytes state) {
    require(centers.ndim() == 2 && centers.shape(0) > 0 &&
                centers.shape(1) >= 64,
            "RaBitQ FHT requires nonempty centers and dimension >= 64");
    dim_ = centers.shape(1);
    centers_count_ = centers.shape(0);
    bits_ = bits;
    require(bits >= 1 && bits <= 9, "RaBitQ bits must be in [1,9]");
    finite(centers.data(), centers.size());
    rotation_.reset(rabitqlib::choose_rotator<float>(
        dim_, rabitqlib::RotatorType::FhtKacRotator, 0, seed));
    padded_ = rotation_->size();
    std::string bytes = state;
    if (!bytes.empty()) {
      require(bytes.size() == rotation_->dump_bytes(),
              "Wrong rotation state length");
      rotation_->load(bytes.data());
    }
    centers_.resize(centers_count_ * padded_);
    for (size_t i = 0; i < centers_count_; ++i)
      rotation_->rotate(centers.data() + i * dim_,
                        centers_.data() + i * padded_);
    parameters_ = {padded_, bits_, rabitqlib::METRIC_L2};
    sizes_ = api::requirements(parameters_, api::CodeLayout::SplitPacked);
  }

  py::bytes state() const {
    std::string bytes(rotation_->dump_bytes(), '\0');
    rotation_->save(bytes.data());
    return py::bytes(bytes);
  }

  py::tuple encode(Array<float> x, Array<uint32_t> center_ids) const {
    require(x.ndim() == 2 && size_t(x.shape(1)) == dim_ &&
                center_ids.ndim() == 1 && center_ids.shape(0) == x.shape(0),
            "Wrong encode dimensions");
    finite(x.data(), x.size());
    size_t n = x.shape(0);
    for (size_t i = 0; i < n; ++i)
      require(center_ids.data()[i] < centers_count_, "Invalid center ID");
    py::array_t<uint8_t> codes({n, sizes_.code_bytes}),
        extra({n, sizes_.extra_bytes});
    py::array_t<float> factors({n, size_t(3)});
    auto cp = codes.mutable_data();
    auto ep = extra.mutable_data();
    auto fp = factors.mutable_data();
    {
      py::gil_scoped_release release;
      std::vector<float> rotated(padded_);
      std::vector<std::byte> scratch(sizes_.workspace_bytes +
                                     sizes_.workspace_alignment);
      void *workspace = scratch.data();
      size_t space = scratch.size();
      require(std::align(sizes_.workspace_alignment, sizes_.workspace_bytes,
                         workspace, space) != nullptr,
              "Workspace alignment failed");
      for (size_t i = 0; i < n; ++i) {
        rotation_->rotate(x.data() + i * dim_, rotated.data());
        const auto factor = api::encode(
            parameters_, rotated.data(),
            centers_.data() + center_ids.data()[i] * padded_,
            {api::CodeLayout::SplitPacked,
             {cp + i * sizes_.code_bytes, sizes_.code_bytes},
             {sizes_.extra_bytes ? ep + i * sizes_.extra_bytes : nullptr,
              sizes_.extra_bytes}},
            {workspace, sizes_.workspace_bytes});
        fp[3 * i] = factor.add;
        fp[3 * i + 1] = factor.rescale;
        fp[3 * i + 2] = factor.error;
      }
    }
    return py::make_tuple(codes, extra, factors);
  }

  struct Prepared {
    std::vector<float> rotated;
    std::vector<api::QueryState> states;
    std::vector<float> norms;
  };

  Prepared prepare_native(const float* query) const {
    Prepared out;
    out.rotated.resize(padded_);
    rotation_->rotate(query, out.rotated.data());
    for (size_t c = 0; c < centers_count_; ++c) {
      out.states.push_back(api::prepare_query(parameters_, out.rotated.data(),
                                              centers_.data() + c * padded_));
      double sum = 0;
      for (size_t j = 0; j < padded_; ++j) {
        double d = double(out.rotated[j]) - centers_[c * padded_ + j];
        sum += d * d;
      }
      out.norms.push_back(std::sqrt(sum));
    }
    return out;
  }

  std::pair<float, float> score_native(const Prepared& query, const uint8_t* codes,
      const uint8_t* extra, const float* factors, uint32_t center) const {
    require(center < centers_count_, "Invalid center ID");
    api::Factors f{factors[0], factors[1], factors[2]};
    return {api::compute(query.states[center],
        {api::CodeLayout::SplitPacked, {codes, sizes_.code_bytes},
         {sizes_.extra_bytes ? extra : nullptr, sizes_.extra_bytes}}, f),
        std::abs(f.error) * query.norms[center]};
  }

  py::tuple score(Array<float> q, Array<uint8_t> codes, Array<uint8_t> extra,
                  Array<float> factors, Array<uint32_t> center_ids) const {
    require(q.ndim() == 1 && size_t(q.size()) == dim_, "Wrong query dimension");
    size_t n = center_ids.size();
    require(center_ids.ndim() == 1 && codes.ndim() == 2 && extra.ndim() == 2 &&
                factors.ndim() == 2 && size_t(codes.shape(0)) == n &&
                size_t(codes.shape(1)) == sizes_.code_bytes &&
                size_t(extra.shape(0)) == n &&
                size_t(extra.shape(1)) == sizes_.extra_bytes &&
                size_t(factors.shape(0)) == n && factors.shape(1) == 3,
            "Invalid code bank layout");
    finite(q.data(), q.size());
    finite(factors.data(), factors.size());
    for (size_t i = 0; i < n; ++i)
      require(center_ids.data()[i] < centers_count_, "Invalid center ID");
    py::array_t<float> distances(n), uncertainty(n);
    auto dp = distances.mutable_data();
    auto up = uncertainty.mutable_data();
    {
      py::gil_scoped_release release;
      std::vector<float> rotated(padded_);
      rotation_->rotate(q.data(), rotated.data());
      std::vector<api::QueryState> states;
      std::vector<float> norms;
      for (size_t c = 0; c < centers_count_; ++c) {
        states.push_back(api::prepare_query(parameters_, rotated.data(),
                                            centers_.data() + c * padded_));
        double sum = 0;
        for (size_t j = 0; j < padded_; ++j) {
          double d = double(rotated[j]) - centers_[c * padded_ + j];
          sum += d * d;
        }
        norms.push_back(std::sqrt(sum));
      }
      for (size_t i = 0; i < n; ++i) {
        api::Factors f{factors.data()[3 * i], factors.data()[3 * i + 1],
                       factors.data()[3 * i + 2]};
        dp[i] = api::compute(
            states[center_ids.data()[i]],
            {api::CodeLayout::SplitPacked,
             {codes.data() + i * sizes_.code_bytes, sizes_.code_bytes},
             {sizes_.extra_bytes ? extra.data() + i * sizes_.extra_bytes
                                 : nullptr,
              sizes_.extra_bytes}},
            f);
        up[i] = std::abs(f.error) * norms[center_ids.data()[i]];
      }
    }
    return py::make_tuple(distances, uncertainty);
  }
};

// Vamana: random initialization, bounded greedy search, RobustPrune, reciprocal
// edges.
std::vector<std::vector<uint32_t>> build_vamana(Array<float> vectors,
                                                size_t degree, size_t width,
                                                std::vector<double> alphas,
                                                uint32_t seed,
                                                py::object progress = py::none(),
                                                size_t threads = 1) {
  require(vectors.ndim() == 2 && vectors.shape(0) > 0 && vectors.shape(1) > 0,
          "Vamana requires a nonempty matrix");
  size_t n = vectors.shape(0), dim = vectors.shape(1);
  require(n < uint64_t(1) << 32 && degree > 0 && width >= degree &&
              !alphas.empty() && threads > 0,
          "Invalid Vamana limits");
  for (double a : alphas)
    require(std::isfinite(a) && a >= 1, "Pruning alpha must be >= 1");
  finite(vectors.data(), vectors.size());
  py::gil_scoped_release release;
  const float *x = vectors.data();
  auto distance = [&](uint32_t a, uint32_t b) {
    double sum = 0;
    #pragma omp simd reduction(+:sum)
    for (size_t j = 0; j < dim; ++j) {
      double d = double(x[a * dim + j]) - x[b * dim + j];
      sum += d * d;
    }
    return sum;
  };
  auto prune = [&](uint32_t node, std::vector<uint32_t> candidates,
                   double alpha) {
    std::sort(candidates.begin(), candidates.end());
    candidates.erase(std::unique(candidates.begin(), candidates.end()),
                     candidates.end());
    std::vector<std::pair<double, uint32_t>> ranked;
    for (auto c : candidates)
      if (c != node)
        ranked.emplace_back(distance(node, c), c);
    std::sort(ranked.begin(), ranked.end());
    std::vector<uint32_t> result;
    std::vector<bool> removed(ranked.size());
    for (size_t i = 0; i < ranked.size() && result.size() < degree; ++i) {
      if (removed[i])
        continue;
      result.push_back(ranked[i].second);
      for (size_t j = i + 1; j < ranked.size(); ++j)
        if (!removed[j] &&
            alpha * alpha * distance(ranked[i].second, ranked[j].second) <=
                ranked[j].first)
          removed[j] = true;
    }
    return result;
  };
  std::mt19937 rng(seed);
  std::vector<std::vector<uint32_t>> graph(n);
  size_t initial_degree = std::min(degree, n - 1);
  for (uint32_t i = 0; i < n; ++i) {
    std::unordered_set<uint32_t> choices;
    while (choices.size() < initial_degree) {
      uint32_t j = rng() % n;
      if (j != i)
        choices.insert(j);
    }
    graph[i].assign(choices.begin(), choices.end());
    std::sort(graph[i].begin(), graph[i].end());
  }
  std::vector<double> mean(dim);
  for (size_t i = 0; i < n; ++i)
    for (size_t j = 0; j < dim; ++j)
      mean[j] += x[i * dim + j] / double(n);
  uint32_t entry = 0;
  double closest = INFINITY;
  for (uint32_t i = 0; i < n; ++i) {
    double d = 0;
    for (size_t j = 0; j < dim; ++j) {
      double delta = x[i * dim + j] - mean[j];
      d += delta * delta;
    }
    if (d < closest) {
      closest = d;
      entry = i;
    }
  }
  std::vector<uint32_t> order(n);
  std::iota(order.begin(), order.end(), 0);
  // Each adjacency list has one lock. No operation holds two graph locks;
  // reciprocal updates and candidate snapshots are race-free.
  std::vector<std::mutex> graph_locks(n);
  for (double alpha : alphas) {
    std::shuffle(order.begin(), order.end(), rng);
    std::atomic<size_t> next{0},completed{0};
    std::atomic<bool> failed{false};
    std::exception_ptr failure;
    std::mutex failure_lock;
    auto work = [&]() {
      try {
      while (!failed.load()) {
      size_t position=next.fetch_add(1);
      if (position>=n) break;
      uint32_t node=order[position];
      using Item = std::pair<double, uint32_t>;
      std::set<Item> frontier, best;
      std::unordered_set<uint32_t> seen;
      std::vector<uint32_t> visited;
      Item start{distance(node, entry), entry};
      frontier.insert(start);
      best.insert(start);
      seen.insert(entry);
      while (!frontier.empty()) {
        Item current = *frontier.begin();
        frontier.erase(frontier.begin());
        if (best.size() >= width && current > *best.rbegin())
          break;
        visited.push_back(current.second);
        std::vector<uint32_t> neighbors;
        {
          std::lock_guard<std::mutex> guard(graph_locks[current.second]);
          neighbors=graph[current.second];
        }
        for (auto neighbor : neighbors)
          if (seen.insert(neighbor).second) {
            Item next{distance(node, neighbor), neighbor};
            if (best.size() < width || next < *best.rbegin()) {
              best.insert(next);
              frontier.insert(next);
              if (best.size() > width)
                best.erase(std::prev(best.end()));
            }
          }
      }
      std::vector<uint32_t> outgoing;
      {
        std::lock_guard<std::mutex> guard(graph_locks[node]);
        visited.insert(visited.end(), graph[node].begin(), graph[node].end());
        graph[node] = prune(node, std::move(visited), alpha);
        outgoing = graph[node];
      }
      for (auto other : outgoing) {
        std::lock_guard<std::mutex> guard(graph_locks[other]);
        auto &reverse = graph[other];
        if (std::find(reverse.begin(), reverse.end(), node) == reverse.end())
          reverse.push_back(node);
        if (reverse.size() > degree)
          reverse = prune(other, reverse, alpha);
      }
      size_t done=completed.fetch_add(1)+1;
      if (!progress.is_none() && (done % std::max(size_t(1), n / 100) == 0 || done == n)) {
        py::gil_scoped_acquire acquire;
        progress(alpha, done, n);
      }
      }
      } catch (...) {
        std::lock_guard<std::mutex> guard(failure_lock);
        if (!failure) failure=std::current_exception();
        failed=true;
      }
    };
    if (threads==1) work();
    else {
      std::vector<std::thread> workers;
      try {
        for (size_t i=0;i<std::min(threads,n);++i)workers.emplace_back(work);
      } catch (...) {
        failed=true;
        for (auto &worker:workers)worker.join();
        throw;
      }
      for (auto &worker:workers)worker.join();
    }
    if (failure) std::rethrow_exception(failure);
  }
  return graph;
}

class Router {
  size_t dim_;
  size_t query_ef_ = 0;
  hnswlib::L2Space space_;
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> index_;

public:
  Router(size_t dim, size_t capacity, size_t degree, size_t construction,
         size_t seed)
      : dim_(dim), space_(dim) {
    require(dim > 0 && capacity > 0 && degree >= 2 && construction >= degree,
            "Invalid HNSW configuration");
    index_ = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        &space_, capacity, degree, construction, seed);
  }
  void add(Array<float> x, Array<uint64_t> ids) {
    require(query_ef_ == 0, "Cannot modify HNSW after serving queries");
    require(x.ndim() == 2 && size_t(x.shape(1)) == dim_ && ids.ndim() == 1 &&
                ids.shape(0) == x.shape(0),
            "HNSW input mismatch");
    finite(x.data(), x.size());
    py::gil_scoped_release release;
    for (ssize_t i = 0; i < x.shape(0); ++i)
      index_->addPoint(x.data() + i * dim_, ids.data()[i]);
  }
  std::vector<uint64_t> search(Array<float> query, size_t k, size_t ef) {
    require(query.ndim() == 1 && size_t(query.size()) == dim_ && k > 0 &&
                ef >= k,
            "Invalid HNSW query");
    finite(query.data(), query.size());
    // Configure once under the GIL, before any query releases it. Concurrent
    // read-only queries must not write hnswlib's shared ef_ field.
    if (query_ef_ == 0) {
      index_->setEf(ef);
      query_ef_ = ef;
    }
    require(query_ef_ == ef,
            "HNSW ef_search is fixed after first query; create a new Router to change it");
    py::gil_scoped_release release;
    auto result = index_->searchKnn(query.data(), k);
    std::vector<uint64_t> ids;
    while (!result.empty()) {
      ids.push_back(result.top().second);
      result.pop();
    }
    std::reverse(ids.begin(), ids.end());
    return ids;
  }
  void save(const std::string &path) { index_->saveIndex(path); }
  void load(const std::string &path) {
    require(query_ef_ == 0, "Cannot reload HNSW after serving queries");
    index_->loadIndex(path, &space_);
  }
};

#include "search_loop.hpp"

void register_async_io(py::module_ &m);
void register_read_metrics(py::module_ &m);

PYBIND11_MODULE(_native, m) {
  register_async_io(m);
  register_read_metrics(m);
  register_native_search(m);
  dhwire::bind_native_wire(m);
  dhcontrol::register_native_control(m);
  m.doc() =
      "DHDGS RaBitQ core integration, Vamana construction and sample HNSW";
  py::class_<Quantizer>(m, "Quantizer")
      .def(py::init<Array<float>, size_t, uint32_t, py::bytes>())
      .def("state", &Quantizer::state)
      .def("encode", &Quantizer::encode)
      .def("score", &Quantizer::score);
  m.def("build_vamana", &build_vamana, py::arg("vectors"), py::arg("degree"),
        py::arg("width"), py::arg("alphas"), py::arg("seed"), py::arg("progress")=py::none(), py::arg("threads")=1);
  py::class_<Router>(m, "Router")
      .def(py::init<size_t, size_t, size_t, size_t, size_t>())
      .def("add", &Router::add)
      .def("search", &Router::search)
      .def("save", &Router::save)
      .def("load", &Router::load);
}
