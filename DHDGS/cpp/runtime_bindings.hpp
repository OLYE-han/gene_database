#include "native_network.hpp"
#pragma once
#include "native_head.hpp"
#include "native_result_sink.hpp"
#include "native_search_api.hpp"
#include "retained_size.hpp"
#include "native_runtime.hpp"
#include "pcg64_compat.hpp"
#include <condition_variable>
#include <shared_mutex>

namespace dhruntime {
struct CompletedRecord {
  V value;
  uint64_t retained_bytes = 0;
  static uint64_t extra_bytes(const V &v) { return retained_extra_bytes(v); }
  std::string request_id() const {
    return dhvalue::get<std::string>(value, "request_id");
  }
  py::object decode() const { return dhwire::to_python(value); }
  py::bytes pack(size_t limit) const {
    std::vector<uint8_t> bytes;
    {
      py::gil_scoped_release release;
      bytes = dhwire::pack(value, limit);
    }
    return py::bytes(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  }
};
class HeadBinding {
  py::object router_owner_, quantizer_owner_;
  py::list owners_;
  Router *router_;
  Quantizer *quantizer_;
  const uint64_t *ids_;
  size_t rows_, dim_, code_bytes_, extra_bytes_, entries_, topk_;
  const uint8_t *codes_, *extra_;
  const float *factors_;
  const uint32_t *centers_;
  bool cosine_, exclude_;
  V config_, manifest_;
  std::string signature_;
  std::unordered_map<uint64_t, size_t> locations_;
  std::unordered_map<uint64_t, std::vector<size_t>> membership_;
  std::unique_ptr<HeadRuntime> runtime_;
  std::unique_ptr<SearchApi> search_api_;
  V route(const std::vector<double> &raw, const V &qid) {
    using namespace dhvalue;
    require(raw.size() == dim_, "Wrong search query dimension");
    double norm = 0.;
    for (double v : raw) {
      require(std::isfinite(v), "Nonfinite query");
      norm += v * v;
    }
    if (cosine_)
      require(norm > 0 && std::isfinite(norm),
              "Cosine requires finite nonzero norm");
    std::vector<float> query;
    query.reserve(dim_);
    for (double v : raw)
      query.push_back(float(cosine_ ? v / std::sqrt(norm) : v));
    finite(query.data(), dim_);
    bool excluded = exclude_ && !null(qid) &&
                    std::binary_search(ids_, ids_ + rows_, cast<uint64_t>(qid));
    require(topk_ <= rows_ - size_t(excluded), "top_k exceeds eligible rows");
    auto entries = router_->search_native(query.data(), entries_);
    require(!entries.empty(), "No HNSW entries");
    auto prepared = quantizer_->prepare_native(query.data());
    A seeds;
    std::vector<float> distances;
    for (auto id : entries) {
      size_t i = locations_.at(id);
      auto score = quantizer_->score_native(prepared, codes_ + i * code_bytes_,
                                            extra_ + i * extra_bytes_,
                                            factors_ + i * 3, centers_[i]);
      distances.push_back(std::abs(score.first));
      seeds.emplace_back(A{id, double(score.first), double(score.second),
                           vector(membership_.at(id)), V(), uint64_t(0),
                           uint64_t(0), id, 0.});
    }
    std::sort(distances.begin(), distances.end());
    float median = distances[distances.size() / 2];
    if (distances.size() % 2 == 0)
      median = (median + distances[distances.size() / 2 - 1]) * 0.5f;
    auto home = membership_.at(entries.front()).front();
    const auto &search = at(config_, "search");
    const auto &peer = at(config_, "peer");
    V::Bytes bytes(dim_ * 4);
    std::memcpy(bytes.data(), query.data(), bytes.size());
    A initial{seeds.front()};
    seeds.erase(seeds.begin());
    return M{{"target_node", uint64_t(home)},
             {"entry_node", uint64_t(home)},
             {"epoch", at(manifest_, "epoch")},
             {"signature", signature_},
             {"query_f32", std::move(bytes)},
             {"query_id", qid},
             {"work_grant", at(search, "max_expansions")},
             {"branch_slots", uint64_t(get<size_t>(peer, "max_branches") - 1)},
             {"seeds", std::move(initial)},
             {"seen", A{entries.front()}},
             {"fallback_entries", std::move(seeds)},
             {"visited_nodes", A{uint64_t(home)}},
             {"remote_frontier", A{}},
             {"fallback_remaining", at(search, "max_fallback_entries")},
             {"scale",
              double(std::max(median, std::numeric_limits<float>::epsilon()))},
             {"ancestry", A{}},
             {"decision_seed", at(config_, "seed")}};
  }

public:
  HeadBinding(py::dict config, py::dict manifest, std::string signature,
              py::object router, py::object quantizer, py::dict bank,
              Array<uint64_t> ids, Array<uint8_t> membership)
      : router_owner_(std::move(router)),
        quantizer_owner_(std::move(quantizer)), owners_(py::list()),
        router_(&router_owner_.cast<Router &>()),
        quantizer_(&quantizer_owner_.cast<Quantizer &>()),
        config_(dhwire::from_python(config)),
        manifest_(dhwire::from_python(manifest)),
        signature_(std::move(signature)) {
    using namespace dhvalue;
    dim_ = get<size_t>(manifest_, "dimension");
    rows_ = get<size_t>(manifest_, "rows");
    cosine_ = get<std::string>(at(config_, "data"), "metric") == "cosine";
    const auto nodes = array(at(at(config_, "cluster"), "nodes")).size();
    require(ids.ndim() == 1 && size_t(ids.size()) == rows_ &&
                membership.ndim() == 2 &&
                size_t(membership.shape(0)) == rows_ &&
                size_t(membership.shape(1)) == nodes,
            "Invalid global directory");
    ids_ = ids.data();
    owners_.append(ids);
    require(std::adjacent_find(ids_, ids_ + rows_,
                               std::greater_equal<uint64_t>()) == ids_ + rows_,
            "IDs must be sorted and unique");
    auto route_ids = bank["ids"].cast<Array<uint64_t>>();
    auto codes = bank["codes"].cast<Array<uint8_t>>();
    auto extra = bank["extra"].cast<Array<uint8_t>>();
    auto factors = bank["factors"].cast<Array<float>>();
    auto centers = bank["center_ids"].cast<Array<uint32_t>>();
    require(route_ids.ndim() == 1 && codes.ndim() == 2 && extra.ndim() == 2 &&
                factors.ndim() == 2 && factors.shape(1) == 3 &&
                centers.ndim() == 1,
            "Invalid route arrays");
    const auto n = route_ids.size();
    require(n > 0 && n == codes.shape(0) && n == extra.shape(0) &&
                n == factors.shape(0) && n == centers.size(),
            "Route bank sizes differ");
    for (auto a : {py::object(route_ids), py::object(codes), py::object(extra),
                   py::object(factors), py::object(centers)})
      owners_.append(a);
    codes_ = codes.data();
    extra_ = extra.data();
    factors_ = factors.data();
    centers_ = centers.data();
    code_bytes_ = codes.shape(1);
    extra_bytes_ = extra.shape(1);
    for (ssize_t i = 0; i < n; ++i) {
      auto id = route_ids.data()[i];
      auto p = std::lower_bound(ids_, ids_ + rows_, id);
      require(p != ids_ + rows_ && *p == id, "Route ID absent from directory");
      require(locations_.emplace(id, size_t(i)).second, "Duplicate route ID");
      auto &owners = membership_[id];
      for (size_t j = 0; j < nodes; ++j)
        if (membership.data()[(p - ids_) * nodes + j])
          owners.push_back(j);
      require(!owners.empty(), "Route ID lacks owner");
    }
    entries_ = std::min(
        size_t(n), get<size_t>(at(config_, "routing"), "entry_candidates"));
    topk_ = get<size_t>(at(config_, "search"), "top_k");
    exclude_ = get<bool>(at(config_, "search"), "exclude_self");
    router_->configure_ef(get<size_t>(at(config_, "routing"), "ef_search"));
    runtime_ = std::make_unique<HeadRuntime>(
        config_, manifest_, [this](const std::vector<double> &q, const V &id) {
          return route(q, id);
        });
  }
  ~HeadBinding() { close(); }
  std::string submit(Array<double> query, py::object id) {
    require(query.ndim() == 1 && size_t(query.size()) == dim_,
            "Wrong query dimensions");
    V qid = dhwire::from_python(id);
    if (!dhvalue::null(qid))
      dhvalue::cast<uint64_t>(qid);
    std::vector<double> input(query.data(), query.data() + query.size());
    py::gil_scoped_release release;
    return runtime_->submit(std::move(input), std::move(qid));
  }
  py::object completed(size_t n, uint64_t ms) {
    V out;
    {
      py::gil_scoped_release release;
      out = runtime_->completed(n, ms);
    }
    return dhwire::to_python(out);
  }
  py::list completed_records(size_t n, uint64_t ms) {
    V out;
    std::vector<CompletedRecord> owned;
    {
      py::gil_scoped_release release;
      out = runtime_->completed(n, ms);
      for (auto &value : std::get<V::Array>(out.data)) {
        auto retained = sizeof(CompletedRecord) + CompletedRecord::extra_bytes(value);
        owned.push_back(CompletedRecord{std::move(value), retained});
      }
    }
    py::list records;
    for (auto &record : owned)
      records.append(py::cast(std::move(record)));
    return records;
  }
  py::object run_to_disk(Array<double> queries, Array<uint64_t> ids, size_t concurrency, const std::string& directory, py::dict options) {
    require(queries.ndim()==2 && size_t(queries.shape(1))==dim_ && ids.ndim()==1 && ids.size()==queries.shape(0), "Invalid disk query batch");
    require(concurrency>0 && concurrency<=dhvalue::get<size_t>(dhvalue::at(config_,"peer"),"max_active_queries"), "Invalid disk concurrency");
    auto settings=dhwire::from_python(options);V result;
    {
      py::gil_scoped_release release;
      result=run_disk_queries(*runtime_,queries.data(),ids.data(),size_t(ids.size()),dim_,concurrency,directory,settings,
           dhvalue::get<size_t>(dhvalue::at(config_,"network"),"max_message_bytes"));
    }
    return dhwire::to_python(result);
  }
  py::object metrics() {
    V out;
    {
      py::gil_scoped_release release;
      out = runtime_->metrics();
    }
    return dhwire::to_python(out);
  }
  uint16_t start_api(py::dict options) {
    require(!search_api_,"Search API already started");auto v=dhwire::from_python(options);
    py::gil_scoped_release release;search_api_=std::make_unique<SearchApi>(*runtime_,dim_,v);return search_api_->port();
  }
  py::object api_metrics(){V v;{py::gil_scoped_release release;require(bool(search_api_),"Search API not started");v=search_api_->metrics();}return dhwire::to_python(v);}
  void close() { if(search_api_)search_api_->close();runtime_->close(); }
};
// Python is used only by this management adapter, never by the data handler.
class ManagedTransport {
  std::shared_mutex gate_;
  std::shared_ptr<DataRuntime> runtime_;
  bool accepting_ = false;
  std::string revision_;
  py::object admin_;
  std::unique_ptr<dhnet::Server> server_;
  std::mutex shutdown_mu_;
  std::condition_variable shutdown_cv_;
  bool shutdown_ = false;

public:
  ManagedTransport(py::dict config, size_t sid,
                   std::shared_ptr<DataRuntime> runtime, py::object admin)
      : runtime_(std::move(runtime)), admin_(std::move(admin)) {
    using namespace dhvalue;
    const auto c = dhwire::from_python(config);
    const auto &nc = at(c, "native_runtime");
    const auto &net = at(c, "network");
    const auto &node = array(at(at(c, "cluster"), "nodes")).at(sid);
    dhwire::ServerConfig sc;
    sc.host = get<std::string>(node, "host");
    sc.port = get<uint16_t>(node, "port");
    sc.workers = get<size_t>(nc, "receive_threads");
    sc.queue_capacity = get<size_t>(nc, "receive_queue_capacity");
    sc.max_connections = get<size_t>(nc, "max_connections");
    sc.max_message_bytes = get<size_t>(net, "max_message_bytes");
    sc.listen_backlog = get<size_t>(net, "listen_backlog");
    sc.idle_timeout_ms = uint64_t(get<double>(net, "timeout_seconds") * 1000);
    server_ = std::make_unique<dhnet::Server>(
        sc,
        [this](V request, uint64_t ns, size_t bytes) {
          std::shared_lock<std::shared_mutex> l(gate_);
          if (dhvalue::get<std::string>(request, "op") == "peer_submit") {
            if (!accepting_)
              throw std::runtime_error("Data node is paused for configuration");
            if (!revision_.empty() &&
                dhvalue::get<std::string>(dhvalue::at(request, "state"),
                                          "cluster_revision") != revision_)
              throw std::invalid_argument(
                  "Query configuration revision mismatch");
          }
          return runtime_->handle(std::move(request), ns, bytes);
        },
        [this](V request, uint64_t, size_t) {
          dhwire::Reply reply;
          // Acquire only on the separate administration worker.
          py::gil_scoped_acquire gil;
          try {
            reply.message =
                dhwire::from_python(admin_(dhwire::to_python(request)));
          } catch (py::error_already_set &e) {
            auto error = std::string(e.what());
            e.restore();
            PyErr_Clear();
            reply.message = V::Map{{"ok", false}, {"error", error}};
            if (dhvalue::has(request, "request_id"))
              reply.message.map()["request_id"] =
                  dhvalue::at(request, "request_id");
          }
          if (dhvalue::get<std::string>(request, "op") == "admin_shutdown" &&
              dhvalue::optional<bool>(reply.message, "ok", false))
            reply.sent = [this](uint64_t, size_t) { shutdown(); };
          return reply;
        },c);
  }
  void configure(std::shared_ptr<DataRuntime> r, bool open,
                 std::string revision) {
    std::unique_lock<std::shared_mutex> l(gate_);
    runtime_ = std::move(r);
    accepting_ = open;
    revision_ = std::move(revision);
  }
  ~ManagedTransport() {
    py::gil_scoped_release release;
    close();
  }
  void close() {
    shutdown();
    if (server_)
      server_->close();
  }
  void shutdown() {
    {
      std::lock_guard<std::mutex> l(shutdown_mu_);
      shutdown_ = true;
    }
    shutdown_cv_.notify_all();
  }
  void wait_shutdown() {
    std::unique_lock<std::mutex> l(shutdown_mu_);
    shutdown_cv_.wait(l, [this] { return shutdown_; });
  }
  uint16_t port() const { return server_->port(); }
  V metrics() const { return server_->metrics(); }
};
inline void bind_runtime(py::module_ &m) {
  py::class_<CompletedRecord>(m, "CompletedRecord")
      .def_property_readonly("request_id", &CompletedRecord::request_id)
      .def_readonly("retained_bytes", &CompletedRecord::retained_bytes)
      .def("decode", &CompletedRecord::decode)
      .def("pack", &CompletedRecord::pack);
  m.def("pack_completed_records", [](py::list records, size_t limit) {
    std::vector<const CompletedRecord *> inputs;
    for (auto record : records)
      inputs.push_back(&record.cast<const CompletedRecord &>());
    struct Packed {
      std::vector<uint8_t> bytes;
      std::string error;
      uint64_t start, end;
    };
    std::vector<Packed> output(inputs.size());
    const auto now = [] {
      return uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());
    };
    {
      // One GIL handoff for the entire completion batch, not per query.
      py::gil_scoped_release release;
      for (size_t i = 0; i < inputs.size(); ++i) {
        auto &out = output[i];
        out.start = now();
        try { out.bytes = dhwire::pack(inputs[i]->value, limit); }
        catch (const std::exception &e) { out.error = e.what(); }
        out.end = now();
      }
    }
    py::list result;
    for (const auto &out : output)
      result.append(py::make_tuple(
          py::bytes(reinterpret_cast<const char *>(out.bytes.data()), out.bytes.size()),
          out.error, out.start, out.end));
    return result;
  });
  py::class_<HeadBinding>(m, "NativeHead")
      .def(py::init<py::dict, py::dict, std::string, py::object, py::object,
                    py::dict, Array<uint64_t>, Array<uint8_t>>())
      .def("start_api", &HeadBinding::start_api)
      .def("api_metrics", &HeadBinding::api_metrics)
      .def("submit", &HeadBinding::submit)
      .def("run_to_disk", &HeadBinding::run_to_disk)
      .def("completed", &HeadBinding::completed, py::arg("maximum") = 32,
           py::arg("timeout_ms") = 50)
      .def("completed_records", &HeadBinding::completed_records,
           py::arg("maximum") = 32, py::arg("timeout_ms") = 50)
      .def("metrics", &HeadBinding::metrics)
      .def("close", &HeadBinding::close,
           py::call_guard<py::gil_scoped_release>());
  m.def("run_network_load",[](Array<double> queries,Array<uint64_t> ids,size_t concurrency,py::dict options,const std::string& directory){
    require(queries.ndim()==2&&ids.ndim()==1&&ids.size()==queries.shape(0),"Invalid network query batch");auto opts=dhwire::from_python(options);V result;
    {py::gil_scoped_release release;result=run_network_load(queries.data(),ids.data(),size_t(ids.size()),size_t(queries.shape(1)),concurrency,opts,directory);}return dhwire::to_python(result);
  });
  m.def("validate_manifests",
        [](py::object reports, std::string root, size_t node, size_t budget,
           size_t slots, size_t nodes) {
          return validate_manifests(dhwire::from_python(reports), root, node,
                                    budget, slots, nodes);
        });
  m.def("aggregate_native", [](py::dict config, py::object reports,
                               py::object transfer, py::object involved) {
    return dhwire::to_python(aggregate_native(
        dhwire::from_python(config), dhwire::from_python(reports),
        dhwire::from_python(transfer), dhwire::from_python(involved)));
  });
  py::class_<DataRuntime, std::shared_ptr<DataRuntime>>(m, "NativeDataRuntime")
      .def(py::init([](py::dict c, py::dict meta, std::string signature,
                       std::shared_ptr<dhsearch::Shard> shard) {
        auto config = dhwire::from_python(c),
             metadata = dhwire::from_python(meta);
        return std::make_shared<DataRuntime>(
            config, std::move(metadata), std::move(signature),
            [shard, config](const V &state, const std::vector<double> &draws,
                            std::shared_ptr<dhsearch::Control> control,
                            const std::function<V(V)> &spawn,
                            std::shared_ptr<dhcontrol::Ticket> ticket) {
              shard->run_value(
                  state, config, draws, std::move(control), {}, [] {}, spawn,
                  std::move(ticket));
            });
      }))
      .def("metrics",
           [](DataRuntime &r) {
             V value;
             {
               py::gil_scoped_release release;
               value = r.metrics();
             }
             return dhwire::to_python(value);
           })
      .def("network_metrics",[](DataRuntime &r){V value;{py::gil_scoped_release release;value=r.network_metrics();}return dhwire::to_python(value);})
      .def("close", &DataRuntime::close,
           py::call_guard<py::gil_scoped_release>());
  py::class_<ManagedTransport>(m, "NativeManagedTransport")
      .def(py::init<py::dict, size_t, std::shared_ptr<DataRuntime>,
                    py::object>())
      // Replacing the last runtime owner may destroy SearchShard's Python
      // initialization owners. Keep the GIL for this management-only swap;
      // native data handlers holding gate_ never need the GIL.
      .def("configure", &ManagedTransport::configure)
      .def("close", &ManagedTransport::close,
           py::call_guard<py::gil_scoped_release>())
      .def("shutdown", &ManagedTransport::shutdown)
      .def("wait_shutdown", &ManagedTransport::wait_shutdown,
           py::call_guard<py::gil_scoped_release>())
      .def_property_readonly("port", &ManagedTransport::port)
      .def("metrics",
           [](ManagedTransport &r) { return dhwire::to_python(r.metrics()); });
  m.def("pcg64_compat", [](uint64_t seed, size_t draws) {
    PCG64 r(seed);
    auto values = r.draws(draws);
    return py::make_tuple(values, r.child_seed());
  });
}
} // namespace dhruntime
