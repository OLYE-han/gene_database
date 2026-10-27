#include "native_network.hpp"
#include "native_runtime.hpp"
#include "native_pool.hpp"
#include "pcg64_compat.hpp"
#include "logical_region.hpp"
#include "decayed_hotness.hpp"
#include <algorithm>
#include <atomic>
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <openssl/sha.h>
#include <set>
#include <sys/random.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <unordered_map>

namespace dhruntime {
using namespace dhvalue;
static uint64_t stamp() { return dhsearch::now_ns(); }
static double seconds() { return stamp() / 1e9; }
static void check(bool x, const char *error) {
  if (!x)
    throw std::invalid_argument(error);
}
std::string identifier() {
  unsigned char bytes[16];
  size_t offset = 0;
  while (offset < sizeof(bytes)) {
    auto n = getrandom(bytes + offset, sizeof(bytes) - offset, 0);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      throw std::runtime_error("Cannot generate query identity");
    offset += size_t(n);
  }
  bytes[6] = (bytes[6] & 15) | 64;
  bytes[8] = (bytes[8] & 63) | 128;
  const char *hex = "0123456789abcdef";
  std::string out;
  for (size_t i = 0; i < 16; ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10)
      out += '-';
    out += hex[bytes[i] >> 4];
    out += hex[bytes[i] & 15];
  }
  return out;
}
bool trace_selected(const V &c, const V &q, bool lifecycle) {
  const auto &m = at(c, "metrics");
  if (get<bool>(m, "detailed_trace") ||
      (lifecycle && optional<bool>(m, "lifecycle_trace", false)))
    return true;
  if (null(q) || !has(m, "trace_query_ids"))
    return false;
  for (const auto &id : array(at(m, "trace_query_ids")))
    if (cast<uint64_t>(id) == cast<uint64_t>(q))
      return true;
  return false;
}
struct DataRuntime::Impl {
  struct Record {
    V state;
    std::shared_ptr<void> search_context;
    std::string digest;
    std::shared_ptr<dhsearch::Control> control;
    std::shared_ptr<dhcontrol::Ticket> ticket;
    uint64_t accepted = 0;
    bool acked = false;
    std::atomic<int> status{0}; // queued / computing / locally finished
    mutable std::mutex trace_mu;
    M trace;
    void mark(const std::string &name, uint64_t value) {
      std::lock_guard<std::mutex> l(trace_mu);
      if (!trace.empty())
        trace[name] = value;
    }
  };
  struct Transfer {
    V message;
    std::shared_ptr<Record> record;
    M metrics;
    size_t node = 0, attempt = 0;
    uint64_t started = 0;
  };
  V config, meta;
  std::string signature;
  size_t sid, dimension, max_active, max_retained, max_tombstones, retries;
  double retention, lease_seconds, timeout;
  SearchFunction search;
  SearchPrepare prepare;
  std::atomic<uint64_t> cpu_ns{0}, work_ns{0}, completed_work{0};
  std::array<std::atomic<uint64_t>,64> wait_hist{};
  std::unique_ptr<Pool> workers;
  std::unique_ptr<dhcontrol::Controller> controller;
  std::unique_ptr<dhnet::Client> client;
  std::vector<dhwire::Endpoint> endpoints;
  mutable std::mutex mu;
  std::mutex close_mu;
  std::condition_variable wake;
  std::unordered_map<std::string, std::shared_ptr<Record>> records;
  std::unordered_map<std::string, std::set<std::string>> query_records;
  std::unordered_map<std::string,std::weak_ptr<dhsearch::ExpansionClaims>> query_claims;
  static std::string branch_key(const V& s) {
    return get<std::string>(s,"query_request_id")+"\n"+get<std::string>(s,"branch_id");
  }
  std::unordered_map<std::string, std::pair<std::string, double>> tombstones;
  std::deque<std::pair<std::string, double>> tomb_order;
  size_t active = 0, rejected = 0;
  bool stopping = false, closed = false;
  std::thread maintenance;

  Impl(V c, V m, std::string sig, SearchFunction fn, SearchPrepare prep)
      : config(std::move(c)), meta(std::move(m)), signature(std::move(sig)),
        search(std::move(fn)), prepare(std::move(prep)) {
    const auto &p = at(config, "peer");
    const auto &net = at(config, "network");
    sid = get<size_t>(meta, "shard_id");
    dimension = get<size_t>(meta, "dimension");
    max_active = get<size_t>(p, "max_active_queries");
    max_tombstones = get<size_t>(p, "max_tombstones");
    max_retained = max_active + max_tombstones;
    retention = get<double>(p, "retention_seconds");
    lease_seconds = get<double>(p, "lease_timeout_ms") / 1000.;
    timeout = get<double>(p, "query_timeout_seconds");
    retries = get<size_t>(net, "max_retries");
    for (const auto &n : array(at(at(config, "cluster"), "nodes")))
      endpoints.emplace_back(get<std::string>(n, "host"),
                             get<uint16_t>(n, "port"));
    dhwire::Config wc;
    const auto &nc = at(config, "native_runtime");
    wc.max_pending = get<size_t>(nc, "network_queue_capacity");
    wc.max_connections_per_endpoint =
        get<size_t>(nc, "connections_per_endpoint");
    wc.max_message_bytes = get<size_t>(net, "max_message_bytes");
    wc.timeout_ms = uint64_t(get<double>(p, "handoff_timeout_seconds") * 1000);
    wc.max_endpoints = std::max(size_t(1), endpoints.size());
    controller = std::make_unique<dhcontrol::Controller>(config);
    client = std::make_unique<dhnet::Client>(wc,config);
    workers = std::make_unique<Pool>(get<size_t>(p, "worker_threads"),
                                     get<size_t>(nc, "compute_queue_capacity"),
                                     optional<std::vector<int>>(nc,"compute_cpu_ids",{}),true,
                                     []{dhsearch::DecayedHotness::flush_current_thread();});
    maintenance = std::thread([this] { maintain(); });
  }
  ~Impl() { close(); }
  void update() {
    controller->poll_reports();
    auto updates = controller->take_updates();
    std::vector<std::shared_ptr<Record>> retired;
    {
      std::lock_guard<std::mutex> l(mu);
      const auto now = seconds();
      for (const auto &u : array(updates)) {
        auto q = branch_key(u);
        auto it = records.find(q);
        if (it == records.end())
          continue;
        if (get<std::string>(it->second->state, "branch_id") !=
            get<std::string>(u, "branch_id"))
          continue;
        if (get<std::string>(u, "kind") == "acked") {
          if (!it->second->acked) {
            it->second->acked = true;
            --active;
          }
        } else if (get<std::string>(u, "kind") == "expired") {
          if (!it->second->acked)
            --active;
          tombstones[q] = {get<std::string>(u, "branch_id"), now};
          tomb_order.emplace_back(q, now);
          auto qi=query_records.find(get<std::string>(it->second->state,"query_request_id"));
          if(qi!=query_records.end()){qi->second.erase(q);if(qi->second.empty()){query_claims.erase(qi->first);query_records.erase(qi);}}
          retired.push_back(std::move(it->second));
          records.erase(it);
        }
      }
      while (!tomb_order.empty() &&
             (tombstones.size() > max_tombstones ||
              now - tomb_order.front().second > retention)) {
        auto [q, t] = tomb_order.front();
        tomb_order.pop_front();
        auto it = tombstones.find(q);
        if (it != tombstones.end() && it->second.second == t)
          tombstones.erase(it);
      }
    } // Destruction of state/report buffers is outside the identity lock.
  }
  void maintain() {
    for (;;) {
      update();
      std::unique_lock<std::mutex> l(mu);
      if (wake.wait_for(l, std::chrono::milliseconds(10),
                        [this] { return stopping; }))
        return;
    }
  }
  void validate(const V &s) {
    check(get<std::string>(s, "epoch") == get<std::string>(meta, "epoch") &&
              get<std::string>(s, "signature") == signature,
          "Peer epoch or execution configuration mismatch");
    check(get<size_t>(s, "target_node") == sid, "Wrong branch destination");
    for (const char *k : {"query_request_id", "branch_id", "token"})
      check(!get<std::string>(s, k).empty() &&
                get<std::string>(s, k).size() <= 256 && get<std::string>(s,k).find('\n')==std::string::npos,
            "Invalid query identity");
    const auto path = get<std::vector<size_t>>(s, "visited_nodes");
    std::set<size_t> visited(path.begin(), path.end());
    check(!path.empty() && path.back() == sid &&
              path.front() == get<size_t>(s, "entry_node") &&
              (optional<bool>(at(config,"peer"),"allow_node_revisit",false)||visited.size()==path.size()) &&
              path.size()<=get<size_t>(at(config,"peer"),"max_branches") &&
              *visited.rbegin() < endpoints.size(),
          "Invalid visited-node path");
    const auto &ancestry = array(at(s, "ancestry"));
    if(optional<bool>(at(config,"peer"),"logical_region_visits",false)){
      const auto rp=get<std::vector<std::string>>(s,"visited_regions");
      check(rp.size()==path.size()&&!rp.empty()&&rp.back()==dhregion::current(s)&&get<std::string>(s,"logical_region")==rp.back(),"Invalid logical region path");
      const auto ids=dhregion::decode(s,get<size_t>(at(config,"peer"),"max_excluded_region_ids"));
      const auto retired=optional<std::vector<std::string>>(s,"excluded_regions",{});
      check(retired.size()<get<size_t>(at(config,"peer"),"max_branches"),"Too many retired query regions");
      if(has(s,"replica_version"))check(std::find(retired.begin(),retired.end(),get<std::string>(s,"replica_version"))==retired.end(),"Retired replica cannot be revisited");
    }
    check(ancestry.size() + 1 == path.size(), "Invalid ancestry");
    for (size_t i = 0; i < ancestry.size(); ++i)
      check(get<size_t>(ancestry[i], "node_id") == path[i],
            "Invalid ancestry path");
    std::set<std::string> branch_path{get<std::string>(s,"branch_id")};
    for(const auto& a:ancestry)check(branch_path.insert(get<std::string>(a,"branch_id")).second,"Repeated ancestry branch");
    check((ancestry.empty() && null(at(s, "parent_branch_id"))) ||
              (!ancestry.empty() &&
               get<std::string>(ancestry.back(), "branch_id") ==
                   get<std::string>(s, "parent_branch_id")),
          "Invalid branch parent");
    check(get<size_t>(s, "work_grant") > 0 &&
              get<size_t>(s, "work_grant") <=
                  get<size_t>(at(config, "search"), "max_expansions"),
          "Invalid work grant");
    check(get<size_t>(s, "branch_slots") <
              get<size_t>(at(config, "peer"), "max_branches"),
          "Invalid branch slots");
    const auto &bytes = std::get<V::Bytes>(at(s, "query_f32").data);
    check(bytes.size() == dimension * 4, "Wrong query bytes");
    for (size_t i = 0; i < dimension; ++i) {
      float f;
      std::memcpy(&f, bytes.data() + i * 4, 4);
      check(std::isfinite(f), "Nonfinite query");
    }
    const auto &seeds = array(at(s, "seeds"));
    const auto &carried = array(at(s, "remote_frontier"));
    check(!seeds.empty() &&
              seeds.size() + carried.size() <=
                  get<size_t>(at(config, "search"), "frontier_size"),
          "Invalid seeds/frontier");
    for (const auto &row : seeds) {
      const auto &a = array(row);
      check(a.size() == 9, "Invalid seed row");
      auto owners = cast<std::vector<size_t>>(a[3]);
      check(std::find(owners.begin(), owners.end(), sid) != owners.end(),
            "Nonlocal branch seed");
    }
    for (const auto &row : carried) {
      const auto &a = array(row);
      check(a.size() == 9, "Invalid frontier row");
      bool eligible = false;
      for (auto n : cast<std::vector<size_t>>(a[3])) {
        check(n < endpoints.size(), "Invalid frontier owner");
        eligible |= optional<bool>(at(config,"peer"),"allow_node_revisit",false) || n == sid || !visited.count(n);
      }
      check(eligible, "Invalid carried frontier");
    }
    const auto &ep = array(at(s, "head_endpoint"));
    check(ep.size() == 2 && !cast<std::string>(ep[0]).empty() &&
              cast<uint16_t>(ep[1]) > 0,
          "Invalid head endpoint");
  }
  std::pair<V, std::shared_ptr<Record>> accept(V s, uint64_t received) {
    if(optional<bool>(at(config,"peer"),"logical_region_visits",false))dhregion::normalize(s);
    validate(s);
    auto raw = dhwire::pack(s);
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(raw.data(), raw.size(), hash);
    std::string digest(reinterpret_cast<char *>(hash), sizeof(hash));
    auto q = branch_key(s);
    auto r = std::make_shared<Record>();
    r->state = std::move(s);
    r->digest = std::move(digest);
    r->accepted = stamp();
    bool traced = trace_selected(config, at(r->state, "query_id"), true);
    const auto now = seconds();
    r->control =
        std::make_shared<dhsearch::Control>(now + lease_seconds, now + timeout);
    M md;
    for (const char *k :
         {"query_request_id", "branch_id", "parent_branch_id", "token",
          "head_endpoint", "entry_node", "target_node", "visited_nodes",
          "ancestry", "work_grant", "branch_slots"})
      md[k] = at(r->state, k);
    md["accepted_ns"] = r->accepted;
    md["created_ns"] = r->accepted;
    md["lease"] = now + lease_seconds;
    md["deadline"] = now + timeout;
    md["trace"] = traced;
    if (traced)
      r->trace = {{"receiver_message_ready_ns", received},
                  {"accepted_ns", r->accepted},
                  {"receiver_thread_id", uint64_t(syscall(SYS_gettid))},
                  {"admission_mode", "native_bounded"}};
    const auto requested = stamp();
    {
      std::lock_guard<std::mutex> l(mu);
      check(!stopping, "Peer runtime closed");
      auto it = records.find(q);
      if (it != records.end()) {
        check(get<std::string>(it->second->state, "branch_id") ==
                  get<std::string>(r->state, "branch_id"),
              "Query already visited this data node");
        check(it->second->digest == r->digest, "Conflicting branch retry");
        return {M{{"accepted", true}, {"duplicate", true}}, nullptr};
      }
      if(!optional<bool>(at(config,"peer"),"allow_node_revisit",false))
        check(!query_records.count(get<std::string>(r->state,"query_request_id")),"Query already visited this data node");
      check(!tombstones.count(q), "Expired branch cannot be replayed");
      if (active >= max_active || records.size() >= max_retained) {
        ++rejected;
        throw std::runtime_error("Peer branch capacity exceeded");
      }
      if(prepare)r->search_context=prepare(r->state);
      if(optional<bool>(at(config,"peer"),"deduplicate_node_expansions",false)){
        auto& weak=query_claims[get<std::string>(r->state,"query_request_id")];auto claims=weak.lock();
        if(!claims){claims=std::make_shared<dhsearch::ExpansionClaims>(get<size_t>(at(config,"search"),"max_expansions"));weak=claims;}
        r->control->claims(std::move(claims));
      }
      r->mark("accept_state_lock_enter_ns", requested);
      r->mark("accept_state_lock_acquired_ns", stamp());
      r->ticket = controller->register_value(std::move(md), r->control);
      records.emplace(q, r);
      query_records[get<std::string>(r->state,"query_request_id")].insert(q);
      ++active;
      r->mark("enqueued_ns", stamp());
      if (!workers->submit([this, r] { run(r); })) {
        ++rejected;
        r->status = 2;
        r->ticket->fail("Native compute queue capacity exceeded");
      }
    }
    r->mark("accept_state_lock_released_ns", stamp());
    return {M{{"accepted", true}, {"duplicate", false}}, r};
  }
  void transfer(std::shared_ptr<Transfer> t) {
    if(t->node==sid&&optional<bool>(at(config,"peer"),"logical_region_visits",false)){
      try{t->record->control->check();auto accepted=accept(at(t->message,"state"),stamp());
        add(t->metrics,"same_host_handoffs",1);t->record->ticket->transfer_done(t->metrics,"");}
      catch(const std::exception& e){t->record->ticket->transfer_done(t->metrics,e.what());}
      return;
    }
    ++t->attempt;
    add(t->metrics, "peer_rpc_count", 1);
    try {
      if (t->record->control->cancelled())
        throw std::runtime_error("Branch transfer cancelled");
      if (!client->submit(
              endpoints[t->node], t->message, [this, t](dhwire::Result r) {
                add(t->metrics, "peer_request_bytes", double(r.request_bytes));
                add(t->metrics, "peer_response_bytes",
                    double(r.response_bytes));
                add(t->metrics, "peer_pool_wait_ms",
                    r.send_start_ns ? (r.send_start_ns - r.queued_ns) / 1e6
                                    : 0.);
                add(t->metrics, "peer_send_ms",
                    r.send_done_ns ? (r.send_done_ns - r.send_start_ns) / 1e6
                                   : 0.);
                add(t->metrics, "peer_response_wait_ms",
                    r.send_done_ns ? (r.done_ns - r.send_done_ns) / 1e6 : 0.);
                add(t->metrics, "peer_connect_ms",
                    r.connect_done_ns
                        ? (r.connect_done_ns - r.connect_start_ns) / 1e6
                        : 0.);
                add(t->metrics,
                    r.connection_created ? "peer_connections_created"
                                         : "peer_connections_reused",
                    1);
                std::string error = r.error;
                try {
                  if (!r.ok)
                    throw std::runtime_error(error);
                  if (!get<bool>(r.response, "ok"))
                    throw std::runtime_error(
                        get<std::string>(r.response, "error"));
                  const auto& reply=at(r.response,"result");
                  auto& child=t->message.map().at("state");
                  if(!get<bool>(reply,"accepted")&&optional<std::string>(reply,"reason","")=="replica_unavailable"&&has(child,"replica_fallback_source")){
                    // Only explicit pre-admission rejection permits relocation;
                    // unknown ACK loss continues retrying the same branch/node.
                    t->node=get<size_t>(child,"replica_fallback_source");
                    auto& m=child.map();m["target_node"]=uint64_t(t->node);
                    std::get<A>(m["visited_nodes"].data).back()=uint64_t(t->node);
                    m.erase("replica_version");m.erase("replica_access_kind");m.erase("replica_fallback_source");
                    m["replica_unavailable_fallback"]=true;
                    if(optional<bool>(at(config,"peer"),"logical_region_visits",false)){
                      m["logical_region"]=dhregion::base(t->node);std::get<A>(m["visited_regions"].data).back()=dhregion::base(t->node);
                    }
                    add(t->metrics,"peer_replica_unavailable_fallback",1);t->attempt=0;transfer(t);return;
                  }
                  check(get<bool>(at(r.response, "result"), "accepted"),
                        "Remote branch not accepted");
                } catch (const std::exception &e) {
                  error = e.what();
                }
                if (!error.empty()) {
                  add(t->metrics, "peer_transfer_errors", 1);
                  if (t->attempt <= retries &&
                      !t->record->control->cancelled()) {
                    transfer(t);
                    return;
                  }
                }
                t->metrics["peer_transfer_wall_ms"] =
                    (stamp() - t->started) / 1e6;
                t->record->mark("transfer_worker_return_ns", stamp());
                t->record->ticket->transfer_done(t->metrics, error);
              }))
        throw std::runtime_error("Native transfer queue capacity exceeded");
    } catch (const std::exception &e) {
      t->record->ticket->transfer_done(t->metrics, e.what());
    }
  }
  V spawn(std::shared_ptr<Record> r, V payload) {
    const auto decision_at = stamp();
    auto used = get<size_t>(payload, "draws_used");
    payload.map().erase("draws_used");
    V child = r->state;
    child.map().erase("replica_version");
    child.map().erase("replica_access_kind");
    child.map().erase("replica_fallback_source");
    child.map().erase("replica_unavailable_fallback");
    for (auto &[k, v] : payload.map())
      child.map()[k] = std::move(v);
    auto &m = child.map();
    m["branch_id"] = identifier();
    m["parent_branch_id"] = at(r->state, "branch_id");
    r->mark("child_dispatch_ns:" + get<std::string>(child, "branch_id"), decision_at);
    std::get<A>(m["visited_nodes"].data).push_back(m["target_node"]);
    std::get<A>(m["ancestry"].data)
        .push_back(M{{"branch_id", at(r->state, "branch_id")},
                     {"node_id", at(r->state, "target_node")}});
    if(optional<bool>(at(config,"peer"),"logical_region_visits",false)){
      auto regions=dhregion::path(r->state);regions.push_back(dhregion::current(child));m["visited_regions"]=vector(regions);m["logical_region"]=regions.back();
      std::get<A>(m["ancestry"].data).back().map()["logical_region"]=dhregion::current(r->state);
    }
    PCG64 rng(get<uint64_t>(r->state, "decision_seed"));
    for (size_t i = 0; i < used; ++i)
      rng.random();
    m["decision_seed"] = uint64_t(rng.child_seed());
    V desc = M{{"decision_id", m["dispatch_decision_id"]},
               {"branch_id", m["branch_id"]},
               {"node_id", m["target_node"]},
               {"work_grant", m["work_grant"]},
               {"branch_slots", m["branch_slots"]},
               {"parent_branch_id", m["parent_branch_id"]}};
    if(has(child,"replica_fallback_source"))desc.map()["fallback_node"]=at(child,"replica_fallback_source");
    if(has(child,"logical_region"))desc.map()["logical_region"]=at(child,"logical_region");
    auto t = std::make_shared<Transfer>();
    t->record = r;
    t->node = get<size_t>(child, "target_node");
    t->started = stamp();
    t->message = M{{"version", uint64_t(1)},
                   {"op", "peer_submit"},
                   {"state", std::move(child)}};
    r->ticket->child(desc);
    r->mark("transfer_worker_enter_ns", stamp());
    transfer(t);
    r->mark("child_submit_done_ns:" + get<std::string>(desc, "branch_id"), stamp());
    return desc;
  }
  void run(std::shared_ptr<Record> r) {
    r->status = 1;
    const auto started = stamp();
    timespec cpu_begin{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu_begin);
    uint64_t wait_us=(started-r->accepted)/1000;size_t bucket=0;while(wait_us>1&&bucket<63){wait_us>>=1;++bucket;}++wait_hist[bucket];
    r->mark("worker_enter_ns", started);
    r->mark("worker_thread_id", uint64_t(syscall(SYS_gettid)));
    try {
      r->control->check();
      r->ticket->start(0., (started - r->accepted) / 1e6,
                       (started - r->accepted) / 1e6);
      PCG64 rng(get<uint64_t>(r->state, "decision_seed"));
      V s = r->state;
      s.map()["trace_enabled"] = trace_selected(config, at(s, "query_id"));
      s.map()["lifecycle_trace_enabled"] =
          trace_selected(config, at(s, "query_id"), true);
      search(
          s, rng.draws(get<size_t>(s, "work_grant")), r->control,
          [this, r](V p) { return spawn(r, std::move(p)); }, r->ticket, r->search_context);
    } catch (const std::exception &e) {
      r->ticket->fail(e.what());
    }
    timespec cpu_end{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu_end);
    cpu_ns.fetch_add(uint64_t((cpu_end.tv_sec-cpu_begin.tv_sec)*1000000000LL+cpu_end.tv_nsec-cpu_begin.tv_nsec));
    work_ns.fetch_add(stamp()-started);++completed_work;r->search_context.reset();
    r->mark("compute_released_ns", stamp());
    r->status = 2;
  }
  dhwire::Reply handle(V request, uint64_t received, size_t) {
    check(get<size_t>(request, "version") == 1, "Unsupported protocol");
    auto op = get<std::string>(request, "op");
    V result;
    std::shared_ptr<Record> trace;
    if (op == "peer_submit") {
      try {
        auto response = accept(at(request, "state"), received);
        result = std::move(response.first);
        trace = std::move(response.second);
      } catch(const std::exception& e) {
        // prepare() runs before registry insertion. This specific rejection is
        // proof of non-admission; lost ACKs and arbitrary failures are not.
        if(std::string(e.what())!="Replica version is not accepting queries")throw;
        result=M{{"accepted",false},{"reason","replica_unavailable"}};
      }
    } else if (op == "peer_lease" || op == "peer_cancel") {
      std::vector<std::shared_ptr<Record>> selected;
      {
        std::lock_guard<std::mutex> l(mu);
        auto i=query_records.find(get<std::string>(request,"query_request_id"));
        if(i!=query_records.end())for(const auto& key:i->second)selected.push_back(records.at(key));
      }
      result = M{{"found", !selected.empty()}, {"branches",uint64_t(selected.size())}};
      for (const auto& r:selected) {
        check(get<std::string>(request, "token") ==
                  get<std::string>(r->state, "token"),
              "Invalid lease token");
        r->ticket->renew(seconds() + lease_seconds);
        if (op == "peer_cancel")
          r->ticket->cancel();
      }
    } else
      throw std::invalid_argument("Unsupported native data operation");
    dhwire::Reply out;
    out.message = M{{"ok", true}, {"result", std::move(result)}};
    if (has(request, "request_id"))
      out.message.map()["request_id"] = at(request, "request_id");
    if (trace) {
      trace->mark("receiver_accept_return_ns", stamp());
      out.sent = [trace](uint64_t ns, size_t) {
        trace->mark("receiver_ack_send_return_ns", ns);
      };
    }
    return out;
  }
  V metrics() const {
    auto control = controller->snapshot();
    auto queue = workers->metrics();
    M out;
    A traces, events, transfers;
    std::vector<std::shared_ptr<Record>> selected;
    {
      std::lock_guard<std::mutex> l(mu);
      out["unacknowledged"] = uint64_t(active);
      out["retained_branches"] = uint64_t(records.size());
      out["tombstones"] = uint64_t(tombstones.size());
      out["admission_capacity_rejections"] = uint64_t(rejected);
      for (const auto &[q, r] : records)
        selected.push_back(r);
    }
    std::unordered_map<std::string, M> ctl;
    size_t waiting = 0;
    for (const auto &b : array(at(control, "branches"))) {
      M transfer{{"query_request_id",at(b,"query_request_id")},{"branch_id",at(b,"branch_id")},{"pending_transfers",at(b,"pending_transfers")}};
      for(const auto& [key,value]:b.map())if(key.rfind("peer_",0)==0)transfer[key]=value;
      transfers.emplace_back(std::move(transfer));
      ctl[branch_key(b)] =
          at(b, "execution_timestamps").map();
      waiting += get<std::string>(b, "status") == "waiting_transfer";
      for (const auto &event : array(at(b, "control_events"))) {
        auto e = event.map();
        e["query_request_id"] = at(b, "query_request_id");
        e["branch_id"] = at(b, "branch_id");
        e["node_id"] = uint64_t(sid);
        events.emplace_back(std::move(e));
      }
    }
    for (const auto &r : selected) {
      M t;
      {
        std::lock_guard<std::mutex> l(r->trace_mu);
        t = r->trace;
      }
      if (t.empty())
        continue;
      auto q = get<std::string>(r->state, "query_request_id");
      for (auto &[k, v] : ctl[branch_key(r->state)])
        t[k] = std::move(v);
      t["query_request_id"] = q;
      t["branch_id"] = at(r->state, "branch_id");
      t["node_id"] = uint64_t(sid);
      traces.emplace_back(std::move(t));
    }
    for (const char *key :
         {"control_totals", "control_scheduler_totals", "control_active"})
      out[key] = at(control, key);
    control.map().erase("branches");
    out["native_control"] = std::move(control);
    out["execution_timestamps"] = std::move(traces);
    out["control_events"] = std::move(events);
    out["transfer_records"]=std::move(transfers);
    out["compute_queue"] = queue;
    out["running"] = at(queue, "active");
    out["queued"] = at(queue, "queued");
    out["waiting_transfer"] = uint64_t(waiting);
    out["network_send"] = client->metrics();
    out["send_queues"] =
        M{{"transfer", M{{"queued", uint64_t(client->pending())},
                         {"active", uint64_t(0)}}}};
    out["execution_model"] = "cpp_runtime";
    out["compute_mode"] = "native_fixed";
    out["io"] = M{{"actual_backend", "sync_pread"}};
    return out;
  }
  void close() {
    std::lock_guard<std::mutex> closing(close_mu);
    if (closed)
      return;
    {
      std::lock_guard<std::mutex> l(mu);
      stopping = true;
      for (auto &[q, r] : records)
        r->control->cancel();
    }
    wake.notify_all();
    if (maintenance.joinable())
      maintenance.join();
    workers->close();
    client->close();
    controller->close();
    closed = true;
  }
};
DataRuntime::DataRuntime(V c, V m, std::string sig, SearchFunction fn, SearchPrepare prep)
    : impl_(std::make_unique<Impl>(std::move(c), std::move(m), std::move(sig),
                                   std::move(fn), std::move(prep))) {}
DataRuntime::~DataRuntime() { close(); }
dhwire::Reply DataRuntime::handle(V r, uint64_t ns, size_t bytes) {
  return impl_->handle(std::move(r), ns, bytes);
}
V DataRuntime::metrics() const { return impl_->metrics(); }
V DataRuntime::network_metrics() const {A hist;for(auto& x:impl_->wait_hist)hist.emplace_back(x.load());return M{{"compute_cpu_ns",impl_->cpu_ns.load()},{"compute_wall_ns",impl_->work_ns.load()},{"compute_completed",impl_->completed_work.load()},{"compute_wait_hist_us_log2",hist},{"compute_queue",impl_->workers->metrics()},
  {"network_send",impl_->client->metrics()},{"report_control",impl_->controller->network_metrics()}};}
void DataRuntime::close() { impl_->close(); }
} // namespace dhruntime
