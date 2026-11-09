#include "native_network.hpp"
#include "native_head.hpp"
#include "entry_return_counters.hpp"
#include "logical_region.hpp"
#include "replica_pair_scope.hpp"
#include "native_pool.hpp"
#include <algorithm>
#include <chrono>
#include <openssl/sha.h>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace dhruntime {
using namespace dhvalue;
namespace {
uint64_t stamp() { return dhsearch::now_ns(); }
double thread_cpu_ms() {
  timespec t{};
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);
  return t.tv_sec * 1000.0 + t.tv_nsec / 1e6;
}
void check(bool yes, const char *msg) {
  if (!yes)
    throw std::invalid_argument(msg);
}
std::string digest(const V &v) {
  auto data = dhwire::pack(v);
  unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(data.data(), data.size(), hash);
  return std::string(reinterpret_cast<char *>(hash), sizeof(hash));
}
const std::vector<std::string> counters = {"demand_wait_ms","demand_resume_batches","demand_fallbacks","demand_fast_rejections","demand_source_already_started","demand_cache_expansions","demand_durable_expansions","remote_first_actions","remote_first_pushes","remote_pull_streak_pushes","remote_pull_disabled_pushes","one_sided_read_ops","one_sided_data_bytes","one_sided_record_ms","one_sided_code_ms","one_sided_filter_ms","one_sided_filter_cpu_ms","one_sided_lane_queue_ms","one_sided_result_cpu_ms","one_sided_result_ms","one_sided_auth_ms","pull_install_ms","pull_install_cpu_ms","tail_policy_expanded","tail_policy_stops","tail_policy_gain_stops","tail_policy_unused_credit","lazy_navigation_vertices","lazy_refined_vectors","lazy_unrefined_vertices","lazy_refine_batches","lazy_refine_cpu_ms","lazy_refine_wait_ms","pull_policy_pause_ms","pull_main_resume_count","pull_tail_resume_count","pull_gate_attempt","pull_gate_disabled","pull_gate_prior_failure","pull_gate_remaining","pull_gate_rounds","pull_gate_bytes","pull_approved_actions","pull_limit_rejections","pull_batches","pull_records","pull_expanded","pull_fallbacks","pull_wait_ms","pull_dispatch_queue_ms","pull_network_ms","pull_resume_queue_ms","pull_source_queue_ms","pull_resume_count","pull_wire_bytes","pull_record_bytes","pull_code_bytes","pull_source_cpu_ms","pull_source_read_ms","pull_source_service_ms","region_exclusion_ids_inherited","region_excluded_candidates","region_exclusion_ids_added","region_unexpanded_at_handoff","region_handoffs","same_host_region_handoffs","hotness_record_ms","hotness_expansions_recorded","duplicate_local_expansions_avoided","physical_node_revisits","cross_dwell_blocked_decisions","cross_dwell_exhaustion_escapes","peer_replica_selected","replica_record_reads","replica_local_expansions","file_read_bytes",
                                           "pread_calls",
                                           "cache_hits",
                                           "scanned_edges",
                                           "rabitq_evaluations",
                                           "exact_distance_evaluations",
                                           "pread_ms",
                                           "rabitq_ms",
                                           "exact_distance_ms",
                                           "service_ms",
                                           "async_read_requests",
                                           "io_batch_wall_ms",
                                           "io_capacity_wait_ms",
                                           "io_queue_wait_ms",
                                           "io_read_residence_ms",
                                           "query_ready_queue_ms",
                                           "query_actor_serial_wait_ms",
                                           "query_runnable_queue_ms",
                                           "query_compute_slice_wall_ms",
                                           "query_admission_queue_ms",
                                           "query_executor_start_wait_ms",
                                           "query_io_wait_ms",
                                           "query_resume_count",
                                           "query_transfer_finalize_wait_ms",
                                           "query_branch_wall_ms",
                                           "query_state_other_ms"};
M counts() {
  return {{"rpc_count", uint64_t(0)},
          {"request_bytes", uint64_t(0)},
          {"response_bytes", uint64_t(0)}};
}
void record_rpc(M &m, const dhwire::Result &r) {
  add(m, "rpc_count", 1);
  add(m, "request_bytes", double(r.request_bytes));
  add(m, "response_bytes", double(r.response_bytes));
  add(m, "pool_wait_ms",
      r.send_start_ns ? (r.send_start_ns - r.queued_ns) / 1e6 : 0.);
  add(m, "send_ms",
      r.send_done_ns ? (r.send_done_ns - r.send_start_ns) / 1e6 : 0.);
  add(m, "response_wait_ms",
      r.send_done_ns ? (r.done_ns - r.send_done_ns) / 1e6 : 0.);
  add(m, "connect_ms",
      r.connect_done_ns ? (r.connect_done_ns - r.connect_start_ns) / 1e6 : 0.);
  add(m, r.connection_created ? "connections_created" : "connections_reused",
      1);
}
} // namespace
bool validate_manifests(const V &reports, const std::string &root, size_t node,
                        size_t budget, size_t slots, size_t node_count, bool revisits, bool fanout) {
  struct Credit {
    std::string parent;
    size_t node, budget, slots;
    std::vector<size_t> path;
    size_t fallback=SIZE_MAX;
    std::vector<std::string> regions;
  };
  std::map<std::string, const V *> by_id;
  for (const auto &r : array(reports))
    check(by_id.emplace(get<std::string>(r, "branch_id"), &r).second,
          "Duplicate branch result");
  std::map<std::string, Credit> expected{
      {root, {"", node, budget, slots, {node}}}};
  std::vector<std::string> pending{root};
  std::set<std::string> visited;
  std::set<size_t> nodes;
  bool missing = false;
  while (!pending.empty()) {
    auto bid = pending.back();
    pending.pop_back();
    check(visited.insert(bid).second, "Branch cycle or duplicate child");
    auto it = by_id.find(bid);
    if (it == by_id.end()) {
      missing = true;
      continue;
    }
    const auto &r = *it->second;
    auto credit = expected.at(bid);
    if(optional<bool>(r,"replica_unavailable_fallback",false)){
      check(credit.fallback<node_count,"Undeclared replica fallback");
      credit.node=credit.fallback;credit.path.back()=credit.node;
      if(!credit.regions.empty())credit.regions.back()=dhregion::base(credit.node);
    }
    if(has(r,"logical_region")){
      const auto rp=get<std::vector<std::string>>(r,"visited_regions");
      const auto region=has(r,"replica_version")?"replica:"+get<std::string>(r,"replica_version"):dhregion::base(credit.node);
      if(credit.regions.empty()){check(bid==root,"Missing child logical identity");credit.regions={region};}
      check(rp==credit.regions&&get<std::string>(r,"logical_region")==region&&rp.back()==region,"Logical branch region/path mismatch");
    }else check(credit.regions.empty(),"Missing logical region report");
    check((revisits || nodes.insert(credit.node).second) &&
              get<std::vector<size_t>>(r, "visited_nodes") == credit.path,
          "Repeated data node or invalid visited-node path");
    const auto &parent = at(r, "parent_branch_id");
    check((credit.parent.empty()
               ? null(parent)
               : (!null(parent) && str(parent) == credit.parent)) &&
              get<size_t>(r, "node_id") == credit.node &&
              get<size_t>(r, "work_grant") == credit.budget &&
              get<size_t>(r, "branch_slots") == credit.slots,
          "Branch credit/parent mismatch");
    const auto spent = get<size_t>(r, "work_spent"),
               unused = get<size_t>(r, "work_unused");
    check(spent <= credit.budget && unused <= credit.budget - spent,
          "Invalid work credit accounting");
    size_t remaining = credit.budget - spent - unused,
           remaining_slots = credit.slots;
    const auto &children = array(at(r, "children"));
    check(fanout||children.size() <= 1, "A data node may forward only once");
    for (const auto &child : children) {
      auto id = get<std::string>(child, "branch_id");
      auto n = get<size_t>(child, "node_id"),
           b = get<size_t>(child, "work_grant"),
           s = get<size_t>(child, "branch_slots");
      check(n < node_count && b > 0 && b <= remaining && s < remaining_slots,
            "Invalid child credit");
      remaining -= b;
      remaining_slots -= 1 + s;
      check(get<std::string>(child, "parent_branch_id") == bid &&
                !expected.count(id),
            "Invalid child manifest");
      const auto child_region=optional<std::string>(child,"logical_region","");
      const bool same_host_region=!credit.regions.empty()&&!child_region.empty()&&child_region!=credit.regions.back();
      check((n != credit.node||same_host_region) && (revisits || std::find(credit.path.begin(), credit.path.end(), n) ==
                credit.path.end()),
            "Child revisits a data node");
      if (has(r, "decisions")) {
        size_t approvals = 0;
        for (const auto &d : array(at(r, "decisions")))
          if (get<std::string>(d, "decision_id") ==
                  get<std::string>(child, "decision_id") &&
              get<bool>(d, "remote") && get<size_t>(d, "selected_node") == n)
            ++approvals;
        check(approvals == 1, "Remote child lacks a unique approved decision");
      }
      auto path = credit.path;
      path.push_back(n);
      auto fallback=optional<size_t>(child,"fallback_node",SIZE_MAX);
      check(fallback==SIZE_MAX||(fallback<node_count&&fallback!=credit.node),"Invalid fallback node");
      auto rp=credit.regions;if(!rp.empty()){check(!child_region.empty(),"Missing child logical region");rp.push_back(child_region);}
      expected.emplace(id, Credit{bid, n, b, s, std::move(path),fallback,std::move(rp)});
      pending.push_back(id);
    }
    check(remaining == 0, "Global work credit conservation violation");
  }
  if (missing)
    return false;
  check(visited.size() == by_id.size(), "Orphan final report");
  return true;
}
V aggregate_native(const V &c, const V &reports, const V &transfer,
                   const V &involved) {
  const auto &rows = array(reports);
  check(!rows.empty(), "No branch results");
  M stats, per_shard;
  std::map<uint64_t, double> merged;
  std::set<uint64_t> expanded;
  size_t total_expanded = 0;
  std::set<size_t> nodes;
  std::set<std::string> backends;
  A path, stop_reasons, result_paths;
  const auto node_count = array(at(at(c, "cluster"), "nodes")).size();
  for (size_t n = 0; n < node_count; ++n)
    per_shard[std::to_string(n)] = M{};
  std::vector<std::string> sums = counters;
  for (const char *k :
       {"expanded", "batches", "fallback_entries", "frontier_dropped",
        "improved_topk_batches", "server_thread_cpu_ms",
        "visited_node_candidates_dropped",
        "remote_candidates_dropped_after_forward","region_candidates_merged","region_frontier_saved","replica_overlay_expansions","replica_host_base_expansions",
        "source_reserved_candidate_checks","source_reserved_credit_unused","candidate_sync_candidates_sent","candidate_sync_batches","candidate_sync_resumes","candidate_sync_resume_queue_ms","candidate_sync_park_ms","candidate_budget_terminal","candidate_receive_to_consume_ms","candidate_timed_consumed","candidate_sync_known_exhausted_skipped","candidate_start_accepted","candidate_start_conflicts","candidate_start_refunded_budget","candidate_early_results"})
    sums.push_back(k);
  for (const auto &key : sums)
    stats[key] = 0.;
  double rpc = 0, tx = 0, rx = 0, retries = 0, unused = 0, pending = 0,
         max_depth = 0, max_cross = 0;
  for (const auto &r : rows) {
    auto ids = get<std::vector<uint64_t>>(r, "ids");
    auto ds = get<std::vector<double>>(r, "squared_distances");
    check(ids.size() == ds.size(), "Result distance count mismatch");
    for (size_t i = 0; i < ids.size(); ++i) {
      double d = ds[i];
      check(std::isfinite(d) && d >= 0, "Invalid branch result distance");
      auto p = merged.find(ids[i]);
      if (p != merged.end()) {
        check(std::abs(p->second - d) <= 1e-8 + 1e-6 * std::abs(d),
              "Replica exact distance mismatch");
        p->second = std::min(p->second, d);
      } else
        merged[ids[i]] = d;
    }
    auto node = get<size_t>(r, "node_id");
    check(node < node_count, "Invalid report node");
    nodes.insert(node);
    const auto &rs = at(r, "stats");
    for (const auto &key : sums)
      add(stats, key, optional<double>(rs, key,0));
    auto &ps = per_shard.at(std::to_string(node)).map();
    for (const auto &key : counters)
      add(ps, key, optional<double>(rs, key,0));
    for (const char *k : {"expanded", "batches", "server_thread_cpu_ms"})
      add(ps, k, get<double>(rs, k));
    for (auto id : get<std::vector<uint64_t>>(r, "expanded_ids")) {
      ++total_expanded;
      expanded.insert(id);
    }
    const V peer = has(r, "peer_transfer") ? at(r, "peer_transfer") : V(M{});
    rpc += optional<double>(peer, "peer_rpc_count", 0);
    tx += optional<double>(peer, "peer_request_bytes", 0);
    rx += optional<double>(peer, "peer_response_bytes", 0);
    retries += std::max(0., optional<double>(peer, "peer_rpc_count", 0) -
                                optional<double>(peer,"peer_visit_rpc_count",0)-optional<double>(peer,"peer_region_reserve_rpc_count",0)-optional<double>(peer,"peer_pull_rpc_count",0)-optional<double>(peer,"peer_candidate_sync_rpc_count",0)-optional<double>(peer,"peer_candidate_claim_rpc_count",0)-optional<double>(peer,"candidate_start_conflict",0)-
                                array(at(r, "children")).size());
    for (const char *k :
         {"peer_transfer_queue_ms", "peer_pool_wait_ms", "peer_connect_ms",
          "peer_send_ms", "peer_response_wait_ms", "peer_transfer_wall_ms",
          "peer_connections_created", "peer_connections_reused","peer_visit_rpc_count","peer_visit_wall_ms",
          "peer_region_reserve_rpc_count","peer_region_reserve_wall_ms","peer_pull_rpc_count","peer_pull_wall_ms"})
      add(stats, k, optional<double>(peer, k, 0));
    for(const auto& [key,value]:peer.map())if(key.rfind("candidate_",0)==0)add(stats,key,cast<double>(value));
    unused += get<double>(r, "work_unused");
    pending += get<double>(rs, "pending_at_stop");
    max_depth = std::max(max_depth, get<double>(rs, "max_expanded_depth"));
    max_cross = std::max(max_cross, get<double>(rs, "max_cross_shard_depth"));
    const auto &visited = array(at(r, "visited_nodes"));
    if (visited.size() > path.size())
      path = visited;
    stop_reasons.push_back(at(rs, "stop_reason"));
    backends.insert(has(r, "execution")
                        ? get<std::string>(at(r, "execution"), "io_backend")
                        : "sync_pread");
  }
  std::vector<std::pair<uint64_t, double>> ranked(merged.begin(), merged.end());
  std::sort(ranked.begin(), ranked.end(), [](const auto &a, const auto &b) {
    return a.second < b.second || (a.second == b.second && a.first < b.first);
  });
  if (ranked.size() > get<size_t>(at(c, "search"), "top_k"))
    ranked.resize(get<size_t>(at(c, "search"), "top_k"));
  A ids, distances;
  std::set<uint64_t> result_ids;
  for (auto [id, d] : ranked) {
    ids.emplace_back(id);
    distances.emplace_back(d);
    result_ids.insert(id);
  }
  for (const auto &r : rows)
    for (const auto &p : array(at(r, "result_paths")))
      if (result_ids.count(get<uint64_t>(p, "id")))
        result_paths.push_back(p);
  M expansions;
  for (const auto &[sid, ps] : per_shard)
    expansions[sid] = optional<double>(ps, "expanded", 0);
  stats["per_shard"] = std::move(per_shard);
  stats["expansions_by_shard"] = std::move(expansions);
  stats["unique_expanded"] = uint64_t(expanded.size());
  stats["duplicate_expansions"] = uint64_t(total_expanded - expanded.size());
  stats["branch_count"] = uint64_t(rows.size());
  stats["branch_unused_credit"] = unused;
  stats["visited_nodes"] = std::move(path);
  stats["node_revisit_count"] = uint64_t(rows.size() - nodes.size());
  stats["shards_visited"] =
      vector(std::vector<size_t>(nodes.begin(), nodes.end()));
  stats["io_backends"] =
      vector(std::vector<std::string>(backends.begin(), backends.end()));
  stats["peer_execution_model"] = "native";
  stats["router_submit_rpc_count"] = at(transfer, "rpc_count");
  stats["peer_branch_rpc_count"] = rpc;
  stats["router_expand_rpc_count"] = uint64_t(0);
  stats["rpc_count"] = get<double>(transfer, "rpc_count") + rpc;
  stats["request_bytes"] = get<double>(transfer, "request_bytes") + tx;
  stats["response_bytes"] = get<double>(transfer, "response_bytes") + rx;
  stats["wire_bytes"] =
      number(stats["request_bytes"]) + number(stats["response_bytes"]);
  stats["peer_request_bytes"] = tx;
  stats["peer_response_bytes"] = rx;
  stats["rpc_retries"] = retries;
  stats["keepalive_nodes"] = involved;
  stats["max_expanded_depth"] = max_depth;
  stats["max_cross_shard_depth"] = max_cross;
  stats["max_discovery_depth"] = V();
  stats["graph_rounds"] = V();
  stats["server_to_server_migrations"] = uint64_t(0);
  // Logical region transitions can stay on the same physical host. Count
  // actual parent/child placements, including a resolved replica fallback.
  std::map<std::string, size_t> placements;
  for (const auto &r : rows)
    placements.emplace(get<std::string>(r,"branch_id"),get<size_t>(r,"node_id"));
  uint64_t crossings=0, local_handoffs=0;
  for (const auto &r : rows) {
    const auto &parent=at(r,"parent_branch_id");
    if (!null(parent)) {
      if (placements.at(str(parent))==get<size_t>(r,"node_id")) ++local_handoffs;
      else ++crossings;
    }
  }
  stats["server_to_server_branches"] = crossings;
  stats["same_host_branches"] = local_handoffs;
  stats["exact_reranked"] = stats["exact_distance_evaluations"];
  // The public API may still use TCP; this field describes inter-node RPCs.
  std::string transport = "tcp";
  if (has(c, "native_runtime") && has(at(c, "native_runtime"), "network_module"))
    transport = optional<std::string>(at(at(c, "native_runtime"), "network_module"),
                                      "transport", "tcp");
  stats["transport"] = transport;
  stats["execution"] = optional<bool>(at(c,"peer"),"allow_node_revisit",false)?"peer_bounded_revisit_parallel_local":"peer_single_visit_parallel_local";
  stats["stop_reason"] = "all_branch_manifests_complete";
  stats["branch_stop_reasons"] = std::move(stop_reasons);
  stats["pending_at_stop"] = pending;
  stats["pure_network_latency_fraction"] = V();
  stats["physical_ssd_latency_fraction"] = V();
  stats["result_paths"] = std::move(result_paths);
  stats["communication_mode"] = "optimized";
  return M{{"ids", std::move(ids)},
           {"squared_distances", std::move(distances)},
           {"stats", std::move(stats)}};
}
struct HeadRuntime::Impl {
  struct Query {
    std::mutex mu;
    std::string id, token, root, error;
    V query_id, state, original_seeds;
    M route_profile;
    bool replica_fallback=false;
    HeadRuntime::Completion completion;
    V replica_validation;
    std::shared_ptr<EntryReturnCounters> entry_returns;
    std::vector<double> input;
    std::map<std::string, V> reports;
    std::set<std::string> sync_delivered;std::map<std::string,V> sync_delivery_metrics;
    std::map<std::string,V> sync_idle;bool sync_probing=false;uint64_t sync_epoch=0,sync_probe_start=0;
    std::map<std::string, std::string> digests;
    std::map<std::string,V> announced_children;
    std::set<size_t> nodes;
    std::set<size_t> lease_inflight;
    M trace, entry = counts(), control = counts(), by_kind, transfer_failures;
    M branch_received, branch_ack;
    bool acked = false, all_reports = false, merging = false, finished = false;
    size_t responses = 0, attempts = 0;
    size_t maintenance_owner = 0;
    uint64_t submitted = 0, deadline = 0, next_lease = 0, finished_ns = 0, transfer_recovery_deadline = 0;
    std::vector<uint64_t> node_next_lease;
    double route_cpu = 0;
    Query() {
      control["errors"] = uint64_t(0);
      for (const char *k : {"heartbeat", "result", "lease", "cancel"})
        by_kind[k] = counts();
    }
  };
  V config, manifest;
  RouteFunction route;
  struct ReplicaRoute {size_t source,target;std::string version,family;double fraction,min_score,max_score;uint64_t expires;std::shared_ptr<EntryReturnCounters> returns{};};
  using ReplicaRoutes=std::unordered_map<uint64_t,std::vector<ReplicaRoute>>;
  std::shared_ptr<const ReplicaRoutes> replica_directory=std::make_shared<const ReplicaRoutes>();
  std::atomic<uint64_t> replica_lookup{0},replica_match{0},replica_stale{0},replica_score_reject{0},replica_probability_reject{0};
  std::atomic<uint64_t> replica_selected{0}, observation_sequence{0}, observation_dropped{0};
  mutable std::mutex observation_mu;
  A observations;
  M replica_access_counts;
  size_t observation_every=0,observation_limit=0;
  bool covered_entry_admission=false;
  size_t entry_counter_stripes=1;
  size_t node_count, capacity, max_retries, budget, slots;
  bool move_head_reports = false;
  bool per_node_maintenance = false;
  size_t maintenance_count = 1, next_maintenance_owner = 0;
  double lease_ms;
  uint64_t timeout_ns, heartbeat_ns, retention_ns, transfer_recovery_ns = 0;
  std::unique_ptr<Pool> routes, merges;
  std::unique_ptr<dhnet::Client> entries, leases;
  std::unique_ptr<dhnet::Server> server;
  std::vector<dhwire::Endpoint> endpoints;
  mutable std::mutex mu;
  std::mutex close_mu;
  std::condition_variable wake, done_cv;
  std::unordered_map<std::string, std::shared_ptr<Query>> queries;
  uint64_t registry_rehashes=0,registry_rehash_ns=0,registry_rehash_max_ns=0;
  // A query is registered with its deadline owner and each involved node.
  // Registry access uses mu; work on each query runs after releasing mu.
  std::vector<std::unordered_map<std::string, std::shared_ptr<Query>>> live;
  std::vector<std::deque<std::pair<std::string, uint64_t>>> expiry;
  std::vector<uint64_t> maintenance_ticks, maintenance_visits, lease_attempts;
  std::deque<V> finished;
  size_t active = 0;
  uint64_t successful_queries = 0, failed_queries = 0;
  std::deque<V> recent_failures;
  bool profile_head = false;
  uint64_t maintenance_registry_hold_ns = 0, maintenance_registry_hold_max_ns = 0;
  bool stopping = false, closed = false;
  std::vector<std::thread> timers;
  A head_endpoint;
  Impl(V c, V m, RouteFunction fn)
      : config(std::move(c)), manifest(std::move(m)), route(std::move(fn)) {
    profile_head = optional<bool>(at(config, "metrics"), "lifecycle_trace", false);
    if(has(config,"load_balance")&&get<bool>(at(config,"load_balance"),"enabled")) {
      observation_every=get<size_t>(at(config,"load_balance"),"sample_every");
      observation_limit=get<size_t>(at(config,"load_balance"),"max_observations");
      const auto& lb=at(config,"load_balance");
      if(has(lb,"entry_admission")){
        const auto& admission=at(lb,"entry_admission");
        covered_entry_admission=get<bool>(admission,"enabled");
        entry_counter_stripes=get<size_t>(admission,"counter_stripes");
        check(entry_counter_stripes>0&&entry_counter_stripes<=64,"Invalid entry counter stripes");
      }
    }
    const auto &p = at(config, "peer");
    const auto &nc = at(config, "native_runtime");
    move_head_reports = optional<bool>(nc, "move_head_reports", false);
    const auto &net = at(config, "network");
    capacity = get<size_t>(p, "max_active_queries");
    const auto reserve=optional<size_t>(nc,"registry_reserve_entries",0);
    if(reserve)queries.reserve(std::min(reserve,capacity+get<size_t>(p,"max_tombstones")));
    budget = get<size_t>(at(config, "search"), "max_expansions");
    slots = get<size_t>(p, "max_branches") - 1;
    max_retries = get<size_t>(net, "max_retries");
    lease_ms = get<double>(p, "lease_timeout_ms");
    timeout_ns = uint64_t(get<double>(p, "query_timeout_seconds") * 1e9);
    const auto recovery_ms=optional<uint64_t>(p,"transfer_failure_grace_ms",0);
    check(recovery_ms<=timeout_ns/1000000,"Transfer recovery grace exceeds query deadline");
    transfer_recovery_ns=recovery_ms*1000000;
    heartbeat_ns = get<uint64_t>(p, "heartbeat_interval_ms") * 1000000;
    retention_ns = uint64_t(get<double>(p, "retention_seconds") * 1e9);
    for (const auto &n : array(at(at(config, "cluster"), "nodes")))
      endpoints.emplace_back(get<std::string>(n, "host"),
                             get<uint16_t>(n, "port"));
    node_count = endpoints.size();
    const auto maintenance_mode = optional<std::string>(nc, "head_maintenance_mode", "single");
    check(maintenance_mode == "single" || maintenance_mode == "per_node", "Invalid head maintenance mode");
    per_node_maintenance = maintenance_mode == "per_node";
    maintenance_count = per_node_maintenance ? node_count : 1;
    check(maintenance_count > 0, "Head requires data nodes");
    live.resize(maintenance_count);
    if(reserve)for(auto& lane:live)lane.reserve(std::min(reserve,capacity));
    expiry.resize(maintenance_count);
    maintenance_ticks.resize(maintenance_count);
    maintenance_visits.resize(maintenance_count);
    lease_attempts.resize(maintenance_count);
    dhwire::Config wc;
    wc.max_pending = get<size_t>(nc, "network_queue_capacity");
    wc.max_connections_per_endpoint =
        get<size_t>(nc, "connections_per_endpoint");
    wc.max_message_bytes = get<size_t>(net, "max_message_bytes");
    wc.max_endpoints = node_count;
    wc.timeout_ms = uint64_t(get<double>(p, "handoff_timeout_seconds") * 1000);
    V entry_config = config;
    if (has(nc, "head_entry_network")) {
      const auto &overrides = at(nc, "head_entry_network");
      if (get<bool>(overrides, "enabled")) {
        check(has(nc, "network_module") && get<bool>(at(nc, "network_module"), "enabled"),
              "head_entry_network requires network_module");
        auto &policy = entry_config.map().at("native_runtime").map().at("network_module").map();
        for (const char *key : {"send_min_reactors", "send_max_reactors", "connections_per_endpoint_per_reactor"})
          policy[key] = at(overrides, key);
      }
    }
    entries = std::make_unique<dhnet::Client>(wc,entry_config);
    wc.timeout_ms = uint64_t(get<double>(p, "control_timeout_seconds") * 1000);
    leases = std::make_unique<dhnet::Client>(wc,config);
    const auto head_cpus=optional<std::vector<int>>(nc,"compute_cpu_ids",{});
    const bool head_pin=optional<bool>(nc,"head_pin_threads",true);
    routes = std::make_unique<Pool>(get<size_t>(nc, "route_threads"), capacity,head_cpus,head_pin,
                                  std::function<void()>{},"dhdgs-route");
    merges = std::make_unique<Pool>(get<size_t>(nc, "merge_threads"), capacity,head_cpus,head_pin,
                                  std::function<void()>{},"dhdgs-merge");
    dhwire::ServerConfig sc;
    sc.host = get<std::string>(p, "head_bind_host");
    sc.port = get<uint16_t>(p, "head_port");
    sc.workers = get<size_t>(nc, "receive_threads");
    sc.queue_capacity = get<size_t>(nc, "receive_queue_capacity");
    sc.max_connections = get<size_t>(nc, "max_connections");
    sc.max_message_bytes = wc.max_message_bytes;
    sc.listen_backlog = get<size_t>(net, "listen_backlog");
    server = std::make_unique<dhnet::Server>(
        sc, [this](V r, uint64_t ns, size_t bytes) {
          return receive(std::move(r), ns, bytes);
        },dhnet::Server::Handler{},config);
    head_endpoint = {get<std::string>(p, "head_advertise_host"),
                     uint64_t(server->port())};
    try {
      for (size_t i = 0; i < maintenance_count; ++i)
        timers.emplace_back([this, i] { maintain(i); });
    } catch (...) {
      { std::lock_guard<std::mutex> l(mu); stopping = true; }
      wake.notify_all();
      for (auto &timer : timers) if (timer.joinable()) timer.join();
      throw;
    }
  }
  ~Impl() { close(); }
  // Caller holds q->mu. All users of mu release it before locking a query.
  void track_node(const std::shared_ptr<Query> &q, size_t node) {
    if (!q->nodes.insert(node).second || !per_node_maintenance || q->finished || q->merging)
      return;
    std::lock_guard<std::mutex> l(mu);
    live.at(node).emplace(q->id, q);
  }
  // Caller holds q.mu. Early reports may precede the final candidate ACKs.
  // Both copying and moving report buffers must apply the delivery closure.
  void apply_delivery_metrics(const Query &q, const std::string &id, V &report) {
    auto it = q.sync_delivery_metrics.find(id);
    if (it == q.sync_delivery_metrics.end()) return;
    auto &peer = report.map()["peer_transfer"].map();
    const auto &final = it->second;
    for (auto pair : {std::pair<const char *, const char *>{"peer_rpc_count", "candidate_sync_rpc_count"},
                      {"peer_candidate_sync_rpc_count", "candidate_sync_rpc_count"},
                      {"peer_request_bytes", "candidate_sync_tx_bytes"},
                      {"peer_response_bytes", "candidate_sync_rx_bytes"}})
      add(peer, pair.first, optional<double>(final, pair.second, 0) - optional<double>(V(peer), pair.second, 0));
    for (const auto &[key, value] : final.map()) peer[key] = value;
  }
  V reports(const Query &q) {
    A result;
    for (const auto &[id, r] : q.reports) {
      V copy = r;
      apply_delivery_metrics(q, id, copy);
      result.push_back(std::move(copy));
    }
    return result;
  }
  void maybe_merge(
      std::shared_ptr<Query> q) { // Query mutex held; never waits for a worker.
    if (q->merging || q->finished)
      return;
    if (q->error.empty() && (!q->acked || !q->all_reports || q->responses))
      return;
    if(q->error.empty())for(const auto& [bid,r]:q->reports)
      if(optional<bool>(r,"candidate_delivery_required",false)&&!q->sync_delivered.count(bid))return;
    q->merging = true;
    q->trace["merge_enqueued_ns"] = stamp();
    if (!merges->submit([this, q] { merge(q); })) {
      q->merging = false;
      throw std::runtime_error("Native merge queue capacity exceeded");
    }
  }
  void dispatch(std::shared_ptr<Query> q) {
    V message;
    size_t node;
    {
      std::lock_guard<std::mutex> l(q->mu);
      if (q->finished || q->merging)
        return;
      ++q->attempts;
      node = get<size_t>(q->state, "target_node");
      message = M{
          {"version", uint64_t(1)}, {"op", "peer_submit"}, {"state", q->state}};
    }
    try {
      if (!entries->submit(
              endpoints.at(node), message, [this, q](dhwire::Result r) {
                bool retry = false;
                {
                  std::lock_guard<std::mutex> l(q->mu);
                  if (q->finished)
                    return;
                  record_rpc(q->entry, r);
                  q->trace["entry_submit_return_ns"] = r.done_ns;
                  q->trace["entry_send_done_ns"] = r.send_done_ns;
                  try {
                    check(r.ok, r.error.c_str());
                    check(get<bool>(r.response, "ok"), "Entry rejected query");
                    const auto& answer=at(r.response,"result");
                    if(!get<bool>(answer,"accepted")&&optional<std::string>(answer,"reason","")=="replica_unavailable"&&
                       has(q->state,"replica_version")&&null(q->replica_validation)) {
                      const auto rejected=get<size_t>(q->state,"target_node");
                      const auto source=get<size_t>(q->state,"original_entry_node");
                      auto& sm=q->state.map();sm.erase("replica_version");
                      sm["seeds"]=std::move(q->original_seeds);sm["target_node"]=uint64_t(source);
                      sm["entry_node"]=uint64_t(source);sm["visited_nodes"]=A{uint64_t(source)};
                      if(optional<bool>(at(config,"peer"),"logical_region_visits",false)){
                        sm["logical_region"]=dhregion::base(source);sm["visited_regions"]=A{dhregion::base(source)};
                      }
                      q->nodes.erase(rejected);track_node(q,source);
                      if(per_node_maintenance){std::lock_guard<std::mutex> registry(mu);
                        live[rejected].erase(q->id);q->maintenance_owner=source;}
                      q->replica_fallback=true;retry=true;
                    } else {
                    check(get<bool>(at(r.response, "result"), "accepted"),
                          "Entry not accepted");
                    q->acked = true;
                    }
                  } catch (const std::exception &e) {
                    retry = q->attempts <= max_retries && stamp() < q->deadline;
                    if (!retry)
                      q->error = e.what();
                  }
                  if (!retry) {
                    const bool already_merging = q->merging;
                    maybe_merge(q);
                    if (!already_merging && q->merging)
                      q->trace["completion_gate_kind"] = "entry_ack";
                  }
                }
                if (retry)
                  dispatch(q);
              }))
        throw std::runtime_error("Native entry queue capacity exceeded");
    } catch (const std::exception &e) {
      std::lock_guard<std::mutex> l(q->mu);
      q->error = e.what();
      maybe_merge(q);
    }
  }
  void route_query(std::shared_ptr<Query> q) {
    const auto start = stamp();
    {
      std::lock_guard<std::mutex> l(q->mu);
      q->trace["route_started_ns"] = start;
    }
    timespec cpu{};
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu);
    double cpu_start = cpu.tv_sec * 1000. + cpu.tv_nsec / 1e6;
    try {
      V s = route(q->input, q->query_id);
      M route_profile;
      if(has(s,"_head_route_profile")) {
        route_profile = std::move(s.map().at("_head_route_profile").map());
        s.map().erase("_head_route_profile"); // Never transmit head-only diagnostics to peers.
      }
      auto &sm=s.map();const auto& entry=array(array(at(s,"seeds")).front());
      const auto entry_id=cast<uint64_t>(entry[0]);const double estimate=number(entry[1]);
      sm["route_entry_id"]=entry_id;sm["route_entry_estimate"]=estimate;
      sm["original_entry_node"]=at(s,"target_node");
      auto directory=std::atomic_load(&replica_directory);auto found=directory->find(entry_id);
      ReplicaRoute forced{};bool validating=!null(q->replica_validation);
      if(!validating){++replica_lookup;if(found!=directory->end())++replica_match;}
      const ReplicaRoute* selected=nullptr;
      if(validating){const auto& v=q->replica_validation;
        check(entry_id==get<uint64_t>(v,"entry_id")&&get<size_t>(s,"target_node")==get<size_t>(v,"source"),"Validation entry changed");
        auto target=get<size_t>(v,"target");check(target<node_count,"Invalid validation destination");
        forced={get<size_t>(v,"source"),target,get<std::string>(v,"version"),"",1.,-1e300,1e300,stamp()+timeout_ns};selected=&forced;
      }else if(found!=directory->end()){
        uint64_t hash=1469598103934665603ULL;
        for(unsigned char ch:q->id){hash^=ch;hash*=1099511628211ULL;}
        const double draw=double(hash>>11)*(1.0/9007199254740992.0);
        double end=0.;bool any_fresh=false,any_score=false;
        std::string admitted_family;
        for(const auto& r:found->second){
          // Overlapping graphs register every included portal. Exactly one
          // positive-share family owns this request; closing its entry exposes
          // the next family without dispatching a second root search.
          if(covered_entry_admission){
            const bool fresh=r.source==get<size_t>(s,"target_node")&&stamp()<r.expires;
            any_fresh|=fresh;any_score|=fresh&&estimate>=r.min_score&&estimate<=r.max_score;
            if(r.fraction<=0 || r.source!=get<size_t>(s,"target_node") || stamp()>=r.expires ||
                estimate<r.min_score || estimate>r.max_score)continue;
            const auto identity=r.family.empty()?r.version:r.family;
            if(admitted_family.empty())admitted_family=identity;
            if(identity!=admitted_family)continue;
          }
          const double begin=end;end+=r.fraction;
          const bool fresh=r.source==get<size_t>(s,"target_node")&&stamp()<r.expires;
          const bool score=estimate>=r.min_score&&estimate<=r.max_score;
          any_fresh|=fresh;any_score|=fresh&&score;
          // Preserve each interval when a destination expires: its probability
          // returns to the source, without silently enlarging other copies.
          if(fresh&&score&&draw>=begin&&draw<end){selected=&r;break;}
        }
        if(!selected){if(!any_fresh)++replica_stale;else if(!any_score)++replica_score_reject;else ++replica_probability_reject;}
      }
      if(selected){const auto& r=*selected;
          if(!validating)q->entry_returns=r.returns;
          q->original_seeds=at(s,"seeds");
          sm["target_node"]=uint64_t(r.target);sm["entry_node"]=uint64_t(r.target);
          sm["visited_nodes"]=A{uint64_t(r.target)};if(!r.version.empty()){sm["replica_version"]=r.version;sm["replica_access_kind"]="entry";
            if(dhpair::scoped(at(config,"peer"))&&get<bool>(at(at(config,"peer"),"candidate_sync"),"enabled")){
              sm["replica_pair_source"]=sm.at("original_entry_node");sm["replica_pair_host"]=uint64_t(r.target);sm["replica_pair_version"]=r.version;}}
          auto &row=std::get<A>(std::get<A>(sm["seeds"].data).front().data);
          auto &owners=std::get<A>(row[3].data);bool present=false;
          for(auto& n:owners)present|=cast<size_t>(n)==r.target;
          if(!present)owners.emplace_back(uint64_t(r.target));
          if(!validating){++replica_selected;std::lock_guard<std::mutex> lock(observation_mu);
            if(!replica_access_counts.count(r.version))replica_access_counts[r.version]=M{};
            add(replica_access_counts[r.version].map(),"entry_selected",1);
          }
      }
      sm["internal_validation"]=validating;
      if(optional<bool>(at(config,"peer"),"logical_region_visits",false))dhregion::normalize(s);
      q->input.clear();
      {
        std::lock_guard<std::mutex> l(q->mu);
        if (q->finished || q->merging)
          return;
        auto &m = s.map();
        m["query_request_id"] = q->id;
        m["branch_id"] = q->root;
        m["parent_branch_id"] = V();
        m["token"] = q->token;
        m["head_endpoint"] = head_endpoint;
        if (has(config, "_cluster_revision"))
          m["cluster_revision"] = at(config, "_cluster_revision");
        const auto target = get<size_t>(s, "target_node");
        track_node(q, target);
        if (per_node_maintenance) {
          std::lock_guard<std::mutex> registry(mu);
          if (!q->nodes.count(q->maintenance_owner))
            live[q->maintenance_owner].erase(q->id);
          q->maintenance_owner = target;
        }
        q->state = std::move(s);
        q->route_profile = std::move(route_profile);
        q->trace["routing_done_ns"] = stamp();
        q->trace["entry_submit_enter_ns"] = stamp();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu);
        q->route_cpu = cpu.tv_sec * 1000. + cpu.tv_nsec / 1e6 - cpu_start;
      }
      dispatch(q);
    } catch (const std::exception &e) {
      std::lock_guard<std::mutex> l(q->mu);
      q->error = e.what();
      maybe_merge(q);
    }
  }
  std::string submit(std::vector<double> query, V qid, std::string request_id, HeadRuntime::Completion completion, V validation) {
    auto q = std::make_shared<Query>();
    q->id = request_id.empty() ? identifier() : std::move(request_id);
    q->token = identifier();
    q->root = identifier();
    q->query_id = std::move(qid);
    q->input = std::move(query);
    q->completion = std::move(completion);
    q->replica_validation=std::move(validation);
    q->submitted = stamp();
    q->deadline = q->submitted + timeout_ns;
    q->next_lease = q->submitted + heartbeat_ns;
    q->node_next_lease.assign(node_count, q->next_lease);
    q->trace = {{"search_enter_ns", q->submitted}};
    const auto registry_requested = profile_head ? stamp() : 0;
    {
      std::lock_guard<std::mutex> l(mu);
      if (profile_head) q->trace["submit_registry_acquired_ns"] = stamp();
      check(!stopping, "Native head closed");
      check(active + finished.size() < capacity,
            "Native head capacity exceeded");
      check(queries.size() <
                capacity + get<size_t>(at(config, "peer"), "max_tombstones"),
            "Native head retention capacity exceeded");
      const auto buckets=queries.bucket_count(),insert_started=stamp();
      check(queries.emplace(q->id, q).second, "Duplicate native request ID");
      if(queries.bucket_count()!=buckets){const auto ns=stamp()-insert_started;++registry_rehashes;registry_rehash_ns+=ns;registry_rehash_max_ns=std::max(registry_rehash_max_ns,ns);}
      q->maintenance_owner = next_maintenance_owner++ % maintenance_count;
      live[q->maintenance_owner].emplace(q->id, q);
      ++active;
    }
    if (profile_head) {
      const auto registry_released = stamp();
      std::lock_guard<std::mutex> query_lock(q->mu);
      q->trace["submit_registry_requested_ns"] = registry_requested;
      q->trace["submit_registry_released_ns"] = registry_released;
      q->trace["route_enqueue_ns"] = stamp();
    }
    if (!routes->submit([this, q] { route_query(q); })) {
      std::lock_guard<std::mutex> l(mu);
      queries.erase(q->id);
      live[q->maintenance_owner].erase(q->id);
      --active;
      throw std::runtime_error("Native route queue capacity exceeded");
    }
    return q->id;
  }
  #include "candidate_sync_head.inc"
  dhwire::Reply receive(V message, uint64_t received, size_t bytes) {
    if(optional<std::string>(message,"op","")=="candidate_delivery_done"||optional<std::string>(message,"op","")=="candidate_delivery_failed")return receive_candidate_delivery(std::move(message),bytes);
    if(optional<std::string>(message,"op","")=="candidate_idle")return receive_candidate_idle(std::move(message),bytes);
    auto id = get<std::string>(message, "query_request_id");
    std::shared_ptr<Query> q;
    const auto registry_requested = profile_head ? stamp() : 0;
    uint64_t registry_acquired = 0;
    {
      std::lock_guard<std::mutex> l(mu);
      if (profile_head) registry_acquired = stamp();
      auto i = queries.find(id);
      check(i != queries.end(), "Unknown query");
      q = i->second;
    }
    const auto node = get<size_t>(message, "node_id");
    check(node < node_count, "Invalid control node");
    auto &incoming = std::get<A>(message.map().at("reports").data);
    const bool error = has(message, "error") && !null(at(message, "error"));
    const std::string kind =
        (!incoming.empty() || error) ? "result" : "heartbeat";
    const auto query_lock_requested = profile_head ? stamp() : 0;
    std::lock_guard<std::mutex> l(q->mu);
    const auto query_lock_acquired = profile_head ? stamp() : 0;
    if (profile_head) {
      add(q->route_profile, "report_registry_lock_wait_ms", (registry_acquired-registry_requested)/1e6);
      add(q->route_profile, "report_query_lock_wait_ms", (query_lock_acquired-query_lock_requested)/1e6);
      add(q->route_profile, "report_receive_to_handler_ms", (registry_requested-received)/1e6);
    }
    check(get<std::string>(message, "token") == q->token,
          "Invalid query control token");
    // A failed query may still have valid, in-flight branch announcements and
    // reports. Acknowledge cancellation without admitting their payloads; an
    // unknown-child error here otherwise makes the sender retry forever.
    const bool cancelled_terminal=!q->error.empty()&&(q->finished||q->merging);
    track_node(q, node);
    for (auto n : get<std::vector<size_t>>(message, "involved_nodes")) {
      check(n < node_count, "Invalid involved node");
      track_node(q, n);
    }
    if(!cancelled_terminal&&has(message,"children"))for(const auto& child:array(at(message,"children"))) {
      const auto bid=get<std::string>(child,"branch_id");
      check(get<std::string>(child,"parent_branch_id")==get<std::string>(message,"branch_id"),"Child announcement parent mismatch");
      check(get<size_t>(child,"node_id")<node_count,"Invalid announced child node");
      auto it=q->announced_children.find(bid);
      if(it!=q->announced_children.end())check(digest(it->second)==digest(child),"Conflicting child announcement");
      else {
        check(!q->finished&&!q->merging,"Late unknown child announcement");
        q->announced_children.emplace(bid,child);
      }
      track_node(q, get<size_t>(child,"node_id"));
      if(!q->reports.count(bid))q->all_reports=false;
    }
    if(cancelled_terminal)incoming.clear();
    for (auto &r : incoming) {
      check(get<size_t>(r, "node_id") == node, "Report origin mismatch");
      auto bid = get<std::string>(r, "branch_id");
      auto hash = digest(r);
      auto it = q->digests.find(bid);
      if (it != q->digests.end())
        check(it->second == hash, "Conflicting duplicate branch result");
      else {
        check(!q->finished && !q->merging, "Late unknown branch result");
        q->digests.emplace(bid, std::move(hash));q->sync_idle.erase(bid);
        q->branch_received[bid] = received;
        // message owns this payload; validation above has already consumed it.
        // Keep the digest for retries, but transfer the large report only once.
        if(move_head_reports)q->reports.emplace(bid, std::move(r));
        else q->reports.emplace(bid, r);
      }
    }
    if (!q->finished && !q->merging) {
      if (error) {
        if(optional<bool>(message,"transfer_failure",false)) {
          // A delayed/lost admission ACK is inconclusive. A valid child report
          // proves execution; otherwise the declared missing branch times out.
          q->transfer_failures[get<std::string>(message,"branch_id")]=at(message,"error");
          // Exhausted transport retries are ambiguous: allow a real child
          // result to recover the query, but optionally bound this error-only
          // grace period. It never returns an incomplete top-k as success.
          if(transfer_recovery_ns&&!q->transfer_recovery_deadline)
            q->transfer_recovery_deadline=std::min(q->deadline,stamp()+transfer_recovery_ns);
        } else q->error = get<std::string>(message, "error");
      }
      if (!incoming.empty()) {
        try {
          q->all_reports = validate_manifests(
              reports(*q), q->root, get<size_t>(q->state, "target_node"),
              budget, slots, node_count, (has(at(config,"peer"),"candidate_sync")&&get<bool>(at(at(config,"peer"),"candidate_sync"),"enabled")&&!dhpair::scoped(at(config,"peer")))?false:optional<bool>(at(config,"peer"),"allow_node_revisit",false),optional<bool>(at(config,"peer"),"region_tasks",false));
        } catch (const std::exception &e) {
          q->error = e.what();
        }
        if(q->all_reports)for(const auto& [bid,child]:q->announced_children) {
          auto found=q->reports.find(bid);
          if(found==q->reports.end()){q->all_reports=false;break;}
          check(get<std::string>(found->second,"parent_branch_id")==get<std::string>(child,"parent_branch_id"),"Announced child result parent mismatch");
        }
        if (q->all_reports)
          q->trace["all_reports_received_ns"] = stamp();
      }
      if(!q->all_reports)maybe_sync_probe(q);
      q->trace["last_report_received_ns"] = received;
    }
    if (profile_head)
      add(q->route_profile, "report_validation_under_query_lock_ms", (stamp()-query_lock_acquired)/1e6);
    ++q->responses;
    add(q->control, "rpc_count", 1);
    add(q->control, "request_bytes", double(bytes));
    add(q->by_kind[kind].map(), "rpc_count", 1);
    add(q->by_kind[kind].map(), "request_bytes", double(bytes));
    dhwire::Reply out;
    out.message =
        M{{"ok", true},
          {"cancelled", !q->error.empty()},
          {"lease_ms", lease_ms},
          {"involved_nodes",
           vector(std::vector<size_t>(q->nodes.begin(), q->nodes.end()))}};
    const auto control_branch = optional<std::string>(message, "branch_id", "");
    out.sent = [this, q, kind, control_branch](uint64_t ns, size_t tx) {
      std::lock_guard<std::mutex> l(q->mu);
      --q->responses;
      add(q->control, "response_bytes", double(tx));
      add(q->by_kind[kind].map(), "response_bytes", double(tx));
      q->trace["last_report_ack_sent_ns"] = ns;
      if (kind == "result") q->branch_ack[control_branch] = ns;
      const bool already_merging = q->merging;
      maybe_merge(q);
      if (!already_merging && q->merging) {
        q->trace["completion_gate_kind"] = kind;
        q->trace["completion_gate_branch_id"] = control_branch;
      }
    };
    return out;
  }
  void lease(std::shared_ptr<Query> q, size_t node, bool cancel) {
    const std::string kind = cancel ? "cancel" : "lease";
    V message = M{{"version", uint64_t(1)},
                  {"op", cancel ? "peer_cancel" : "peer_lease"},
                  {"query_request_id", q->id},
                  {"token", q->token}};
    try {
      if (!leases->submit(
              endpoints.at(node), message, [q, node, kind](dhwire::Result r) {
                std::lock_guard<std::mutex> l(q->mu);
                q->lease_inflight.erase(node);
                record_rpc(q->control, r);
                record_rpc(q->by_kind[kind].map(), r);
                if (!r.ok || !optional<bool>(r.response, "ok", false))
                  add(q->control, "errors", 1);
              })) {
        std::lock_guard<std::mutex> l(q->mu);
        q->lease_inflight.erase(node);
        add(q->control, "errors", 1);
      }
    } catch (...) {
      std::lock_guard<std::mutex> l(q->mu);
      q->lease_inflight.erase(node);
      add(q->control, "errors", 1);
    }
  }
  void maintain(size_t lane) {
    for (;;) {
      std::vector<std::shared_ptr<Query>> selected, retired;
      {
        std::unique_lock<std::mutex> l(mu);
        if (wake.wait_for(l, std::chrono::milliseconds(10),
                          [this] { return stopping; }))
          return;
        const auto now = stamp();
        const auto registry_work_started = profile_head ? now : 0;
        ++maintenance_ticks[lane];
        auto &expired = expiry[lane];
        while (!expired.empty() && now - expired.front().second > retention_ns) {
          auto it = queries.find(expired.front().first);
          if (it != queries.end()) {
            retired.push_back(std::move(it->second));
            queries.erase(it);
          }
          expired.pop_front();
        }
        for (const auto &[id, q] : live[lane])
          selected.push_back(q);
        maintenance_visits[lane] += selected.size();
        if (profile_head) {
          const auto elapsed = stamp()-registry_work_started;
          maintenance_registry_hold_ns += elapsed;
          maintenance_registry_hold_max_ns = std::max(maintenance_registry_hold_max_ns, elapsed);
        }
      }
      for (auto q : selected) {
        std::vector<size_t> renew;
        {
          std::lock_guard<std::mutex> l(q->mu);
          const auto now = stamp();
          if (q->finished || q->merging)
            continue;
          if(lane==q->maintenance_owner&&q->transfer_recovery_deadline&&
             now>=q->transfer_recovery_deadline&&!q->all_reports) {
            q->error="Child report recovery grace exceeded after transfer failure";
            maybe_merge(q);continue;
          }
          if (lane == q->maintenance_owner && now >= q->deadline) {
            q->error = q->transfer_failures.empty()?"Global query deadline exceeded":"Global query deadline exceeded; child report missing after transfer failure";
            maybe_merge(q);
            continue;
          }
          if (per_node_maintenance) {
            if (q->nodes.count(lane) && now >= q->node_next_lease[lane]) {
              q->node_next_lease[lane] = now + heartbeat_ns;
              if (q->lease_inflight.insert(lane).second)
                renew.push_back(lane);
            }
          } else if (now >= q->next_lease) {
            q->next_lease = now + heartbeat_ns;
            for (auto n : q->nodes)
              if (q->lease_inflight.insert(n).second)
                renew.push_back(n);
          }
        }
        if (!renew.empty()) {
          std::lock_guard<std::mutex> l(mu);
          lease_attempts[lane] += renew.size();
        }
        for (auto n : renew)
          lease(q, n, false);
      }
    }
  }
  void merge(std::shared_ptr<Query> q) {
    const double cpu_start = thread_cpu_ms();
    const auto worker_enter = stamp();
    M profile = M{};
    V result;
    std::vector<size_t> cancel_nodes;
    {
      std::lock_guard<std::mutex> l(q->mu);
      const auto query_locked = stamp();
      q->trace["merge_started_ns"] = query_locked;
      q->trace["merge_worker_enter_ns"] = worker_enter;
      profile = q->route_profile;
      profile["merge_query_lock_wait_ms"] = (query_locked-worker_enter)/1e6;
      const double prepare_cpu = thread_cpu_ms();
      try {
        if (!q->error.empty())
          throw std::runtime_error(q->error);
        V rs;
        if(move_head_reports) {
          // merging fences new reports; duplicate RPCs are checked by digest.
          // The final result becomes the sole owner of the report buffers.
          A owned;owned.reserve(q->reports.size());
          for(auto& [id,report]:q->reports) {
            apply_delivery_metrics(*q,id,report);
            owned.push_back(std::move(report));
          }
          q->reports.clear();rs=std::move(owned);
        } else rs = reports(*q);
        const double aggregate_cpu = thread_cpu_ms();
        const auto aggregate_wall = stamp();
        profile["merge_report_prepare_cpu_ms"] = aggregate_cpu-prepare_cpu;
        result = aggregate_native(
            config, rs, q->entry,
            vector(std::vector<size_t>(q->nodes.begin(), q->nodes.end())));
        const double aggregate_done_cpu = thread_cpu_ms();
        profile["merge_aggregate_cpu_ms"] = aggregate_done_cpu-aggregate_cpu;
        profile["merge_aggregate_wall_ms"] = (stamp()-aggregate_wall)/1e6;
        auto &out = result.map();
        auto &stats = out.at("stats").map();
        check(number(stats.at("expanded")) <= budget, "Global budget exceeded");
        out["status"] = "ok";
        out["request_id"] = q->id;
        out["epoch"] = at(manifest, "epoch");
        out["metric"] = at(manifest, "metric");
        out["branch_reports"] = std::move(rs);
        stats["route_entry_id"]=at(q->state,"route_entry_id");
        stats["original_entry_node"]=at(q->state,"original_entry_node");
        stats["replica_unavailable_fallback"]=q->replica_fallback;
        if(has(q->state,"replica_version"))stats["replica_version"]=at(q->state,"replica_version");
        const bool has_replica_reports = std::any_of(array(out.at("branch_reports")).begin(),
            array(out.at("branch_reports")).end(), [](const V& b){return has(b,"replica_version");});
        if(q->entry_returns && !q->replica_fallback && has_replica_reports){
          const auto version=get<std::string>(q->state,"replica_version");
          const auto source=get<size_t>(q->state,"original_entry_node");
          bool returned=false;uint64_t remaining=0, first=UINT64_MAX;
          for(const auto& b:array(out.at("branch_reports"))){
            if(optional<std::string>(b,"replica_version","")!=version || !has(b,"fork_events"))continue;
            for(const auto& event:array(at(b,"fork_events"))){
              if(get<size_t>(event,"target")!=source)continue;
              auto when=get<uint64_t>(event,"dispatch_submitted_ns");
              if(when<first){first=when;remaining=get<uint64_t>(event,"remaining_before");returned=true;}
            }
          }
          q->entry_returns->record(returned,remaining);
        }
        if(null(q->replica_validation) && has_replica_reports){
          std::lock_guard<std::mutex> lock(observation_mu);
          for(const auto& b:array(out.at("branch_reports")))if(has(b,"replica_version")){
            auto version=get<std::string>(b,"replica_version");
            if(!replica_access_counts.count(version))replica_access_counts[version]=M{};
            auto& counts=replica_access_counts[version].map();
            const auto kind=optional<std::string>(b,"replica_access_kind",null(at(b,"parent_branch_id"))?"entry":"peer");
            add(counts,kind+"_completed",1);add(counts,kind+"_expanded",double(get<size_t>(b,"work_spent")));
          }
        }
        if(null(q->replica_validation)&&observation_every&&observation_sequence.fetch_add(1)%observation_every==0) {
          A branches;for(const auto& b:array(out.at("branch_reports"))) {
            const auto& bs=at(b,"stats");
            branches.emplace_back(M{{"node",at(b,"node_id")},{"expanded_ids",at(b,"expanded_ids")},
              {"cpu_ms",at(bs,"server_thread_cpu_ms")},{"work",at(b,"work_spent")}});
          }
          V observation=M{{"entry",at(q->state,"route_entry_id")},{"source",at(q->state,"original_entry_node")},
            {"query_id",q->query_id},{"score",at(q->state,"route_entry_estimate")},
            {"replica",has(q->state,"replica_version")?at(q->state,"replica_version"):V()},
            {"branches",std::move(branches)},{"result_ids",out.at("ids")}};
          if(optional<std::string>(at(config,"load_balance"),"validation_mode","ground_truth")=="shadow")
            observation.map()["query_f32"]=at(q->state,"query_f32");
          std::lock_guard<std::mutex> lock(observation_mu);
          if(observations.size()<observation_limit)observations.push_back(std::move(observation));else ++observation_dropped;
        }
        stats["entry_rpc"] = q->entry;
        stats["branch_report_received_ns"] = q->branch_received;
        stats["branch_report_ack_sent_ns"] = q->branch_ack;
        stats["transfer_failures_observed"]=q->transfer_failures;
        stats["announced_child_count"]=uint64_t(q->announced_children.size());
        stats["head_route_cpu_ms"] = q->route_cpu;
        for (const char *key :
             {"rpc_count", "request_bytes", "response_bytes", "errors"})
          stats[std::string("control_") + key] = q->control.at(key);
        stats["control_wire_bytes"] = number(q->control.at("request_bytes")) +
                                      number(q->control.at("response_bytes"));
        stats["all_protocol_bytes"] =
            number(stats["wire_bytes"]) + number(stats["control_wire_bytes"]);
        stats["all_protocol_rpc_count"] =
            number(stats["rpc_count"]) + number(q->control.at("rpc_count"));
        stats["control_by_kind"] = q->by_kind;
        stats["control_error_details"] = A{};
        stats["control_inflight_at_return"] =
            uint64_t(q->lease_inflight.size());
        stats["metrics_complete"] = number(stats["rpc_retries"]) == 0 &&
                                    number(q->control["errors"]) == 0 &&
                                    q->attempts == 1 &&
                                    q->lease_inflight.empty();
        stats["metrics_complete_excluding_pending_transfers"]=stats["metrics_complete"];
        bool transfers_complete=true;
        for(const auto& report:array(out.at("branch_reports")))transfers_complete &= optional<bool>(report,"transfer_metrics_complete",true)&&!optional<bool>(report,"region_task_mode",false);
        stats["transfer_metrics_complete"]=transfers_complete;
        stats["metrics_complete"]=cast<bool>(stats["metrics_complete"])&&transfers_complete;
        stats["head_merge_cpu_ms"] = thread_cpu_ms() - cpu_start;
        profile["merge_metadata_cpu_ms"] = thread_cpu_ms()-aggregate_done_cpu;
        stats["head_stage_timings_ms"] = profile;
        stats["coordinator_thread_cpu_ms"] =
            q->route_cpu + number(stats["head_merge_cpu_ms"]);
        const auto finished_at = stamp();
        q->trace["native_ready_ns"] = finished_at;
        stats["native_head_timestamps"] = q->trace;
        stats["head_timestamps"] = q->trace;
        stats["head_timestamps"].map()["search_return_ns"] = finished_at;
        double route_started = number(q->trace.at("route_started_ns")),
               routing_done = number(q->trace.at("routing_done_ns")),
               enqueued = number(q->trace.at("merge_enqueued_ns")),
               merge_started = number(q->trace.at("merge_started_ns"));
        M parts{{"head_route_queue_ms", (route_started - q->submitted) / 1e6},
                {"routing_wall_ms", (routing_done - route_started) / 1e6},
                {"peer_execution_and_control_wait_ms",
                 (enqueued - routing_done) / 1e6},
                {"head_merge_queue_ms", (merge_started - enqueued) / 1e6},
                {"head_merge_and_lifecycle_ms",
                 (finished_at - merge_started) / 1e6}};
        stats["latency_ms"] = (finished_at - q->submitted) / 1e6;
        stats["native_latency_ms"] = stats["latency_ms"];
        stats["latency_breakdown_ms"] = parts;
        M fractions;
        for (const auto &[k, v] : parts)
          fractions[k] = number(v) / number(stats["latency_ms"]);
        stats["latency_breakdown_fraction"] = std::move(fractions);
      } catch (const std::exception &e) {
        q->error = e.what();
        // Errors share the same completion boundary as successful searches.
        // Diagnostic timestamps must include this field before finalization.
        q->trace["native_ready_ns"] = stamp();
        result =
            M{{"status", "error"},
              {"error", q->error},
              {"request_id", q->id},
              {"ids", A{}},
              {"stats", M{{"metrics_complete", false},
                          {"latency_ms", (stamp() - q->submitted) / 1e6},
                          {"transfer_failures_observed",q->transfer_failures},
                          {"announced_children",q->announced_children},
                          {"branch_report_received_ns",q->branch_received},
                          {"entry_rpc",q->entry},
                          {"native_head_timestamps",q->trace}}}};
        cancel_nodes.assign(q->nodes.begin(), q->nodes.end());
      }
      q->finished = true;
      q->finished_ns = stamp();
      q->reports.clear();
      q->state = V();
    }
    const auto registry_requested = stamp();
    {
      std::lock_guard<std::mutex> l(mu);
      if(has(result,"stats"))
        result.map().at("stats").map()["head_finalize_registry_wait_ms"] = (stamp()-registry_requested)/1e6;
      if (!q->completion && has(result,"stats") && has(at(result,"stats"),"native_head_timestamps"))
        result.map().at("stats").map().at("native_head_timestamps").map()["completed_enqueued_ns"]=stamp();
      // Direct native capture bypasses the consumer queue. Errors also have
      // a completion boundary; they must release the benchmark admission slot.
      result.map()["retrieval_done_ns"] = has(at(result,"stats"),"native_head_timestamps")
          ? at(at(at(result,"stats"),"native_head_timestamps"),"native_ready_ns") : V(stamp());
      // Observe terminal outcomes without consuming result payloads in Python.
      // This uses the existing finalization critical section; failure history
      // is bounded and contains no vector, token or intermediate search state.
      if (q->error.empty()) ++successful_queries;
      else {
        ++failed_queries;
        if (recent_failures.size() == 16) recent_failures.pop_front();
        recent_failures.emplace_back(M{{"request_id", q->id},
            {"error", q->error.substr(0, 512)}, {"time_ns", stamp()}});
      }
      if (!q->completion) finished.push_back(std::move(result));
      for (auto &registry : live) registry.erase(q->id);
      expiry[q->maintenance_owner].emplace_back(q->id, stamp());
      --active;
    }
    if (q->completion) {
      auto completion = std::move(q->completion);
      completion(std::move(result));
    } else done_cv.notify_one();
    for (auto n : cancel_nodes)
      lease(q, n, true);
  }
  V completed(size_t maximum, uint64_t timeout) {
    A out;
    std::unique_lock<std::mutex> l(mu);
    done_cv.wait_for(l, std::chrono::milliseconds(timeout),
                     [this] { return !finished.empty() || closed; });
    while (!finished.empty() && out.size() < maximum) {
      auto& value=finished.front();
      if(has(value,"stats") && has(at(value,"stats"),"native_head_timestamps"))
        value.map().at("stats").map().at("native_head_timestamps").map()["completed_dequeued_ns"]=stamp();
      out.push_back(std::move(value));
      finished.pop_front();
    }
    return out;
  }
  V metrics() const {
    M snapshot;
    {
      // Only registry counters require mu. Network metrics can wait for a
      // reactor resize; doing that under mu stalls submit, receive and merge
      // together, allowing monitoring to amplify a transient ingress backlog.
      std::lock_guard<std::mutex> l(mu);
      A maintenance;
      for (size_t i = 0; i < maintenance_count; ++i)
        maintenance.emplace_back(M{{"node_id", per_node_maintenance ? V(uint64_t(i)) : V()},
                                 {"tracked_queries", uint64_t(live[i].size())},
                                 {"retained_queries", uint64_t(expiry[i].size())},
                                 {"ticks", maintenance_ticks[i]}, {"query_visits", maintenance_visits[i]},
                                 {"lease_attempts", lease_attempts[i]}});
      snapshot = M{{"active", uint64_t(active)},
             {"successful_queries", successful_queries},
             {"failed_queries", failed_queries},
             {"recent_failures", A(recent_failures.begin(), recent_failures.end())},
             {"maintenance_mode", per_node_maintenance ? "per_node" : "single"},
             {"maintenance_threads", uint64_t(maintenance_count)},
             {"maintenance_lanes", std::move(maintenance)},
             {"completed_pending", uint64_t(finished.size())},
             {"profile_head_enabled", profile_head},
             {"maintenance_registry_hold_ns", maintenance_registry_hold_ns},
             {"maintenance_registry_hold_max_ns", maintenance_registry_hold_max_ns},
             {"retained_identities", uint64_t(queries.size())},
             {"registry_buckets",uint64_t(queries.bucket_count())},{"registry_rehashes",registry_rehashes},
             {"registry_rehash_ns",registry_rehash_ns},{"registry_rehash_max_ns",registry_rehash_max_ns}};
    }
    // Resource counts are individual snapshots, not one atomic system state.
    snapshot["routes"] = routes->metrics();
    snapshot["merges"] = merges->metrics();
    snapshot["receiver"] = server->metrics();
    snapshot["network_entry"] = entries->metrics();
    snapshot["network_lease"] = leases->metrics();
    snapshot["entry_pending"] = uint64_t(entries->pending());
    snapshot["replica_selected"]=replica_selected.load();
    snapshot["replica_routing_funnel"]=M{{"queries",replica_lookup.load()},{"directory_match",replica_match.load()},
      {"stale_or_source_mismatch",replica_stale.load()},{"score_rejected",replica_score_reject.load()},
      {"probability_rejected",replica_probability_reject.load()},{"selected",replica_selected.load()}};
    {std::lock_guard<std::mutex> lock(observation_mu);snapshot["replica_access_counts"]=replica_access_counts;}
    snapshot["replica_observation_dropped"]=observation_dropped.load();
    return snapshot;
  }
  void replica_routes(V rows) {
    auto next=std::make_shared<ReplicaRoutes>();
    auto previous=std::atomic_load(&replica_directory);
    for(const auto& row:array(rows)) {
      auto source=get<size_t>(row,"source"),target=get<size_t>(row,"target");
      auto fraction=get<double>(row,"fraction");auto version=get<std::string>(row,"version");
      double lower=get<double>(row,"min_score"),upper=get<double>(row,"max_score");
      auto ttl=get<uint64_t>(row,"ttl_ms");
      check(source<node_count&&target<node_count&&source!=target&&std::isfinite(fraction)&&fraction>=0&&fraction<=1&&
        !version.empty()&&version.size()<=128&&std::isfinite(lower)&&std::isfinite(upper)&&lower<=upper&&ttl>0&&ttl<=60000,"Invalid replica route");
      auto family=optional<std::string>(row,"family","");check(family.size()<=128,"Invalid replica family");
      ReplicaRoute r{source,target,version,family,fraction,lower,upper,stamp()+ttl*1000000};
      for(const auto& id:array(at(row,"entries"))){auto weighted=r;
        if(covered_entry_admission){
          auto old=previous->find(cast<uint64_t>(id));
          if(old!=previous->end())for(const auto& choice:old->second)
            if(choice.version==version&&choice.source==source&&choice.target==target)weighted.returns=choice.returns;
          if(!weighted.returns)weighted.returns=std::make_shared<EntryReturnCounters>(entry_counter_stripes);
        }
        if(has(row,"entry_weights")){const auto& weights=at(row,"entry_weights").map();auto i=weights.find(std::to_string(cast<uint64_t>(id)));
          check(i!=weights.end(),"Missing entry weight");double weight=number(i->second);check(std::isfinite(weight)&&weight>=0&&weight<=1,"Invalid entry weight");weighted.fraction*=weight;}
        auto& choices=(*next)[cast<uint64_t>(id)];double total=weighted.fraction;
        if(!choices.empty())check(optional<bool>(at(config,"peer"),"region_tasks",false)&&
          !optional<bool>(at(config,"load_balance"),"peer_routes_enabled",false),"Multiple replica routes require root-only region tasks");
        for(const auto& old:choices){
          if(covered_entry_admission && old.family!=family && !old.family.empty() && !family.empty())continue;
          check(!family.empty()&&old.family==family&&old.source==source&&old.target!=target&&old.version!=version&&
                old.min_score==lower&&old.max_score==upper,"Ambiguous replica entry");
          total+=old.fraction;
        }
        check(total<=1.+1e-12,"Replica entry fractions exceed one");choices.push_back(std::move(weighted));
      }
    }
    std::atomic_store(&replica_directory,std::shared_ptr<const ReplicaRoutes>(next));
  }
  V replica_observations(){A out;std::lock_guard<std::mutex> lock(observation_mu);out.swap(observations);return out;}
  V replica_entry_statistics(){
    A out;auto directory=std::atomic_load(&replica_directory);
    for(const auto& [entry,choices]:*directory)for(const auto& r:choices)if(r.returns){
      auto value=r.returns->snapshot();auto& m=value.map();
      if(get<uint64_t>(value,"completed")==0)continue;
      m["entry"]=entry;m["version"]=r.version;m["initial_budget"]=uint64_t(budget);
      out.push_back(std::move(value));
    }
    return out;
  }
  void close() {
    std::lock_guard<std::mutex> closing(close_mu);
    if (closed)
      return;
    std::vector<std::shared_ptr<Query>> selected;
    {
      std::lock_guard<std::mutex> l(mu);
      stopping = true;
      for (const auto &[id, q] : queries)
        selected.push_back(q);
    }
    wake.notify_all();
    for (auto &timer : timers)
      if (timer.joinable()) timer.join();
    routes->close();
    entries->close();
    for (auto q : selected) {
      std::lock_guard<std::mutex> l(q->mu);
      if (!q->finished && !q->merging) {
        q->error = "Native head closed";
        maybe_merge(q);
      }
    }
    server->close();
    merges->close();
    leases->close();
    {
      std::lock_guard<std::mutex> l(mu);
      closed = true;
    }
    done_cv.notify_all();
  }
};
HeadRuntime::HeadRuntime(V c, V m, RouteFunction r)
    : impl_(std::make_unique<Impl>(std::move(c), std::move(m), std::move(r))) {}
HeadRuntime::~HeadRuntime() { close(); }
std::string HeadRuntime::submit(std::vector<double> q, V id, std::string request_id, Completion completion, V validation) {
  return impl_->submit(std::move(q), std::move(id), std::move(request_id), std::move(completion),std::move(validation));
}
V HeadRuntime::completed(size_t n, uint64_t ms) {
  return impl_->completed(n, ms);
}
V HeadRuntime::metrics() const { return impl_->metrics(); }
void HeadRuntime::replica_routes(V routes){impl_->replica_routes(std::move(routes));}
V HeadRuntime::replica_observations(){return impl_->replica_observations();}
V HeadRuntime::replica_entry_statistics(){return impl_->replica_entry_statistics();}
void HeadRuntime::close() { impl_->close(); }
} // namespace dhruntime
