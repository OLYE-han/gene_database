#include "native_network.hpp"
#include "native_runtime.hpp"
#include "native_pool.hpp"
#include "pcg64_compat.hpp"
#include "logical_region.hpp"
#include "replica_pair_scope.hpp"
#include "search_resume.hpp"
#include "pull_batch.hpp"
#include "decayed_hotness.hpp"
#include <algorithm>
#include <atomic>
#include <array>
#include <chrono>
#include <cstring>
#include <deque>
#include <future>
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
static double thread_cpu_ms(){timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return t.tv_sec*1000.+t.tv_nsec/1e6;}
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
  struct RegionActor;
  struct QueryContext;
  struct SyncState {
    struct Outgoing {std::deque<V> rows;V metadata;bool busy=false;};
    std::map<size_t,Outgoing> outgoing;
    std::mutex mu;size_t phase=0,pending=0,remaining=0;uint64_t version=1,sequence=0,frozen_epoch=0;std::set<int> exhausted_targets;
    M delivery_metrics;uint64_t search_done_ns=0;size_t starts_pending=0;V::Array settlements;bool waiting_starts=false,delivery_required=false,delivery_notified=false;
    V idle,reply;std::map<std::string,V> receipts;std::unordered_set<uint64_t> missed;
    std::function<void(V)> resume;
  };
  struct Record {
    std::shared_ptr<SyncState> sync;bool candidate_park=false,candidate_resume=false;V candidate_request,candidate_reply;
    uint64_t candidate_ready=0,candidate_park_at=0;
    V state;
    std::shared_ptr<void> search_context;
    std::string digest;
    std::shared_ptr<dhsearch::Control> control;
    std::shared_ptr<dhcontrol::Ticket> ticket;
    uint64_t accepted = 0, search_ready = 0, executor_ready = 0;
    bool acked = false;
    std::atomic<int> status{0}; // queued / computing / locally finished / pull suspended
    bool tail_admitted=false;
    std::function<void(V)> continuation;
    std::vector<uint64_t> pull_ids;
    dhonesided::CodeLookup pull_code_lookup;
    std::string pull_mode="full";
    std::function<void(dhgrowth::Read,std::function<void(V)>)> demand_start;
    uint64_t pull_auth_begin=0,pull_auth_ready=0;
    dhwire::Result pull_result;
    std::shared_ptr<const V> pull_common;
    size_t pull_group_size=1;
    uint64_t pull_queued=0,pull_sent=0,pull_ready=0;
    size_t pull_source=0;
    std::shared_ptr<RegionActor> actor;
    std::shared_ptr<QueryContext> context;
    mutable std::mutex trace_mu;
    M trace;
    void mark(const std::string &name, uint64_t value) {
      std::lock_guard<std::mutex> l(trace_mu);
      if (!trace.empty())
        trace[name] = value;
    }
  };
  struct RegionActor {
    std::string id,region;
    std::mutex mu;
    std::deque<std::shared_ptr<Record>> grants;
    std::shared_ptr<dhsearch::RegionInbox> inbox;
    bool scheduled=false;
    std::atomic<bool> source_reserved{false};
  };
  struct QueryContext {
    std::string token;
    double expires=0;
    bool admission_counted=false; // guarded by the node registry mutex
    std::mutex admission_mu;
    std::shared_ptr<dhsearch::ExpansionClaims> claims;
    std::shared_ptr<dhsearch::RegionVisits> visits=std::make_shared<dhsearch::RegionVisits>();
    std::unordered_map<std::string,std::shared_ptr<RegionActor>> actors;
    // candidate_sync owns one physical branch even when its logical identity
    // is a replica. That branch searches both its pinned overlay and local base.
    std::shared_ptr<RegionActor> canonical_actor;
    struct Claim {std::string owner,id;std::shared_ptr<dhsearch::RegionInbox> inbox;std::map<std::string,V> receipts;};
    std::map<size_t,Claim> starts;
  };
  using ContextMap = std::unordered_map<std::string,std::shared_ptr<QueryContext>>;
  struct RetiredBatch {
    std::vector<std::shared_ptr<Record>> records;
    std::vector<ContextMap::node_type> contexts;
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
  RecordRead read_records;
  std::unique_ptr<dhonesided::Source> one_source;
  std::unique_ptr<dhonesided::Client> one_client;
  std::shared_ptr<dhgrowth::ReadGate> demand_gate=std::make_shared<dhgrowth::ReadGate>();
  bool shared_compute=false;
  std::string one_source_error;
  std::atomic<uint64_t> one_control_rpcs{0},one_control_bytes{0};
  std::atomic<uint64_t> cpu_ns{0}, work_ns{0}, completed_work{0};
  std::atomic<uint64_t> cleanup_contexts{0}, cleanup_lock_max_ns{0}, cleanup_destroy_max_ns{0};
  size_t cleanup_max_contexts=256;
  size_t cleanup_max_records=256, reclaim_capacity=8;
  std::unique_ptr<Pool> reclaim_workers;
  std::deque<std::string> expired_records;
  std::atomic<uint64_t> reclaim_pending{0}, cleanup_records{0}, expiry_pending{0};
  std::atomic<uint64_t> reclaim_destroy_ns{0};
  std::array<std::atomic<uint64_t>,64> wait_hist{}, ready_wait_hist{}, executor_wait_hist{}, actor_wait_hist{};
  std::atomic<uint64_t> ready_clock_fallbacks{0};
  std::unique_ptr<Pool> workers,tail_workers,read_workers,candidate_workers,start_workers;
  std::atomic<uint64_t> deferred_admissions{0},start_queue_ns{0},start_cpu_ns{0},start_wall_ns{0},start_completed{0};
  std::unique_ptr<dhnet::Client> pull_client;
  bool async_pull=false;
  bool coalesce=false,coalesce_stop=false,threaded_pull_dispatch=false;
  std::vector<std::deque<std::shared_ptr<Record>>> pull_sources;
  size_t pull_dispatch_cursor=0;
  std::atomic<uint64_t> pull_lock_calls{0},pull_lock_wait_ns{0},pull_lock_hold_ns{0},pull_scan_steps{0},pull_pump_calls{0};
  std::atomic<uint64_t> pull_callback_cpu_ns{0},pull_callback_wall_ns{0},pull_submit_cpu_ns{0};
  size_t coalesce_queries=4,coalesce_bytes=2097152,coalesce_wait_us=0,coalesce_reserved=0,coalesce_capacity=0;
  std::condition_variable pull_wake;
  std::thread pull_dispatcher;
  std::atomic<uint64_t> batch_rpcs{0},batch_queries{0},batch_multi{0},batch_tx{0},batch_rx{0};
  std::atomic<uint64_t> batch_unique_records{0},batch_logical_records{0},batch_unique_bytes{0},batch_logical_bytes{0};
  std::atomic<uint64_t> batch_union_failures{0},batch_source_cpu_ns{0},batch_gather_cpu_ns{0};

  std::vector<int> tail_cpus;
  bool pin_tail=false;
  size_t tail_capacity=64,pull_limit=16,tail_active=0,pull_peak=0;
  mutable std::mutex pull_mu;
  std::condition_variable pull_idle;
  bool pull_closing=false;
  std::deque<std::shared_ptr<Record>> pull_pending;
  std::vector<size_t> pull_inflight;
  struct PullLock {
    Impl& owner;std::unique_lock<std::mutex> guard;uint64_t entered;
    PullLock(Impl& p):owner(p),guard(p.pull_mu,std::defer_lock){auto begin=stamp();guard.lock();entered=stamp();owner.pull_lock_wait_ns+=entered-begin;++owner.pull_lock_calls;}
    ~PullLock(){owner.pull_lock_hold_ns+=stamp()-entered;}
  };
  std::atomic<uint64_t> tail_rejected{0},tail_resumes{0},read_rejected{0};
  std::atomic<uint64_t> tail_cpu_ns{0},tail_wall_ns{0};
  size_t help_limit=0,help_reserve=0,help_queue_low=0;
  std::atomic<uint64_t> help_active{0},help_peak{0},help_resumes{0},help_declined{0};
  std::atomic<uint64_t> help_cpu_ns{0},help_wall_ns{0};
  std::unique_ptr<dhcontrol::Controller> controller;
  std::unique_ptr<dhnet::Client> client;
  std::unique_ptr<dhnet::Client> visit_client;
  std::vector<dhwire::Endpoint> endpoints;
  mutable std::mutex mu;
  std::mutex close_mu;
  std::condition_variable wake;
  std::unordered_map<std::string, std::shared_ptr<Record>> records;
  std::unordered_map<std::string, std::set<std::string>> query_records;
  std::unordered_map<std::string,std::weak_ptr<dhsearch::ExpansionClaims>> query_claims;
  ContextMap query_contexts;
  std::deque<std::pair<std::string,double>> context_expiry;
  bool region_tasks=false,concurrent_region_admission=false,candidate_sync=false;
  std::atomic<uint64_t> sync_sent{0},sync_received{0},sync_exhausted{0},sync_reactivated{0},sync_claims{0},sync_claim_conflicts{0};
  std::atomic<uint64_t> region_created{0},region_merged{0},region_reactivated{0},region_reserved_skips{0};
  std::atomic<uint64_t> region_pending_grants{0};
  std::atomic<uint64_t> visit_rpcs{0},visit_errors{0},visit_tx_bytes{0},visit_rx_bytes{0};
  static std::string branch_key(const V& s) {
    return get<std::string>(s,"query_request_id")+"\n"+get<std::string>(s,"branch_id");
  }
  std::unordered_map<std::string, std::pair<std::string, double>> tombstones;
  std::deque<std::pair<std::string, double>> tomb_order;
  size_t active = 0, admitting = 0, reserved_admissions = 0, rejected = 0;
  uint64_t registry_rehashes=0,registry_rehash_ns=0,registry_rehash_max_ns=0;
  uint64_t accepted_branches=0,accepted_queries=0,entry_admissions=0,peer_admissions=0;
  bool stopping = false, closed = false;
  std::thread maintenance;

  Impl(V c, V m, std::string sig, SearchFunction fn, SearchPrepare prep, RecordRead rr,dhonesided::Export export_image)
      : config(std::move(c)), meta(std::move(m)), signature(std::move(sig)),
        search(std::move(fn)), prepare(std::move(prep)), read_records(std::move(rr)) {
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
    region_tasks=optional<bool>(p,"region_tasks",false);
    concurrent_region_admission=optional<bool>(p,"concurrent_region_admission",false);
    candidate_sync=has(p,"candidate_sync")&&get<bool>(at(p,"candidate_sync"),"enabled");
    for (const auto &n : array(at(at(config, "cluster"), "nodes")))
      endpoints.emplace_back(get<std::string>(n, "host"),
                             get<uint16_t>(n, "port"));
    dhwire::Config wc;
    const auto &nc = at(config, "native_runtime");
    const auto reserve=std::min(optional<size_t>(nc,"registry_reserve_entries",0),max_retained);
    if(reserve){records.reserve(reserve);query_records.reserve(reserve);query_contexts.reserve(reserve);query_claims.reserve(reserve);tombstones.reserve(std::min(reserve,max_tombstones));}
    cleanup_max_contexts=optional<size_t>(nc,"lifecycle_cleanup_max_contexts",256);
    check(cleanup_max_contexts>0&&cleanup_max_contexts<=65536,"Invalid lifecycle cleanup batch");
    cleanup_max_records=optional<size_t>(nc,"lifecycle_cleanup_max_records",256);
    reclaim_capacity=optional<size_t>(nc,"lifecycle_reclaim_batches",8);
    const auto reclaim_threads=optional<size_t>(nc,"lifecycle_reclaim_threads",2);
    check(cleanup_max_records>0&&cleanup_max_records<=65536&&reclaim_capacity>0&&reclaim_capacity<=1024&&reclaim_threads>0&&reclaim_threads<=16,"Invalid lifecycle reclaimer limits");
    if(optional<bool>(nc,"lifecycle_async_reclaim",false))
      reclaim_workers=std::make_unique<Pool>(reclaim_threads,reclaim_capacity,
          optional<std::vector<int>>(at(nc,"network_module"),"cpu_ids",{}),false);
    wc.max_pending = get<size_t>(nc, "network_queue_capacity");
    wc.max_connections_per_endpoint =
        get<size_t>(nc, "connections_per_endpoint");
    wc.max_message_bytes = get<size_t>(net, "max_message_bytes");
    wc.timeout_ms = uint64_t(get<double>(p, "handoff_timeout_seconds") * 1000);
    wc.max_endpoints = std::max(size_t(1), endpoints.size());
    controller = std::make_unique<dhcontrol::Controller>(config);
    client = std::make_unique<dhnet::Client>(wc,config);
    if(candidate_sync){const auto& sc=at(p,"candidate_sync");auto count=optional<size_t>(sc,"completion_threads",0);
      if(count)candidate_workers=std::make_unique<Pool>(count,optional<size_t>(sc,"completion_capacity",8192),optional<std::vector<int>>(at(nc,"network_module"),"cpu_ids",{}),false);
      const auto starts=optional<size_t>(sc,"start_threads",0);
      if(starts)start_workers=std::make_unique<Pool>(starts,optional<size_t>(sc,"start_capacity",8192),optional<std::vector<int>>(at(nc,"network_module"),"cpu_ids",{}),false);
    }
    if(region_tasks&&optional<bool>(p,"region_visit_broadcast",true)){
      auto advisory=wc;advisory.max_pending=optional<size_t>(p,"region_visit_queue_capacity",512);
      V advisory_config=config;
      advisory_config.map().at("native_runtime").map().at("network_module").map()["send_min_reactors"]=uint64_t(1);
      visit_client=std::make_unique<dhnet::Client>(advisory,advisory_config);
    }
    const size_t main_count=optional<size_t>(nc,"compute_threads",0)?get<size_t>(nc,"compute_threads"):get<size_t>(p,"worker_threads");
    workers = std::make_unique<Pool>(main_count,
                                     get<size_t>(nc, "compute_queue_capacity"),
                                     optional<std::vector<int>>(nc,"compute_cpu_ids",{}),true,
                                     []{dhsearch::DecayedHotness::flush_current_thread();});
    if(has(p,"replica_pull")) {
      const auto& pull=at(p,"replica_pull");
      if(get<bool>(pull,"enabled")&&has(pull,"async")) {
        const auto& a=at(pull,"async");
        auto selected=[&](const char* key) {
          const auto nodes=optional<std::vector<size_t>>(a,key,{});
          return nodes.empty()||std::find(nodes.begin(),nodes.end(),sid)!=nodes.end();
        };
        async_pull=get<bool>(a,"enabled")&&selected("target_nodes");
        threaded_pull_dispatch=optional<bool>(a,"dedicated_dispatcher",false);
        if(has(a,"coalesce")) {
          const auto& b=at(a,"coalesce");coalesce=get<bool>(b,"enabled");
          coalesce_queries=get<size_t>(b,"max_queries");coalesce_bytes=get<size_t>(b,"max_bytes");coalesce_wait_us=get<size_t>(b,"wait_us");
        }
        coalesce_capacity=get<size_t>(a,"response_bytes");
        if(coalesce)check(get<bool>(a,"enabled")&&get<size_t>(a,"source_read_threads")&&coalesce_queries>=1&&coalesce_queries<=32&&
          coalesce_wait_us<=10000&&coalesce_bytes>=get<size_t>(pull,"batch_bytes")&&coalesce_bytes<=coalesce_capacity&&
          coalesce_bytes+262144<=get<size_t>(at(config,"network"),"max_message_bytes")&&
          coalesce_queries*get<size_t>(pull,"batch_records")<=1024,"Invalid coalesced pull bounds");
        tail_capacity=get<size_t>(a,"max_contexts");pull_limit=get<size_t>(a,"max_inflight_per_source");
        check(tail_capacity&&pull_limit&&get<size_t>(a,"response_bytes")/get<size_t>(pull,"batch_bytes")>=tail_capacity,"Invalid async response capacity");
        pull_inflight.resize(endpoints.size());pull_sources.resize(endpoints.size());
        if(async_pull) {
          check(region_tasks&&concurrent_region_admission,"Async pull requires concurrent regions");
          help_limit=optional<size_t>(a,"main_help_max_tasks",0);
          help_reserve=optional<size_t>(a,"main_help_reserve_workers",0);
          help_queue_low=optional<size_t>(a,"main_help_queue_low",0);
          check(!help_limit||(help_limit<=main_count&&help_reserve<main_count),"Invalid main help capacity");
          tail_cpus=optional<std::vector<int>>(nc,"tail_cpu_ids",{});
          const bool explicit_cpus=!tail_cpus.empty();
          if(!explicit_cpus)tail_cpus=optional<std::vector<int>>(nc,"compute_cpu_ids",{});
          const auto n=get<size_t>(a,"tail_threads");
          const bool share_requested=has(a,"shared_compute")&&get<bool>(at(a,"shared_compute"),"enabled");
          check(n>0||share_requested,"Async pull requires tail workers or shared compute");
          pin_tail=optional<bool>(a,"pin_tail_threads",true);
          // A shared affinity mask lets a bounded tail pool use idle allowed
          // CPUs instead of competing with the first N pinned main workers.
          if(!explicit_cpus&&pin_tail&&tail_cpus.size()>=main_count+n)
            tail_cpus.erase(tail_cpus.begin(),tail_cpus.begin()+main_count);
          shared_compute=has(a,"shared_compute")&&get<bool>(at(a,"shared_compute"),"enabled");
          if(shared_compute){const auto& share=at(a,"shared_compute");help_limit=0;
            workers->share_tail(n,tail_capacity,tail_cpus,pin_tail,get<double>(share,"tail_fraction"),get<double>(share,"credit_limit_ms"));}
          else tail_workers=std::make_unique<Pool>(n,tail_capacity,tail_cpus,pin_tail,[]{dhsearch::DecayedHotness::flush_current_thread();});
          V netconfig=config;auto& mod=netconfig.map().at("native_runtime").map().at("network_module").map();
          mod["send_min_reactors"]=mod["send_max_reactors"]=at(a,"send_reactors");
          mod["connections_per_endpoint_per_reactor"]=at(a,"connections_per_reactor");
          mod["min_connections_per_endpoint_per_reactor"]=uint64_t(0);
          mod["send_queue_capacity"]=uint64_t(tail_capacity);
          auto pc=wc;pc.max_pending=tail_capacity;pc.max_connections_per_endpoint=get<size_t>(a,"connections_per_reactor");
          pull_client=std::make_unique<dhnet::Client>(pc,netconfig);
        }
        if(selected("source_nodes")&&get<size_t>(a,"source_read_threads"))read_workers=std::make_unique<Pool>(get<size_t>(a,"source_read_threads"),get<size_t>(a,"source_read_queue"),optional<std::vector<int>>(nc,"compute_cpu_ids",{}),false);
        if(has(pull,"one_sided")&&get<bool>(at(pull,"one_sided"),"enabled")) {
          const auto& one=at(pull,"one_sided");
          check(get<bool>(a,"enabled")&&!coalesce&&bool(export_image),"One-sided requires async, export and coalesce disabled");
          if(selected("source_nodes"))try{one_source=std::make_unique<dhonesided::Source>(one,export_image(one));}
            catch(const std::exception& e){one_source_error=e.what();if(!get<bool>(one,"fallback_to_rpc"))throw;}
          if(async_pull)one_client=std::make_unique<dhonesided::Client>(one,get<size_t>(pull,"batch_bytes"),tail_capacity,
            [this](size_t source,V message,dhonesided::Complete done){return pull_client->submit(endpoints.at(source),message,[this,done=std::move(done)](dhwire::Result out)mutable{
              ++one_control_rpcs;one_control_bytes+=out.request_bytes+out.response_bytes;done(std::move(out));});});
        }
      }
    }
    demand_gate->submit=[this](size_t source,const V& auth,const std::vector<uint64_t>& ids,dhonesided::Complete done,dhonesided::CodeLookup lookup){
      return one_client&&one_client->submit(source,auth,ids,std::move(done),std::move(lookup));
    };
    if(async_pull&&coalesce)pull_dispatcher=std::thread([this]{dispatch_pull_batches();});
    else if(async_pull&&threaded_pull_dispatch)pull_dispatcher=std::thread([this]{dispatch_single_pulls();});
    maintenance = std::thread([this] { maintain(); });
  }
  ~Impl() { close(); }
  void update() {
    controller->poll_reports();
    auto updates = controller->take_updates();
    auto retired=std::make_shared<RetiredBatch>();
    // Removing identity is synchronized; destroying the potentially large
    // candidate inbox (including allocator page purging) must not block
    // admission or leases for every query on this node. Extract the map node
    // as well, so neither its value nor its allocation is freed under mu.
    const bool can_retire=!reclaim_workers||reclaim_pending.load()<reclaim_capacity;
    uint64_t lock_started=0, unlocked=0;
    {
      std::lock_guard<std::mutex> l(mu);
      lock_started=stamp();
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
          // ACK accounting is always consumed, even if the destructor queue
          // is full. Expiry identities wait in the bounded retained registry.
          expired_records.push_back(std::move(q));
        }
      }
      for(size_t n=0;can_retire&&n<cleanup_max_records&&!expired_records.empty();++n){
          auto q=std::move(expired_records.front());expired_records.pop_front();
          auto it=records.find(q);if(it==records.end())continue;
          if (!it->second->acked)
            --active;
          tombstones[q] = {get<std::string>(it->second->state, "branch_id"), now};
          tomb_order.emplace_back(q, now);
          auto qi=query_records.find(get<std::string>(it->second->state,"query_request_id"));
          if(qi!=query_records.end()){qi->second.erase(q);if(qi->second.empty()){query_claims.erase(qi->first);query_records.erase(qi);}}
          retired->records.push_back(std::move(it->second));
          records.erase(it);
      }
      for (size_t n=0;n<cleanup_max_records&&!tomb_order.empty() &&
             (tombstones.size() > max_tombstones ||
              now - tomb_order.front().second > retention);++n) {
        auto [q, t] = tomb_order.front();
        tomb_order.pop_front();
        auto it = tombstones.find(q);
        if (it != tombstones.end() && it->second.second == t)
          tombstones.erase(it);
      }
      // Bound destruction per maintenance iteration too: moving it outside
      // mu alone must not starve ACK/expiry bookkeeping for seconds.
      for(size_t checked=0;can_retire&&checked<cleanup_max_contexts&&!context_expiry.empty()&&context_expiry.front().second<=now;++checked){
        auto [id,expires]=context_expiry.front();context_expiry.pop_front();
        auto i=query_contexts.find(id);
        if(i!=query_contexts.end()&&i->second->expires==expires){
          if(!query_records.count(id)&&i->second.use_count()==1)
            retired->contexts.push_back(query_contexts.extract(i));
          else{i->second->expires=now+retention;context_expiry.emplace_back(id,i->second->expires);}
        }
      }
      unlocked=stamp();
      expiry_pending.store(expired_records.size());
    } // Destruction of state/report buffers AND query contexts is outside mu.
    auto maximum=[](std::atomic<uint64_t>& value,uint64_t sample){
      auto old=value.load(std::memory_order_relaxed);
      while(old<sample&&!value.compare_exchange_weak(old,sample,std::memory_order_relaxed)){}
    };
    maximum(cleanup_lock_max_ns,unlocked-lock_started);
    if(retired->records.empty()&&retired->contexts.empty())return;
    cleanup_contexts.fetch_add(retired->contexts.size(),std::memory_order_relaxed);
    cleanup_records.fetch_add(retired->records.size(),std::memory_order_relaxed);
    auto destroy=[this,retired,maximum]{
      const auto started=stamp();for(auto& r:retired->records)cancel_candidate(r);retired->records.clear();retired->contexts.clear();
      const auto elapsed=stamp()-started;reclaim_destroy_ns.fetch_add(elapsed);
      maximum(cleanup_destroy_max_ns,elapsed);
      if(reclaim_workers)reclaim_pending.fetch_sub(1);
    };
    if(reclaim_workers){
      reclaim_pending.fetch_add(1);
      // One producer, and pending includes active jobs as well as queued jobs.
      // Maintenance joins before this pool closes, so submission has a slot.
      if(!reclaim_workers->submit(destroy))destroy();
    }else destroy();
  }
  // Caller holds mu. All task ownership changes are linearized here; search
  // never holds mu. Notifications only update advisory query-scoped snapshots.
  std::shared_ptr<QueryContext> context(const V& state) {
    auto id=get<std::string>(state,"query_request_id"),token=get<std::string>(state,"token");
    check(!id.empty()&&id.size()<=256&&id.find('\n')==std::string::npos&&!token.empty()&&token.size()<=256,"Invalid region query identity");
    auto found=query_contexts.find(id);
    if(found!=query_contexts.end()){check(found->second->token==token,"Conflicting query region token");return found->second;}
    check(query_contexts.size()<max_retained,"Query region directory capacity exceeded");
    auto c=std::make_shared<QueryContext>();c->token=token;c->expires=seconds()+timeout+retention;
    const auto buckets=query_contexts.bucket_count(),begin=stamp();
    query_contexts.emplace(id,c);record_registry_growth(begin,query_contexts.bucket_count()!=buckets);
    context_expiry.emplace_back(id,c->expires);return c;
  }
  // Node registry mutex held. Count only mutations that actually grow a hash
  // table; the elapsed interval includes its insertion and rehash work.
  void record_registry_growth(uint64_t begin,bool changed) {
    if(changed){const auto ns=stamp()-begin;++registry_rehashes;registry_rehash_ns+=ns;registry_rehash_max_ns=std::max(registry_rehash_max_ns,ns);}
  }
  // Called only on first branch commit, while mu is held. Broadcast-only
  // contexts do not count; retries do not count. Region contexts survive all
  // branches of a query until its configured lifetime expires.
  void count_admission(const std::shared_ptr<Record>& r) {
    ++accepted_branches;
    if(null(at(r->state,"parent_branch_id")))++entry_admissions;else ++peer_admissions;
    if(r->context){if(!r->context->admission_counted){r->context->admission_counted=true;++accepted_queries;}}
    else if(query_records.at(get<std::string>(r->state,"query_request_id")).size()==1)++accepted_queries;
  }
  V admission_metrics() const {
    auto pool=start_workers?start_workers->metrics():V();
    std::lock_guard<std::mutex> lock(mu);
    return M{{"accepted_branches",accepted_branches},{"unique_queries",accepted_queries},
      {"entry_branches",entry_admissions},{"peer_branches",peer_admissions},
      {"capacity_rejections",uint64_t(rejected)},{"inflight",uint64_t(admitting)},{"deferred_admissions",deferred_admissions.load()},
      {"unacknowledged",uint64_t(active)}, {"start_workers",std::move(pool)},
      {"start_completed",start_completed.load()},{"start_queue_ns",start_queue_ns.load()},
      {"start_cpu_ns",start_cpu_ns.load()},{"start_wall_ns",start_wall_ns.load()},
      {"registry_rehashes",registry_rehashes},{"registry_rehash_ns",registry_rehash_ns},{"registry_rehash_max_ns",registry_rehash_max_ns},
      {"registry_record_buckets",uint64_t(records.bucket_count())},{"registry_context_buckets",uint64_t(query_contexts.bucket_count())}};
  }
  std::pair<V,std::shared_ptr<Record>> admit_concurrent(
      const std::shared_ptr<Record>& r,M md,uint64_t requested) {
    const bool candidate_sync=dhpair::active(at(config,"peer"),r->state);
    const auto key=branch_key(r->state),qid=get<std::string>(r->state,"query_request_id");
    auto duplicate=[&] {
      auto i=records.find(key);
      if(i==records.end())return false;
      check(i->second->digest==r->digest,"Conflicting branch retry");return true;
    };
    {
      const auto begin=stamp();std::lock_guard<std::mutex> lock(mu);
      r->mark("accept_registry_reserve_wait_ns",stamp()-begin);
      check(!stopping,"Peer runtime closed");
      if(duplicate())return {M{{"accepted",true},{"duplicate",true}},nullptr};
      check(!tombstones.count(key),"Expired branch cannot be replayed");
      r->context=context(r->state);++admitting;
    }
    // Capacity remains reserved while admission runs without the node-wide
    // mutex. Duplicate retries serialize only against this query.
    struct Reservation {
      Impl* self;bool credit=false;
      ~Reservation(){{std::lock_guard<std::mutex> lock(self->mu);--self->admitting;if(credit)--self->reserved_admissions;}self->wake.notify_all();}
    } reservation{this};
    bool committed=false,reserved_skip=false;
    try {
      const auto wait=stamp();std::unique_lock<std::mutex> query_lock(r->context->admission_mu);
      r->mark("accept_state_lock_enter_ns",wait);
      r->mark("accept_state_lock_acquired_ns",stamp());
      r->mark("accept_reservation_phase_ns",wait-requested);
      if(candidate_sync&&optional<bool>(r->state,"candidate_nonblocking_start",false)){
        auto& c=*r->context;const auto bid=get<std::string>(r->state,"branch_id");
        auto a=c.canonical_actor;auto st=c.starts.find(sid);
        if((a&&a->id!=bid)||(st!=c.starts.end()&&!st->second.id.empty()&&st->second.id!=bid))
          return {merge_start_candidates(r),nullptr};
        if(st==c.starts.end()) {QueryContext::Claim x;x.inbox=std::make_shared<dhsearch::RegionInbox>(get<size_t>(at(config,"search"),"frontier_size"));st=c.starts.emplace(sid,std::move(x)).first;}
        st->second.id=bid;st->second.owner=get<std::string>(r->state,"dispatch_decision_id");c.visits->announce(dhpair::region(at(config,"peer"),r->state,sid));
      }
      {
        std::lock_guard<std::mutex> lock(mu);
        check(!stopping,"Peer runtime closed");
        if(duplicate())return {M{{"accepted",true},{"duplicate",true}},nullptr};
        check(!tombstones.count(key),"Expired branch cannot be replayed");
        if(active+reserved_admissions>=max_active||records.size()+reserved_admissions>=max_retained){
          ++rejected;throw std::runtime_error("Peer branch capacity exceeded");
        }
        ++reserved_admissions;reservation.credit=true;
      }
      auto& c=*r->context;
      if(has(r->state,"region_directory"))c.visits->merge(at(r->state,"region_directory"),endpoints.size());
      const auto region=dhregion::current(r->state);
      const auto actor_key=candidate_sync&&dhpair::scoped(at(config,"peer"))?dhpair::region(at(config,"peer"),r->state,sid):region;
      auto ai=c.actors.find(actor_key);const bool merged=ai!=c.actors.end();
      if(candidate_sync){
        check(!merged,"Unique branch already exists; use candidate sync");
        check(!c.canonical_actor,"Physical branch already exists; use candidate sync");
        auto claim=c.starts.find(sid);
        if(!null(at(r->state,"parent_branch_id")))check(claim!=c.starts.end()&&claim->second.id==get<std::string>(r->state,"branch_id"),"Unreserved unique branch");
        r->sync=std::make_shared<SyncState>();r->sync->remaining=get<size_t>(r->state,"work_grant");
        std::weak_ptr<Record> weak=r;
        r->control->candidate_pending_starts=[weak]{if(auto r=weak.lock()){std::lock_guard<std::mutex> lock(r->sync->mu);return r->sync->starts_pending+r->sync->settlements.size();}return size_t(0);};
        if(optional<bool>(at(at(config,"peer"),"candidate_sync"),"nonblocking_start",false))
          r->control->candidate_settlements=[weak]{if(auto r=weak.lock()){std::lock_guard<std::mutex> lock(r->sync->mu);A rows;rows.swap(r->sync->settlements);return V(M{{"pending",uint64_t(r->sync->starts_pending)},{"rows",std::move(rows)}});}return V(M{{"pending",uint64_t(0)},{"rows",A{}}});};
        if(optional<bool>(at(at(config,"peer"),"candidate_sync"),"early_result",false))
          r->control->candidate_finish=[this,weak]{if(auto r=weak.lock())return finish_candidate_search(r);return false;};
        r->control->candidate_send=[this,weak](int n,V v){if(auto r=weak.lock())send_candidates(r,size_t(n),std::move(v));};
        if(optional<bool>(at(at(config,"peer"),"candidate_sync"),"skip_exhausted_targets",false))
          r->control->candidate_exhausted_targets=[weak]{if(auto r=weak.lock()){std::lock_guard<std::mutex> lock(r->sync->mu);return r->sync->exhausted_targets;}return std::set<int>{};};
      }
      const auto parent=array(at(r->state,"ancestry")).empty()?std::string{}:get<std::string>(array(at(r->state,"ancestry")).back(),"logical_region");
      auto snap=c.visits->snapshot();auto guard=snap->reserved.find(sid);
      reserved_skip=!has(r->state,"replica_version")&&!merged&&guard!=snap->reserved.end()&&!guard->second.count(parent);
      if(!reserved_skip){
        if(prepare)r->search_context=prepare(r->state);
        if(merged)r->actor=ai->second;
        else {r->actor=std::make_shared<RegionActor>();r->actor->id=get<std::string>(r->state,"branch_id");r->actor->region=region;
          r->actor->inbox=std::make_shared<dhsearch::RegionInbox>(get<size_t>(at(config,"search"),"frontier_size"));}
        if(candidate_sync){auto claim=c.starts.find(sid);if(claim!=c.starts.end()){r->actor->inbox=claim->second.inbox;r->sync->receipts=std::move(claim->second.receipts);}}
        r->state.map()["region_executor_id"]=r->actor->id;r->state.map()["region_merged_admission"]=merged;
        r->control->region_task(r->actor->inbox,c.visits);
        if(!c.claims)c.claims=std::make_shared<dhsearch::ExpansionClaims>(get<size_t>(at(config,"search"),"max_expansions"));
        r->control->claims(c.claims);
      }
      r->mark("accept_region_ready_ns",stamp());
      r->ticket=controller->register_value(std::move(md),r->control);
      r->mark("accept_register_done_ns",stamp());
      {
        const auto begin=stamp();std::lock_guard<std::mutex> lock(mu);
        r->mark("accept_registry_commit_wait_ns",stamp()-begin);
        check(!stopping,"Peer runtime closed");
        const auto old_records=records.bucket_count(),old_queries=query_records.bucket_count(),insert_started=stamp();
        records.emplace(key,r);query_records[qid].insert(key);++active;count_admission(r);
        record_registry_growth(insert_started,records.bucket_count()!=old_records||query_records.bucket_count()!=old_queries);
        --reserved_admissions;reservation.credit=false;committed=true;
      }
      wake.notify_all();
      r->mark("enqueued_ns",stamp());
      if(reserved_skip)++region_reserved_skips;
      else {
        if(merged)++region_merged;else {c.actors.emplace(actor_key,r->actor);++region_created;}
        c.visits->open(region);
        if(candidate_sync){c.canonical_actor=r->actor;c.visits->open(dhpair::region(at(config,"peer"),r->state,sid));}
        r->mark("accept_inbox_enter_ns",stamp());
        std::lock_guard<std::mutex> actor_lock(r->actor->mu);
        r->actor->inbox->offer(array(at(r->state,"seeds")));
        r->actor->inbox->offer(array(at(r->state,"remote_frontier")));
        r->mark("accept_inbox_done_ns",stamp());
        r->search_ready=stamp();r->mark("search_ready_ns",r->search_ready);
        r->actor->grants.push_back(r);++region_pending_grants;
        if(!r->actor->scheduled){
          if(merged)++region_reactivated;
          r->actor->scheduled=true;
          r->executor_ready=stamp();r->mark("executor_ready_ns",r->executor_ready);
          if(!workers->submit([this,a=r->actor]{run_region(a);})){
            r->actor->scheduled=false;r->actor->grants.pop_back();--region_pending_grants;
            r->status=2;r->ticket->fail("Native compute queue capacity exceeded");
          }
        }
      }
      query_lock.unlock();r->mark("accept_state_lock_released_ns",stamp());
    }catch(const std::exception& e){
      if(r->ticket){r->status=2;r->ticket->fail(e.what());}
      if(!committed)r->search_context.reset();
      throw;
    }
    if(reserved_skip)reserved_receipt(r);
    else if(!get<bool>(r->state,"region_merged_admission")&&!null(at(r->state,"parent_branch_id")))notify_regions(r);
    M answer{{"accepted",true},{"duplicate",false},{"region_admission",reserved_skip?"reserved":get<bool>(r->state,"region_merged_admission")?"merged":"created"}};
    if(!reserved_skip)answer["region_executor_id"]=r->actor->id;
    return {std::move(answer),r};
  }
  void notify_regions(const std::shared_ptr<Record>& r) {
    if(!region_tasks||!optional<bool>(at(config,"peer"),"region_visit_broadcast",true))return;
    V message=M{{"version",uint64_t(1)},{"op","peer_region_update"},
      {"query_request_id",at(r->state,"query_request_id")},{"token",at(r->state,"token")},
      {"epoch",at(r->state,"epoch")},{"signature",signature},{"directory",r->context->visits->value()}};
    for(size_t n=0;n<endpoints.size();++n)if(n!=sid){
      ++visit_rpcs;
      r->ticket->network_event(M{{"peer_visit_pending",1.}});
      bool sent=false;
      try{sent=visit_client->submit(endpoints[n],message,[this,r](dhwire::Result reply){
        visit_tx_bytes+=reply.request_bytes;visit_rx_bytes+=reply.response_bytes;
        if(!reply.ok||!optional<bool>(reply.response,"ok",false))++visit_errors;
        r->ticket->network_event(M{{"peer_visit_pending",-1.},{"peer_rpc_count",1.},{"peer_visit_rpc_count",1.},
          {"peer_request_bytes",double(reply.request_bytes)},{"peer_response_bytes",double(reply.response_bytes)},
          {"peer_visit_wall_ms",(reply.done_ns-reply.queued_ns)/1e6}});
      });}catch(...){sent=false;}
      if(!sent){++visit_errors;r->ticket->network_event(M{{"peer_visit_pending",-1.},{"peer_visit_rejected",1.}});}
    }
  }
  void run_region(const std::shared_ptr<RegionActor>& actor) {
    std::shared_ptr<Record> r;
    {std::lock_guard<std::mutex> lock(actor->mu);r=actor->grants.front();actor->grants.pop_front();--region_pending_grants;}
    execute_record(r);
  }
  void finish_region(const std::shared_ptr<Record>& r) {
    const auto& actor=r->actor;
    // Releasing a query worker must not wait for other queries' admissions or
    // directory updates under the node-wide identity mutex.
    r->mark("region_release_lock_enter_ns",stamp());
    std::lock_guard<std::mutex> lock(actor->mu);
    r->mark("region_release_lock_acquired_ns",stamp());
    if(actor->grants.empty())actor->scheduled=false;
    else {
      auto next=actor->grants.front();next->executor_ready=stamp();next->mark("executor_ready_ns",next->executor_ready);
      if(!workers->submit([this,actor]{run_region(actor);})){actor->scheduled=false;
      for(auto& pending:actor->grants){--region_pending_grants;pending->status=2;pending->ticket->fail("Native region queue capacity exceeded");}
      actor->grants.clear();}}
    r->mark("region_worker_return_ns",stamp());
  }
  void reserved_receipt(const std::shared_ptr<Record>& r) {
    // A reservation race is an explicit unspent credit receipt, never a second
    // executor or a false claim that the source vectors have been expanded.
    const auto& s=r->state;M report;
    for(auto k:{"branch_id","parent_branch_id","visited_nodes","work_grant","branch_slots","logical_region","visited_regions"})report[k]=at(s,k);
    report["node_id"]=uint64_t(sid);report["work_spent"]=uint64_t(0);report["work_unused"]=at(s,"work_grant");
    for(auto k:{"children","ids","squared_distances","expanded_ids","decisions","fork_events","result_paths","excluded_regions"})report[k]=A{};
    report["stats"]=M{{"expanded",uint64_t(0)},{"batches",uint64_t(0)},{"server_thread_cpu_ms",0.},
      {"max_expanded_depth",uint64_t(0)},{"max_cross_shard_depth",uint64_t(0)},
      {"source_reserved_credit_unused",at(s,"work_grant")},{"stop_reason","source_reserved_not_started"},{"pending_at_stop",uint64_t(0)}};
    report["region_task_mode"]=true;report["region_admission"]="source_reserved_not_started";
    r->status=2;r->search_context.reset();r->ticket->complete(std::move(report));
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
      check(!region_tasks||ids.empty(),"Region tasks cannot inherit whole-region exclusions");
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
    const auto accept_enter=stamp();
    if(optional<bool>(at(config,"peer"),"logical_region_visits",false))dhregion::normalize(s);
    validate(s);
    const auto validated=stamp();
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
                  {"receiver_worker_enter_ns",dhwire::handler_timing.worker_enter_ns},
                  {"receiver_decode_done_ns",dhwire::handler_timing.decode_done_ns},
                  {"receiver_handler_enter_ns",dhwire::handler_timing.handler_enter_ns},
                  {"managed_gate_enter_ns",dhwire::handler_timing.gate_enter_ns},
                  {"managed_gate_acquired_ns",dhwire::handler_timing.gate_acquired_ns},
                  {"accept_enter_ns",accept_enter},{"accept_validated_ns",validated},
                  {"accepted_ns", r->accepted},
                  {"receiver_thread_id", uint64_t(syscall(SYS_gettid))},
                  {"admission_mode", "native_bounded"}};
    const auto requested = stamp();
    if(region_tasks&&concurrent_region_admission)return admit_concurrent(r,std::move(md),requested);
    bool reserved_skip=false;
    {
      std::lock_guard<std::mutex> l(mu);
      r->mark("accept_state_lock_enter_ns",requested);
      r->mark("accept_state_lock_acquired_ns",stamp());
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
      if(region_tasks){
        r->context=context(r->state);
        if(has(r->state,"region_directory"))r->context->visits->merge(at(r->state,"region_directory"),endpoints.size());
        const auto region=dhregion::current(r->state);
        auto ai=r->context->actors.find(region);
        const auto parent_region=array(at(r->state,"ancestry")).empty()?std::string{}:get<std::string>(array(at(r->state,"ancestry")).back(),"logical_region");
        const auto snapshot=r->context->visits->snapshot();auto guard=snapshot->reserved.find(sid);
        reserved_skip=!has(r->state,"replica_version")&&ai==r->context->actors.end()&&guard!=snapshot->reserved.end()&&!guard->second.count(parent_region);
        if(!reserved_skip){
          if(prepare)r->search_context=prepare(r->state);
          const bool merged=ai!=r->context->actors.end();
          if(!merged){auto a=std::make_shared<RegionActor>();a->id=get<std::string>(r->state,"branch_id");a->region=region;
            a->inbox=std::make_shared<dhsearch::RegionInbox>(get<size_t>(at(config,"search"),"frontier_size"));
            ai=r->context->actors.emplace(region,std::move(a)).first;++region_created;
          }else ++region_merged;
          r->actor=ai->second;r->state.map()["region_executor_id"]=r->actor->id;r->state.map()["region_merged_admission"]=merged;
          r->control->region_task(r->actor->inbox,r->context->visits);
          r->context->visits->open(region);
        }else ++region_reserved_skips;
      }else if(prepare)r->search_context=prepare(r->state);
      if(optional<bool>(at(config,"peer"),"deduplicate_node_expansions",false)){
        auto& weak=query_claims[get<std::string>(r->state,"query_request_id")];auto claims=weak.lock();
        if(!claims){claims=std::make_shared<dhsearch::ExpansionClaims>(get<size_t>(at(config,"search"),"max_expansions"));weak=claims;}
        r->control->claims(std::move(claims));
      }
      r->mark("accept_region_ready_ns",stamp());
      r->ticket = controller->register_value(std::move(md), r->control);
      r->mark("accept_register_done_ns",stamp());
      records.emplace(q, r);
      query_records[get<std::string>(r->state,"query_request_id")].insert(q);
      ++active;count_admission(r);
      r->mark("enqueued_ns", stamp());
      bool submitted=true;
      if(region_tasks&&!reserved_skip){
        r->mark("accept_inbox_enter_ns",stamp());
        std::lock_guard<std::mutex> actor_lock(r->actor->mu);
        r->actor->inbox->offer(array(at(r->state,"seeds")));
        r->actor->inbox->offer(array(at(r->state,"remote_frontier")));
        r->mark("accept_inbox_done_ns",stamp());
        r->search_ready=stamp();r->mark("search_ready_ns",r->search_ready);
        r->actor->grants.push_back(r);++region_pending_grants;
        if(!r->actor->scheduled){
          if(get<bool>(r->state,"region_merged_admission"))++region_reactivated;
          r->actor->scheduled=true;r->executor_ready=stamp();r->mark("executor_ready_ns",r->executor_ready);submitted=workers->submit([this,a=r->actor]{run_region(a);});
          if(!submitted){r->actor->scheduled=false;r->actor->grants.pop_back();--region_pending_grants;}
        }
      }else if(!reserved_skip){r->search_ready=stamp();r->mark("search_ready_ns",r->search_ready);r->executor_ready=r->search_ready;r->mark("executor_ready_ns",r->executor_ready);submitted=workers->submit([this,r]{run(r);});}
      if (!submitted) {
        ++rejected;
        r->status = 2;
        r->ticket->fail("Native compute queue capacity exceeded");
      }
    }
    r->mark("accept_state_lock_released_ns", stamp());
    // The root has no pre-existing peer branches. Every child already carries
    // its opened root in the directory; avoid two idle-peer RPCs per query.
    if(region_tasks){if(reserved_skip)reserved_receipt(r);else if(!get<bool>(r->state,"region_merged_admission")&&!null(at(r->state,"parent_branch_id")))notify_regions(r);}
    M answer{{"accepted",true},{"duplicate",false}};
    if(region_tasks){answer["region_admission"]=reserved_skip?"reserved":get<bool>(r->state,"region_merged_admission")?"merged":"created";
      if(!reserved_skip)answer["region_executor_id"]=r->actor->id;}
    return {std::move(answer), r};
  }
  void dispatch_transfer(std::shared_ptr<Transfer> t,size_t attempt=0) {
    auto r=t->record;
    if(!region_tasks){transfer(t);return;}
    auto ready=[this,t,r]{
      t->message.map().at("state").map()["region_directory"]=r->context->visits->value();transfer(t);
    };
    const auto& peer=at(config,"peer");
    const bool continuous=has(peer,"remote_pull")&&get<bool>(at(peer,"remote_pull"),"enabled");
    if(continuous||!has(r->state,"replica_version")||r->actor->source_reserved.load()){ready();return;}
    const auto source=has(r->state,"replica_fallback_source")?get<size_t>(r->state,"replica_fallback_source"):get<size_t>(r->state,"original_entry_node");
    check(source<endpoints.size(),"Invalid replica source reservation");
    V request=M{{"version",uint64_t(1)},{"op","peer_region_reserve"},
      {"query_request_id",at(r->state,"query_request_id")},{"token",at(r->state,"token")},
      {"epoch",at(r->state,"epoch")},{"signature",signature},{"owner",dhregion::current(r->state)},
      {"directory",r->context->visits->value()}};
    add(t->metrics,"peer_rpc_count",1);add(t->metrics,"peer_region_reserve_rpc_count",1);
    try{
      if(!client->submit(endpoints[source],request,[this,t,r,attempt](dhwire::Result reply){
        add(t->metrics,"peer_request_bytes",reply.request_bytes);add(t->metrics,"peer_response_bytes",reply.response_bytes);
        add(t->metrics,"peer_region_reserve_wall_ms",(reply.done_ns-reply.queued_ns)/1e6);
        try{
          check(reply.ok,reply.error.c_str());check(get<bool>(reply.response,"ok"),"Source reservation rejected");
          r->context->visits->merge(at(at(reply.response,"result"),"directory"),endpoints.size());
          r->actor->source_reserved.store(true);notify_regions(r);
          t->message.map().at("state").map()["region_directory"]=r->context->visits->value();transfer(t);
        }catch(const std::exception& e){
          if(attempt<retries&&!r->control->cancelled())dispatch_transfer(t,attempt+1);
          else r->ticket->transfer_done(t->metrics,e.what());
        }
      }))throw std::runtime_error("Source reservation network queue full");
    }catch(const std::exception& e){r->ticket->transfer_done(t->metrics,e.what());}
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
    const bool candidate_sync=dhpair::target(at(config,"peer"),r->state,sid,get<size_t>(child,"target_node"));
    if(dhpair::scoped(at(config,"peer"))&&!candidate_sync)dhpair::clear(child);
    const bool nonblocking=candidate_sync&&optional<bool>(at(at(config,"peer"),"candidate_sync"),"nonblocking_start",false);
    m["branch_id"] = candidate_sync&&!nonblocking?m.at("claimed_branch_id"):V(identifier());
    m.erase("candidate_nonblocking_start");if(nonblocking)m["candidate_nonblocking_start"]=true;
    m.erase("claimed_branch_id");
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
    if(nonblocking){
      if(region_tasks&&r->context->visits->announce(dhpair::region(at(config,"peer"),at(t->message,"state"),t->node)))notify_regions(r);
      {std::lock_guard<std::mutex> lock(r->sync->mu);++r->sync->starts_pending;}
      t->message.map()["op"]="peer_candidate_start";
      start_candidate_branch(t,desc);return desc;
    }
    r->ticket->child(desc);
    // Subsequent grants to an opened/pending region merge candidates without
    // changing the visit directory. Do not rebroadcast an identical state.
    if(region_tasks&&r->context->visits->announce(dhregion::current(at(t->message,"state"))))notify_regions(r);
    r->mark("transfer_worker_enter_ns", stamp());
    dispatch_transfer(t);
    r->mark("child_submit_done_ns:" + get<std::string>(desc, "branch_id"), stamp());
    return desc;
  }
  V pull_message(const std::shared_ptr<Record>& r,const std::vector<uint64_t>& ids) {
    V message=M{{"version",uint64_t(1)},{"op","peer_replica_read"},{"query_request_id",at(r->state,"query_request_id")},
      {"token",at(r->state,"token")},{"epoch",at(r->state,"epoch")},{"signature",signature},
      {"owner",dhregion::current(r->state)},{"directory",r->context->visits->value()},{"ids",vector(ids)},{"read_mode",r->pull_mode}};
    dhpair::copy(r->state,message);return message;
  }
  V pull_reply(const std::shared_ptr<Record>& r,dhwire::Result out) {
    const bool direct=out.ok&&has(out.response,"result")&&optional<bool>(at(out.response,"result"),"one_sided",false);
    const double rpc_share=direct?0.:1./r->pull_group_size;
    r->ticket->network_event(M{{"peer_rpc_count",rpc_share},{"peer_pull_rpc_count",rpc_share},{"peer_pull_logical_count",1.},
      {"peer_request_bytes",double(out.request_bytes)},{"peer_response_bytes",double(out.response_bytes)},
      {"peer_pull_wall_ms",out.done_ns>=out.queued_ns?(out.done_ns-out.queued_ns)/1e6:0.}});
    check(out.ok,out.error.c_str());check(get<bool>(out.response,"ok"),"Source record read failed");
    V reply=std::move(out.response.map().at("result"));
    if(r->pull_common){const auto start=thread_cpu_ms();auto data=std::move(r->pull_common);if(optional<bool>(reply,"shared",false))reply=dhpull::gather(*data,reply);batch_gather_cpu_ns+=uint64_t((thread_cpu_ms()-start)*1e6);}
    if(has(reply,"directory"))r->context->visits->merge(at(reply,"directory"),endpoints.size());
    reply.map()["wire_bytes"]=uint64_t(out.request_bytes+out.response_bytes);return reply;
  }
  V fetch(const std::shared_ptr<Record>& r,int requested_source,const std::vector<uint64_t>& ids) {
    const auto& peer=at(config,"peer");
    const bool general=has(peer,"remote_pull")&&get<bool>(at(peer,"remote_pull"),"enabled")&&!has(r->state,"replica_version")&&!dhpair::scoped(peer);
    check(region_tasks&&concurrent_region_admission&&(general||has(r->state,"replica_version")),"Pull requires native region runtime");
    check(get<bool>(at(at(config,"peer"),"replica_pull"),"enabled"),"Replica pull disabled");r->control->check();
    const auto source=size_t(requested_source);
    check(source<endpoints.size()&&source!=sid,"Invalid pull source");
    if(!general){const auto expected=has(r->state,"replica_fallback_source")?get<size_t>(r->state,"replica_fallback_source"):get<size_t>(r->state,"original_entry_node");check(source==expected,"Replica pull source mismatch");}
    check(!general||async_pull,"General remote pull must yield, never block a search worker");
    if(async_pull){
      {std::lock_guard<std::mutex> lock(pull_mu);check(!pull_closing,"Async pull shutting down");
        if(!r->tail_admitted){if(tail_active>=tail_capacity){++tail_rejected;throw std::runtime_error("Async tail capacity full");}
          ++tail_active;pull_peak=std::max(pull_peak,tail_active);r->tail_admitted=true;}}
      r->pull_source=source;throw dhsearch::PullRequest{ids,{}};
    }
    auto promise=std::make_shared<std::promise<dhwire::Result>>();auto future=promise->get_future();
    check(client->submit(endpoints[source],pull_message(r,ids),[promise](dhwire::Result out){promise->set_value(std::move(out));}),"Replica pull network queue full");
    return pull_reply(r,future.get());
  }
  void release_tail(const std::shared_ptr<Record>& r) {
    r->pull_common.reset();
    if(!r->tail_admitted)return;
    std::lock_guard<std::mutex> lock(pull_mu);r->tail_admitted=false;--tail_active;pull_idle.notify_all();
  }
  void queue_resume(const std::shared_ptr<Record>& r,dhwire::Result reply) {
    r->pull_result=std::move(reply);r->pull_ready=stamp();
    if(shared_compute){
      if(!workers->submit_tail([this,r]{++tail_resumes;execute_record(r);})){
        r->continuation={};r->status=2;r->search_context.reset();r->ticket->fail("Shared tail capacity exceeded");release_tail(r);finish_region(r);}
      return;
    }
    if(help_limit) {
      auto active=help_active.load();
      while(active<help_limit&&!help_active.compare_exchange_weak(active,active+1)){}
      if(active<help_limit) {
        auto peak=help_peak.load();while(peak<active+1&&!help_peak.compare_exchange_weak(peak,active+1)){}
        if(workers->submit_if_idle([this,r]{
          struct Release {std::atomic<uint64_t>& active;~Release(){--active;}} release{help_active};
          ++tail_resumes;++help_resumes;execute_record(r,true);
        },help_reserve,help_queue_low))return;
        --help_active;
      }
      ++help_declined;
    }
    auto* pool=tail_workers?tail_workers.get():workers.get();
    // One continuation per admitted context; capacity reserves every callback.
    if(!pool->submit([this,r]{++tail_resumes;execute_record(r);})){r->continuation={};r->status=2;r->search_context.reset();r->ticket->fail("Tail resume capacity exceeded");release_tail(r);finish_region(r);}
  }
  void pump_pulls() {
    if(coalesce||threaded_pull_dispatch){pull_wake.notify_one();return;}
    ++pull_pump_calls;std::vector<std::shared_ptr<Record>> send;
    {PullLock lock(*this);
      for(auto it=pull_pending.begin();it!=pull_pending.end();) {
        ++pull_scan_steps;auto r=*it;if(pull_inflight[r->pull_source]>=pull_limit){++it;continue;}
        ++pull_inflight[r->pull_source];r->pull_sent=stamp();send.push_back(r);it=pull_pending.erase(it);
      }}
    send_pulls(std::move(send));
  }
  void dispatch_single_pulls() {
    for(;;){
      std::vector<std::shared_ptr<Record>> send;
      {std::unique_lock<std::mutex> lock(pull_mu);
        pull_wake.wait(lock,[this]{if(coalesce_stop)return true;for(size_t i=0;i<pull_sources.size();++i)if(!pull_sources[i].empty()&&pull_inflight[i]<pull_limit)return true;return false;});
        if(coalesce_stop)return;
        const auto start=stamp();++pull_pump_calls;
        // Source queues make selecting an eligible request O(nodes), not
        // O(all suspended queries). Round-robin service preserves source FIFO.
        size_t empty=0;
        while(send.size()<64&&empty<pull_sources.size()){
          size_t n=pull_dispatch_cursor++%pull_sources.size();++pull_scan_steps;
          if(pull_sources[n].empty()||pull_inflight[n]>=pull_limit){++empty;continue;}
          empty=0;auto r=std::move(pull_sources[n].front());pull_sources[n].pop_front();
          ++pull_inflight[n];r->pull_sent=stamp();send.push_back(std::move(r));
        }
        pull_lock_hold_ns+=stamp()-start;
      }
      send_pulls(std::move(send));
    }
  }
  void send_pulls(std::vector<std::shared_ptr<Record>> send) {
    const auto submit_cpu=thread_cpu_ms();
    for(auto& r:send) {
      r->pull_auth_begin=r->pull_auth_ready=0;
      auto once=std::make_shared<std::atomic<bool>>(false);
      auto complete=[this,r,once](dhwire::Result reply){
        if(once->exchange(true))return;
        const auto begin=stamp();const auto cpu=thread_cpu_ms();
        {PullLock lock(*this);--pull_inflight[r->pull_source];}
        queue_resume(r,std::move(reply));pump_pulls();
        pull_callback_wall_ns+=stamp()-begin;pull_callback_cpu_ns+=uint64_t((thread_cpu_ms()-cpu)*1e6);
      };
      try{r->control->check();V message=pull_message(r,r->pull_ids);
        if(one_client){message.map()["one_sided"]=true;r->pull_auth_begin=stamp();
          auto response=[this,r,complete,message](dhwire::Result reply)mutable{
            r->pull_auth_ready=stamp();
            try{
              if(reply.ok&&get<bool>(reply.response,"ok")&&optional<bool>(at(reply.response,"result"),"one_sided_ready",false)){
                ++one_control_rpcs;one_control_bytes+=reply.request_bytes+reply.response_bytes;
                r->ticket->network_event(M{{"peer_rpc_count",1.},{"peer_pull_rpc_count",1.},{"peer_request_bytes",double(reply.request_bytes)},{"peer_response_bytes",double(reply.response_bytes)}});
                V directory=at(at(reply.response,"result"),"directory");r->control->check();
                auto ready=[complete,directory=std::move(directory)](dhwire::Result out)mutable{
                  if(out.ok)out.response.map().at("result").map()["directory"]=std::move(directory);complete(std::move(out));};
                if(!one_client->submit(r->pull_source,std::move(message),r->pull_ids,std::move(ready),r->pull_code_lookup))throw std::runtime_error("One-sided reader capacity exceeded");
              }else complete(std::move(reply));
            }catch(const std::exception& e){dhwire::Result error;error.error=e.what();complete(std::move(error));}
          };
          if(!pull_client->submit(endpoints[r->pull_source],message,std::move(response)))throw std::runtime_error("One-sided authorization queue full");
        }else if(!pull_client->submit(endpoints[r->pull_source],message,complete))throw std::runtime_error("Async pull network capacity exceeded");}
      catch(const std::exception& e){dhwire::Result error;error.error=e.what();complete(std::move(error));}
    }
    pull_submit_cpu_ns+=uint64_t((thread_cpu_ms()-submit_cpu)*1e6);
  }
  void dispatch_pull_batches() {
    const size_t unit=get<size_t>(at(at(config,"peer"),"replica_pull"),"batch_bytes");
    for(;;) {
      std::vector<std::shared_ptr<Record>> group;size_t source=0;
      {
        std::unique_lock<std::mutex> lock(pull_mu);
        auto first=[&]{return std::find_if(pull_pending.begin(),pull_pending.end(),[&](const auto& r){return pull_inflight[r->pull_source]<pull_limit;});};
        pull_wake.wait(lock,[&]{return coalesce_stop||(coalesce_reserved+unit<=coalesce_capacity&&first()!=pull_pending.end());});
        if(coalesce_stop)return;
        auto it=first();source=(*it)->pull_source;
        const uint64_t due=(*it)->pull_queued+coalesce_wait_us*1000;
        size_t count=std::count_if(pull_pending.begin(),pull_pending.end(),[&](const auto& r){return r->pull_source==source;});
        const size_t cap=std::min({coalesce_queries,coalesce_bytes/unit,pull_limit-pull_inflight[source],(coalesce_capacity-coalesce_reserved)/unit});
        const auto now=stamp();
        if(!pull_closing&&count<cap&&now<due){pull_wake.wait_for(lock,std::chrono::nanoseconds(due-now));continue;}
        for(auto i=pull_pending.begin();i!=pull_pending.end()&&group.size()<cap;)
          if((*i)->pull_source==source){group.push_back(*i);i=pull_pending.erase(i);}else ++i;
        pull_inflight[source]+=group.size();coalesce_reserved+=group.size()*unit;
        for(auto& r:group)r->pull_sent=stamp();
      }
      const auto reserved=group.size()*unit;
      auto once=std::make_shared<std::atomic<bool>>(false);
      auto complete=[this,group,source,reserved,once](dhwire::Result reply) mutable {
        if(once->exchange(true))return;
        batch_tx+=reply.request_bytes;batch_rx+=reply.response_bytes;
        // A retained wire buffer holds its full reservation until every consumer
        // has gathered its rows. Inflight-query credits and byte credits differ.
        auto common=std::shared_ptr<V>(new V(M{}),[this,reserved](V* v){delete v;{std::lock_guard<std::mutex> lock(pull_mu);coalesce_reserved-=reserved;}pull_wake.notify_one();});
        if(group.size()==1) {
          {std::lock_guard<std::mutex> lock(pull_mu);--pull_inflight[source];}
          group[0]->pull_common=common;group[0]->pull_group_size=1;
          queue_resume(group[0],std::move(reply));pull_wake.notify_one();return;
        }
        try {
          check(reply.ok,reply.error.c_str());check(get<bool>(reply.response,"ok"),"Coalesced source read failed");
          *common=std::move(reply.response.map().at("result"));
          check(array(at(*common,"items")).size()==group.size(),"Coalesced response count mismatch");
        }catch(const std::exception& e){reply.ok=false;reply.error=e.what();}
        {std::lock_guard<std::mutex> lock(pull_mu);pull_inflight[source]-=group.size();}
        for(size_t i=0;i<group.size();++i){auto& r=group[i];dhwire::Result one;
          one.ok=reply.ok;one.error=reply.error;one.queued_ns=reply.queued_ns;one.done_ns=reply.done_ns;
          one.request_bytes=reply.request_bytes/group.size()+(i==0?reply.request_bytes%group.size():0);
          one.response_bytes=reply.response_bytes/group.size()+(i==0?reply.response_bytes%group.size():0);
          if(reply.ok)one.response=array(at(*common,"items"))[i];
          r->pull_common=common;r->pull_group_size=group.size();queue_resume(r,std::move(one));
        }
        pull_wake.notify_one();
      };
      try {
        A items;for(const auto& r:group)items.push_back(pull_message(r,r->pull_ids));
        V message=group.size()==1?std::move(items.front()):V(M{{"version",uint64_t(1)},{"op","peer_replica_read_batch"},{"epoch",at(meta,"epoch")},{"signature",signature},{"items",std::move(items)}});
        ++batch_rpcs;batch_queries+=group.size();if(group.size()>1)++batch_multi;
        if(!pull_client->submit(endpoints[source],std::move(message),complete))throw std::runtime_error("Coalesced network capacity exceeded");
      }catch(const std::exception& e){dhwire::Result error;error.error=e.what();complete(std::move(error));}
    }
  }

  dhwire::Reply handle_pull_batch(const V& request) {
    const auto& p=at(at(config,"peer"),"replica_pull");
    check(coalesce&&get<bool>(p,"enabled")&&region_tasks&&concurrent_region_admission&&bool(read_records)&&bool(read_workers),"Coalesced pull disabled");
    check(get<std::string>(request,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(request,"signature")==signature,"Pull epoch/configuration mismatch");
    const auto& items=array(at(request,"items"));
    const size_t limit=get<size_t>(p,"batch_bytes");
    check(!items.empty()&&items.size()<=coalesce_queries&&items.size()<=coalesce_bytes/limit,"Coalesced request bound exceeded");
    A results(items.size()),accepted;std::vector<size_t> positions;std::vector<std::shared_ptr<QueryContext>> contexts(items.size());
    for(size_t i=0;i<items.size();++i)try{
      const auto& item=items[i];
      check(get<std::string>(item,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(item,"signature")==signature,"Pull epoch/configuration mismatch");
      auto ids=get<std::vector<uint64_t>>(item,"ids");check(!ids.empty()&&ids.size()<=get<size_t>(p,"batch_records"),"Pull batch bound exceeded");
      auto owner=get<std::string>(item,"owner");check(owner.rfind("replica:",0)==0&&owner.size()>8,"Pull requires replica owner");
      std::shared_ptr<QueryContext> c;{std::lock_guard<std::mutex> lock(mu);check(!stopping,"Peer runtime closed");c=context(item);}contexts[i]=c;
      bool granted;
      {std::lock_guard<std::mutex> lock(c->admission_mu);c->visits->merge(at(item,"directory"),endpoints.size());auto snapshot=c->visits->snapshot();auto it=snapshot->reserved.find(sid);
        granted=!snapshot->opened.count(dhregion::base(sid))&&!snapshot->pending.count(dhregion::base(sid))&&!c->actors.count(dhregion::base(sid))&&
          (it==snapshot->reserved.end()||(it->second.size()==1&&it->second.count(owner)));
        if(granted)c->visits->reserve(sid,owner);}
      if(granted){accepted.push_back(M{{"ids",vector(ids)}});positions.push_back(i);}
      else results[i]=M{{"ok",true},{"result",M{{"accepted",false},{"reason","source_active_announced_or_reserved"}}}};
    }catch(const std::exception& e){results[i]=M{{"ok",false},{"error",e.what()}};}
    V rid=has(request,"request_id")?at(request,"request_id"):V();const auto ready=stamp();
    dhwire::Reply deferred;
    deferred.deferred=[this,accepted=std::move(accepted),positions,contexts,results=std::move(results),limit,ready,rid](auto deliver)mutable{
      auto task=[this,accepted=std::move(accepted),positions,contexts,results=std::move(results),limit,ready,rid,deliver]()mutable{
        dhwire::Reply out;
        try{
          const auto begun=stamp();const auto cpu=thread_cpu_ms();V common=M{};
          if(!accepted.empty()){
            common=dhpull::read(accepted,dimension,limit,accepted.size()*limit,read_records);
            batch_union_failures+=optional<bool>(common,"union_failed",false);
            batch_unique_records+=optional<uint64_t>(common,"unique_records",0);batch_logical_records+=optional<uint64_t>(common,"logical_records",0);
            batch_unique_bytes+=optional<uint64_t>(common,"unique_bytes",0);batch_logical_bytes+=optional<uint64_t>(common,"logical_bytes",0);
            auto parts=std::move(std::get<A>(common.map().at("items").data));
            for(size_t j=0;j<positions.size();++j)results[positions[j]]=std::move(parts[j]);
          }
          for(size_t i=0;i<results.size();++i)if(get<bool>(results[i],"ok")){
            auto& value=results[i].map().at("result");value.map()["source_queue_ms"]=(begun-ready)/1e6;
            if(contexts[i])value.map()["directory"]=contexts[i]->visits->value();
          }
          common.map()["items"]=std::move(results);batch_source_cpu_ns+=uint64_t((thread_cpu_ms()-cpu)*1e6);
          out.message=M{{"ok",true},{"result",std::move(common)},{"request_id",rid}};
        }catch(const std::exception& e){out.message=M{{"ok",false},{"error",e.what()},{"request_id",rid}};}
        deliver(std::move(out));
      };
      if(!read_workers->submit(std::move(task))){++read_rejected;dhwire::Reply error;error.message=M{{"ok",false},{"error","source_read_queue_full"},{"request_id",rid}};deliver(std::move(error));}
    };
    return deferred;
  }

  #include "candidate_sync_runtime.inc"
  void execute_record(const std::shared_ptr<Record>& r,bool main_help=false) {
    if(run(r,main_help)){release_tail(r);if(r->actor)finish_region(r);return;}
    if(r->candidate_park){release_tail(r);park_candidate(r);return;}
    r->pull_queued=stamp();
    if(r->demand_start){
      r->pull_sent=stamp();auto start=std::move(r->demand_start);auto gate=demand_gate;auto source=r->pull_source;
      V auth=M{{"version",uint64_t(1)},{"op","peer_one_sided_connect"},{"demand_shared",true},{"epoch",at(meta,"epoch")},{"signature",signature},
        {"owner",dhregion::current(r->state)},{"replica_version",at(r->state,"replica_version")},{"source",uint64_t(source)},{"read_mode","full"}};
      dhgrowth::Read read=[gate,source,auth](const std::vector<uint64_t>& ids,dhonesided::Complete done,dhonesided::CodeLookup lookup){auto a=auth;a.map()["ids"]=vector(ids);return gate->read(source,a,ids,std::move(done),std::move(lookup));};
      start(std::move(read),[this,r](V reply){dhwire::Result out;out.ok=true;out.response=M{{"ok",true},{"result",std::move(reply)}};out.queued_ns=r->pull_sent;out.done_ns=stamp();queue_resume(r,std::move(out));});return;
    }
    {PullLock lock(*this);if(threaded_pull_dispatch&&!coalesce)pull_sources[r->pull_source].push_back(r);else pull_pending.push_back(r);}
    pump_pulls();
  }
  bool run(std::shared_ptr<Record> r,bool main_help=false) {
    const bool resumed=bool(r->continuation);bool suspended=false;
    r->status = 1;
    const auto started = stamp();
    timespec cpu_begin{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu_begin);
    if(!resumed) {
    uint64_t wait_us=(started-r->accepted)/1000;size_t bucket=0;while(wait_us>1&&bucket<63){wait_us>>=1;++bucket;}++wait_hist[bucket];
    const uint64_t ready_ns=r->search_ready?r->search_ready:r->accepted;
    const uint64_t eligible_ns=r->executor_ready?r->executor_ready:ready_ns;
    if(!r->search_ready||!r->executor_ready)++ready_clock_fallbacks;
    auto wait_bucket=[](uint64_t ns){uint64_t us=ns/1000;size_t b=0;while(us>1&&b<63){us>>=1;++b;}return b;};
    ++executor_wait_hist[wait_bucket(started-eligible_ns)];++actor_wait_hist[wait_bucket(eligible_ns-ready_ns)];
    uint64_t ready_us=(started-ready_ns)/1000;size_t ready_bucket=0;while(ready_us>1&&ready_bucket<63){ready_us>>=1;++ready_bucket;}++ready_wait_hist[ready_bucket];
    r->mark("worker_enter_ns", started);
    r->mark("worker_thread_id", uint64_t(syscall(SYS_gettid)));
    }
    try {
      if(resumed&&r->candidate_resume){
        r->candidate_resume=false;auto reply=std::move(r->candidate_reply);reply.map()["resume_queue_ms"]=(started-r->candidate_ready)/1e6;
        auto continuation=std::move(r->continuation);continuation(std::move(reply));
      } else if(resumed) {
        r->pull_code_lookup={};
        V reply;try{reply=pull_reply(r,std::move(r->pull_result));}catch(const std::exception& e){reply=M{{"pull_error",e.what()}};}
        if(r->pull_auth_ready>=r->pull_auth_begin&&r->pull_auth_begin){
          reply.map()["one_sided_auth_ms"]=(r->pull_auth_ready-r->pull_auth_begin)/1e6;
          reply.map()["one_sided_auth_begin_ns"]=r->pull_auth_begin;reply.map()["one_sided_auth_ready_ns"]=r->pull_auth_ready;
        }
        reply.map()["pull_dispatch_queue_ms"]=(r->pull_sent-r->pull_queued)/1e6;
        reply.map()["pull_network_ms"]=(r->pull_ready-r->pull_sent)/1e6;
        reply.map()["pull_resume_queue_ms"]=(started-r->pull_ready)/1e6;
        reply.map()["pull_resume_enter_ns"]=started;
        reply.map()["pull_queued_ns"]=r->pull_queued;
        reply.map()["pull_sent_ns"]=r->pull_sent;
        reply.map()["pull_ready_ns"]=r->pull_ready;
        reply.map()["pull_resume_on_main"]=main_help;
        auto continuation=std::move(r->continuation);continuation(std::move(reply));
      } else {
      r->control->check();
      const uint64_t ready_ns=r->search_ready?r->search_ready:r->accepted;
      const uint64_t eligible_ns=r->executor_ready?r->executor_ready:ready_ns;
      r->ticket->start((ready_ns-r->accepted)/1e6, (started-eligible_ns)/1e6,
                       (started-ready_ns)/1e6, (eligible_ns-ready_ns)/1e6);
      PCG64 rng(get<uint64_t>(r->state, "decision_seed"));
      V s = r->state;
      s.map()["trace_enabled"] = trace_selected(config, at(s, "query_id"));
      s.map()["lifecycle_trace_enabled"] =
          trace_selected(config, at(s, "query_id"), true);
      search(
          s, rng.draws(get<size_t>(s, "work_grant")), r->control,
          [this, r](V p) { return spawn(r, std::move(p)); }, r->ticket, r->search_context,
          [this,r](int source,const std::vector<uint64_t>& ids){return fetch(r,source,ids);});
      }
    } catch (dhsearch::CandidatePause& pending) {
      r->continuation=std::move(pending.resume);r->candidate_request=std::move(pending.request);r->candidate_park=true;suspended=true;
    } catch (dhsearch::PullRequest& pending) {
      check(bool(pending.resume),"Missing native search continuation");
      r->continuation=std::move(pending.resume);r->pull_ids=std::move(pending.ids);r->pull_code_lookup=std::move(pending.code_lookup);r->pull_mode=std::move(pending.mode);r->demand_start=std::move(pending.demand_start);suspended=true;
    } catch (const std::exception &e) {
      r->ticket->fail(e.what());
    }
    timespec cpu_end{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu_end);
    const auto slice_cpu_ns=uint64_t((cpu_end.tv_sec-cpu_begin.tv_sec)*1000000000LL+cpu_end.tv_nsec-cpu_begin.tv_nsec);
    const auto slice_wall_ns=stamp()-started;
    cpu_ns.fetch_add(slice_cpu_ns);work_ns.fetch_add(slice_wall_ns);
    if(resumed){tail_cpu_ns.fetch_add(slice_cpu_ns);tail_wall_ns.fetch_add(slice_wall_ns);
      if(main_help){help_cpu_ns.fetch_add(slice_cpu_ns);help_wall_ns.fetch_add(slice_wall_ns);}}
    if(!suspended){++completed_work;r->search_context.reset();r->mark("compute_released_ns",stamp());}
    r->status=suspended?3:2;return !suspended;
  }
  dhwire::Reply handle(V request, uint64_t received, size_t bytes,bool on_start_worker=false) {
    check(get<size_t>(request, "version") == 1, "Unsupported protocol");
    auto op = get<std::string>(request, "op");
    if(op=="peer_one_sided_connect"&&optional<bool>(request,"demand_shared",false)){
      const auto& peer=at(config,"peer");check(has(peer,"demand_growth")&&get<bool>(at(peer,"demand_growth"),"enabled")&&bool(one_source),"Demand source unavailable");
      check(get<std::string>(request,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(request,"signature")==signature&&get<size_t>(request,"source")==sid,"Demand source identity mismatch");
      const auto version=get<std::string>(request,"replica_version");check(!version.empty()&&version.size()<=96&&get<std::string>(request,"owner")=="replica:"+version,"Demand READ requires replica owner");
      const auto ids=get<std::vector<uint64_t>>(request,"ids");check(!ids.empty()&&ids.size()<=get<size_t>(at(peer,"demand_growth"),"read_batch_records"),"Demand batch limit");
      return dhwire::Reply{M{{"ok",true},{"result",one_source->connect(at(request,"qp"))},{"request_id",has(request,"request_id")?at(request,"request_id"):V()}}};
    }
    if(start_workers&&!on_start_worker&&(op=="peer_submit"||op=="peer_candidate_start")){
      // Large first-task validation/registration must not sit ahead of short
      // candidate and RDMA authorization messages on every receive lane.
      // The ACK still follows actual atomic admission, never just enqueue.
      ++deferred_admissions;
      auto credit=std::shared_ptr<void>(new char,[this](void* p){delete static_cast<char*>(p);--deferred_admissions;});
      auto timing=dhwire::handler_timing;const auto queued=stamp();dhwire::Reply out;
      out.deferred=[this,request=std::move(request),received,bytes,timing,queued,credit](auto deliver)mutable{
        auto task=[this,request=std::move(request),received,bytes,timing,queued,credit,deliver]()mutable{
          const auto begin=stamp();const double cpu=thread_cpu_ms();start_queue_ns+=begin-queued;
          auto old=dhwire::handler_timing;dhwire::handler_timing=timing;
          dhwire::Reply answer;V rid=has(request,"request_id")?at(request,"request_id"):V();
          try{answer=handle(std::move(request),received,bytes,true);}
          catch(const std::exception& e){answer.message=M{{"ok",false},{"error",e.what()},{"request_id",std::move(rid)}};}
          dhwire::handler_timing=old;start_wall_ns+=stamp()-begin;start_cpu_ns+=uint64_t((thread_cpu_ms()-cpu)*1e6);++start_completed;
          deliver(std::move(answer));
        };
        if(!start_workers->submit(std::move(task)))deliver(dhwire::Reply{M{{"ok",false},{"error","First-task admission queue full"}}});
      };
      return out;
    }
    V result;
    std::shared_ptr<Record> trace;
    if(op.rfind("peer_candidate_",0)==0&&op!="peer_candidate_start")return handle_candidate(request);
    if(op=="peer_replica_read_batch")return handle_pull_batch(request);
    if (op == "peer_submit"||op=="peer_candidate_start") {
      if(op=="peer_candidate_start")check(candidate_sync&&optional<bool>(at(at(config,"peer"),"candidate_sync"),"nonblocking_start",false)&&optional<bool>(at(request,"state"),"candidate_nonblocking_start",false),"Nonblocking start disabled");
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
    } else if(op=="peer_replica_read"||op=="peer_one_sided_connect"){
      check(region_tasks&&concurrent_region_admission&&bool(read_records),"Replica pull runtime unavailable");
      const auto& p=at(at(config,"peer"),"replica_pull");check(get<bool>(p,"enabled"),"Replica pull disabled");
      check(get<std::string>(request,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(request,"signature")==signature,"Pull epoch/configuration mismatch");
      const auto ids=get<std::vector<uint64_t>>(request,"ids");check(!ids.empty()&&ids.size()<=get<size_t>(p,"batch_records"),"Pull batch bound exceeded");
      const auto read_mode=optional<std::string>(request,"read_mode","full");
      check(read_mode=="full"||((read_mode=="adjacency"||read_mode=="vectors")&&has(p,"lazy_rerank")&&get<bool>(at(p,"lazy_rerank"),"enabled")&&bool(one_source)),"Unsupported selective record read");
      auto owner=get<std::string>(request,"owner");
      const auto& peer=at(config,"peer");
      const bool general=has(peer,"remote_pull")&&get<bool>(at(peer,"remote_pull"),"enabled");
      if(general&&dhpair::scoped(peer))check(dhpair::member(request)&&get<size_t>(request,"replica_pair_source")==sid&&owner=="replica:"+get<std::string>(request,"replica_pair_version"),"READ outside replica/source pair");
      bool valid_base=false;if(general&&!dhpair::scoped(peer))for(size_t n=0;n<endpoints.size();++n)if(n!=sid&&owner==dhregion::base(n))valid_base=true;
      check((general&&valid_base)||(owner.rfind("replica:",0)==0&&owner.size()>8),"Pull requires authorized region owner");
      std::shared_ptr<QueryContext> c;{std::lock_guard<std::mutex> lock(mu);check(!stopping,"Peer runtime closed");c=context(request);}
      bool granted=false;
      {std::lock_guard<std::mutex> lock(c->admission_mu);
        c->visits->merge(at(request,"directory"),endpoints.size());auto snapshot=c->visits->snapshot();auto it=snapshot->reserved.find(sid);
        // A same-owner handoff can be in flight ahead of actor creation.
        // Treat its announced region as started, too: do not race that grant
        // with an unnecessary data pull while its submit ACK is pending.
        granted=!snapshot->opened.count(dhregion::base(sid))&&!snapshot->pending.count(dhregion::base(sid))&&
          !c->actors.count(dhregion::base(sid))&&
          (it==snapshot->reserved.end()||(it->second.size()==1&&it->second.count(owner)));
        // Continuous reads (including replica roots) do not reserve a remote
        // graph. Legacy replica-tail mode retains its ownership protection. A concurrent
        // branch may start there; credit remains conserved, and the later
        // push merges candidates into its single existing executor.
        if(granted&&!general)c->visits->reserve(sid,owner);
      }
      // No source search actor, query budget, distance evaluation or expansion
      // claim is created by a data read. Only the replica spends search credit.
      if(op=="peer_one_sided_connect") {
        // A base-region read was authorized before QP bootstrap. Another
        // branch starting in the meantime must not destroy a reusable QP.
        // Per-batch admission still applies; connection setup grants no work.
        check((granted||general)&&bool(one_source),"One-sided source unavailable or ownership denied");
        return dhwire::Reply{M{{"ok",true},{"result",one_source->connect(at(request,"qp"))},{"request_id",has(request,"request_id")?at(request,"request_id"):V()}}};
      }
      if(granted&&one_source&&optional<bool>(request,"one_sided",false)) {
        V value=M{{"accepted",true},{"one_sided_ready",true},{"directory",c->visits->value()}};
        return dhwire::Reply{M{{"ok",true},{"result",std::move(value)},{"request_id",has(request,"request_id")?at(request,"request_id"):V()}}};
      }
      if(granted&&read_workers) {
        const auto ready=stamp();const auto limit=get<size_t>(p,"batch_bytes");
        V rid=has(request,"request_id")?at(request,"request_id"):V();dhwire::Reply deferred;
        deferred.deferred=[this,c,ids,limit,ready,rid](auto deliver){
          auto task=[this,c,ids,limit,ready,rid,deliver]()mutable{
            dhwire::Reply out;try{const auto begun=stamp();auto value=read_records(ids,limit);
              value.map()["source_queue_ms"]=(begun-ready)/1e6;value.map()["directory"]=c->visits->value();
              out.message=M{{"ok",true},{"result",std::move(value)},{"request_id",rid}};
            }catch(const std::exception& e){out.message=M{{"ok",false},{"error",e.what()},{"request_id",rid}};}
            deliver(std::move(out));};
          if(!read_workers->submit(std::move(task))){++read_rejected;dhwire::Reply error;error.message=M{{"ok",true},{"result",M{{"accepted",false},{"reason","source_read_queue_full"}}},{"request_id",rid}};deliver(std::move(error));}
        };return deferred;
      }
      if(granted)result=read_records(ids,get<size_t>(p,"batch_bytes"));
      else result=M{{"accepted",false},{"reason","source_active_announced_or_reserved"}};
      result.map()["directory"]=c->visits->value();
    } else if(op=="peer_region_update"||op=="peer_region_reserve"){
      check(region_tasks,"Region task protocol disabled");
      check(get<std::string>(request,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(request,"signature")==signature,"Region protocol configuration mismatch");
      std::shared_ptr<QueryContext> c;
      if(concurrent_region_admission){
        {std::lock_guard<std::mutex> lock(mu);check(!stopping,"Peer runtime closed");c=context(request);}
        std::lock_guard<std::mutex> query_lock(c->admission_mu);
        c->visits->merge(at(request,"directory"),endpoints.size());
        if(op=="peer_region_reserve"){
          auto owner=get<std::string>(request,"owner");check(owner.rfind("replica:",0)==0&&owner.size()>8,"Source reservation requires replica owner");
          c->visits->reserve(sid,owner);
          if(c->actors.count(dhregion::base(sid)))c->visits->open(dhregion::base(sid));
        }
      }else if(op=="peer_region_reserve"){
        // First-start reservation is atomic with actor creation.
        std::lock_guard<std::mutex> lock(mu);check(!stopping,"Peer runtime closed");
        c=context(request);c->visits->merge(at(request,"directory"),endpoints.size());
        auto owner=get<std::string>(request,"owner");check(owner.rfind("replica:",0)==0&&owner.size()>8,"Source reservation requires replica owner");
        c->visits->reserve(sid,owner);
        if(c->actors.count(dhregion::base(sid)))c->visits->open(dhregion::base(sid));
      }else{
        {std::lock_guard<std::mutex> lock(mu);check(!stopping,"Peer runtime closed");c=context(request);}
        c->visits->merge(at(request,"directory"),endpoints.size());
      }
      result=M{{"directory",c->visits->value()}};
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
        if (op == "peer_cancel"){
          r->ticket->cancel();cancel_candidate(r);
        }
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
  V compute_metrics()const {
    auto q=workers->metrics();if(candidate_workers)q.map()["candidate_completion"]=candidate_workers->metrics();if(tail_workers){auto t=tail_workers->metrics();q.map()["main_workers"]=at(q,"workers");q.map()["tail"]=t;
      for(const char* k:{"workers","active","queued"})q.map()[k]=uint64_t(get<size_t>(q,k)+get<size_t>(t,k));}
    return q;
  }
  V pull_metrics()const {
    M out;{std::lock_guard<std::mutex> lock(pull_mu);size_t queued=pull_pending.size();for(const auto& q:pull_sources)queued+=q.size();out=M{{"enabled",async_pull},{"contexts",uint64_t(tail_active)},{"peak_contexts",uint64_t(pull_peak)},{"pending",uint64_t(queued)},{"inflight",vector(pull_inflight)}};}
    out["dedicated_dispatcher"]=threaded_pull_dispatch;out["queue_lock_calls"]=pull_lock_calls.load();out["queue_lock_wait_ns"]=pull_lock_wait_ns.load();out["queue_lock_hold_ns"]=pull_lock_hold_ns.load();
    out["dispatch_scan_steps"]=pull_scan_steps.load();out["dispatch_calls"]=pull_pump_calls.load();out["completion_callback_cpu_ns"]=pull_callback_cpu_ns.load();out["completion_callback_wall_ns"]=pull_callback_wall_ns.load();out["submit_cpu_ns"]=pull_submit_cpu_ns.load();
    out["one_sided_client"]=one_client?one_client->metrics():V();out["one_sided_source"]=one_source?one_source->metrics():V();out["one_sided_source_error"]=one_source_error;out["one_sided_control_rpcs"]=one_control_rpcs.load();out["one_sided_control_bytes"]=one_control_bytes.load();
    out["shared_compute"]=shared_compute;out["shared_workers"]=shared_compute?get<uint64_t>(workers->metrics(),"workers"):uint64_t(0);
    out["coalesce_enabled"]=coalesce;out["batch_rpcs"]=batch_rpcs.load();out["batch_queries"]=batch_queries.load();out["batch_multi"]=batch_multi.load();
    out["batch_tx_bytes"]=batch_tx.load();out["batch_rx_bytes"]=batch_rx.load();
    out["batch_unique_records"]=batch_unique_records.load();out["batch_logical_records"]=batch_logical_records.load();
    out["batch_unique_bytes"]=batch_unique_bytes.load();out["batch_logical_bytes"]=batch_logical_bytes.load();
    out["batch_union_failures"]=batch_union_failures.load();out["batch_source_cpu_ns"]=batch_source_cpu_ns.load();out["batch_gather_cpu_ns"]=batch_gather_cpu_ns.load();
    {std::lock_guard<std::mutex> lock(pull_mu);out["batch_reserved_bytes"]=uint64_t(coalesce_reserved);}
    out["capacity_rejections"]=tail_rejected.load();out["resumes"]=tail_resumes.load();out["read_rejections"]=read_rejected.load();
    out["compute_cpu_ns"]=tail_cpu_ns.load();out["compute_wall_ns"]=tail_wall_ns.load();
    out["main_help_max_tasks"]=uint64_t(help_limit);out["main_help_reserve_workers"]=uint64_t(help_reserve);out["main_help_queue_low"]=uint64_t(help_queue_low);
    out["main_help_active"]=help_active.load();out["main_help_peak_reserved"]=help_peak.load();
    out["main_help_resumes"]=help_resumes.load();out["main_help_declined"]=help_declined.load();
    out["main_help_cpu_ns"]=help_cpu_ns.load();out["main_help_wall_ns"]=help_wall_ns.load();
    out["tail_cpu_ids"]=vector(tail_cpus);out["pin_tail_threads"]=pin_tail;
    out["main_workers"]=get<uint64_t>(workers->metrics(),"workers");
    out["tail_workers"]=tail_workers?get<uint64_t>(tail_workers->metrics(),"workers"):uint64_t(0);
    out["source_read_workers"]=read_workers?get<uint64_t>(read_workers->metrics(),"workers"):uint64_t(0);
    if(tail_workers)out["compute"]=tail_workers->metrics();else if(shared_compute){auto q=workers->metrics();q.map()["scope"]="shared physical pool; active/queued here are tail tasks";q.map()["active"]=at(q,"tail_active");q.map()["queued"]=at(q,"tail_queued");out["compute"]=std::move(q);}if(read_workers)out["source_read"]=read_workers->metrics();if(pull_client)out["network"]=pull_client->metrics();return out;
  }
  V metrics() const {
    auto control = controller->snapshot();
    auto queue = compute_metrics();
    if(region_tasks){queue.map()["queued_region_executors"]=at(queue,"queued");queue.map()["queued"]=region_pending_grants.load();}
    M out;
    out["admissions"]=admission_metrics();
    A traces, events, transfers;
    std::vector<std::shared_ptr<Record>> selected;
    {
      std::lock_guard<std::mutex> l(mu);
      out["unacknowledged"] = uint64_t(active+admitting)+deferred_admissions.load();
      out["admission_inflight"] = uint64_t(admitting)+deferred_admissions.load();
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
    out["compute_queue"] = queue;out["pull_scheduler"]=pull_metrics();
    out["running"] = at(queue, "active");
    out["queued"] = at(queue, "queued");
    out["waiting_transfer"] = uint64_t(waiting);
    out["network_send"] = client->metrics();
    out["send_queues"] =
        M{{"transfer", M{{"queued", uint64_t(client->pending())},
                         {"active", uint64_t(0)}}}};
    if(visit_client){out["network_visits"]=visit_client->metrics();out["send_queues"].map()["region_visits"]=M{{"queued",uint64_t(visit_client->pending())},{"active",uint64_t(0)}};}
    out["execution_model"] = "cpp_runtime";
    out["compute_mode"] = async_pull?"native_resumable_pull":"native_fixed";
    out["candidate_sync"]=M{{"enabled",candidate_sync},{"sent",sync_sent.load()},{"received",sync_received.load()},{"budget_exhausted_candidates",sync_exhausted.load()},{"reactivated",sync_reactivated.load()},{"claims",sync_claims.load()},{"claim_conflicts",sync_claim_conflicts.load()}};
    out["region_tasks"]=M{{"enabled",region_tasks},{"created",region_created.load()},{"merged_grants",region_merged.load()},
      {"reactivated",region_reactivated.load()},{"reserved_skips",region_reserved_skips.load()},
      {"visit_rpcs",visit_rpcs.load()},{"visit_errors",visit_errors.load()},
      {"visit_request_bytes",visit_tx_bytes.load()},{"visit_response_bytes",visit_rx_bytes.load()}};
    out["io"] = M{{"actual_backend", "sync_pread"}};
    return out;
  }
  void close() {
    std::lock_guard<std::mutex> closing(close_mu);
    if (closed)
      return;
    {
      std::unique_lock<std::mutex> l(mu);
      stopping = true;
      wake.wait(l,[this]{return admitting==0;});
      for (auto &[q, r] : records)
        r->control->cancel();
    }
    wake.notify_all();
    if(start_workers)start_workers->close();
    if (maintenance.joinable())
      maintenance.join();
    if(reclaim_workers)reclaim_workers->close();
    {std::lock_guard<std::mutex> lock(pull_mu);pull_closing=true;}
    workers->drain();
    {std::vector<std::shared_ptr<Record>> pending;{std::lock_guard<std::mutex> lock(mu);for(auto& [key,r]:records)pending.push_back(r);}for(auto& r:pending)cancel_candidate(r);}
    {std::unique_lock<std::mutex> lock(pull_mu);pull_idle.wait(lock,[this]{return tail_active==0;});}
    {std::lock_guard<std::mutex> lock(pull_mu);coalesce_stop=true;}
    pull_wake.notify_all();if(pull_dispatcher.joinable())pull_dispatcher.join();
    if(read_workers)read_workers->close();
    demand_gate->close();
    if(one_client)one_client->close();
    if(tail_workers)tail_workers->close();
    workers->close();if(pull_client)pull_client->close();
    if(visit_client)visit_client->close();
    client->close();
    if(candidate_workers)candidate_workers->close();
    one_source.reset();
    controller->close();
    closed = true;
  }
};
DataRuntime::DataRuntime(V c, V m, std::string sig, SearchFunction fn, SearchPrepare prep, RecordRead rr,dhonesided::Export export_image)
    : impl_(std::make_unique<Impl>(std::move(c), std::move(m), std::move(sig),
                                   std::move(fn), std::move(prep), std::move(rr),std::move(export_image))) {}
DataRuntime::~DataRuntime() { close(); }
dhwire::Reply DataRuntime::handle(V r, uint64_t ns, size_t bytes) {
  return impl_->handle(std::move(r), ns, bytes);
}
V DataRuntime::metrics() const { return impl_->metrics(); }
V DataRuntime::network_metrics() const {A hist,ready_hist,executor_hist,actor_hist;for(auto& x:impl_->wait_hist)hist.emplace_back(x.load());for(auto& x:impl_->ready_wait_hist)ready_hist.emplace_back(x.load());for(auto& x:impl_->executor_wait_hist)executor_hist.emplace_back(x.load());for(auto& x:impl_->actor_wait_hist)actor_hist.emplace_back(x.load());auto queue=impl_->compute_metrics();
  if(impl_->region_tasks){queue.map()["queued_region_executors"]=at(queue,"queued");queue.map()["queued"]=impl_->region_pending_grants.load();}
  return M{{"candidate_sync",M{{"enabled",impl_->candidate_sync},{"sent",impl_->sync_sent.load()},{"received",impl_->sync_received.load()},{"budget_exhausted_candidates",impl_->sync_exhausted.load()},{"reactivated",impl_->sync_reactivated.load()},{"claims",impl_->sync_claims.load()},{"claim_conflicts",impl_->sync_claim_conflicts.load()}}},{"admissions",impl_->admission_metrics()},{"lifecycle_cleanup",M{{"contexts_retired",impl_->cleanup_contexts.load()},{"records_retired",impl_->cleanup_records.load()},{"async_reclaim",bool(impl_->reclaim_workers)},{"pending_reclaim_batches",impl_->reclaim_pending.load()},{"pending_expired_records",impl_->expiry_pending.load()},{"reclaim_destroy_ns",impl_->reclaim_destroy_ns.load()},{"registry_lock_max_ns",impl_->cleanup_lock_max_ns.load()},{"outside_lock_destroy_max_ns",impl_->cleanup_destroy_max_ns.load()}}},{"pull_scheduler",impl_->pull_metrics()},{"compute_cpu_ns",impl_->cpu_ns.load()},{"compute_wall_ns",impl_->work_ns.load()},{"compute_completed",impl_->completed_work.load()},{"compute_wait_hist_us_log2",hist},{"compute_ready_wait_hist_us_log2",ready_hist},{"compute_executor_wait_hist_us_log2",executor_hist},{"compute_actor_wait_hist_us_log2",actor_hist},{"ready_clock_fallbacks",impl_->ready_clock_fallbacks.load()},{"region_ready_grants",impl_->region_pending_grants.load()},{"compute_queue",queue},
  {"network_send",impl_->client->metrics()},{"report_control",impl_->controller->network_metrics()}};}
void DataRuntime::close() { impl_->close(); }
} // namespace dhruntime
