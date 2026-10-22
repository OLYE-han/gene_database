#include "native_network.hpp"
#include "native_control.hpp"
#include "search_control.hpp"
#include <pybind11/pybind11.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cmath>
#include <deque>
#include <mutex>
#include <queue>
#include <set>
#include <thread>
#include <tuple>
#include <time.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace py = pybind11;
namespace dhcontrol {
using V = dhwire::Value;
using M = V::Map;
using A = V::Array;
static uint64_t ns() { timespec t{};clock_gettime(CLOCK_MONOTONIC,&t);return uint64_t(t.tv_sec)*1000000000+t.tv_nsec; }
static double cpu_ms() { timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return t.tv_sec*1000.+t.tv_nsec/1e6; }
static double seconds() { return ns()/1e9; }
static double number(const V& v) {
  if(auto p=std::get_if<double>(&v.data))return *p;
  if(auto p=std::get_if<int64_t>(&v.data))return double(*p);
  if(auto p=std::get_if<uint64_t>(&v.data))return double(*p);
  throw std::invalid_argument("Expected numeric native controller field");
}
static double getnum(const M& m,const char* key,double fallback=0.) { auto i=m.find(key);return i==m.end()?fallback:number(i->second); }
static std::string str(const V& v) { return std::get<std::string>(v.data); }
static bool boolean(const V& v) { return std::get<bool>(v.data); }
static uint64_t tid() { return uint64_t(syscall(SYS_gettid)); }

struct Branch {
  std::weak_ptr<Engine> owner;
  M metadata, trace, peer;
  A children, events;
  std::shared_ptr<dhsearch::Control> control;
  std::shared_ptr<dhwire::Endpoint> endpoint;
  std::string key,status="queued",error;
  V report,pending_report;
  bool done=false,acked=false,inflight=false,cancelled=false,ready=false,expired=false;
  bool traced=false,ready_result=false,submitted_result=false;
  bool late_error=false,late_error_acked=false;
  size_t transfers=0;
  double created=0.,touched=0.,lease=0.,deadline=0.,next=0.,done_at=0.;
  double admission_ms=0.,executor_ms=0.,ready_ms=0.;
  uint64_t generation=0;
};
struct Timer {
  double due;uint64_t serial,generation;std::weak_ptr<Branch> branch;
  bool operator>(const Timer& other)const{return std::tie(due,serial)>std::tie(other.due,other.serial);}
};
struct Engine : std::enable_shared_from_this<Engine> {
  std::mutex queue_mu,state_mu,close_mu,updates_mu,registration_mu;
  // Only identity reservation and immutable endpoint caching use this mutex.
  // Never hold it while acquiring queue_mu/state_mu or performing DNS.
  std::set<std::string> registered_keys;
  std::map<std::string,std::weak_ptr<dhwire::Endpoint>> registration_endpoints;
  std::condition_variable wake;
  std::deque<std::function<void()>> commands;
  std::atomic<uint64_t> command_depth{0},command_peak{0};
  bool stopping=false,closed=false,register_release_gil=false,early_report=false,queued_registration=false;
  std::thread thread;
  std::map<std::string,std::shared_ptr<Branch>> branches;
  std::map<std::string,std::weak_ptr<dhwire::Endpoint>> endpoints;
  std::deque<std::shared_ptr<Branch>> results,regular;
  std::priority_queue<Timer,std::vector<Timer>,std::greater<Timer>> timers;
  A updates;
  size_t capacity=0,active=0,max_endpoints=0;
  uint64_t serial=0;
  double interval=0.,retention=0.,lease_seconds=0.,initial_delay=0.;
  M totals{{"sent",uint64_t(0)},{"errors",uint64_t(0)},{"queue_ms",0.},{"wall_ms",0.},
           {"connections_created",uint64_t(0)},{"connections_reused",uint64_t(0)}};
  M scheduler{{"iterations",uint64_t(0)},{"records_scanned",uint64_t(0)},{"selected",uint64_t(0)},
              {"no_capacity_iterations",uint64_t(0)},{"cpu_ms",0.},{"lock_wait_ms",0.},{"lock_held_ms",0.}};
  std::unique_ptr<dhnet::Client> client;

  static void add(M& m,const std::string& key,double value) { auto i=m.find(key);m[key]=(i==m.end()?0.:number(i->second))+value; }
  void queued_command() {
    const uint64_t depth=command_depth.fetch_add(1,std::memory_order_relaxed)+1;
    if(depth>command_peak.load(std::memory_order_relaxed))command_peak.store(depth,std::memory_order_relaxed);
  }
  bool enqueue(std::function<void()> f) {
    {std::lock_guard<std::mutex> lock(queue_mu);if(stopping)return false;commands.push_back(std::move(f));queued_command();}
    wake.notify_one();return true;
  }
  void timer(const std::shared_ptr<Branch>& b,double due) {
    timers.push({due,++serial,++b->generation,b});
  }
  void ready(const std::shared_ptr<Branch>& b) {
    if(b->expired||b->inflight||(b->acked&&(!b->late_error||b->late_error_acked)))return;
    if(!b->ready){b->ready=true;b->ready_result=b->done;(b->done?results:regular).push_back(b);}
    else if(b->done&&!b->ready_result){b->ready_result=true;results.push_back(b);}
  }
  void update(const std::shared_ptr<Branch>& b,const char* kind) {
    M m{{"query_request_id",b->metadata.at("query_request_id")},{"branch_id",b->metadata.at("branch_id")},
        {"kind",kind},{"status",b->status},{"acked",b->acked},{"touched",b->touched}};
    std::lock_guard<std::mutex> lock(updates_mu);updates.emplace_back(std::move(m));
  }
  void terminal(const std::shared_ptr<Branch>& b,std::string error) {
    if(b->expired||b->done)return;
    b->error=std::move(error);b->status="failed";b->done=true;b->pending_report=V();
    b->touched=seconds();ready(b);
  }
  void publish(const std::shared_ptr<Branch>& b) {
    if(b->done||b->expired||std::holds_alternative<std::monostate>(b->pending_report.data)||(!early_report&&b->transfers))return;
    if(b->control->cancelled()){terminal(b,"TimeoutError: Branch cancelled or expired before publication");return;}
    if(b->traced){b->trace["finalize_enter_ns"]=ns();b->trace["finalize_acquired_ns"]=ns();}
    auto report=std::move(b->pending_report);auto& r=report.map();auto& st=r.at("stats").map();
    const double now=seconds();
    st["query_admission_queue_ms"]=b->admission_ms;st["query_executor_start_wait_ms"]=b->executor_ms;
    st["query_ready_queue_ms"]=b->ready_ms;
    st["query_transfer_finalize_wait_ms"]=(now-b->done_at)*1000;
    st["query_branch_wall_ms"]=(now-b->created)*1000;
    st["query_state_other_ms"]=std::max(0.,getnum(st,"query_branch_wall_ms")-
      getnum(st,"query_ready_queue_ms")-getnum(st,"query_compute_slice_wall_ms")-
      getnum(st,"query_io_wait_ms")-getnum(st,"query_transfer_finalize_wait_ms"));
    r["peer_transfer"]=b->peer;
    r["transfer_metrics_complete"]=b->transfers==0;
    r["pending_transfers_at_publication"]=uint64_t(b->transfers);
    b->report=std::move(report);b->status="complete";b->done=true;b->touched=now;
    if(b->traced)b->trace["report_ready_ns"]=ns();
    ready(b);
  }
  void received(const std::shared_ptr<Branch>& b,dhwire::Result result,bool terminal_message,size_t event_index,double selected,bool late_notification) {
    if(!b->inflight)return;
    b->inflight=false;--active;
    bool success=false;
    try {
      if(!result.ok)throw std::runtime_error(result.error);
      const auto& reply=result.response.map();
      if(!boolean(reply.at("ok")))throw std::runtime_error("Head rejected native control message");
      const double lease_ms=number(reply.at("lease_ms"));
      if(!std::isfinite(lease_ms)||lease_ms<=0)throw std::runtime_error("Invalid head lease");
      const bool cancelled=boolean(reply.at("cancelled"));
      b->lease=seconds()+lease_ms/1000.;b->control->renew(b->lease);
      b->cancelled|=cancelled;if(b->cancelled)b->control->cancel();
      if(late_notification)b->late_error_acked=true;
      else if(terminal_message&&!b->acked){b->acked=true;update(b,"acked");}
      if(b->cancelled&&!b->done)terminal(b,"TimeoutError: Branch cancelled by head before publication");
      success=true;
    } catch(const std::exception&) {}
    const double now=seconds();
    add(totals,"sent",1);add(totals,"errors",!success);add(totals,"queue_ms",(result.send_start_ns?result.send_start_ns/1e9-selected:0.)*1000);
    add(totals,"wall_ms",(now-selected)*1000);
    add(totals,result.connection_created?"connections_created":"connections_reused",1);
    if(b->traced) {
      M counts{{"request_bytes",result.request_bytes},{"response_bytes",result.response_bytes},{"rpc_count",uint64_t(1)},
        {"connections_created",uint64_t(result.connection_created)},{"connections_reused",uint64_t(!result.connection_created)},
        {"pool_wait_ms",result.send_start_ns>=result.queued_ns?(result.send_start_ns-result.queued_ns)/1e6:0.},
        {"connect_ms",result.connect_done_ns>=result.connect_start_ns?(result.connect_done_ns-result.connect_start_ns)/1e6:0.},
        {"send_ms",result.send_done_ns>=result.send_start_ns?(result.send_done_ns-result.send_start_ns)/1e6:0.},
        {"response_wait_ms",result.done_ns>=result.send_done_ns&&result.send_done_ns?(result.done_ns-result.send_done_ns)/1e6:0.}};
      auto& ev=b->events.at(event_index).map();ev["sender_rpc_return_ns"]=result.done_ns;ev["rpc_counts"]=std::move(counts);
      ev["succeeded"]=success;ev["sender_enter_ns"]=result.send_start_ns?result.send_start_ns:uint64_t(selected*1e9);
    }
    b->next=now+interval;
    if(b->late_error&&!b->late_error_acked){if(late_notification)timer(b,b->next);else ready(b);return;}
    if(b->acked){timer(b,b->touched+retention);return;}
    // Completion arriving during a heartbeat must bypass heartbeat delay.
    if(b->done&&!terminal_message)ready(b);
    else timer(b,b->next);
  }
  void dispatch() {
    while(active<capacity&&(!results.empty()||!regular.empty())) {
      const bool from_result=!results.empty();auto& queue=from_result?results:regular;
      auto b=queue.front();queue.pop_front();
      if(!b->ready||(!from_result&&b->ready_result))continue;
      b->ready=false;b->ready_result=false;
      if(b->expired||b->inflight||(b->acked&&(!b->late_error||b->late_error_acked)))continue;
      const bool late_notification=b->late_error&&!b->late_error_acked;
      const bool terminal_message=b->done;
      A reports;if(!late_notification&&!std::holds_alternative<std::monostate>(b->report.data))reports.push_back(b->report);
      std::set<uint64_t> nodes;nodes.insert(uint64_t(getnum(b->metadata,"entry_node")));nodes.insert(uint64_t(getnum(b->metadata,"target_node")));
      for(const auto& ch:b->children)nodes.insert(uint64_t(getnum(ch.map(),"node_id")));
      for(const auto& v:std::get<A>(b->metadata.at("visited_nodes").data))nodes.insert(uint64_t(number(v)));
      A involved;for(auto n:nodes)involved.emplace_back(n);
      M message{{"query_request_id",b->metadata.at("query_request_id")},{"token",b->metadata.at("token")},
        {"branch_id",b->metadata.at("branch_id")},{"node_id",b->metadata.at("target_node")},
        {"status",b->status},{"involved_nodes",std::move(involved)},{"reports",std::move(reports)},
        {"children",b->children},{"transfer_failure",late_notification},
        {"error",b->error.empty()?V():V(b->error)}};
      const auto selected=seconds();const size_t index=b->events.size();
      std::weak_ptr<Engine> weak=shared_from_this();
      bool accepted=false;
      try { accepted=client->submit(*b->endpoint,V(std::move(message)),[weak,b,terminal_message,index,selected,late_notification](dhwire::Result result){
        if(auto e=weak.lock())e->enqueue([e,b,result=std::move(result),terminal_message,index,selected,late_notification]()mutable{
          e->received(b,std::move(result),terminal_message,index,selected,late_notification);
        });
      }); } catch(const std::exception& exc) {
        // A never-submitted report has no externally visible identity yet.
        // Replace an oversize/invalid report by an explicit bounded error;
        // never mutate a report after a possibly successful transmission.
        if(!b->submitted_result) {
          b->report=V();b->pending_report=V();b->done=true;b->status="failed";
          b->error=(std::string("Native control encode error: ")+exc.what()).substr(0,256);
          b->touched=seconds();
        }
        add(scheduler,"encoding_errors",1);
        b->next=seconds()+interval;timer(b,b->next);continue;
      }
      if(!accepted){ready(b);return;}
      b->inflight=true;++active;++b->generation;add(scheduler,"selected",1);
      b->submitted_result|=terminal_message;
      if(b->traced){b->trace["control_selected_ns"]=ns();if(terminal_message)b->trace["control_result_selected_ns"]=ns();
        b->events.emplace_back(M{{"queued_ns",uint64_t(selected*1e9)},{"sender_enter_ns",ns()},
          {"control_thread_id",tid()},{"message_ready_ns",ns()},{"kind",late_notification?"transfer_failure":terminal_message?"result":"heartbeat"}});}
    }
  }
  void timers_due() {
    const double now=seconds();
    while(!timers.empty()&&timers.top().due<=now) {
      auto item=timers.top();timers.pop();auto b=item.branch.lock();
      if(!b||b->expired||item.generation!=b->generation)continue;
      if(b->done&&!b->inflight) {
        if(b->transfers){timer(b,now+interval);ready(b);continue;}
        double expiry=b->touched+retention;if(!b->acked||(b->late_error&&!b->late_error_acked))expiry=std::max(expiry,b->deadline);
        if(now>=expiry){b->expired=true;branches.erase(b->key);
          if(queued_registration){std::lock_guard<std::mutex> l(registration_mu);registered_keys.erase(b->key);}
          update(b,"expired");continue;}
        if(b->acked&&(!b->late_error||b->late_error_acked)){timer(b,expiry);continue;}
      }
      ready(b);
    }
  }
  void run() {
    for(;;) {
      std::function<void()> command;
      {
        std::unique_lock<std::mutex> lock(queue_mu);
        if(stopping)break;
        if(commands.empty()) {
          double wait=-1.;{std::lock_guard<std::mutex> state(state_mu);if(!timers.empty())wait=std::max(0.,timers.top().due-seconds());}
          if(wait<0)wake.wait(lock,[&]{return stopping||!commands.empty();});
          else wake.wait_for(lock,std::chrono::duration<double>(wait),[&]{return stopping||!commands.empty();});
        }
        if(stopping)break;
        if(!commands.empty()){command=std::move(commands.front());commands.pop_front();command_depth.fetch_sub(1,std::memory_order_relaxed);}
      }
      const double before_cpu=cpu_ms(),before_lock=seconds();
      std::lock_guard<std::mutex> state(state_mu);
      const double acquired=seconds();
      try {if(command)command();timers_due();dispatch();add(scheduler,"iterations",1);}
      catch(const std::exception& error){add(scheduler,"event_errors",1);scheduler["last_event_error"]=error.what();}
      add(scheduler,"lock_wait_ms",(acquired-before_lock)*1000.);
      add(scheduler,"lock_held_ms",(seconds()-acquired)*1000.);
      add(scheduler,"cpu_ms",cpu_ms()-before_cpu);
    }
  }
  void close() {
    std::lock_guard<std::mutex> closing(close_mu);
    {std::lock_guard<std::mutex> lock(queue_mu);if(closed)return;stopping=true;}wake.notify_all();
    if(thread.joinable())thread.join();
    client->close();
    std::lock_guard<std::mutex> state(state_mu);
    // Stop network first; drain ownership events without dispatching new RPCs.
    // Fence publication before consuming a completion queued just before close.
    for(auto& item:branches){item.second->control->cancel();item.second->cancelled=true;}
    while(!commands.empty()){auto f=std::move(commands.front());commands.pop_front();command_depth.fetch_sub(1,std::memory_order_relaxed);try{f();}catch(...) {}}
    for(auto& item:branches){auto& b=item.second;b->control->cancel();b->cancelled=true;
      if(!b->done)terminal(b,"RuntimeError: Native controller closed");
      b->inflight=false;}
    active=0;closed=true;
  }
};

Ticket::Ticket(std::weak_ptr<Engine> e,std::shared_ptr<Branch> b):engine_(std::move(e)),branch_(std::move(b)){}
void Ticket::start(double a,double x,double r) { if(auto e=engine_.lock()){auto b=branch_;e->enqueue([b,a,x,r]{if(b->done)return;b->status="running";b->admission_ms=a;b->executor_ms=x;b->ready_ms=r;});} }
void Ticket::child(V descriptor) {if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b,d=std::move(descriptor)]()mutable{if(b->done)return;b->children.push_back(std::move(d));++b->transfers;e->ready(b);});}}
void Ticket::transfer_done(V metrics,std::string error) {if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b,m=std::move(metrics),error=std::move(error)]{
  if(b->transfers)--b->transfers;
  b->touched=seconds();
  for(const auto& item:m.map()) {if(item.first.rfind("peer_",0)==0)Engine::add(b->peer,item.first,number(item.second));}
  if(!error.empty()) {
    if(e->early_report&&b->done&&!std::holds_alternative<std::monostate>(b->report.data)) {
      // Already published results are immutable; report a separate diagnostic.
      b->late_error=true;b->error=error;b->touched=seconds();e->ready(b);
    } else e->terminal(b,error);
  } else e->publish(b);
});}}
void Ticket::complete(V report) {const auto stamp=ns();if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b,report=std::move(report),stamp]()mutable{
  if(b->done)return;
  b->done_at=stamp/1e9;b->pending_report=std::move(report);b->status=e->early_report?"report_pending":"waiting_transfer";
  try {
  if(b->traced){auto timestamps=b->pending_report.map().find("native_timestamps");
    if(timestamps!=b->pending_report.map().end())for(const auto& t:timestamps->second.map())b->trace[t.first]=t.second;
    b->trace["local_search_done_ns"]=stamp;b->trace["local_done_enqueued_ns"]=stamp;b->trace["local_done_dequeued_ns"]=ns();b->trace["local_done_consumed_ns"]=ns();b->trace["completion_publish_path"]="native";}
  e->publish(b);
  } catch(const std::exception& error) {e->terminal(b,std::string("Native completion error: ")+error.what());}
});}}
void Ticket::fail(std::string error) {if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b,error=std::move(error)]{e->terminal(b,error);});}}
void Ticket::renew(double lease) {branch_->control->renew(lease);if(auto e=engine_.lock()){auto b=branch_;e->enqueue([b,lease]{b->lease=lease;});}}
void Ticket::cancel() {branch_->control->cancel();if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b]{b->cancelled=true;if(!b->done)e->terminal(b,"TimeoutError: Branch cancelled before publication");});}}

static std::shared_ptr<Engine> make_engine(const V& c) {
  const auto& cfg=c.map();const auto& p=cfg.at("peer").map();const auto& comm=p.at("communication").map();
  const auto& net=cfg.at("network").map();auto e=std::make_shared<Engine>();
  auto nc=cfg.find("native_runtime");if(nc!=cfg.end()){auto x=nc->second.map().find("early_parent_report");if(x!=nc->second.map().end())e->early_report=boolean(x->second);}
  if(nc!=cfg.end()){auto x=nc->second.map().find("queued_control_registration");if(x!=nc->second.map().end())e->queued_registration=boolean(x->second);}
  auto locking=p.find("locking");
  if(locking!=p.end())e->register_release_gil=boolean(locking->second.map().at("native_register_release_gil"));
  e->capacity=size_t(getnum(comm,"max_pending_control"));e->interval=getnum(p,"heartbeat_interval_ms")/1000.;
  e->retention=getnum(p,"retention_seconds");e->lease_seconds=getnum(p,"lease_timeout_ms")/1000.;
  e->initial_delay=getnum(p.at("control").map(),"initial_heartbeat_delay_ms")/1000.;
  if(!e->capacity||e->interval<=0||e->retention<=0)throw std::invalid_argument("Invalid native control capacity/timing");
  dhwire::Config wire;wire.max_pending=e->capacity;wire.max_connections_per_endpoint=size_t(getnum(comm,"connections_per_endpoint"));
  wire.max_endpoints=size_t(getnum(comm,"max_connection_endpoints"));wire.max_message_bytes=size_t(getnum(net,"max_message_bytes"));
  e->max_endpoints=wire.max_endpoints;
  wire.timeout_ms=uint64_t(getnum(p,"control_timeout_seconds")*1000.);e->client=std::make_unique<dhnet::Client>(wire,c);
  e->thread=std::thread([e]{e->run();});
  return e;
}

// A branch never migrates: each lane owns its state mutex and wire reactor.
struct Fleet {
  std::mutex mu,close_mu;
  std::condition_variable wake;
  std::vector<std::shared_ptr<Engine>> lanes;
  std::map<std::string,std::weak_ptr<Branch>> directory;
  V config;
  std::thread monitor;
  bool stopping=false;
  size_t minimum=1,maximum=1,queue_high=4,pending_high=4,high_samples=2;
  uint64_t poll_ms=20,cooldown_ms=100,idle_ms=1000;
  size_t rr=0,up=0,down=0,peak=1;
  M retired_totals,retired_scheduler;
  A events,retired_updates;
  explicit Fleet(V c,std::shared_ptr<Engine> first):config(std::move(c)) {
    const auto& a=config.map().at("native_runtime").map().at("adaptive_control").map();
    minimum=size_t(getnum(a,"min_lanes"));maximum=size_t(getnum(a,"max_lanes"));
    queue_high=size_t(getnum(a,"command_queue_high"));pending_high=size_t(getnum(a,"network_pending_high"));
    high_samples=size_t(getnum(a,"high_samples"));poll_ms=uint64_t(getnum(a,"sample_interval_ms"));
    cooldown_ms=uint64_t(getnum(a,"cooldown_ms"));idle_ms=uint64_t(getnum(a,"idle_ms"));
    if(!minimum||maximum<minimum||maximum>64||!queue_high||!pending_high||!high_samples||!poll_ms||!cooldown_ms||!idle_ms)
      throw std::invalid_argument("Invalid adaptive control limits");
    lanes.push_back(std::move(first));
    try {for(size_t i=1;i<minimum;++i)lanes.push_back(make_engine(config));}
    catch(...) {for(auto& e:lanes)e->close();throw;}
    peak=lanes.size();
  }
  static void accumulate(M& out,const M& values) {
    for(const auto& [k,v]:values)if(std::holds_alternative<double>(v.data)||std::holds_alternative<uint64_t>(v.data)||std::holds_alternative<int64_t>(v.data))Engine::add(out,k.c_str(),number(v));
  }
  void event(const char* reason) {
    events.emplace_back(M{{"time_ns",ns()},{"reason",reason},{"lanes",uint64_t(lanes.size())}});
    if(events.size()>128)events.erase(events.begin());
  }
  void run() {
    size_t high=0;uint64_t changed=ns(),idle_since=0,pruned=0;
    std::unique_lock<std::mutex> l(mu);
    while(!wake.wait_for(l,std::chrono::milliseconds(poll_ms),[&]{return stopping;})) {
      bool pressure=false;size_t depth=0,pending=0;
      for(auto& e:lanes){auto d=e->command_depth.load();auto p=e->client->pending();depth+=d;pending+=p;pressure|=d>=queue_high||p>=pending_high;}
      high=pressure?high+1:0;const auto now=ns();
      if(depth||pending)idle_since=0;else if(!idle_since)idle_since=now;
      if(high>=high_samples&&lanes.size()<maximum&&now-changed>=cooldown_ms*1000000) {
        try {lanes.push_back(make_engine(config));++up;peak=std::max(peak,lanes.size());event("queue_pressure");changed=now;high=0;}
        catch(const std::exception&){event("scale_up_failed");changed=now;high=0;}
      }
      // Retained identities and timers stay with their original lane. Reclaim
      // only after retention expires; never drop live queries to shrink.
      if(idle_since&&now-idle_since>=idle_ms*1000000&&lanes.size()>minimum) {
        for(size_t i=lanes.size();i-->minimum;) {
          auto e=lanes[i];bool empty;
          {std::lock_guard<std::mutex> state(e->state_mu);empty=e->branches.empty()&&!e->active&&!e->command_depth.load()&&!e->client->pending();
            if(empty){accumulate(retired_totals,e->totals);accumulate(retired_scheduler,e->scheduler);}}
          if(empty) {
            e->close();{std::lock_guard<std::mutex> u(e->updates_mu);for(auto& v:e->updates)retired_updates.push_back(std::move(v));e->updates.clear();}
            lanes.erase(lanes.begin()+i);++down;event("idle_after_retention");changed=now;
          }
        }
      }
      if(now-pruned>=idle_ms*1000000){for(auto it=directory.begin();it!=directory.end();)if(it->second.expired())it=directory.erase(it);else ++it;pruned=now;}
    }
  }
  std::shared_ptr<Engine> choose(const std::string& key) { // mu held through registration
    auto it=directory.find(key);
    if(it!=directory.end())if(auto b=it->second.lock())if(auto e=b->owner.lock())return e;
    size_t best=rr++%lanes.size(),load=lanes[best]->command_depth.load()+lanes[best]->client->pending();
    for(size_t i=0;i<lanes.size();++i){size_t x=lanes[i]->command_depth.load()+lanes[i]->client->pending();if(x<load){best=i;load=x;}}
    return lanes[best];
  }
  void close() {
    std::lock_guard<std::mutex> closing(close_mu);
    {std::lock_guard<std::mutex> l(mu);stopping=true;}wake.notify_all();
    if(monitor.joinable())monitor.join();
    for(auto& e:lanes)e->close();
  }
};
Controller::Controller(py::dict c):Controller(dhwire::from_python(c)) {}
Controller::Controller(const V& c):engine_(make_engine(c)) {
  try {
    auto n=c.map().find("native_runtime");
    if(n!=c.map().end()) {auto a=n->second.map().find("adaptive_control");
      if(a!=n->second.map().end()&&boolean(a->second.map().at("enabled"))) {
        fleet_=std::make_shared<Fleet>(c,engine_);auto f=fleet_;
        f->monitor=std::thread([f]{f->run();});
      }
    }
  } catch(...) {if(fleet_)fleet_->close();else engine_->close();throw;}
}
Controller::~Controller(){close();}
std::shared_ptr<Ticket> Controller::register_branch(py::dict metadata,std::shared_ptr<dhsearch::Control> control) {
  auto values=dhwire::from_python(metadata);
  // All Python conversion is complete. No Python API or object destructor may
  // run until this guard restores the GIL, including exceptional returns.
  std::unique_ptr<py::gil_scoped_release> released;
  if(engine_->register_release_gil)released=std::make_unique<py::gil_scoped_release>();
  return register_value(std::move(values),std::move(control));
}
std::shared_ptr<Ticket> Controller::register_value(V metadata,std::shared_ptr<dhsearch::Control> control) {
  auto b=std::make_shared<Branch>();b->metadata=std::move(metadata.map());b->control=std::move(control);
  std::unique_lock<std::mutex> fleet_lock;
  if(fleet_)fleet_lock=std::unique_lock<std::mutex>(fleet_->mu);
  if(fleet_&&fleet_->stopping)throw std::runtime_error("Native controller is closed");
  const auto register_started=ns();uint64_t state_wait=0,queue_wait=0,held=0;
  b->key=str(b->metadata.at("query_request_id"))+"\n"+str(b->metadata.at("branch_id"));
  auto e=fleet_?fleet_->choose(b->key):engine_;b->owner=e;
  b->created=getnum(b->metadata,"created_ns",double(ns()))/1e9;b->touched=b->created;
  b->lease=getnum(b->metadata,"lease",seconds()+e->lease_seconds);b->deadline=getnum(b->metadata,"deadline",b->lease);
  auto trace=b->metadata.find("trace");if(trace==b->metadata.end())trace=b->metadata.find("tracebool");
  b->traced=trace!=b->metadata.end()&&boolean(trace->second);
  if(b->traced)b->trace["accepted_ns"]=uint64_t(getnum(b->metadata,"accepted_ns",double(ns())));
  b->peer={{"peer_rpc_count",uint64_t(0)},{"peer_request_bytes",uint64_t(0)},{"peer_response_bytes",uint64_t(0)},{"peer_transfer_errors",uint64_t(0)}};
  auto endpoint=std::get<A>(b->metadata.at("head_endpoint").data);const auto host=str(endpoint.at(0));const auto port=uint16_t(number(endpoint.at(1)));
  const auto key=host+":"+std::to_string(port);
  if(e->queued_registration) {
    // Reserve synchronously (duplicates remain immediate errors), then publish
    // a registration command before returning the Ticket. All Ticket commands
    // therefore follow registration in the same FIFO, without waiting for the
    // controller's state mutex on the data admission path.
    {
      std::lock_guard<std::mutex> l(e->registration_mu);
      auto it=e->registration_endpoints.find(key);
      if(it!=e->registration_endpoints.end())b->endpoint=it->second.lock();
    }
    if(!b->endpoint)b->endpoint=std::make_shared<dhwire::Endpoint>(host,port);
    auto ticket=std::make_shared<Ticket>(e,b);
    {
      std::lock_guard<std::mutex> l(e->registration_mu);
      if(!e->registered_keys.insert(b->key).second)
        throw std::invalid_argument("Duplicate native control branch");
    }
    try {
      {
        std::lock_guard<std::mutex> l(e->registration_mu);
        for(auto it=e->registration_endpoints.begin();it!=e->registration_endpoints.end();)
          if(it->second.expired())it=e->registration_endpoints.erase(it);else ++it;
        if(e->registration_endpoints.count(key)||e->registration_endpoints.size()<e->max_endpoints)
          e->registration_endpoints[key]=b->endpoint;
      }
      const auto enter=ns();std::lock_guard<std::mutex> queue(e->queue_mu);
      const auto acquired=ns();
      if(e->stopping)throw std::runtime_error("Native controller is closed");
      if(b->traced) {
        b->trace["native_register_start_ns"]=register_started;
        b->trace["native_register_end_ns"]=acquired;
        b->trace["native_register_state_wait_ns"]=uint64_t(0);
        b->trace["native_register_queue_wait_ns"]=acquired-enter;
        b->trace["native_register_lock_held_ns"]=uint64_t(0);
        b->trace["native_register_enqueued_ns"]=ns();
      }
      e->commands.push_back([e,b]{
        if(b->traced)b->trace["native_register_consumed_ns"]=ns();
        e->branches.emplace(b->key,b);
        if(e->initial_delay>0)e->timer(b,seconds()+e->initial_delay);else e->ready(b);
      });
      e->queued_command();
    } catch(...) {
      std::lock_guard<std::mutex> l(e->registration_mu);e->registered_keys.erase(b->key);throw;
    }
    if(fleet_)fleet_->directory[b->key]=b;
    e->wake.notify_one();
    return ticket;
  }
  {const auto enter=ns();std::lock_guard<std::mutex> state(e->state_mu);const auto acquired=ns();state_wait+=acquired-enter;
    auto it=e->endpoints.find(key);if(it!=e->endpoints.end())b->endpoint=it->second.lock();held+=ns()-acquired;}
  // DNS is configuration work. It must never hold the control state mutex.
  if(!b->endpoint)b->endpoint=std::make_shared<dhwire::Endpoint>(host,port);
  {
    // Same queue -> state order as the consumer's wait setup. Reservation is
    // synchronous so duplicate identity/close cannot disappear as loop errors.
    const auto queue_enter=ns();std::lock_guard<std::mutex> queue(e->queue_mu);queue_wait+=ns()-queue_enter;
    if(e->stopping)throw std::runtime_error("Native controller is closed");
    const auto state_enter=ns();std::lock_guard<std::mutex> state(e->state_mu);const auto acquired=ns();state_wait+=acquired-state_enter;
    if(e->branches.count(b->key))throw std::invalid_argument("Duplicate native control branch");
    for(auto it=e->endpoints.begin();it!=e->endpoints.end();) {
      if(it->second.expired())it=e->endpoints.erase(it);else ++it;
    }
    if(e->endpoints.count(key)||e->endpoints.size()<e->max_endpoints)e->endpoints[key]=b->endpoint;
    e->branches.emplace(b->key,b);
    e->commands.push_back([e,b]{if(e->initial_delay>0)e->timer(b,seconds()+e->initial_delay);else e->ready(b);});
    e->queued_command();
    if(b->traced){b->trace["native_register_start_ns"]=register_started;b->trace["native_register_end_ns"]=ns();
      b->trace["native_register_state_wait_ns"]=state_wait;b->trace["native_register_queue_wait_ns"]=queue_wait;
      b->trace["native_register_lock_held_ns"]=held+ns()-acquired;}
  }
  if(fleet_)fleet_->directory[b->key]=b;
  e->wake.notify_one();
  return std::make_shared<Ticket>(e,b);
}
static V snapshot_engine(const std::shared_ptr<Engine>& e,bool include_reports) {
 std::lock_guard<std::mutex> state(e->state_mu);A out;
  for(const auto& item:e->branches){const auto& b=item.second;M row=b->peer;
    row["query_request_id"]=b->metadata.at("query_request_id");row["branch_id"]=b->metadata.at("branch_id");
    row["node_id"]=b->metadata.at("target_node");row["status"]=b->status;row["acked"]=b->acked;
    row["pending_transfers"]=uint64_t(b->transfers);row["late_error_acked"]=b->late_error_acked;
    row["touched"]=b->touched;row["lease"]=b->lease;row["cancelled"]=b->cancelled;
    if(include_reports)row["report"]=b->report;
    row["error"]=b->error.empty()?V():V(b->error);
    row["execution_timestamps"]=b->trace;row["control_events"]=b->events;out.emplace_back(std::move(row));}
  return M{{"branches",std::move(out)},{"control_totals",e->totals},{"control_scheduler_totals",e->scheduler},
           {"control_active",uint64_t(e->active)},{"command_queue_depth",e->command_depth.load(std::memory_order_relaxed)},
           {"command_queue_peak",e->command_peak.load(std::memory_order_relaxed)},
           {"network_send",e->client->metrics()},{"client_pending",uint64_t(e->client->pending())},{"retained_branches",uint64_t(e->branches.size())}};
}
V Controller::snapshot(bool include_reports)const {
  if(!fleet_)return snapshot_engine(engine_,include_reports);
  std::lock_guard<std::mutex> l(fleet_->mu);
  M out{{"branches",A{}},{"control_totals",fleet_->retired_totals},{"control_scheduler_totals",fleet_->retired_scheduler}};
  A lane_stats;
  for(auto& e:fleet_->lanes) {
    auto row=snapshot_engine(e,include_reports).map();
    for(auto& b:std::get<A>(row.at("branches").data))std::get<A>(out.at("branches").data).push_back(std::move(b));
    for(const char* k:{"control_totals","control_scheduler_totals"})Fleet::accumulate(out.at(k).map(),row.at(k).map());
    for(const char* k:{"control_active","command_queue_depth","command_queue_peak","client_pending","retained_branches"})Engine::add(out,k,number(row.at(k)));
    row.erase("branches");lane_stats.emplace_back(std::move(row));
  }
  out["adaptive_control"]=M{{"lanes",uint64_t(fleet_->lanes.size())},{"peak_lanes",uint64_t(fleet_->peak)},
    {"scale_ups",uint64_t(fleet_->up)},{"scale_downs",uint64_t(fleet_->down)},{"events",fleet_->events},{"lane_stats",std::move(lane_stats)}};
  return out;
}
V Controller::take_updates(){
  std::unique_lock<std::mutex> l;if(fleet_)l=std::unique_lock<std::mutex>(fleet_->mu);
  A out;if(fleet_)out.swap(fleet_->retired_updates);auto collect=[&](const std::shared_ptr<Engine>& e){std::lock_guard<std::mutex> lock(e->updates_mu);for(auto& v:e->updates)out.push_back(std::move(v));e->updates.clear();};
  if(fleet_)for(auto& e:fleet_->lanes)collect(e);else collect(engine_);
  return out;
}
void Controller::close(){if(fleet_)fleet_->close();else if(engine_)engine_->close();}
void register_native_control(py::module_& m) {
  py::class_<Ticket,std::shared_ptr<Ticket>>(m,"ControlTicket")
    .def("start",&Ticket::start).def("child",[](Ticket& t,py::object d){t.child(dhwire::from_python(d));})
    .def("transfer_done",[](Ticket& t,py::object m,const std::string& e){t.transfer_done(dhwire::from_python(m),e);},py::arg("metrics"),py::arg("error")="")
    .def("complete",[](Ticket& t,py::object r){t.complete(dhwire::from_python(r));})
    .def("fail",&Ticket::fail).def("renew",&Ticket::renew).def("cancel",&Ticket::cancel);
  py::class_<Controller,std::shared_ptr<Controller>>(m,"NativeController")
    .def(py::init<py::dict>()).def("register_branch",&Controller::register_branch)
    .def("snapshot",[](Controller& c,bool include_reports){V v;{py::gil_scoped_release release;v=c.snapshot(include_reports);}return dhwire::to_python(v);},py::arg("include_reports")=false)
    .def("take_updates",[](Controller& c){V v;{py::gil_scoped_release release;v=c.take_updates();}return dhwire::to_python(v);})
    .def("close",&Controller::close,py::call_guard<py::gil_scoped_release>());
}
} // namespace dhcontrol
