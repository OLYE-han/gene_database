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
  M metadata, trace, peer;
  A children, events;
  std::shared_ptr<dhsearch::Control> control;
  std::shared_ptr<dhwire::Endpoint> endpoint;
  std::string key,status="queued",error;
  V report,pending_report;
  bool done=false,acked=false,inflight=false,cancelled=false,ready=false,expired=false;
  bool traced=false,ready_result=false,submitted_result=false;
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
  std::mutex queue_mu,state_mu,close_mu,updates_mu;
  std::condition_variable wake;
  std::deque<std::function<void()>> commands;
  std::atomic<uint64_t> command_depth{0},command_peak{0};
  bool stopping=false,closed=false;
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
  std::unique_ptr<dhwire::Client> client;

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
    if(b->expired||b->acked||b->inflight)return;
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
    if(b->done||b->expired||std::holds_alternative<std::monostate>(b->pending_report.data)||b->transfers)return;
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
    b->report=std::move(report);b->status="complete";b->done=true;b->touched=now;
    if(b->traced)b->trace["report_ready_ns"]=ns();
    ready(b);
  }
  void received(const std::shared_ptr<Branch>& b,dhwire::Result result,bool terminal_message,size_t event_index,double selected) {
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
      if(terminal_message&&!b->acked){b->acked=true;update(b,"acked");}
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
      if(b->expired||b->acked||b->inflight)continue;
      const bool terminal_message=b->done;
      A reports;if(!std::holds_alternative<std::monostate>(b->report.data))reports.push_back(b->report);
      std::set<uint64_t> nodes;nodes.insert(uint64_t(getnum(b->metadata,"entry_node")));nodes.insert(uint64_t(getnum(b->metadata,"target_node")));
      for(const auto& ch:b->children)nodes.insert(uint64_t(getnum(ch.map(),"node_id")));
      for(const auto& v:std::get<A>(b->metadata.at("visited_nodes").data))nodes.insert(uint64_t(number(v)));
      A involved;for(auto n:nodes)involved.emplace_back(n);
      M message{{"query_request_id",b->metadata.at("query_request_id")},{"token",b->metadata.at("token")},
        {"branch_id",b->metadata.at("branch_id")},{"node_id",b->metadata.at("target_node")},
        {"status",b->status},{"involved_nodes",std::move(involved)},{"reports",std::move(reports)},
        {"error",b->error.empty()?V():V(b->error)}};
      const auto selected=seconds();const size_t index=b->events.size();
      std::weak_ptr<Engine> weak=shared_from_this();
      bool accepted=false;
      try { accepted=client->submit(*b->endpoint,V(std::move(message)),[weak,b,terminal_message,index,selected](dhwire::Result result){
        if(auto e=weak.lock())e->enqueue([e,b,result=std::move(result),terminal_message,index,selected]()mutable{
          e->received(b,std::move(result),terminal_message,index,selected);
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
          {"control_thread_id",tid()},{"message_ready_ns",ns()},{"kind",terminal_message?"result":"heartbeat"}});}
    }
  }
  void timers_due() {
    const double now=seconds();
    while(!timers.empty()&&timers.top().due<=now) {
      auto item=timers.top();timers.pop();auto b=item.branch.lock();
      if(!b||b->expired||item.generation!=b->generation)continue;
      if(b->done&&!b->inflight) {
        double expiry=b->touched+retention;if(!b->acked)expiry=std::max(expiry,b->deadline);
        if(now>=expiry){b->expired=true;branches.erase(b->key);update(b,"expired");continue;}
        if(b->acked){timer(b,expiry);continue;}
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
  for(const auto& item:m.map()) {if(item.first.rfind("peer_",0)==0)Engine::add(b->peer,item.first,number(item.second));}
  if(!error.empty())e->terminal(b,error);else e->publish(b);
});}}
void Ticket::complete(V report) {const auto stamp=ns();if(auto e=engine_.lock()){auto b=branch_;e->enqueue([e,b,report=std::move(report),stamp]()mutable{
  if(b->done)return;
  b->done_at=stamp/1e9;b->pending_report=std::move(report);b->status="waiting_transfer";
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

Controller::Controller(py::dict c):engine_(std::make_shared<Engine>()) {
  auto cfg=dhwire::from_python(c).map();const auto& p=cfg.at("peer").map();const auto& comm=p.at("communication").map();
  const auto& net=cfg.at("network").map();auto e=engine_;
  e->capacity=size_t(getnum(comm,"max_pending_control"));e->interval=getnum(p,"heartbeat_interval_ms")/1000.;
  e->retention=getnum(p,"retention_seconds");e->lease_seconds=getnum(p,"lease_timeout_ms")/1000.;
  e->initial_delay=getnum(p.at("control").map(),"initial_heartbeat_delay_ms")/1000.;
  if(!e->capacity||e->interval<=0||e->retention<=0)throw std::invalid_argument("Invalid native control capacity/timing");
  dhwire::Config wire;wire.max_pending=e->capacity;wire.max_connections_per_endpoint=size_t(getnum(comm,"connections_per_endpoint"));
  wire.max_endpoints=size_t(getnum(comm,"max_connection_endpoints"));wire.max_message_bytes=size_t(getnum(net,"max_message_bytes"));
  e->max_endpoints=wire.max_endpoints;
  wire.timeout_ms=uint64_t(getnum(p,"control_timeout_seconds")*1000.);e->client=std::make_unique<dhwire::Client>(wire);
  e->thread=std::thread([e]{e->run();});
}
Controller::~Controller(){close();}
std::shared_ptr<Ticket> Controller::register_branch(py::dict metadata,std::shared_ptr<dhsearch::Control> control) {
  auto b=std::make_shared<Branch>();b->metadata=dhwire::from_python(metadata).map();b->control=std::move(control);
  b->key=str(b->metadata.at("query_request_id"))+"\n"+str(b->metadata.at("branch_id"));
  b->created=getnum(b->metadata,"created_ns",double(ns()))/1e9;b->touched=b->created;
  b->lease=getnum(b->metadata,"lease",seconds()+engine_->lease_seconds);b->deadline=getnum(b->metadata,"deadline",b->lease);
  auto trace=b->metadata.find("trace");if(trace==b->metadata.end())trace=b->metadata.find("tracebool");
  b->traced=trace!=b->metadata.end()&&boolean(trace->second);
  if(b->traced)b->trace["accepted_ns"]=uint64_t(getnum(b->metadata,"accepted_ns",double(ns())));
  b->peer={{"peer_rpc_count",uint64_t(0)},{"peer_request_bytes",uint64_t(0)},{"peer_response_bytes",uint64_t(0)},{"peer_transfer_errors",uint64_t(0)}};
  auto endpoint=std::get<A>(b->metadata.at("head_endpoint").data);const auto host=str(endpoint.at(0));const auto port=uint16_t(number(endpoint.at(1)));
  const auto key=host+":"+std::to_string(port);
  auto e=engine_;
  {std::lock_guard<std::mutex> state(e->state_mu);
    auto it=e->endpoints.find(key);if(it!=e->endpoints.end())b->endpoint=it->second.lock();}
  // DNS is configuration work. It must never hold the control state mutex.
  if(!b->endpoint)b->endpoint=std::make_shared<dhwire::Endpoint>(host,port);
  {
    // Same queue -> state order as the consumer's wait setup. Reservation is
    // synchronous so duplicate identity/close cannot disappear as loop errors.
    std::lock_guard<std::mutex> queue(e->queue_mu);
    if(e->stopping)throw std::runtime_error("Native controller is closed");
    std::lock_guard<std::mutex> state(e->state_mu);
    if(e->branches.count(b->key))throw std::invalid_argument("Duplicate native control branch");
    for(auto it=e->endpoints.begin();it!=e->endpoints.end();) {
      if(it->second.expired())it=e->endpoints.erase(it);else ++it;
    }
    if(e->endpoints.count(key)||e->endpoints.size()<e->max_endpoints)e->endpoints[key]=b->endpoint;
    e->branches.emplace(b->key,b);
    e->commands.push_back([e,b]{if(e->initial_delay>0)e->timer(b,seconds()+e->initial_delay);else e->ready(b);});
    e->queued_command();
  }
  e->wake.notify_one();
  return std::make_shared<Ticket>(e,b);
}
V Controller::snapshot(bool include_reports)const {
  auto e=engine_;std::lock_guard<std::mutex> state(e->state_mu);A out;
  for(const auto& item:e->branches){const auto& b=item.second;M row=b->peer;
    row["query_request_id"]=b->metadata.at("query_request_id");row["branch_id"]=b->metadata.at("branch_id");
    row["node_id"]=b->metadata.at("target_node");row["status"]=b->status;row["acked"]=b->acked;
    row["touched"]=b->touched;row["lease"]=b->lease;row["cancelled"]=b->cancelled;
    if(include_reports)row["report"]=b->report;
    row["error"]=b->error.empty()?V():V(b->error);
    row["execution_timestamps"]=b->trace;row["control_events"]=b->events;out.emplace_back(std::move(row));}
  return M{{"branches",std::move(out)},{"control_totals",e->totals},{"control_scheduler_totals",e->scheduler},
           {"control_active",uint64_t(e->active)},{"command_queue_depth",e->command_depth.load(std::memory_order_relaxed)},
           {"command_queue_peak",e->command_peak.load(std::memory_order_relaxed)},
           {"client_pending",uint64_t(e->client->pending())},{"retained_branches",uint64_t(e->branches.size())}};
}
V Controller::take_updates(){std::lock_guard<std::mutex> lock(engine_->updates_mu);A out;out.swap(engine_->updates);return out;}
void Controller::close(){if(engine_)engine_->close();}
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
