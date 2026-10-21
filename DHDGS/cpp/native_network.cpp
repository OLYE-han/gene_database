#include "native_network.hpp"
#include "value_utils.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <pybind11/pybind11.h>
namespace dhnet {
using V=dhwire::Value;using M=V::Map;using A=V::Array;
static thread_local const void* callback_owner=nullptr;
static uint64_t now(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
static size_t num(const V& v){return size_t(dhvalue::number(v));}
Policy::Policy(const V& config) {
  if(!dhvalue::has(config,"native_runtime"))return;
  const auto& n=dhvalue::at(config,"native_runtime");if(!dhvalue::has(n,"network_module"))return;
  const auto& p=dhvalue::at(n,"network_module");enabled=dhvalue::get<bool>(p,"enabled");if(!enabled)return;
  if(dhvalue::has(n,"adaptive_control")&&dhvalue::get<bool>(dhvalue::at(n,"adaptive_control"),"enabled"))throw std::invalid_argument("Use either adaptive_control or network_module, not both");
  tx_min=dhvalue::get<size_t>(p,"send_min_reactors");tx_max=dhvalue::get<size_t>(p,"send_max_reactors");
  rx_min=dhvalue::get<size_t>(p,"receive_min_reactors");rx_max=dhvalue::get<size_t>(p,"receive_max_reactors");
  handlers=dhvalue::get<size_t>(p,"handlers_per_receive_reactor");connections=dhvalue::get<size_t>(p,"connections_per_endpoint_per_reactor");
  send_capacity=dhvalue::get<size_t>(p,"send_queue_capacity");receive_capacity=dhvalue::get<size_t>(p,"receive_queue_capacity_per_reactor");max_connections=dhvalue::get<size_t>(p,"max_receive_connections");
  tx_high=dhvalue::get<size_t>(p,"send_pending_high");rx_high=dhvalue::get<size_t>(p,"receive_queue_high");high_samples=dhvalue::get<size_t>(p,"high_samples");
  sample_ms=dhvalue::get<uint64_t>(p,"sample_interval_ms");cooldown_ms=dhvalue::get<uint64_t>(p,"cooldown_ms");idle_ms=dhvalue::get<uint64_t>(p,"idle_ms");
  if(!tx_min||tx_min>tx_max||tx_max>64||!rx_min||rx_min>rx_max||rx_max>64||!handlers||!connections||!send_capacity||!receive_capacity||!max_connections||!tx_high||!rx_high||!high_samples||!sample_ms||!cooldown_ms||!idle_ms)throw std::invalid_argument("Invalid native network module policy");
}
struct Client::Core {
  struct Lane {std::unique_ptr<dhwire::Client> wire;std::atomic<size_t> pending{0};explicit Lane(dhwire::Config c):wire(std::make_unique<dhwire::Client>(c)){} };
  Policy p;dhwire::Config config;mutable std::mutex mu;std::mutex close_mu;std::condition_variable wake;
  std::vector<std::shared_ptr<Lane>> lanes;std::thread monitor;bool stopping=false;
  std::atomic<size_t> total{0};size_t rr=0,ups=0,downs=0,peak=1;A events;
  explicit Core(dhwire::Config c,const V& v):p(v),config(c){
    if(p.enabled){config.max_pending=std::min(c.max_pending,p.send_capacity);config.max_connections_per_endpoint=p.connections;}
    for(size_t i=0;i<p.tx_min;++i){lanes.push_back(std::make_shared<Lane>(config));}
    peak=lanes.size();
  }
  void event(const char* reason){events.emplace_back(M{{"time_ns",now()},{"reason",reason},{"reactors",uint64_t(lanes.size())}});if(events.size()>128)events.erase(events.begin());}
  void run(){
    size_t high=0;uint64_t changed=now(),idle=0;std::unique_lock<std::mutex> l(mu);
    while(!wake.wait_for(l,std::chrono::milliseconds(p.sample_ms),[&]{return stopping;})){
      bool busy=false;for(auto& x:lanes)busy|=x->pending>=p.tx_high;high=busy?high+1:0;auto t=now();
      if(total)idle=0;else if(!idle)idle=t;
      if(high>=p.high_samples&&lanes.size()<p.tx_max&&t-changed>=p.cooldown_ms*1000000){
        try{lanes.push_back(std::make_shared<Lane>(config));++ups;peak=std::max(peak,lanes.size());event("send_pressure");}catch(const std::exception&){event("scale_up_failed");}
        changed=t;high=0;
      }
      if(idle&&t-idle>=p.idle_ms*1000000&&lanes.size()>p.tx_min){
        auto x=lanes.back();if(!x->pending){lanes.pop_back();++downs;event("send_idle");l.unlock();x->wire->close();l.lock();changed=t;}
      }
    }
    l.unlock();for(auto& x:lanes)x->wire->close();
  }
  void close(){std::lock_guard<std::mutex> c(close_mu);{std::lock_guard<std::mutex> l(mu);stopping=true;}wake.notify_all();if(monitor.joinable())monitor.join();for(auto& x:lanes)x->wire->close();}
};
Client::Client(dhwire::Config c,const V& v):core_(std::make_shared<Core>(c,v)){if(core_->p.enabled){auto x=core_;try{x->monitor=std::thread([x]{x->run();});}catch(...){x->close();throw;}}}
Client::~Client(){close();}
bool Client::submit(const dhwire::Endpoint& ep,const V& value,Callback callback){
  if(!callback)throw std::invalid_argument("RPC callback is required");
  auto c=core_;std::shared_ptr<Core::Lane> lane;
  {std::lock_guard<std::mutex> l(c->mu);if(c->stopping||c->total>=c->config.max_pending)return false;
    size_t best=c->rr++%c->lanes.size();for(size_t i=0;i<c->lanes.size();++i)if(c->lanes[i]->pending<c->lanes[best]->pending)best=i;
    lane=c->lanes[best];++lane->pending;++c->total;
  }
  try {
    bool ok=lane->wire->submit(ep,value,[c,lane,cb=std::move(callback)](dhwire::Result r)mutable{--lane->pending;--c->total;const void* previous=callback_owner;callback_owner=c.get();try{cb(std::move(r));}catch(...){callback_owner=previous;throw;}callback_owner=previous;});
    if(!ok){--lane->pending;--c->total;}return ok;
  }catch(...){--lane->pending;--c->total;throw;}
}
size_t Client::pending()const{return core_->total;}
void Client::close(){if(!core_)return;if(callback_owner==core_.get()){
  {std::lock_guard<std::mutex> l(core_->mu);core_->stopping=true;}core_->wake.notify_all();
  if(!core_->p.enabled){core_->lanes.front()->wire->close();}
  return;
}core_->close();}
V Client::metrics()const{auto c=core_;std::lock_guard<std::mutex> l(c->mu);A depths;for(auto& x:c->lanes)depths.emplace_back(uint64_t(x->pending));return M{{"enabled",c->p.enabled},{"reactors",uint64_t(c->lanes.size())},{"peak_reactors",uint64_t(c->peak)},{"scale_ups",uint64_t(c->ups)},{"scale_downs",uint64_t(c->downs)},{"pending",uint64_t(c->total)},{"pending_limit",uint64_t(c->config.max_pending)},{"lane_pending",std::move(depths)},{"events",c->events}};}
struct Server::Core {
  struct Lane {std::unique_ptr<dhwire::Server> wire;bool draining=false;};
  Policy p;dhwire::ServerConfig config;Handler data,admin;uint16_t bound;
  mutable std::mutex mu;std::mutex close_mu;std::condition_variable wake;std::vector<std::shared_ptr<Lane>> lanes;
  std::thread monitor;bool stopping=false;size_t ups=0,downs=0,peak=1;A events;
  M retired{{"requests",uint64_t(0)},{"errors",uint64_t(0)},{"rejected",uint64_t(0)}};
  std::shared_ptr<Lane> make(){auto x=std::make_shared<Lane>();x->wire=std::make_unique<dhwire::Server>(config,data,admin);return x;}
  Core(dhwire::ServerConfig c,Handler d,Handler a,const V& v):p(v),config(c),data(std::move(d)),admin(std::move(a)){
    if(p.enabled){config.reuse_port=true;config.workers=p.handlers;config.queue_capacity=p.receive_capacity;config.max_connections=p.max_connections;config.connection_budget=std::make_shared<std::atomic<size_t>>(0);}
    try{lanes.push_back(make());bound=lanes[0]->wire->port();config.port=bound;for(size_t i=1;i<p.rx_min;++i)lanes.push_back(make());peak=lanes.size();}catch(...){for(auto& x:lanes)x->wire->close();throw;}
  }
  void event(const char* reason){events.emplace_back(M{{"time_ns",now()},{"reason",reason},{"reactors",uint64_t(lanes.size())}});if(events.size()>128)events.erase(events.begin());}
  void run(){
    size_t high=0;uint64_t changed=now(),idle=0,last_requests=0;std::unique_lock<std::mutex> l(mu);
    while(!wake.wait_for(l,std::chrono::milliseconds(p.sample_ms),[&]{return stopping;})){
      bool busy=false,active=false;size_t requests=0,accepting=0;
      for(auto& x:lanes){auto s=x->wire->metrics();const auto& w=dhvalue::at(s,"workers");requests+=dhvalue::get<size_t>(s,"requests");active|=dhvalue::get<size_t>(w,"active")||dhvalue::get<size_t>(w,"queued");busy|=dhvalue::get<size_t>(w,"queued")>=p.rx_high;accepting+=!x->draining;}
      high=busy?high+1:0;auto t=now();if(active||requests!=last_requests)idle=0;else if(!idle)idle=t;last_requests=requests;
      if(high>=p.high_samples&&lanes.size()<p.rx_max&&t-changed>=p.cooldown_ms*1000000){
        try{lanes.push_back(make());++ups;peak=std::max(peak,lanes.size());event("receive_pressure");}catch(const std::exception&){event("scale_up_failed");}changed=t;high=0;
      }
      if(idle&&t-idle>=p.idle_ms*1000000&&accepting>p.rx_min){for(size_t i=lanes.size();i-->0;)if(!lanes[i]->draining){lanes[i]->draining=true;lanes[i]->wire->stop_accepting();event("receive_drain");break;}idle=t;}
      for(size_t i=lanes.size();i-->0;){auto x=lanes[i];if(!x->draining)continue;auto s=x->wire->metrics();const auto& w=dhvalue::at(s,"workers");
        if(!dhvalue::get<bool>(s,"accepting")&&!dhvalue::get<size_t>(s,"connections")&&!dhvalue::get<size_t>(w,"active")&&!dhvalue::get<size_t>(w,"queued")){
          // Stop accepting is asynchronous. Join and drain before reclaiming.
          l.unlock();x->wire->close();l.lock();s=x->wire->metrics();
          for(const char* k:{"requests","errors","rejected"})retired[k]=uint64_t(num(retired[k])+num(dhvalue::at(s,k)));
          lanes.erase(lanes.begin()+i);++downs;event("receive_reclaimed");
        }
      }
    }
  }
  void close(){std::lock_guard<std::mutex> c(close_mu);{std::lock_guard<std::mutex> l(mu);stopping=true;}wake.notify_all();if(monitor.joinable())monitor.join();for(auto& x:lanes)x->wire->close();}
};
Server::Server(dhwire::ServerConfig c,Handler d,Handler a,const V& v):core_(std::make_shared<Core>(c,std::move(d),std::move(a),v)){if(core_->p.enabled){auto x=core_;try{x->monitor=std::thread([x]{x->run();});}catch(...){x->close();throw;}}}
Server::~Server(){close();}void Server::close(){if(core_)core_->close();}uint16_t Server::port()const{return core_->bound;}
V Server::metrics()const{
  auto c=core_;std::lock_guard<std::mutex> l(c->mu);M out=c->retired;out["connections"]=uint64_t(0);M workers;A lanes;size_t draining=0;
  for(auto& x:c->lanes){auto s=x->wire->metrics();for(const char* k:{"connections","requests","errors","rejected"})out[k]=uint64_t(num(out[k])+num(dhvalue::at(s,k)));
    for(const auto& [k,v]:dhvalue::at(s,"workers").map()){workers[k]=uint64_t((workers.count(k)?num(workers[k]):0)+num(v));}
    lanes.push_back(std::move(s));draining+=x->draining;}
  out["workers"]=std::move(workers);out["network_module"]=M{{"enabled",c->p.enabled},{"reactors",uint64_t(c->lanes.size())},{"draining",uint64_t(draining)},{"peak_reactors",uint64_t(c->peak)},{"scale_ups",uint64_t(c->ups)},{"scale_downs",uint64_t(c->downs)},{"events",c->events},{"lanes",std::move(lanes)}};return out;
}
} // namespace dhnet
namespace dhnet {
namespace py=pybind11;
struct ProbeClient {
  Client wire;std::mutex mu;A done;
  explicit ProbeClient(V c):wire(dhwire::Config{},c){}
  ~ProbeClient(){wire.close();}
  bool submit(const std::string& host,uint16_t port,V v){
    dhwire::Endpoint ep(host,port);
    return wire.submit(ep,v,[this](dhwire::Result r){std::lock_guard<std::mutex> l(mu);done.emplace_back(M{{"ok",r.ok},{"error",r.error},{"response",std::move(r.response)}});});
  }
};
struct ProbeServer {
  Server wire;
  ProbeServer(V c,uint64_t delay):wire(dhwire::ServerConfig{},[delay](V v,uint64_t,size_t){
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));return dhwire::Reply{std::move(v),{}};
  },{},c){}
};
void bind_native_network(py::module_& m){
  py::class_<ProbeServer>(m,"NetworkProbeServer")
    .def(py::init([](py::dict c,uint64_t delay){return std::make_unique<ProbeServer>(dhwire::from_python(c),delay);}),py::arg("config"),py::arg("delay_ms")=0)
    .def("port",[](ProbeServer& s){return s.wire.port();})
    .def("metrics",[](ProbeServer& s){return dhwire::to_python(s.wire.metrics());})
    .def("close",[](ProbeServer& s){py::gil_scoped_release g;s.wire.close();});
  py::class_<ProbeClient>(m,"NetworkProbeClient")
    .def(py::init([](py::dict c){return std::make_unique<ProbeClient>(dhwire::from_python(c));}))
    .def("submit",[](ProbeClient& c,const std::string& host,uint16_t port,py::dict v){auto value=dhwire::from_python(v);py::gil_scoped_release g;return c.submit(host,port,std::move(value));})
    .def("poll",[](ProbeClient& c){A out;{std::lock_guard<std::mutex> l(c.mu);out.swap(c.done);}return dhwire::to_python(out);})
    .def("metrics",[](ProbeClient& c){return dhwire::to_python(c.wire.metrics());})
    .def("close",[](ProbeClient& c){py::gil_scoped_release g;c.wire.close();});
}
}
