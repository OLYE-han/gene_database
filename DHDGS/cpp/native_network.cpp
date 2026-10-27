#include "native_network.hpp"
#include "native_affinity.hpp"
#include "value_utils.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <future>
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
  const auto& p=dhvalue::at(n,"network_module");transport=dhvalue::optional<std::string>(p,"transport","tcp");rdma_poll_interval_us=dhvalue::optional<uint64_t>(p,"rdma_poll_interval_us",50);dhwire::Transport check(transport,rdma_poll_interval_us);enabled=dhvalue::get<bool>(p,"enabled");if(!enabled)return;
  cpu_ids=dhvalue::optional<std::vector<int>>(p,"cpu_ids",{});
  warm_connections=dhvalue::optional<size_t>(p,"min_connections_per_endpoint_per_reactor",0);
  max_connecting=dhvalue::optional<size_t>(p,"max_connecting_per_reactor",4);
  async_connect=dhvalue::optional<bool>(p,"async_connect",false);
  async_connect_progress=dhvalue::optional<bool>(p,"async_connect_progress",true);
  auto pressure=dhvalue::optional<std::string>(p,"send_pressure_mode","pending");
  idle_probe_ms=dhvalue::optional<uint64_t>(p,"idle_probe_ms",1000);
  if(pressure!="pending"&&pressure!="queued")throw std::invalid_argument("Invalid send pressure mode");
  queued_pressure=pressure=="queued";
  scale_busy_fraction=dhvalue::optional<double>(p,"scale_up_busy_fraction",0.5);
  if(!(scale_busy_fraction>0&&scale_busy_fraction<=1))throw std::invalid_argument("Invalid busy fraction");
  rebalance_connections=dhvalue::optional<bool>(p,"rebalance_connections",false);
  rebalance_interval_ms=dhvalue::optional<uint64_t>(p,"rebalance_interval_ms",20);
  rebalance_batch=dhvalue::optional<size_t>(p,"rebalance_batch",8);
  if(dhvalue::has(n,"adaptive_control")&&dhvalue::get<bool>(dhvalue::at(n,"adaptive_control"),"enabled"))throw std::invalid_argument("Use either adaptive_control or network_module, not both");
  tx_min=dhvalue::get<size_t>(p,"send_min_reactors");tx_max=dhvalue::get<size_t>(p,"send_max_reactors");
  rx_min=dhvalue::get<size_t>(p,"receive_min_reactors");rx_max=dhvalue::get<size_t>(p,"receive_max_reactors");
  handlers=dhvalue::get<size_t>(p,"handlers_per_receive_reactor");connections=dhvalue::get<size_t>(p,"connections_per_endpoint_per_reactor");
  send_capacity=dhvalue::get<size_t>(p,"send_queue_capacity");receive_capacity=dhvalue::get<size_t>(p,"receive_queue_capacity_per_reactor");max_connections=dhvalue::get<size_t>(p,"max_receive_connections");
  tx_high=dhvalue::get<size_t>(p,"send_pending_high");rx_high=dhvalue::get<size_t>(p,"receive_queue_high");high_samples=dhvalue::get<size_t>(p,"high_samples");
  sample_ms=dhvalue::get<uint64_t>(p,"sample_interval_ms");cooldown_ms=dhvalue::get<uint64_t>(p,"cooldown_ms");idle_ms=dhvalue::get<uint64_t>(p,"idle_ms");
  if((connections&&warm_connections>connections)||!max_connecting||!rebalance_interval_ms||!rebalance_batch||!tx_min||(tx_max&&tx_min>tx_max)||!rx_min||(rx_max&&rx_min>rx_max)||!handlers||!send_capacity||!receive_capacity||!tx_high||!rx_high||!high_samples||!sample_ms||!cooldown_ms||!idle_ms)throw std::invalid_argument("Invalid native network module policy");
}
struct Client::Core {
  struct Lane {std::unique_ptr<dhwire::Client> wire;std::atomic<size_t> pending{0};bool warming=false;explicit Lane(dhwire::Config c):wire(std::make_unique<dhwire::Client>(c)){} };
  Policy p;dhwire::Config config;mutable std::mutex mu;std::mutex close_mu;std::condition_variable wake;
  std::vector<std::shared_ptr<Lane>> lanes;std::map<std::string,dhwire::Endpoint> known;std::thread monitor;bool stopping=false;
  std::atomic<size_t> total{0};size_t rr=0,ups=0,downs=0,peak=1,peak_pending=0,rejected=0;A events;
  explicit Core(dhwire::Config c,const V& v):p(v),config(c){
    config.cpu_ids=p.cpu_ids;config.transport=p.transport;config.rdma_poll_interval_us=p.rdma_poll_interval_us;config.async_connect=p.async_connect;config.async_connect_progress=p.async_connect_progress;config.idle_probe_ms=p.idle_probe_ms;
    if(p.enabled){config.max_pending=std::min(c.max_pending,p.send_capacity);config.max_connections_per_endpoint=p.connections;config.connection_idle_ms=p.connections?0:p.idle_ms;config.min_connections_per_endpoint=p.warm_connections;config.max_connecting=p.max_connecting;}
    for(size_t i=0;i<p.tx_min;++i){lanes.push_back(std::make_shared<Lane>(config));}
    peak=lanes.size();
  }
  void event(const char* reason){events.emplace_back(M{{"time_ns",now()},{"reason",reason},{"reactors",uint64_t(lanes.size())}});if(events.size()>128)events.erase(events.begin());}
  void run(){
    size_t high=0;uint64_t changed=now(),idle=0;std::unique_lock<std::mutex> l(mu);
    while(!wake.wait_for(l,std::chrono::milliseconds(p.sample_ms),[&]{return stopping;})){
      size_t busy_lanes=0;for(auto& x:lanes){busy_lanes+=(p.queued_pressure?x->wire->queued():x->pending.load())>=p.tx_high;if(x->warming&&x->wire->ready_connections()>=known.size()*p.warm_connections)x->warming=false;}bool busy=busy_lanes>=std::max(size_t(1),size_t(std::ceil(p.scale_busy_fraction*lanes.size())));high=busy?high+1:0;auto t=now();
      if(total)idle=0;else if(!idle)idle=t;
      if(high>=p.high_samples&&(!p.tx_max||lanes.size()<p.tx_max)&&t-changed>=p.cooldown_ms*1000000){
        try{auto targets=known;l.unlock();std::shared_ptr<Lane> fresh;try{fresh=std::make_shared<Lane>(config);fresh->warming=p.warm_connections&&!targets.empty();for(const auto& [key,ep]:targets)fresh->wire->prepare(ep);}catch(...){l.lock();throw;}l.lock();lanes.push_back(std::move(fresh));++ups;peak=std::max(peak,lanes.size());event("send_pressure");}catch(const std::exception&){event("scale_up_failed");}
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
Client::Client(dhwire::Config c,const V& v):core_(std::make_shared<Core>(c,v)){if(core_->p.enabled){auto x=core_;try{x->monitor=std::thread([x]{x->run();});dhruntime::bind_thread(x->monitor,x->p.cpu_ids);}catch(...){x->close();throw;}}}
Client::~Client(){close();}
bool Client::submit(const dhwire::Endpoint& ep,const V& value,Callback callback){
  if(!callback)throw std::invalid_argument("RPC callback is required");
  auto c=core_;std::shared_ptr<Core::Lane> lane;
  {std::lock_guard<std::mutex> l(c->mu);if(c->stopping||c->total>=c->config.max_pending){++c->rejected;return false;}
    if(c->p.warm_connections&&!c->known.count(ep.key)){if(c->known.size()>=c->config.max_endpoints)c->known.erase(c->known.begin());c->known.emplace(ep.key,ep);for(auto& x:c->lanes)x->wire->prepare(ep);}
    size_t best=c->rr++%c->lanes.size();
    for(size_t i=0;i<c->lanes.size();++i)if(!c->lanes[i]->warming&&(c->lanes[best]->warming||c->lanes[i]->pending<c->lanes[best]->pending))best=i;
    lane=c->lanes[best];++lane->pending;++c->total;c->peak_pending=std::max(c->peak_pending,c->total.load());
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
V Client::metrics()const{auto c=core_;std::lock_guard<std::mutex> l(c->mu);A depths,wire;for(auto& x:c->lanes){depths.emplace_back(uint64_t(x->pending));wire.emplace_back(x->wire->metrics());}return M{{"transport",c->p.transport},{"enabled",c->p.enabled},{"reactors",uint64_t(c->lanes.size())},{"peak_reactors",uint64_t(c->peak)},{"scale_ups",uint64_t(c->ups)},{"scale_downs",uint64_t(c->downs)},{"pending",uint64_t(c->total)},{"peak_pending",uint64_t(c->peak_pending)},{"capacity_rejections",uint64_t(c->rejected)},{"connections_per_endpoint_per_reactor",uint64_t(c->config.max_connections_per_endpoint)},{"max_reactors",uint64_t(c->p.tx_max)},{"pending_limit",uint64_t(c->config.max_pending)},{"lane_pending",std::move(depths)},{"wire_lanes",std::move(wire)},{"events",c->events}};}
struct Server::Core {
  struct Lane {std::unique_ptr<dhwire::Server> wire;bool draining=false;};
  Policy p;dhwire::ServerConfig config;Handler data,admin;uint16_t bound;
  std::unique_ptr<dhwire::RdmaAcceptor> acceptor;
  std::unique_ptr<dhwire::Server> management;size_t accept_rr=0;
  mutable std::mutex mu;std::mutex close_mu;std::condition_variable wake;std::vector<std::shared_ptr<Lane>> lanes;
  std::thread monitor;bool stopping=false;size_t ups=0,downs=0,peak=1;A events;
  M retired{{"requests",uint64_t(0)},{"errors",uint64_t(0)},{"rejected",uint64_t(0)},{"migrated_connections",uint64_t(0)}};
  std::shared_ptr<Lane> make(){auto x=std::make_shared<Lane>();x->wire=std::make_unique<dhwire::Server>(config,data,admin);return x;}
  Core(dhwire::ServerConfig c,Handler d,Handler a,const V& v):p(v),config(c),data(std::move(d)),admin(std::move(a)){
    if(p.enabled){config.reuse_port=true;config.workers=p.handlers;config.queue_capacity=p.receive_capacity;config.max_connections=p.max_connections;config.connection_budget=std::make_shared<std::atomic<size_t>>(0);}
    config.cpu_ids=p.cpu_ids;config.transport=p.transport;config.rdma_poll_interval_us=p.rdma_poll_interval_us;
    if(p.transport=="rdma"){
      config.reuse_port=false;config.external_accept=true;
      acceptor=std::make_unique<dhwire::RdmaAcceptor>(config.host,config.port,config.listen_backlog,config.rdma_poll_interval_us,config.cpu_ids);
      config.port=acceptor->port();
    }
    try{
      lanes.push_back(make());bound=lanes[0]->wire->port();config.port=bound;
      for(size_t i=1;i<p.rx_min;++i)lanes.push_back(make());peak=lanes.size();
      if(acceptor){
        // Management stays TCP on the same numeric port, in the TCP port space.
        // Normal graph/query/control/report traffic exclusively uses RDMA.
        if(admin){auto mc=config;mc.transport="tcp";mc.external_accept=false;mc.reuse_port=false;
          mc.workers=1;mc.max_connections=64;mc.connection_budget.reset();
          management=std::make_unique<dhwire::Server>(mc,[](V,uint64_t,size_t)->dhwire::Reply{
            throw std::runtime_error("Query traffic requires RDMA; TCP is management-only");
          },admin);
        }
        acceptor->start([this](int fd){std::lock_guard<std::mutex> l(mu);
          if(stopping||lanes.empty())return false;
          for(size_t n=0;n<lanes.size();++n){auto& lane=lanes[accept_rr++%lanes.size()];
            if(!lane->draining&&lane->wire->adopt(fd))return true;}
          return false;
        });
      }
    }catch(...){if(acceptor)acceptor->close();if(management)management->close();for(auto& x:lanes)x->wire->close();throw;}
  }
  void event(const char* reason){events.emplace_back(M{{"time_ns",now()},{"reason",reason},{"reactors",uint64_t(lanes.size())}});if(events.size()>128)events.erase(events.begin());}
  void run(){
    size_t high=0;uint64_t changed=now(),idle=0,last_requests=0,last_balance=0;std::unique_lock<std::mutex> l(mu);
    while(!wake.wait_for(l,std::chrono::milliseconds(p.sample_ms),[&]{return stopping;})){
      bool active=false;size_t requests=num(retired.at("requests")),accepting=0,busy_lanes=0;
      for(auto& x:lanes){auto s=x->wire->metrics();const auto& w=dhvalue::at(s,"workers");requests+=dhvalue::get<size_t>(s,"requests");active|=dhvalue::get<size_t>(w,"active")||dhvalue::get<size_t>(w,"queued");busy_lanes+=!x->draining&&dhvalue::get<size_t>(w,"queued")>=p.rx_high;accepting+=!x->draining;}
      bool busy=busy_lanes>=std::max(size_t(1),size_t(std::ceil(p.scale_busy_fraction*accepting)));high=busy?high+1:0;auto t=now();if(active||requests!=last_requests)idle=0;else if(!idle)idle=t;last_requests=requests;
      if(high>=p.high_samples&&(!p.rx_max||lanes.size()<p.rx_max)&&t-changed>=p.cooldown_ms*1000000){
        // Starting and pinning the new workers can take much longer than a
        // queue operation. Existing acceptors and metric readers must keep
        // progressing while the new lane becomes ready. Only this monitor
        // changes the lane vector; close joins it before reclaiming lanes.
        try{l.unlock();std::shared_ptr<Lane> fresh;try{fresh=make();}catch(...){l.lock();throw;}l.lock();lanes.push_back(std::move(fresh));++ups;peak=std::max(peak,lanes.size());event("receive_pressure");}catch(const std::exception&){event("scale_up_failed");}changed=t;high=0;
      }
      if(p.rebalance_connections&&lanes.size()>1&&t-last_balance>=p.rebalance_interval_ms*1000000){
        std::shared_ptr<Lane> low,high_lane,drain;size_t low_n=SIZE_MAX,high_n=0,drain_n=0;
        for(auto& x:lanes){auto m=x->wire->metrics();auto n=dhvalue::get<size_t>(m,"connections");if(!x->draining&&n<low_n){low=x;low_n=n;}if(n>high_n){high_lane=x;high_n=n;}if(x->draining&&n>drain_n){drain=x;drain_n=n;}}
        // A balanced retiring lane still owns persistent sockets. Prioritize
        // evacuating it; ordinary balancing alone can leave it alive forever.
        if(drain){high_lane=drain;high_n=drain_n;}
        if(low&&high_lane&&low!=high_lane&&(high_lane->draining||high_n>low_n+1)){
          auto amount=std::min(p.rebalance_batch,high_lane->draining?high_n:(high_n-low_n)/2);
          high_lane->wire->migrate_idle([low](int fd){return low->wire->adopt_transferred(fd);},amount);
        }
        last_balance=t;
      }
      // idle_ms establishes sustained inactivity for the whole receiver, not
      // a new full wait for each surplus lane. Drain one lane per sample while
      // idle; in-flight frames stay owned until safely completed/migrated.
      if(idle&&t-idle>=p.idle_ms*1000000&&accepting>p.rx_min){for(size_t i=lanes.size();i-->0;)if(!lanes[i]->draining){lanes[i]->draining=true;lanes[i]->wire->stop_accepting();event("receive_drain");break;}}
      for(size_t i=lanes.size();i-->0;){auto x=lanes[i];if(!x->draining)continue;auto s=x->wire->metrics();const auto& w=dhvalue::at(s,"workers");
        if(!dhvalue::get<bool>(s,"accepting")&&!dhvalue::get<size_t>(s,"connections")&&!dhvalue::get<size_t>(w,"active")&&!dhvalue::get<size_t>(w,"queued")){
          // Stop accepting is asynchronous. Join and drain before reclaiming.
          l.unlock();x->wire->close();l.lock();s=x->wire->metrics();
          for(const char* k:{"requests","errors","rejected","migrated_connections"})retired[k]=uint64_t(num(retired[k])+num(dhvalue::at(s,k)));
          lanes.erase(lanes.begin()+i);++downs;event("receive_reclaimed");
        }
      }
    }
  }
  void close(){std::lock_guard<std::mutex> c(close_mu);{std::lock_guard<std::mutex> l(mu);stopping=true;}wake.notify_all();if(acceptor)acceptor->close();if(monitor.joinable())monitor.join();for(auto& x:lanes)x->wire->close();if(management)management->close();}
};
Server::Server(dhwire::ServerConfig c,Handler d,Handler a,const V& v):core_(std::make_shared<Core>(c,std::move(d),std::move(a),v)){if(core_->p.enabled){auto x=core_;try{x->monitor=std::thread([x]{x->run();});dhruntime::bind_thread(x->monitor,x->p.cpu_ids);}catch(...){x->close();throw;}}}
Server::~Server(){close();}void Server::close(){if(core_)core_->close();}uint16_t Server::port()const{return core_->bound;}
V Server::metrics()const{
  auto c=core_;std::lock_guard<std::mutex> l(c->mu);M out=c->retired;out["transport"]=c->p.transport;out["rdma_acceptors"]=uint64_t(bool(c->acceptor));if(c->management)out["management_tcp"]=c->management->metrics();out["connections"]=uint64_t(0);M workers;A lanes;size_t draining=0;
  for(auto& x:c->lanes){auto s=x->wire->metrics();for(const char* k:{"connections","requests","errors","rejected","migrated_connections"})out[k]=uint64_t(num(out[k])+num(dhvalue::at(s,k)));
    for(const auto& [k,v]:dhvalue::at(s,"workers").map()){workers[k]=uint64_t((workers.count(k)?num(workers[k]):0)+num(v));}
    lanes.push_back(std::move(s));draining+=x->draining;}
  out["max_connections"]=uint64_t(c->config.max_connections);out["workers"]=std::move(workers);out["network_module"]=M{{"enabled",c->p.enabled},{"max_reactors",uint64_t(c->p.rx_max)},{"handlers_per_reactor",uint64_t(c->p.handlers)},{"reactors",uint64_t(c->lanes.size())},{"draining",uint64_t(draining)},{"peak_reactors",uint64_t(c->peak)},{"scale_ups",uint64_t(c->ups)},{"scale_downs",uint64_t(c->downs)},{"events",c->events},{"lanes",std::move(lanes)}};return out;
}
} // namespace dhnet
namespace dhnet {
namespace py=pybind11;
// Separate bounded maintenance channel. Query RPCs never acquire the GIL.
class MaintenanceServer {
  py::object handler_;
  std::unique_ptr<Server> server_;
public:
  MaintenanceServer(const V& c,const std::string& host,uint16_t port,py::object handler):handler_(std::move(handler)) {
    dhwire::ServerConfig sc;sc.host=host;sc.port=port;
    sc.max_message_bytes=dhvalue::get<size_t>(dhvalue::at(c,"network"),"max_message_bytes");
    sc.idle_timeout_ms=uint64_t(dhvalue::get<double>(dhvalue::at(c,"network"),"timeout_seconds")*1000);
    server_=std::make_unique<Server>(sc,[this](V request,uint64_t,size_t){
      py::gil_scoped_acquire gil;V response;
      try{response=dhwire::from_python(handler_(dhwire::to_python(request)));}
      catch(py::error_already_set& e){auto error=std::string(e.what());e.restore();PyErr_Clear();response=M{{"ok",false},{"error",error}};}
      return dhwire::Reply{std::move(response),{}};
    },Server::Handler{},c);
  }
  ~MaintenanceServer(){py::gil_scoped_release release;close();}
  void close(){if(server_)server_->close();}
  uint16_t port()const{return server_->port();}
  V metrics()const{return server_->metrics();}
};
class MaintenanceClient {
  Client client_;
  static dhwire::Config wire_config(const V& c){dhwire::Config w;const auto& n=dhvalue::at(c,"network");
    w.max_message_bytes=dhvalue::get<size_t>(n,"max_message_bytes");w.timeout_ms=uint64_t(dhvalue::get<double>(n,"timeout_seconds")*1000);return w;}
public:
  explicit MaintenanceClient(const V& c):client_(wire_config(c),c){}
  V request(const std::string& host,uint16_t port,V value){
    auto promise=std::make_shared<std::promise<dhwire::Result>>();auto result=promise->get_future();
    if(!client_.submit(dhwire::Endpoint(host,port),value,[promise](dhwire::Result r){promise->set_value(std::move(r));}))throw std::runtime_error("Maintenance channel capacity exceeded");
    auto r=result.get();if(!r.ok)throw std::runtime_error(r.error);
    return M{{"response",std::move(r.response)},{"request_bytes",r.request_bytes},{"response_bytes",r.response_bytes},
      {"elapsed_ms",double(r.done_ns-r.queued_ns)/1e6}};
  }
  void close(){client_.close();}
  V metrics()const{return client_.metrics();}
};
struct ProbeClient {
  Client wire;std::mutex mu;A done;
  explicit ProbeClient(V c):wire(dhwire::Config{},c){}
  ~ProbeClient(){wire.close();}
  bool submit(const std::string& host,uint16_t port,V v){
    dhwire::Endpoint ep(host,port);
    return wire.submit(ep,v,[this](dhwire::Result r){std::lock_guard<std::mutex> l(mu);done.emplace_back(M{{"ok",r.ok},{"error",r.error},{"response",std::move(r.response)}});});
  }
};
static dhwire::ServerConfig probe_config(const V& c){dhwire::ServerConfig sc;
  sc.host=dhvalue::optional<std::string>(c,"probe_host","127.0.0.1");
  sc.port=dhvalue::optional<uint16_t>(c,"probe_port",0);return sc;}
struct ProbeServer {
  Server wire;
  ProbeServer(V c,uint64_t delay):wire(probe_config(c),[delay](V v,uint64_t,size_t){
    std::this_thread::sleep_for(std::chrono::milliseconds(delay));return dhwire::Reply{std::move(v),{}};
  },{},c){}
};
void bind_native_network(py::module_& m){
  py::class_<MaintenanceServer>(m,"MaintenanceServer")
    .def(py::init([](py::dict c,std::string host,uint16_t port,py::object handler){return std::make_unique<MaintenanceServer>(dhwire::from_python(c),host,port,std::move(handler));}))
    .def("port",&MaintenanceServer::port)
    .def("metrics",[](MaintenanceServer& s){return dhwire::to_python(s.metrics());})
    .def("close",&MaintenanceServer::close,py::call_guard<py::gil_scoped_release>());
  py::class_<MaintenanceClient>(m,"MaintenanceClient")
    .def(py::init([](py::dict c){return std::make_unique<MaintenanceClient>(dhwire::from_python(c));}))
    .def("request",[](MaintenanceClient& c,std::string host,uint16_t port,py::object request){auto v=dhwire::from_python(request);V reply;
      {py::gil_scoped_release release;reply=c.request(host,port,std::move(v));}return dhwire::to_python(reply);})
    .def("metrics",[](MaintenanceClient& c){return dhwire::to_python(c.metrics());})
    .def("close",&MaintenanceClient::close,py::call_guard<py::gil_scoped_release>());
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
