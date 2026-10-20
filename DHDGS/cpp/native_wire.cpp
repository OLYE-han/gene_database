#include "native_wire.hpp"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <deque>
#include <limits>
#include <mutex>
#include <thread>
#include <stdexcept>
#include <cerrno>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>
namespace dhwire {
namespace {
uint64_t now_ns() { return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count(); }
bool valid_utf8(const uint8_t* p, size_t size) {
  for (size_t i=0; i<size;) {
    uint8_t first=p[i++];
    if(first<0x80) continue;
    unsigned extra; uint32_t code, minimum;
    if(first>=0xc2 && first<=0xdf) {extra=1;code=first&31;minimum=0x80;}
    else if(first>=0xe0 && first<=0xef) {extra=2;code=first&15;minimum=0x800;}
    else if(first>=0xf0 && first<=0xf4) {extra=3;code=first&7;minimum=0x10000;}
    else return false;
    if(extra>size-i) return false;
    while(extra--) {auto next=p[i++];if((next&0xc0)!=0x80)return false;code=(code<<6)|(next&63);}
    if(code<minimum || code>0x10ffff || (code>=0xd800 && code<=0xdfff))return false;
  }
  return true;
}
struct Encoder {
  std::vector<uint8_t> out; size_t limit;
  void byte(uint8_t x) { if(out.size() >= limit) throw std::length_error("MessagePack exceeds max_message_bytes"); out.push_back(x); }
  void number(uint64_t x, unsigned n) { for(unsigned i=n;i;--i) byte(uint8_t(x >> ((i-1)*8))); }
  void length(size_t n, uint8_t a, uint8_t b, uint8_t c) {
    if(n <= 255 && a) { byte(a); number(n,1); }
    else if(n <= 65535) { byte(b); number(n,2); }
    else if(n <= UINT32_MAX) { byte(c); number(n,4); }
    else throw std::length_error("MessagePack container too large");
  }
  void raw(const uint8_t* p, size_t n) {
    if(n > limit-out.size()) throw std::length_error("MessagePack exceeds max_message_bytes");
    out.insert(out.end(),p,p+n);
  }
  void value(const Value& v, unsigned depth=0) {
    if(depth>64) throw std::invalid_argument("MessagePack nesting too deep");
    switch(v.data.index()) {
      case 0: byte(0xc0); break;
      case 1: byte(std::get<bool>(v.data)?0xc3:0xc2); break;
      case 2: { auto x=std::get<int64_t>(v.data); if(x>=0) { value(Value(uint64_t(x)),depth); } else if(x>=-32) byte(uint8_t(x)); else {byte(0xd3);number(uint64_t(x),8);} break; }
      case 3: { auto x=std::get<uint64_t>(v.data); if(x<128) byte(uint8_t(x)); else if(x<=255){byte(0xcc);number(x,1);} else if(x<=65535){byte(0xcd);number(x,2);} else if(x<=UINT32_MAX){byte(0xce);number(x,4);} else {byte(0xcf);number(x,8);} break; }
      case 4: { uint64_t x; auto f=std::get<double>(v.data); std::memcpy(&x,&f,8); byte(0xcb);number(x,8);break; }
      case 5: { const auto& s=std::get<std::string>(v.data); if(s.size()<32) byte(0xa0|s.size()); else length(s.size(),0xd9,0xda,0xdb);raw(reinterpret_cast<const uint8_t*>(s.data()),s.size());break; }
      case 6: { const auto& b=std::get<Value::Bytes>(v.data);length(b.size(),0xc4,0xc5,0xc6);if(!b.empty()) raw(b.data(),b.size());break; }
      case 7: { const auto& a=std::get<Value::Array>(v.data);if(a.size()<16)byte(0x90|a.size());else length(a.size(),0,0xdc,0xdd);for(auto& x:a)value(x,depth+1);break; }
      case 8: { const auto& m=v.map();if(m.size()<16)byte(0x80|m.size());else length(m.size(),0,0xde,0xdf);for(auto& x:m){value(Value(x.first),depth+1);value(x.second,depth+1);}break; }
    }
  }
};
struct Decoder {
  const uint8_t* p; size_t n, at=0, depth_limit;
  uint64_t num(unsigned k) { if(k>n-at)throw std::invalid_argument("Truncated MessagePack");uint64_t v=0;while(k--)v=(v<<8)|p[at++];return v; }
  Value str(size_t k, bool binary) { if(k>n-at)throw std::invalid_argument("Truncated MessagePack string");if(!binary&&!valid_utf8(p+at,k))throw std::invalid_argument("Invalid MessagePack UTF-8");Value v=binary?Value(Value::Bytes(p+at,p+at+k)):Value(std::string(reinterpret_cast<const char*>(p+at),k));at+=k;return v; }
  Value array(size_t k, size_t d) { if(k>n-at)throw std::invalid_argument("Invalid MessagePack array length");Value::Array a;a.reserve(k);while(k--)a.push_back(value(d+1));return Value(std::move(a)); }
  Value map(size_t k,size_t d) { if(k>(n-at)/2)throw std::invalid_argument("Invalid MessagePack map length");Value::Map m;while(k--){auto key=value(d+1);if(!std::holds_alternative<std::string>(key.data))throw std::invalid_argument("MessagePack map keys must be strings");auto s=std::get<std::string>(std::move(key.data));auto v=value(d+1);if(!m.emplace(std::move(s),std::move(v)).second)throw std::invalid_argument("Duplicate MessagePack map key");}return Value(std::move(m)); }
  Value value(size_t d=0) {
    if(d>depth_limit)throw std::invalid_argument("MessagePack nesting too deep");
    auto t=num(1);
    if(t<128)return Value(uint64_t(t));
    if(t>=224)return Value(int64_t(int8_t(t)));
    if((t&0xe0)==0xa0)return str(t&31,false);
    if((t&0xf0)==0x90)return array(t&15,d);
    if((t&0xf0)==0x80)return map(t&15,d);
    switch(t) {
      case 0xc0:return Value();case 0xc2:return Value(false);case 0xc3:return Value(true);
      case 0xcc:return Value(num(1));case 0xcd:return Value(num(2));case 0xce:return Value(num(4));case 0xcf:return Value(num(8));
      case 0xd0:return Value(int64_t(int8_t(num(1))));case 0xd1:return Value(int64_t(int16_t(num(2))));case 0xd2:return Value(int64_t(int32_t(num(4))));case 0xd3:return Value(int64_t(num(8)));
      case 0xca:{uint32_t b=num(4);float f;std::memcpy(&f,&b,4);return Value(double(f));}case 0xcb:{auto b=num(8);double f;std::memcpy(&f,&b,8);return Value(f);}
      case 0xd9:return str(num(1),false);case 0xda:return str(num(2),false);case 0xdb:return str(num(4),false);
      case 0xc4:return str(num(1),true);case 0xc5:return str(num(2),true);case 0xc6:return str(num(4),true);
      case 0xdc:return array(num(2),d);case 0xdd:return array(num(4),d);case 0xde:return map(num(2),d);case 0xdf:return map(num(4),d);
      default:throw std::invalid_argument("Unsupported MessagePack tag");
    }
  }
};
}
std::vector<uint8_t> pack(const Value& v,size_t limit) { Encoder e{{},limit};e.value(v);return std::move(e.out); }
Value unpack(const uint8_t* p,size_t n,size_t depth) { Decoder d{p,n,0,depth};auto v=d.value();if(d.at!=n)throw std::invalid_argument("Trailing MessagePack bytes");return v; }
Endpoint::Endpoint(const std::string& host,uint16_t port) {
  if(host.empty() || !port)throw std::invalid_argument("Invalid RPC endpoint");
  addrinfo hints{},*result=nullptr;hints.ai_socktype=SOCK_STREAM;hints.ai_family=AF_UNSPEC;
  auto service=std::to_string(port);int rc=getaddrinfo(host.c_str(),service.c_str(),&hints,&result);
  if(rc)throw std::runtime_error(std::string("Resolve endpoint: ")+gai_strerror(rc));
  // Prefer IPv4 for localhost: existing Python listeners use AF_INET.
  auto chosen=result;for(auto p=result;p;p=p->ai_next)if(p->ai_family==AF_INET){chosen=p;break;}
  if(!chosen || chosen->ai_addrlen>sizeof(address)){freeaddrinfo(result);throw std::runtime_error("Invalid resolved endpoint");}
  family=chosen->ai_family;address_length=chosen->ai_addrlen;std::memcpy(&address,chosen->ai_addr,address_length);freeaddrinfo(result);
  char numeric[NI_MAXHOST];getnameinfo(reinterpret_cast<sockaddr*>(&address),address_length,numeric,sizeof(numeric),nullptr,0,NI_NUMERICHOST);key=std::string(numeric)+":"+service;
}
struct Client::Impl {
  struct Job { Endpoint endpoint;std::vector<uint8_t> frame;Callback callback;Result result;uint64_t deadline;std::vector<uint8_t> request_id; };
  struct Conn { int fd=-1;std::string endpoint;bool connecting=false;std::unique_ptr<Job> job;size_t sent=0;std::vector<uint8_t> received;size_t expected=0; };
  Config config; mutable std::mutex mutex;std::deque<std::unique_ptr<Job>> incoming;std::atomic<size_t> count{0};bool stopping=false;int wake=-1;std::thread thread;std::thread::id reactor_id;std::mutex close_mutex;
  std::vector<Conn> conns;std::deque<std::unique_ptr<Job>> waiting;std::vector<std::pair<Callback,Result>> callbacks;struct EndpointInfo { Endpoint endpoint;size_t pending=0;uint64_t last_used=0; };
  std::map<std::string,EndpointInfo> endpoints;
  std::vector<std::string> evicted_endpoints;
  explicit Impl(Config c):config(c) {
    if(!c.max_pending || !c.max_connections_per_endpoint || !c.max_endpoints || !c.max_message_bytes || c.max_message_bytes>UINT32_MAX || !c.timeout_ms || c.timeout_ms>UINT64_MAX/1000000)throw std::invalid_argument("Invalid native RPC limits");
    wake=eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);if(wake<0)throw std::runtime_error("eventfd failed");
    try{thread=std::thread([this]{run();});reactor_id=thread.get_id();}catch(...){::close(wake);throw;}
  }
  void signal() { uint64_t one=1;ssize_t rc;do{rc=write(wake,&one,sizeof(one));}while(rc<0&&errno==EINTR); }
  void finish(std::unique_ptr<Job> j,const std::string& error={}) { j->result.done_ns=now_ns();j->result.ok=error.empty();j->result.error=error;
    {std::lock_guard<std::mutex> lock(mutex);auto it=endpoints.find(j->endpoint.key);if(it!=endpoints.end()){--it->second.pending;it->second.last_used=now_ns();}}
    count.fetch_sub(1);callbacks.emplace_back(std::move(j->callback),std::move(j->result)); }
  void close_conn(Conn& c,const std::string& why) { if(c.fd>=0)::close(c.fd);c.fd=-1;c.connecting=false;if(c.job)finish(std::move(c.job),why);c.received.clear();c.expected=0;c.sent=0; }
  void invoke() { auto ready=std::move(callbacks);callbacks.clear();for(auto& cb:ready)try{cb.first(std::move(cb.second));}catch(...){/* application callback must not kill reactor */} }
  void start(Conn& c,std::unique_ptr<Job> j) {
    c.job=std::move(j);c.sent=0;c.received.clear();c.expected=0;c.job->result.send_start_ns=now_ns();
    if(c.fd>=0)return;
    c.job->result.connection_created=true;c.job->result.connect_start_ns=now_ns();
    c.fd=socket(c.job->endpoint.family,SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0);
    if(c.fd<0){close_conn(c,"socket failed");return;}int one=1;setsockopt(c.fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
    int rc=connect(c.fd,reinterpret_cast<const sockaddr*>(&c.job->endpoint.address),c.job->endpoint.address_length);
    if(rc==0)c.job->result.connect_done_ns=now_ns();else if(errno==EINPROGRESS)c.connecting=true;else close_conn(c,std::string("connect: ")+std::strerror(errno));
  }
  void assign() {
    auto n=waiting.size();while(n--){auto j=std::move(waiting.front());waiting.pop_front();
      if(now_ns()>=j->deadline){finish(std::move(j),"RPC timeout in queue");continue;}
      Conn* available=nullptr;size_t occupied=0;for(auto& c:conns)if(c.endpoint==j->endpoint.key){++occupied;if(!c.job)available=&c;}
      if(!available && occupied<config.max_connections_per_endpoint){conns.emplace_back();conns.back().endpoint=j->endpoint.key;available=&conns.back();}
      if(available)start(*available,std::move(j));else waiting.push_back(std::move(j));
    }
  }
  void progress(Conn& c,short events) {
    if(!c.job)return;
    if(c.connecting && events){int err=0;socklen_t len=sizeof(err);if(getsockopt(c.fd,SOL_SOCKET,SO_ERROR,&err,&len)<0 || err){close_conn(c,"RPC connect failed");return;}c.connecting=false;c.job->result.connect_done_ns=now_ns();}
    if(events&POLLOUT){while(c.sent<c.job->frame.size()) {auto n=::send(c.fd,c.job->frame.data()+c.sent,c.job->frame.size()-c.sent,MSG_NOSIGNAL);if(n>0){c.sent+=n;c.job->result.request_bytes+=n;}else if(n<0&&errno==EINTR)continue;else if(n<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break;else{close_conn(c,"RPC send failed");return;}}if(c.sent==c.job->frame.size()&&!c.job->result.send_done_ns)c.job->result.send_done_ns=now_ns();}
    if(events&(POLLIN|POLLHUP|POLLERR)) {
      while(c.job){uint8_t buf[65536];size_t needed=c.expected?c.expected-c.received.size():4-c.received.size();auto n=recv(c.fd,buf,std::min(sizeof(buf),needed),0);
        if(n>0){c.received.insert(c.received.end(),buf,buf+n);c.job->result.response_bytes+=n;}
        else if(n<0&&errno==EINTR)continue;else if(n<0&&(errno==EAGAIN||errno==EWOULDBLOCK))break;else{close_conn(c,"RPC connection closed during frame");return;}
        if(!c.expected&&c.received.size()==4){uint64_t len=0;for(auto b:c.received)len=(len<<8)|b;if(!len||len>config.max_message_bytes){close_conn(c,"Invalid RPC frame size");return;}c.expected=len+4;}
        if(c.expected&&c.received.size()==c.expected){try{c.job->result.response=unpack(c.received.data()+4,c.expected-4);if(!std::holds_alternative<Value::Map>(c.job->result.response.data))throw std::invalid_argument("RPC response must be a mapping");if(!c.job->request_id.empty()){auto it=c.job->result.response.map().find("request_id");if(it==c.job->result.response.map().end()||pack(it->second)!=c.job->request_id)throw std::invalid_argument("RPC request ID mismatch");}finish(std::move(c.job));c.received.clear();c.expected=0;c.sent=0;}catch(const std::exception& e){close_conn(c,e.what());}return;}
      }
    }
    if(c.job && (events&(POLLNVAL|POLLERR|POLLHUP)))close_conn(c,"RPC socket error");
  }
  void run() {
    for(;;){bool stop;std::vector<std::string> evicted;
      {std::lock_guard<std::mutex> lock(mutex);stop=stopping;evicted.swap(evicted_endpoints);while(!incoming.empty()){waiting.push_back(std::move(incoming.front()));incoming.pop_front();}}
      // Only endpoints with no accepted work can be evicted. Socket ownership
      // stays on the reactor; no connect/send/close occurs under producer locks.
      for(auto& key:evicted){for(auto& c:conns)if(c.endpoint==key&&!c.job)close_conn(c,"Endpoint evicted");
        conns.erase(std::remove_if(conns.begin(),conns.end(),[&key](const Conn& c){return c.endpoint==key&&!c.job;}),conns.end());}
      if(stop)break;
      assign();invoke();
      std::vector<pollfd> fds{{wake,POLLIN,0}};std::vector<size_t> indices;uint64_t deadline=now_ns()+1000000000;
      for(auto& j:waiting)deadline=std::min(deadline,j->deadline);
      for(size_t i=0;i<conns.size();++i){auto& c=conns[i];if(c.job){deadline=std::min(deadline,c.job->deadline);short events=POLLIN;if(c.connecting||c.sent<c.job->frame.size())events|=POLLOUT;fds.push_back({c.fd,events,0});indices.push_back(i);}}
      auto now=now_ns();int timeout=deadline<=now?0:int(std::min<uint64_t>(1000,(deadline-now+999999)/1000000));int rc=poll(fds.data(),fds.size(),timeout);
      if(rc<0&&errno!=EINTR){for(auto& c:conns)close_conn(c,"poll failed");}
      if(fds[0].revents){uint64_t v;while(read(wake,&v,sizeof(v))>0){}}
      for(size_t i=0;i<indices.size();++i){auto& c=conns[indices[i]];if(c.job&&now_ns()>=c.job->deadline)close_conn(c,"RPC timeout");else if(fds[i+1].revents)progress(c,fds[i+1].revents);}
      invoke();
    }
    for(auto& c:conns)close_conn(c,"RPC client closed");
    while(!waiting.empty()){finish(std::move(waiting.front()),"RPC client closed");waiting.pop_front();}invoke();
  }
  void close() {
    // A callback may request close; it cannot join its own reactor thread.
    {std::lock_guard<std::mutex> lock(mutex);stopping=true;signal();}
    if(std::this_thread::get_id()==reactor_id)return;
    std::lock_guard<std::mutex> lock(close_mutex);if(thread.joinable())thread.join();
  }
  ~Impl(){close();::close(wake);}
};
Client::Client(Config config):impl_(new Impl(config)){}
Client::~Client()=default;
bool Client::submit(const Endpoint& ep,const Value& value,Callback callback) {
  if(!callback)throw std::invalid_argument("RPC callback is required");
  if(!std::holds_alternative<Value::Map>(value.data))throw std::invalid_argument("RPC request must be a mapping");
  auto body=pack(value,impl_->config.max_message_bytes);std::vector<uint8_t> frame;frame.reserve(body.size()+4);for(int i=3;i>=0;--i)frame.push_back(uint8_t(body.size()>>(8*i)));frame.insert(frame.end(),body.begin(),body.end());
  auto t=now_ns();auto j=std::make_unique<Impl::Job>(Impl::Job{ep,std::move(frame),std::move(callback),{},t+impl_->config.timeout_ms*1000000,{}});j->result.queued_ns=t;auto id=value.map().find("request_id");if(id!=value.map().end())j->request_id=pack(id->second);
  std::lock_guard<std::mutex> lock(impl_->mutex);if(impl_->stopping||impl_->count.load()>=impl_->config.max_pending)return false;
  auto endpoint=impl_->endpoints.find(ep.key);
  if(endpoint==impl_->endpoints.end()){
    if(impl_->endpoints.size()>=impl_->config.max_endpoints){
      auto oldest=impl_->endpoints.end();
      for(auto it=impl_->endpoints.begin();it!=impl_->endpoints.end();++it)
        if(!it->second.pending && (oldest==impl_->endpoints.end()||it->second.last_used<oldest->second.last_used))oldest=it;
      if(oldest==impl_->endpoints.end())return false; // bounded active capacity
      impl_->evicted_endpoints.push_back(oldest->first);impl_->endpoints.erase(oldest);
    }
    endpoint=impl_->endpoints.emplace(ep.key,Impl::EndpointInfo{ep,0,t}).first;
  }
  ++endpoint->second.pending;endpoint->second.last_used=t;
  ++impl_->count;impl_->incoming.push_back(std::move(j));impl_->signal();return true;
}
void Client::close(){impl_->close();}
size_t Client::pending() const{return impl_->count.load();}
namespace py=pybind11;
namespace {
Value convert_python(py::handle x,unsigned depth) {
  if(depth>64)throw std::invalid_argument("Native value nesting too deep");
  if(x.is_none())return Value();
  if(py::isinstance<py::bool_>(x))return Value(py::cast<bool>(x));
  if(py::isinstance<py::int_>(x)){int overflow=0;auto i=PyLong_AsLongLongAndOverflow(x.ptr(),&overflow);if(!overflow){if(PyErr_Occurred())throw py::error_already_set();return Value(int64_t(i));}PyErr_Clear();auto u=PyLong_AsUnsignedLongLong(x.ptr());if(PyErr_Occurred())throw py::error_already_set();return Value(uint64_t(u));}
  if(py::isinstance<py::float_>(x))return Value(py::cast<double>(x));
  if(py::isinstance<py::str>(x))return Value(py::cast<std::string>(x));
  if(py::isinstance<py::bytes>(x)){auto s=py::cast<std::string>(x);return Value(Value::Bytes(s.begin(),s.end()));}
  if(py::isinstance<py::dict>(x)){Value::Map out;for(auto item:py::reinterpret_borrow<py::dict>(x)){if(!py::isinstance<py::str>(item.first))throw std::invalid_argument("Native map keys must be strings");out.emplace(py::cast<std::string>(item.first),convert_python(item.second,depth+1));}return Value(std::move(out));}
  if(py::isinstance<py::list>(x)||py::isinstance<py::tuple>(x)){Value::Array out;for(auto item:py::reinterpret_borrow<py::sequence>(x))out.push_back(convert_python(item,depth+1));return Value(std::move(out));}
  throw std::invalid_argument("Unsupported native wire Python value");
}
struct TestClient {
  Client client;std::mutex mutex;std::vector<std::pair<uint64_t,Result>> completed;std::atomic<uint64_t> next{0};
  explicit TestClient(Config c):client(c){}
  ~TestClient(){client.close();}
};
}
Value from_python(py::handle x){return convert_python(x,0);}
py::object to_python(const Value& v) {
  switch(v.data.index()) {
    case 0:return py::none();case 1:return py::bool_(std::get<bool>(v.data));case 2:return py::int_(std::get<int64_t>(v.data));case 3:return py::int_(std::get<uint64_t>(v.data));case 4:return py::float_(std::get<double>(v.data));case 5:return py::str(std::get<std::string>(v.data));
    case 6:{auto& b=std::get<Value::Bytes>(v.data);return py::bytes(reinterpret_cast<const char*>(b.data()),b.size());}
    case 7:{py::list a;for(auto& x:std::get<Value::Array>(v.data))a.append(to_python(x));return a;}
    case 8:{py::dict m;for(auto& x:v.map())m[py::str(x.first)]=to_python(x.second);return m;}
  }throw std::logic_error("Invalid native value");
}
void bind_native_wire(py::module_& m) {
  m.def("native_wire_pack",[](py::handle v,size_t limit){auto b=pack(from_python(v),limit);return py::bytes(reinterpret_cast<const char*>(b.data()),b.size());},py::arg("value"),py::arg("limit")=67108864);
  m.def("native_wire_unpack",[](py::bytes b){auto s=py::cast<std::string>(b);return to_python(unpack(reinterpret_cast<const uint8_t*>(s.data()),s.size()));});
  py::class_<Endpoint>(m,"NativeWireEndpoint").def(py::init<const std::string&,uint16_t>()).def_readonly("key",&Endpoint::key);
  py::class_<TestClient>(m,"NativeWireClient")
    .def(py::init([](size_t pending,size_t connections,size_t bytes,uint64_t timeout,size_t endpoints){Config c;c.max_pending=pending;c.max_connections_per_endpoint=connections;c.max_message_bytes=bytes;c.timeout_ms=timeout;c.max_endpoints=endpoints;return std::make_unique<TestClient>(c);}),py::arg("max_pending")=4096,py::arg("connections")=2,py::arg("max_message_bytes")=67108864,py::arg("timeout_ms")=5000,py::arg("max_endpoints")=64)
    .def("submit",[](TestClient& c,const Endpoint& ep,py::handle value){auto v=from_python(value);uint64_t id=++c.next;py::gil_scoped_release release;bool ok=c.client.submit(ep,v,[&c,id](Result r){std::lock_guard<std::mutex> lock(c.mutex);c.completed.emplace_back(id,std::move(r));});return ok?id:uint64_t(0);})
    .def("poll",[](TestClient& c){std::vector<std::pair<uint64_t,Result>> done;{std::lock_guard<std::mutex> lock(c.mutex);done.swap(c.completed);}py::list out;for(auto& x:done){auto& r=x.second;py::dict d;d["id"]=x.first;d["ok"]=r.ok;d["error"]=r.error;d["response"]=to_python(r.response);d["request_bytes"]=r.request_bytes;d["response_bytes"]=r.response_bytes;d["queued_ns"]=r.queued_ns;d["send_start_ns"]=r.send_start_ns;d["send_done_ns"]=r.send_done_ns;d["done_ns"]=r.done_ns;d["connect_start_ns"]=r.connect_start_ns;d["connect_done_ns"]=r.connect_done_ns;d["connection_created"]=r.connection_created;out.append(d);}return out;})
    .def("pending",[](TestClient& c){return c.client.pending();})
    .def("close",[](TestClient& c){py::gil_scoped_release release;c.client.close();});
}
} // namespace dhwire
