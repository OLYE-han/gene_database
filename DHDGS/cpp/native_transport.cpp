#include "native_transport.hpp"
#include "native_affinity.hpp"
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <atomic>
#include <thread>
#include <chrono>
#include <vector>
#include <mutex>
#include <cerrno>
#include <cstring>
#ifdef DHDGS_RDMA
#include <rdma/rsocket.h>
#endif
namespace dhwire {
Transport::Transport(const std::string& backend,uint64_t poll_interval_us):rdma_(backend=="rdma"),poll_interval_us_(poll_interval_us){
  if(!poll_interval_us||poll_interval_us>100000)throw std::invalid_argument("Invalid RDMA poll interval");
  if(backend!="tcp"&&backend!="rdma")throw std::invalid_argument("network_module.transport must be tcp or rdma");
  if(rdma_&&!rdma_available())throw std::runtime_error("RDMA requested but DHDGS was built without librdmacm/rsockets");
}
bool Transport::rdma_available(){
#ifdef DHDGS_RDMA
  return true;
#else
  return false;
#endif
}
int Transport::socket(int family)const{
#ifdef DHDGS_RDMA
  if(rdma_){int fd=rsocket(family,SOCK_STREAM,0);if(fd<0)return fd;
    if(rfcntl(fd,F_SETFL,O_NONBLOCK)<0){int e=errno;rclose(fd);errno=e;return -1;}
    rfcntl(fd,F_SETFD,FD_CLOEXEC);return fd;}
#endif
  return ::socket(family,SOCK_STREAM|SOCK_NONBLOCK|SOCK_CLOEXEC,0);
}
int Transport::accept(int fd)const{
#ifdef DHDGS_RDMA
  if(rdma_){int x=raccept(fd,nullptr,nullptr);if(x<0)return x;
    if(rfcntl(x,F_SETFL,O_NONBLOCK)<0){int e=errno;rclose(x);errno=e;return -1;}
    rfcntl(x,F_SETFD,FD_CLOEXEC);return x;}
#endif
  return ::accept4(fd,nullptr,nullptr,SOCK_NONBLOCK|SOCK_CLOEXEC);
}
#ifdef DHDGS_RDMA
#define CALL_RDMA(fn,args) if(rdma_)return r##fn args
#else
#define CALL_RDMA(fn,args)
#endif
int Transport::close(int fd)const{if(fd<0)return 0;CALL_RDMA(close,(fd));return ::close(fd);}
int Transport::bind(int fd,const sockaddr* a,socklen_t n)const{CALL_RDMA(bind,(fd,a,n));return ::bind(fd,a,n);}
int Transport::listen(int fd,int n)const{CALL_RDMA(listen,(fd,n));return ::listen(fd,n);}
int Transport::connect(int fd,const sockaddr* a,socklen_t n)const{CALL_RDMA(connect,(fd,a,n));return ::connect(fd,a,n);}
int Transport::getsockname(int fd,sockaddr* a,socklen_t* n)const{CALL_RDMA(getsockname,(fd,a,n));return ::getsockname(fd,a,n);}
int Transport::getsockopt(int fd,int l,int o,void* v,socklen_t* n)const{CALL_RDMA(getsockopt,(fd,l,o,v,n));return ::getsockopt(fd,l,o,v,n);}
int Transport::setsockopt(int fd,int l,int o,const void* v,socklen_t n)const{CALL_RDMA(setsockopt,(fd,l,o,v,n));return ::setsockopt(fd,l,o,v,n);}
ssize_t Transport::send(int fd,const void* d,size_t n,int f)const{CALL_RDMA(send,(fd,d,n,f));return ::send(fd,d,n,f);}
ssize_t Transport::recv(int fd,void* d,size_t n,int f)const{CALL_RDMA(recv,(fd,d,n,f));return ::recv(fd,d,n,f);}
int Transport::poll(pollfd* f,nfds_t n,int timeout,int wake_index)const{
#ifdef DHDGS_RDMA
  if(rdma_){
    // Keep ordinary kernel eventfds OUT of rpoll. Mixed-descriptor rpoll in
    // rdma-core uses a process-global helper and can spin/serialize many reactors.
    // Each descriptor here has an explicit type; RDMA-only polling is nonblocking.
    std::vector<pollfd> rdma_fds;std::vector<size_t> indices;
    for(size_t i=0;i<n;++i){f[i].revents=0;if(int(i)!=wake_index&&f[i].fd>=0){rdma_fds.push_back(f[i]);indices.push_back(i);}}
    if(rdma_fds.empty())return ::poll(f,n,timeout);
    auto until=std::chrono::steady_clock::now()+std::chrono::milliseconds(std::max(0,timeout));
    for(;;){
      pollfd wakefd{f[wake_index].fd,POLLIN,0};
      int ready=::poll(&wakefd,1,0);if(ready<0)return ready;
      int rc=rpoll(rdma_fds.data(),rdma_fds.size(),0);if(rc<0)return rc;
      for(size_t i=0;i<indices.size();++i)f[indices[i]].revents=rdma_fds[i].revents;
      f[wake_index].revents=wakefd.revents;
      if(rc||ready||timeout==0)return rc+ready;
      auto left=until-std::chrono::steady_clock::now();if(timeout>=0&&left<=decltype(left)::zero())return 0;
      auto ns=std::min<int64_t>(poll_interval_us_*1000,std::chrono::duration_cast<std::chrono::nanoseconds>(left).count());
      if(timeout<0)ns=poll_interval_us_*1000;
      timespec delay{0,std::max<int64_t>(1,ns)};
      rc=::ppoll(&wakefd,1,&delay,nullptr);if(rc<0)return rc;
    }
  }
#endif
  return ::poll(f,n,timeout);
}
#undef CALL_RDMA
struct RdmaAcceptor::Impl {
  Transport io;std::vector<int> cpus;int fd=-1,wake=-1;uint16_t port=0;
  std::atomic<bool> stopping{false};std::thread thread;std::mutex close_mu;
  Impl(const std::string& host,uint16_t p,size_t backlog,uint64_t us,const std::vector<int>& ids):io("rdma",us),cpus(ids){
    try{
      fd=io.socket(AF_INET);if(fd<0)throw std::runtime_error("RDMA listener socket failed");
      int one=1;io.setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));
      sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(p);
      if(inet_pton(AF_INET,host.c_str(),&a.sin_addr)!=1)throw std::invalid_argument("RDMA listen host must be IPv4");
      if(io.bind(fd,reinterpret_cast<sockaddr*>(&a),sizeof(a))<0||io.listen(fd,int(backlog))<0)
        throw std::runtime_error(std::string("RDMA bind/listen: ")+strerror(errno));
      socklen_t len=sizeof(a);if(io.getsockname(fd,reinterpret_cast<sockaddr*>(&a),&len)<0)throw std::runtime_error("RDMA getsockname failed");
      port=ntohs(a.sin_port);wake=eventfd(0,EFD_NONBLOCK|EFD_CLOEXEC);
      if(wake<0)throw std::runtime_error("RDMA acceptor eventfd failed");
    }catch(...){io.close(fd);if(wake>=0)::close(wake);throw;}
  }
  void start(std::function<bool(int)> adopt){
    if(thread.joinable()||stopping)throw std::logic_error("RDMA acceptor already started/closed");
    thread=std::thread([this,adopt=std::move(adopt)]{
      while(!stopping){pollfd fds[]={{fd,POLLIN,0},{wake,POLLIN,0}};
        int n=io.poll(fds,2,100,1);if(n<0){if(errno==EINTR)continue;break;}
        if(stopping)break;
        if(fds[0].revents&POLLIN)for(size_t count=0;count<64&&!stopping;++count){
          int x=io.accept(fd);if(x<0){if(errno==EINTR)continue;break;}
          bool taken=false;try{taken=adopt(x);}catch(...){}
          if(!taken)io.close(x);
        }
      }
    });
    try{dhruntime::bind_thread(thread,cpus);}catch(...){close();throw;}
  }
  void close(){std::lock_guard<std::mutex> l(close_mu);stopping=true;
    if(wake>=0){uint64_t one=1;auto ignored=::write(wake,&one,sizeof(one));(void)ignored;}
    if(thread.joinable())thread.join();io.close(fd);fd=-1;if(wake>=0)::close(wake);wake=-1;
  }
  ~Impl(){close();}
};
RdmaAcceptor::RdmaAcceptor(const std::string& h,uint16_t p,size_t b,uint64_t us,const std::vector<int>& cpus):impl_(std::make_unique<Impl>(h,p,b,us,cpus)){}
RdmaAcceptor::~RdmaAcceptor()=default;
uint16_t RdmaAcceptor::port()const{return impl_->port;}
void RdmaAcceptor::start(std::function<bool(int)> adopt){impl_->start(std::move(adopt));}
void RdmaAcceptor::close(){impl_->close();}
}
