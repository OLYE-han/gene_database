#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <stdexcept>
#include <functional>
#include <memory>
#include <sys/socket.h>
#include <poll.h>

namespace dhwire {
// Explicit backend selection. No TCP fallback is permitted for an RDMA request.
class Transport {
  bool rdma_;
  uint64_t poll_interval_us_;
public:
  explicit Transport(const std::string& backend="tcp",uint64_t poll_interval_us=50);
  static bool rdma_available();
  bool rdma()const{return rdma_;}
  int socket(int family)const;
  int close(int fd)const;
  int bind(int fd,const sockaddr* address,socklen_t len)const;
  int listen(int fd,int backlog)const;
  int accept(int fd)const;
  int connect(int fd,const sockaddr* address,socklen_t len)const;
  int getsockname(int fd,sockaddr* address,socklen_t* len)const;
  int getsockopt(int fd,int level,int option,void* value,socklen_t* len)const;
  int setsockopt(int fd,int level,int option,const void* value,socklen_t len)const;
  ssize_t send(int fd,const void* data,size_t len,int flags)const;
  ssize_t recv(int fd,void* data,size_t len,int flags)const;
  int poll(pollfd* fds,nfds_t n,int timeout,int wake_index=0)const;
};
// rsockets do not implement SO_REUSEPORT. One acceptor hands established RDMA
// connections to the existing receive reactors; it never handles query frames.
class RdmaAcceptor {
  struct Impl;std::unique_ptr<Impl> impl_;
public:
  RdmaAcceptor(const std::string& host,uint16_t port,size_t backlog,uint64_t poll_interval_us=50,const std::vector<int>& cpus={});
  ~RdmaAcceptor();
  uint16_t port()const;
  void start(std::function<bool(int)> adopt);
  void close();
};
}
