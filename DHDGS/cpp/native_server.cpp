#include "native_server.hpp"
#include "native_pool.hpp"
#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <map>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace dhwire {
namespace {
uint64_t stamp() {
  timespec t{};
  clock_gettime(CLOCK_MONOTONIC, &t);
  return uint64_t(t.tv_sec) * 1000000000 + t.tv_nsec;
}
} // namespace
struct Server::Impl {
  struct Conn {
    int fd = -1;
    uint64_t id = 0, touched = 0;
    bool processing = false;
    std::vector<uint8_t> input, output;
    size_t expected = 0, written = 0;
    std::function<void(uint64_t, size_t)> sent;
  };
  struct Completed {
    uint64_t id;
    Reply reply;
  };
  struct AsyncMailbox {std::mutex mu;Impl* owner=nullptr;};
  std::shared_ptr<AsyncMailbox> mailbox=std::make_shared<AsyncMailbox>();
  ServerConfig config;
  Transport io;
  std::deque<std::pair<int,bool>> accepted;
  std::function<bool(int)> migration;size_t migration_left=0;
  std::atomic<size_t> migrated{0};
  Handler data, admin;
  int listener = -1, wake = -1;
  uint16_t bound_port = 0;
  std::atomic<bool> stopping{false},draining{false},listener_stopped{false};
  bool closed=false;
  std::atomic<size_t> clients{0}, requests{0}, errors{0}, rejected{0};
  mutable std::mutex mu;
  std::mutex close_mu;
  std::deque<Completed> completed;
  std::unique_ptr<dhruntime::Pool> workers, administrators;
  std::thread thread;
  std::map<uint64_t, Conn> conns;
  uint64_t next_id = 0;
  Impl(ServerConfig c, Handler d, Handler a)
      : config(c), io(c.transport,c.rdma_poll_interval_us), data(std::move(d)), admin(std::move(a)) {
    if (!c.max_message_bytes ||
        c.max_message_bytes > UINT32_MAX || !c.idle_timeout_ms)
      throw std::invalid_argument("Invalid native server limits");
    mailbox->owner=this;
    try {
      if (!c.external_accept) {
      listener = io.socket(AF_INET);
      if (listener < 0)
        throw std::runtime_error("Native listener socket failed");
      int one = 1;
      io.setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
      if(c.reuse_port && io.setsockopt(listener,SOL_SOCKET,SO_REUSEPORT,&one,sizeof(one))<0)
        throw std::runtime_error("SO_REUSEPORT failed");
      sockaddr_in address{};
      address.sin_family = AF_INET;
      address.sin_port = htons(c.port);
      if (inet_pton(AF_INET, c.host.c_str(), &address.sin_addr) != 1)
        throw std::invalid_argument(
            "Native listen host must be an IPv4 address");
      if (io.bind(listener, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)) < 0 ||
          io.listen(listener, int(c.listen_backlog)) < 0)
        throw std::runtime_error(std::string("Native bind/listen: ") +
                                 std::strerror(errno));
      socklen_t len = sizeof(address);
      if (io.getsockname(listener, reinterpret_cast<sockaddr *>(&address), &len) <
          0)
        throw std::runtime_error("getsockname failed");
      bound_port = ntohs(address.sin_port);
      } else { bound_port = c.port; }
      wake = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
      if (wake < 0)
        throw std::runtime_error("Native server eventfd failed");
      workers = std::make_unique<dhruntime::Pool>(c.workers, c.queue_capacity, c.cpu_ids, false);
      if (admin)
        administrators = std::make_unique<dhruntime::Pool>(1, c.queue_capacity, c.cpu_ids, false);
      thread = std::thread([this] { run(); });
      dhruntime::bind_thread(thread,c.cpu_ids);
    } catch (...) {
      stopping=true;if(wake>=0)signal();if(thread.joinable())thread.join();
      if (workers)
        workers->close();
      if (administrators)
        administrators->close();
      if (listener >= 0)
        io.close(listener);
      if (wake >= 0)
        ::close(wake);
      throw;
    }
  }
  void signal() {
    uint64_t one = 1;
    ssize_t rc;
    do {
      rc = write(wake, &one, sizeof(one));
    } while (rc < 0 && errno == EINTR);
  }
  void finish(uint64_t id, Reply response) {
    {
      std::lock_guard<std::mutex> lock(mu);
      completed.push_back({id, std::move(response)});
    }
    signal();
  }
  void drop(uint64_t id) {
    auto it = conns.find(id);
    if (it != conns.end()) {
      auto callback = std::move(it->second.sent);
      const auto bytes = it->second.written;
      io.close(it->second.fd);
      conns.erase(it);
      --clients;
      if(config.connection_budget)--*config.connection_budget;
      if (callback)
        try {
          callback(stamp(), bytes);
        } catch (...) {
          ++errors;
        }
    }
  }
  void response(Conn &c, Reply reply) {
    try {
      auto payload = pack(reply.message, config.max_message_bytes);
      size_t n = payload.size();
      c.output = {uint8_t(n >> 24), uint8_t(n >> 16), uint8_t(n >> 8),
                  uint8_t(n)};
      c.output.insert(c.output.end(), payload.begin(), payload.end());
      c.written = 0;
      c.sent = std::move(reply.sent);
    } catch (...) {
      ++errors;
      if (reply.sent)
        try {
          reply.sent(stamp(), 0);
        } catch (...) {
          ++errors;
        }
      drop(c.id);
    }
  }
  bool adopt(int fd,bool credited=false) {
    std::lock_guard<std::mutex> lock(mu);
    if(stopping||draining||(config.max_connections&&accepted.size()>=config.max_connections)){++rejected;return false;}
    accepted.emplace_back(fd,credited);signal();return true;
  }
  void attach(int fd,bool credited=false) {
    if(config.max_connections&&clients>=config.max_connections){io.close(fd);if(credited&&config.connection_budget)--*config.connection_budget;++rejected;return;}
    if(!credited && config.connection_budget){
      auto previous=config.connection_budget->fetch_add(1);
      if(config.max_connections&&previous>=config.max_connections){--*config.connection_budget;io.close(fd);++rejected;return;}
    }
    int one=1;io.setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
    Conn c;c.fd=fd;c.id=++next_id;c.touched=stamp();conns.emplace(c.id,std::move(c));++clients;
  }
  void accept_all() {
    for(;;){int fd=io.accept(listener);if(fd<0){if(errno==EINTR)continue;return;}attach(fd);}
  }
  void input(Conn &c) {
    while (!c.processing) {
      uint8_t buffer[65536];
      size_t nleft = (c.expected ? c.expected : 4) - c.input.size();
      auto n = io.recv(c.fd, buffer, std::min(nleft, sizeof(buffer)), 0);
      if (n < 0 && errno == EINTR)
        continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return;
      if (n <= 0) {
        drop(c.id);
        return;
      }
      c.touched = stamp();
      c.input.insert(c.input.end(), buffer, buffer + n);
      if (!c.expected && c.input.size() == 4) {
        uint64_t bytes = 0;
        for (auto b : c.input)
          bytes = (bytes << 8) | b;
        if (!bytes || bytes > config.max_message_bytes) {
          ++errors;
          drop(c.id);
          return;
        }
        c.expected = bytes + 4;
      }
      if (c.expected && c.input.size() == c.expected) {
        const auto received = stamp(), id = c.id;
        const size_t bytes = c.input.size();
        auto frame = std::move(c.input);
        c.input.clear();
        c.expected = 0;
        c.processing = true;
        // Decoding occurs on workers; the reactor never blocks on the GIL.
        auto task = [this, id, received, bytes,
                     frame = std::move(frame)]() mutable {
          Value request;
          Reply answer;
          try {
            request = unpack(frame.data() + 4, frame.size() - 4);
            ++requests;
            auto op = dhvalue::optional<std::string>(request, "op", "");
            if (admin && (op.rfind("admin_", 0) == 0 || op == "metrics")) {
              auto work = [this, id, received, bytes,
                           request = std::move(request)]() mutable {
                Reply result;
                try {
                  result = admin(request, received, bytes);
                } catch (const std::exception &e) {
                  ++errors;
                  result.message = Value::Map{
                      {"ok", false},
                      {"error", e.what()},
                      {"request_id", dhvalue::has(request, "request_id")
                                         ? dhvalue::at(request, "request_id")
                                         : Value()}};
                }
                finish(id, std::move(result));
              };
              if (!administrators->submit(std::move(work)))
                throw std::runtime_error(
                    "Native admin queue capacity exceeded");
              return;
            }
            answer = data(request, received, bytes);
          } catch (const std::exception &e) {
            ++errors;
            Value rid;
            if (std::holds_alternative<Value::Map>(request.data) &&
                dhvalue::has(request, "request_id"))
              rid = dhvalue::at(request, "request_id");
            answer.message = Value::Map{{"ok", false},
                                        {"error", e.what()},
                                        {"request_id", std::move(rid)}};
          }
          if(answer.deferred) {
            auto channel=mailbox;auto once=std::make_shared<std::atomic<bool>>(false);
            auto deliver=[channel,once,id](Reply reply){
              if(once->exchange(true))return;
              std::lock_guard<std::mutex> l(channel->mu);
              if(channel->owner)channel->owner->finish(id,std::move(reply));
            };
            try{answer.deferred(deliver);}
            catch(const std::exception& e){Reply r;r.message=Value::Map{{"ok",false},{"error",e.what()}};deliver(std::move(r));}
          } else finish(id, std::move(answer));
        };
        if (!workers->submit(std::move(task))) {
          ++rejected;
          response(c,
                   Reply{Value::Map{{"ok", false},
                                    {"error",
                                     "Native receive queue capacity exceeded"}},
                         {}});
        }
        return;
      }
    }
  }
  void output(Conn &c) {
    while (c.written < c.output.size()) {
      auto n = io.send(c.fd, c.output.data() + c.written,
                    c.output.size() - c.written, MSG_NOSIGNAL);
      if (n < 0 && errno == EINTR)
        continue;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        return;
      if (n <= 0) {
        drop(c.id);
        return;
      }
      c.written += n;
      c.touched = stamp();
    }
    auto callback = std::move(c.sent);
    auto bytes = c.output.size();
    c.output.clear();
    c.processing = false;
    if (callback)
      try {
        callback(stamp(), bytes);
      } catch (...) {
        ++errors;
      }
  }
  void run() {
    while (!stopping) {
      if(draining){if(listener>=0){io.close(listener);listener=-1;}listener_stopped=true;}
      std::deque<std::pair<int,bool>> incoming;
      {std::lock_guard<std::mutex> lock(mu);incoming.swap(accepted);}
      for(auto [fd,credited]:incoming)attach(fd,credited);
      std::deque<Completed> ready;
      {
        std::lock_guard<std::mutex> lock(mu);
        ready.swap(completed);
      }
      for (auto &r : ready) {
        auto p = conns.find(r.id);
        if (p != conns.end())
          response(p->second, std::move(r.reply));
        else if (r.reply.sent)
          try {
            r.reply.sent(stamp(), 0);
          } catch (...) {
            ++errors;
          }
      }
      // Ownership moves only between frames, with no handler/reply in flight.
      // Keep the shared connection credit while the descriptor is in transit.
      std::function<bool(int)> move_to;size_t move_left=0;
      {std::lock_guard<std::mutex> l(mu);move_to=migration;move_left=migration_left;}
      size_t moved=0;
      if(move_to&&move_left){for(auto it=conns.begin();it!=conns.end()&&moved<move_left;){auto& c=it->second;
        if(!c.processing&&c.input.empty()&&c.output.empty()&&move_to(c.fd)){it=conns.erase(it);--clients;++migrated;++moved;}else ++it;
      }
      std::lock_guard<std::mutex> l(mu);migration_left-=std::min(migration_left,moved);if(!migration_left)migration={};}
      std::vector<pollfd> fds{{listener, POLLIN, 0}, {wake, POLLIN, 0}};
      std::vector<uint64_t> ids;
      for (const auto &[id, c] : conns) {
        short events =
            (!c.processing ? POLLIN : 0) | (!c.output.empty() ? POLLOUT : 0);
        if (events) {
          fds.push_back({c.fd, events, 0});
          ids.push_back(id);
        }
      }
      int rc = io.poll(fds.data(), fds.size(), 100, 1);
      if (rc < 0 && errno != EINTR) {
        ++errors;
        break;
      }
      if (fds[0].revents & POLLIN)
        accept_all();
      if (fds[1].revents & POLLIN) {
        uint64_t v;
        while (read(wake, &v, sizeof(v)) > 0) {
        }
      }
      for (size_t i = 0; i < ids.size(); ++i) {
        auto p = conns.find(ids[i]);
        if (p == conns.end())
          continue;
        auto events = fds[i + 2].revents;
        if (events & POLLIN)
          input(p->second);
        p = conns.find(ids[i]);
        if (p == conns.end())
          continue;
        if (events & POLLOUT)
          output(p->second);
        if (events & (POLLERR | POLLHUP | POLLNVAL))
          drop(ids[i]);
      }
      std::vector<uint64_t> expired;
      const auto now = stamp();
      for (const auto &[id, c] : conns)
        if ((!c.processing || !c.output.empty()) &&
            now - c.touched > config.idle_timeout_ms * 1000000)
          expired.push_back(id);
      for (auto id : expired)
        drop(id);
    }
    while (!conns.empty())
      drop(conns.begin()->first);
  }
  void close() {
    std::lock_guard<std::mutex> l(close_mu);
    if (closed) return;
    closed=true;
    {std::lock_guard<std::mutex> l(mailbox->mu);mailbox->owner=nullptr;}
    stopping = true;
    signal();
    if (thread.joinable())
      thread.join();
    workers->close();
    if (administrators)
      administrators->close();
    std::deque<Completed> remaining;
    {
      std::lock_guard<std::mutex> lock(mu);
      remaining.swap(completed);
      for(auto [fd,credited]:accepted){io.close(fd);if(credited&&config.connection_budget)--*config.connection_budget;}accepted.clear();migration={};migration_left=0;
    }
    for (auto &r : remaining)
      if (r.reply.sent)
        try {
          r.reply.sent(stamp(), 0);
        } catch (...) {
          ++errors;
        }
    io.close(listener);
    ::close(wake);
    listener = wake = -1;
  }
};
Server::Server(ServerConfig c, Handler d, Handler a)
    : impl_(std::make_unique<Impl>(std::move(c), std::move(d), std::move(a))) {}
Server::~Server() { close(); }
bool Server::adopt(int fd){return impl_->adopt(fd);}
bool Server::adopt_transferred(int fd){return impl_->adopt(fd,true);}
void Server::migrate_idle(std::function<bool(int)> target,size_t maximum){std::lock_guard<std::mutex> l(impl_->mu);if(!impl_->stopping&&!impl_->migration_left){impl_->migration=std::move(target);impl_->migration_left=maximum;impl_->signal();}}
void Server::stop_accepting(){impl_->draining=true;impl_->signal();}
void Server::close() { impl_->close(); }
uint16_t Server::port() const { return impl_->bound_port; }
Value Server::metrics() const {
  return Value::Map{{"transport",impl_->config.transport},{"accepting",!impl_->listener_stopped.load()},{"connections", uint64_t(impl_->clients)},
                    {"migrated_connections",uint64_t(impl_->migrated)},
                    {"requests", uint64_t(impl_->requests)},
                    {"errors", uint64_t(impl_->errors)},
                    {"rejected", uint64_t(impl_->rejected)},
                    {"workers", impl_->workers->metrics()}};
}
} // namespace dhwire
