#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <chrono>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <vector>
#include <unordered_map>
#ifdef DHDGS_LIBURING
#include <liburing.h>
#include <unistd.h>
#include <cstring>
#endif
namespace py=pybind11;
#ifdef DHDGS_LIBURING
// A single Python reactor owns this object. Buffers live until their CQE is consumed.
class AsyncReader {
  struct Read { std::vector<char> data; std::chrono::steady_clock::time_point start;
    explicit Read(size_t n):data(n),start(std::chrono::steady_clock::now()){} };
  io_uring ring{};
  int fd=-1;
  bool open=false;
  std::unordered_map<uint64_t,std::unique_ptr<Read>> reads;
  std::atomic<uint64_t> submits{0};
  size_t accepted=0;
  void flush() {
    if (!io_uring_sq_ready(&ring)) return;
    int rc=io_uring_submit(&ring); ++submits;
    if(rc<0) throw std::runtime_error(std::string("io_uring_submit: ")+strerror(-rc));
    accepted+=size_t(rc);
    // liburing retains the unaccepted SQ tail. poll() flushes it again, without
    // allocating new tokens or resubmitting already accepted SQEs.
  }
public:
  AsyncReader(int source,unsigned depth,int event_fd=-1) {
    fd=dup(source);
    if(fd<0) throw std::runtime_error("dup index fd failed");
    int rc=io_uring_queue_init(depth,&ring,0);
    if(rc<0){::close(fd);fd=-1;throw std::runtime_error(std::string("io_uring_queue_init: ")+strerror(-rc));}
    open=true;
    if(event_fd>=0) {
      rc=io_uring_register_eventfd(&ring,event_fd);
      if(rc<0){drain_and_close();throw std::runtime_error(std::string("io_uring_register_eventfd: ")+strerror(-rc));}
    }
  }
  void submit(const std::vector<std::tuple<uint64_t,uint64_t,unsigned>>& requests) {
    if(!open) throw std::runtime_error("Reader closed");
    if(requests.size()>io_uring_sq_space_left(&ring)) throw std::runtime_error("SQ capacity exceeded");
    for(auto [token,offset,size]:requests) {
      if(reads.count(token)||!size) throw std::runtime_error("Invalid read token/size");
      auto read=std::make_unique<Read>(size);
      auto sqe=io_uring_get_sqe(&ring);
      io_uring_prep_read(sqe,fd,read->data.data(),size,offset);
      // The user_data ABI predates the convenience helpers in newer liburing.
      sqe->user_data=token;
      reads.emplace(token,std::move(read));
    }
    py::gil_scoped_release release;
    flush();
  }
  py::list poll(unsigned limit) {
    if(!open) throw std::runtime_error("Reader closed");
    {py::gil_scoped_release release;flush();}
    py::list out;
    for(unsigned i=0;i<limit;++i) {
      io_uring_cqe* cqe=nullptr;
      if(io_uring_peek_cqe(&ring,&cqe)!=0) break;
      uint64_t token=cqe->user_data;
      auto it=reads.find(token);
      if(it==reads.end()) throw std::runtime_error("Unknown CQE token");
      auto &read=*it->second;
      int result=cqe->res;
      double ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-read.start).count();
      std::string error;
      if(result<0) error=strerror(-result);
      else if(size_t(result)!=read.data.size()) error="Short node read";
      out.append(py::make_tuple(token,py::bytes(read.data.data(),result>0?size_t(result):0),error,ms));
      io_uring_cqe_seen(&ring,cqe);--accepted;reads.erase(it);
    }
    return out;
  }
  uint64_t submission_calls() const {return submits.load();}
  void drain_and_close() {
    if(!open)return;
    // Do not rely on close(fd) to synchronously cancel kernel accesses. In a
    // fatal submit path, discard only unaccepted SQEs and drain every accepted
    // read before destroying its buffer. Normal shutdown reaches here empty.
    while(accepted) {
      io_uring_cqe* cqe=nullptr;
      int rc=io_uring_wait_cqe(&ring,&cqe);
      if(rc==-EINTR)continue;
      if(rc<0) {
        // Catastrophic ring failure: retaining buffers is safer than allowing
        // a late kernel write into freed memory. This is not a runtime fallback.
        for(auto &entry:reads)entry.second.release();
        break;
      }
      io_uring_cqe_seen(&ring,cqe);--accepted;
    }
    io_uring_queue_exit(&ring);::close(fd);open=false;fd=-1;
  }
  void close() {
    {py::gil_scoped_release release;drain_and_close();}
    reads.clear();
  }
  ~AsyncReader(){drain_and_close();}

};
#endif
void register_async_io(py::module_ &m) {
#ifdef DHDGS_LIBURING
  m.attr("io_uring_compiled")=true;
  py::class_<AsyncReader>(m,"AsyncReader")
    .def(py::init<int,unsigned,int>(),py::arg("fd"),py::arg("depth"),py::arg("event_fd")=-1)
    .def("submit",&AsyncReader::submit)
    .def("poll",&AsyncReader::poll).def("close",&AsyncReader::close)
    .def_property_readonly("submission_calls",&AsyncReader::submission_calls);
#else
  m.attr("io_uring_compiled")=false;
#endif
}
