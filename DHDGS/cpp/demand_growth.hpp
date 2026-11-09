#pragma once
#include "replica_overlay.hpp"
#include "one_sided.hpp"
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace dhgrowth {
using V=dhwire::Value;
using Id=uint64_t;
using Read=std::function<bool(const std::vector<Id>&,dhonesided::Complete,dhonesided::CodeLookup)>;
// Closing this gate prevents background maintenance from calling a destroyed
// runtime. submit must only enqueue; the gate is never held during network I/O.
struct ReadGate {
  std::mutex mutex;
  std::function<bool(size_t,const V&,const std::vector<Id>&,dhonesided::Complete,dhonesided::CodeLookup)> submit;
  bool read(size_t source,const V& auth,const std::vector<Id>& ids,dhonesided::Complete done,dhonesided::CodeLookup lookup){
    std::lock_guard<std::mutex> lock(mutex);return submit&&submit(source,auth,ids,std::move(done),std::move(lookup));
  }
  void close(){std::lock_guard<std::mutex> lock(mutex);submit={};}
};
struct File {int fd=-1;~File();};
struct Entry {
  Id id=0;
  std::vector<Id> path; // starts at an immutable core/durable vertex
  std::vector<uint8_t> bytes;
  std::shared_ptr<dhsearch::ReplicaOverlay> codes;
  std::shared_ptr<File> file;
  uint64_t offset=0;
  std::atomic<uint64_t> count{0},last{0};
  bool ready=false,promoting=false,promoted=false;
};
using Entries=std::vector<std::shared_ptr<Entry>>;
using Done=std::function<void(Entries,std::string)>;
class Cache : public std::enable_shared_from_this<Cache> {
  struct Impl;std::unique_ptr<Impl> p_;
public:
  Cache(std::shared_ptr<dhsearch::ReplicaOverlay>,V settings,size_t source_bytes,
        size_t source,std::string epoch,size_t dim,size_t nodes,std::string file,std::function<void()> before_write={});
  ~Cache();
  std::shared_ptr<Entry> acquire(Id);
  bool contains_durable(Id)const;
  bool can_request(const std::vector<Id>&)const;
  void request(const std::vector<Id>&,const std::vector<std::vector<Id>>&,Read,dhonesided::CodeLookup,Done);
  void expanded(const std::shared_ptr<Entry>&);
  void read(const std::shared_ptr<Entry>&,void*,size_t)const;
  V metrics()const;
  void seal(const std::string& reason="training_finished");
  void close();
  bool learning()const;
  size_t source()const;
  std::string version()const;
};
void bind(pybind11::module_&);
}
