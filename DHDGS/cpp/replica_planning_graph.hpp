#pragma once
#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>

namespace dhplanning {
using Id=uint64_t;
using Rows=std::unordered_map<Id,size_t>;
// One immutable-index build session, never shared with query workers. Futures
// retain adjacency storage until the session closes; no eviction can invalidate
// a reference held by a verifier. Both cache bytes and queued reads are bounded.
class Graph {
 struct Item {Id id;std::promise<std::vector<Id>> promise;
   std::shared_future<std::vector<Id>> ready=promise.get_future().share();};
 int fd_=-1;size_t dim_,stride_,header_,degree_,limit_,batch_,queue_limit_,cost_;
 struct stat identity_{};std::vector<Id> ids_;std::string epoch_;
 std::mutex mu_;std::condition_variable wake_;bool closed_=false;
 std::unordered_map<Id,std::shared_ptr<Item>> cache_;
 std::deque<std::shared_ptr<Item>> queue_;std::vector<std::thread> workers_;
 size_t peak_=0;
 std::atomic<size_t> reads_{0},bytes_{0},hits_{0},waits_{0};
 std::atomic<uint64_t> read_ns_{0},wait_ns_{0};
 std::atomic<bool> cancelled_{false};
 static uint64_t ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
 static bool same(const struct stat&a,const struct stat&b){return a.st_dev==b.st_dev&&a.st_ino==b.st_ino&&a.st_size==b.st_size&&a.st_mtim.tv_sec==b.st_mtim.tv_sec&&a.st_mtim.tv_nsec==b.st_mtim.tv_nsec&&a.st_ctim.tv_sec==b.st_ctim.tv_sec&&a.st_ctim.tv_nsec==b.st_ctim.tv_nsec;}
 void read_item(const std::shared_ptr<Item>& item) noexcept {
  try {
   if(cancelled_)throw std::runtime_error("Planning reads cancelled");
   std::vector<char> raw(4+8*degree_);size_t done=0;auto started=ns();
   uint64_t off=header_+rows->at(item->id)*stride_+4*dim_;
   while(done<raw.size()) {ssize_t n=::pread(fd_,raw.data()+done,raw.size()-done,off+done);
    if(n<0&&errno==EINTR)continue;if(n<=0)throw std::runtime_error("Short planning adjacency read");done+=n;}
   uint32_t d;std::memcpy(&d,raw.data(),4);
   if(d>degree_)throw std::invalid_argument("Invalid planning degree");
   std::vector<Id> out(d);if(d)std::memcpy(out.data(),raw.data()+4,d*8);
   ++reads_;bytes_+=raw.size();read_ns_+=ns()-started;item->promise.set_value(std::move(out));
  }catch(...){item->promise.set_exception(std::current_exception());}
 }
 void worker(){for(;;){std::shared_ptr<Item> item;{
   std::unique_lock<std::mutex> lock(mu_);wake_.wait(lock,[&]{return closed_||!queue_.empty();});
   if(queue_.empty())return;item=std::move(queue_.front());queue_.pop_front();}
   read_item(item);
  }}
 std::shared_ptr<Item> request(Id id,bool speculative){
  std::shared_ptr<Item> item;bool inline_read=false;
  {std::lock_guard<std::mutex> lock(mu_);
   if(closed_||cancelled_)throw std::runtime_error("Planning graph closed/cancelled");
   auto it=cache_.find(id);if(it!=cache_.end()){++hits_;return it->second;}
   if(!rows->count(id))throw std::invalid_argument("Nonlocal planning adjacency");
   if(cache_.size()>=limit_/cost_) {if(speculative)return {};throw std::runtime_error("Planning adjacency cache limit");}
   if(speculative&&(workers_.empty()||queue_.size()>=queue_limit_))return {};
   item=std::make_shared<Item>();item->id=id;cache_.emplace(id,item);
   inline_read=workers_.empty()||queue_.size()>=queue_limit_;
   if(!inline_read){queue_.push_back(item);peak_=std::max(peak_,queue_.size());}
  }
  if(inline_read)read_item(item);else wake_.notify_one();return item;
 }
public:
 std::shared_ptr<Rows> rows=std::make_shared<Rows>();
 Graph(int fd,Array<Id> ids,py::dict meta,size_t limit,size_t batch,size_t threads,size_t queue)
  :dim_(meta["dimension"].cast<size_t>()),stride_(meta["record_bytes"].cast<size_t>()),header_(meta["header_bytes"].cast<size_t>()),degree_(meta["max_degree"].cast<size_t>()),limit_(limit),batch_(batch),queue_limit_(queue),cost_(8*degree_+256),epoch_(meta["epoch"].cast<std::string>()) {
  require(ids.ndim()==1&&batch>0&&queue>0&&threads<=16&&limit>=cost_&&stride_>=4*dim_+4+8*degree_,"Invalid planning graph limits/layout");
  ids_.assign(ids.data(),ids.data()+ids.size());rows->reserve(ids_.size());
  for(size_t i=0;i<ids_.size();++i)require(rows->emplace(ids_[i],i).second,"Duplicate planning ID");
  fd_=::dup(fd);if(fd_<0||::fstat(fd_,&identity_)){if(fd_>=0)::close(fd_);throw std::runtime_error("Cannot pin planning index");}
  try{for(size_t i=0;i<threads;++i)workers_.emplace_back([this]{worker();});}
  catch(...){close();throw;}
 }
 ~Graph(){close();}
 void validate(int fd,const Array<Id>& ids,const py::dict& meta){
  struct stat s{};
  if(cancelled_||::fstat(fd,&s)||!same(identity_,s)||meta["epoch"].cast<std::string>()!=epoch_
    ||ids.ndim()!=1||size_t(ids.size())!=ids_.size()||std::memcmp(ids.data(),ids_.data(),ids_.size()*8)
    ||meta["dimension"].cast<size_t>()!=dim_||meta["record_bytes"].cast<size_t>()!=stride_
    ||meta["header_bytes"].cast<size_t>()!=header_||meta["max_degree"].cast<size_t>()!=degree_)
     throw std::invalid_argument("Planning index identity changed");
 }
 void check(){if(cancelled_)throw std::runtime_error("Planning cancelled");}
 void cancel(){cancelled_=true;wake_.notify_all();}
 size_t batch()const{return batch_;}
 size_t used(){std::lock_guard<std::mutex> lock(mu_);return cache_.size()*cost_;}
 template<class It> void prefetch(It begin,It end){
  std::vector<Id> batch;batch.reserve(batch_);
  for(;begin!=end&&batch.size()<batch_;++begin)batch.push_back(*begin);
  std::sort(batch.begin(),batch.end(),[&](Id a,Id b){return rows->at(a)<rows->at(b);});
  for(auto id:batch)request(id,true);
 }
 const std::vector<Id>& adjacency(Id id){auto p=request(id,false);auto t=ns();
  if(p->ready.wait_for(std::chrono::seconds(0))!=std::future_status::ready)++waits_;
  const auto& out=p->ready.get();wait_ns_+=ns()-t;return out;
 }
 py::dict balls(const std::vector<std::vector<Id>>& roots,int hops,size_t maxrows,size_t maxscans,double seconds){
  require(hops>0&&hops<=8&&maxrows&&maxscans&&std::isfinite(seconds)&&seconds>0,"Invalid complete-ball limits");
  std::vector<std::vector<Id>> result;size_t scans=0;
  const auto deadline=std::chrono::steady_clock::now()+std::chrono::duration<double>(seconds);
  {py::gil_scoped_release release;
   for(const auto& group:roots){std::unordered_set<Id> seen(group.begin(),group.end());
    if(seen.empty()||seen.size()>maxrows)throw std::invalid_argument("Invalid complete-ball seeds");
    for(auto id:seen)if(!rows->count(id))throw std::invalid_argument("Nonlocal complete-ball seed");
    std::vector<Id> front(seen.begin(),seen.end());
    for(int level=0;level<hops&&!front.empty();++level){
     std::sort(front.begin(),front.end());std::unordered_set<Id> next;
     for(size_t i=0;i<front.size();++i){check();
      if(std::chrono::steady_clock::now()>deadline)throw std::runtime_error("Complete-ball deadline");
      if(i%batch_==0)prefetch(front.begin()+i,front.end());
      for(auto v:adjacency(front[i])){if(++scans>maxscans)throw std::runtime_error("Complete-ball edge budget");
       if(rows->count(v)&&!seen.count(v))next.insert(v);}
      if(seen.size()+next.size()>maxrows)throw std::runtime_error("Complete-ball row budget");
     }
     seen.insert(next.begin(),next.end());front.assign(next.begin(),next.end());
    }
    result.emplace_back(seen.begin(),seen.end());std::sort(result.back().begin(),result.back().end());
   }
  }
  py::dict out;out["members"]=result;out["edge_scans"]=scans;return out;
 }
 void close(){ {std::lock_guard<std::mutex> lock(mu_);closed_=true;}wake_.notify_all();
  for(auto& worker:workers_)if(worker.joinable())worker.join();workers_.clear();
  if(fd_>=0){::close(fd_);fd_=-1;}
 }
 py::dict stats(){py::dict d;std::lock_guard<std::mutex> lock(mu_);
  d["adjacency_reads"]=reads_.load();d["logical_read_bytes"]=bytes_.load();d["cache_hits"]=hits_.load();
  d["cache_reserved_bytes"]=cache_.size()*cost_;d["queue_peak"]=peak_;d["waits"]=waits_.load();
  d["read_seconds"]=read_ns_.load()/1e9;d["wait_seconds"]=wait_ns_.load()/1e9;return d;}
};
// Share the immutable ID directory without copying it into every verifier.
struct RowView {
 std::shared_ptr<Rows> value=std::make_shared<Rows>();
 auto find(Id id)const{return value->find(id);}auto end()const{return value->end();}
 size_t count(Id id)const{return value->count(id);}size_t size()const{return value->size();}
 void reserve(size_t n){value->reserve(n);}auto emplace(Id id,size_t row){return value->emplace(id,row);}
};
inline void bind(py::module_& m){py::class_<Graph,std::shared_ptr<Graph>>(m,"ReplicaPlanningGraph")
 .def(py::init<int,Array<Id>,py::dict,size_t,size_t,size_t,size_t>())
 .def("validate",&Graph::validate).def("stats",&Graph::stats).def("cancel",&Graph::cancel)
 .def("balls",&Graph::balls)
 .def("close",&Graph::close,py::call_guard<py::gil_scoped_release>());}
}
