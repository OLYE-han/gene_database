#pragma once
#include "value_utils.hpp"
#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <chrono>

namespace dhsearch {
// Query-scoped hints. Admission at the destination remains authoritative;
// delayed/lost notifications must never create a second region executor.
class RegionVisits {
public:
  struct Snapshot {
    std::set<std::string> opened;
    std::set<std::string> pending;
    std::map<size_t,std::set<std::string>> reserved;
    bool permits(size_t node,const std::string& from) const {
      if(opened.count("base:"+std::to_string(node)))return true;
      auto i=reserved.find(node);
      return i==reserved.end()||i->second.count(from);
    }
  };
private:
  std::mutex mu_;
  std::shared_ptr<const Snapshot> state_=std::make_shared<const Snapshot>();
public:
  std::shared_ptr<const Snapshot> snapshot()const{return std::atomic_load(&state_);}
  void merge(const dhwire::Value& value,size_t nodes) {
    using namespace dhvalue;
    if(array(at(value,"opened")).size()>4096||array(at(value,"reserved")).size()>nodes)throw std::invalid_argument("Region directory bounds exceeded");
    std::lock_guard<std::mutex> lock(mu_);auto next=std::make_shared<Snapshot>(*snapshot());
    for(const auto& r:array(at(value,"opened")))next->opened.insert(cast<std::string>(r));
    if(has(value,"pending"))for(const auto& r:array(at(value,"pending")))next->pending.insert(cast<std::string>(r));
    for(const auto& r:next->opened)next->pending.erase(r);
    for(const auto& r:array(at(value,"reserved"))){
      const auto n=get<size_t>(r,"node");
      if(n>=nodes)throw std::invalid_argument("Invalid reserved source node");
      const auto owners=get<std::vector<std::string>>(r,"owners");
      if(owners.size()>4096)throw std::invalid_argument("Region owner bounds exceeded");
      for(auto& owner:owners)next->reserved[n].insert(owner);
    }
    std::atomic_store(&state_,std::shared_ptr<const Snapshot>(next));
  }
  void open(const std::string& region){
    std::lock_guard<std::mutex> lock(mu_);auto old=snapshot();if(old->opened.count(region))return;
    auto next=std::make_shared<Snapshot>(*old);next->opened.insert(region);next->pending.erase(region);
    std::atomic_store(&state_,std::shared_ptr<const Snapshot>(next));
  }
  bool announce(const std::string& region){
    std::lock_guard<std::mutex> lock(mu_);auto old=snapshot();if(old->opened.count(region)||old->pending.count(region))return false;
    auto next=std::make_shared<Snapshot>(*old);next->pending.insert(region);
    std::atomic_store(&state_,std::shared_ptr<const Snapshot>(next));
    return true;
  }
  void reserve(size_t node,const std::string& owner){
    std::lock_guard<std::mutex> lock(mu_);auto next=std::make_shared<Snapshot>(*snapshot());
    next->reserved[node].insert(owner);std::atomic_store(&state_,std::shared_ptr<const Snapshot>(next));
  }
  dhwire::Value value()const{
    using namespace dhvalue;auto s=snapshot();A opened,reserved,pending;
    for(auto& r:s->opened)opened.emplace_back(r);
    for(auto& r:s->pending)pending.emplace_back(r);
    for(auto& [n,owners]:s->reserved){A ids;for(auto& o:owners)ids.emplace_back(o);reserved.emplace_back(M{{"node",uint64_t(n)},{"owners",std::move(ids)}});}
    return M{{"opened",std::move(opened)},{"reserved",std::move(reserved)},{"pending",std::move(pending)}};
  }
};

// One mailbox per (query, logical region). A region has at most one active
// executor. Additional conserved credit grants reuse it and may wake it later.
class RegionInbox {
  mutable std::mutex mu_;
  std::map<uint64_t,dhwire::Value> candidates_;
  std::map<uint64_t,int64_t> received_;
  size_t capacity_;
public:
  std::atomic<uint64_t> offered{0},pruned{0},taken{0};
  std::atomic<uint64_t> receive_wait_ns{0},timed_consumed{0};
  explicit RegionInbox(size_t capacity):capacity_(capacity){}
  bool empty()const{std::lock_guard<std::mutex> lock(mu_);return candidates_.empty();}
  void offer(const dhwire::Value::Array& rows,bool timed=false){
    using namespace dhvalue;std::lock_guard<std::mutex> lock(mu_);
    const auto now=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    for(const auto& row:rows){const auto& a=array(row);if(a.size()!=9)throw std::invalid_argument("Invalid region candidate");
      auto id=cast<uint64_t>(a[0]);++offered;if(candidates_.emplace(id,row).second&&timed)received_[id]=now;}
    while(candidates_.size()>capacity_){
      auto worst=std::max_element(candidates_.begin(),candidates_.end(),[](const auto& a,const auto& b){
        const double x=cast<double>(array(a.second)[1]),y=cast<double>(array(b.second)[1]);return x<y||(x==y&&a.first<b.first);});
      received_.erase(worst->first);candidates_.erase(worst);++pruned;
    }
  }
  dhwire::Value::Array take(){
    std::lock_guard<std::mutex> lock(mu_);dhwire::Value::Array out;out.reserve(candidates_.size());
    const auto now=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    uint64_t wait=0;for(auto& [id,at]:received_)wait+=now-at;receive_wait_ns+=wait;timed_consumed+=received_.size();received_.clear();
    for(auto& [id,row]:candidates_)out.push_back(std::move(row));
    candidates_.clear();taken+=out.size();return out;
  }
};
}
