#pragma once
#include "value_utils.hpp"
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace dhsearch {
// Updated only through the authenticated maintenance channel. Search reads one
// immutable snapshot after the boundary heuristic has approved a remote action.
class ReplicaDirectory {
public:
  using V=dhwire::Value;
  struct Rule {size_t source,target;std::string version;double fraction,weight;uint64_t expires;};
  using Rows=std::unordered_map<uint64_t,Rule>;
private:
  std::shared_ptr<const Rows> rows_=std::make_shared<const Rows>();
  mutable std::mutex mu_;
  std::unordered_map<std::string,uint64_t> selected_;
public:
  void update(const V& rows,uint64_t now,size_t nodes) {
    using namespace dhvalue;
    auto next=std::make_shared<Rows>();
    for(const auto& r:array(rows)){
      auto source=get<size_t>(r,"source"),target=get<size_t>(r,"target");
      auto ttl=get<uint64_t>(r,"ttl_ms");double p=get<double>(r,"fraction");
      if(source>=nodes||target>=nodes||source==target||ttl==0||ttl>60000||!std::isfinite(p)||p<0||p>1)throw std::invalid_argument("Invalid peer replica route");
      for(const auto& e:array(at(r,"weighted_entries"))){const auto& a=array(e);
        if(a.size()!=2)throw std::invalid_argument("Invalid weighted replica entry");
        double weight=number(a[1]);if(!std::isfinite(weight)||weight<0||weight>1)throw std::invalid_argument("Invalid replica entry weight");
        if(!next->emplace(cast<uint64_t>(a[0]),Rule{source,target,get<std::string>(r,"version"),p,weight,now+ttl*1000000}).second)throw std::invalid_argument("Ambiguous peer replica route");
      }
    }
    std::atomic_store(&rows_,std::shared_ptr<const Rows>(next));
  }
  std::shared_ptr<const Rows> snapshot() const {return std::atomic_load(&rows_);}
  void selected(const std::string& version){std::lock_guard<std::mutex> l(mu_);++selected_[version];}
  V counters()const{dhvalue::M out;std::lock_guard<std::mutex> l(mu_);for(auto& [k,n]:selected_)out[k]=n;return out;}
};
}
