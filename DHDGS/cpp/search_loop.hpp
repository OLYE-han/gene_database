#pragma once
#include "read_metrics.hpp"
#include "search_control.hpp"
#include "native_control.hpp"
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <optional>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <unordered_map>

namespace dhsearch {
using Id = uint64_t;
using Stats = std::unordered_map<std::string, double>;
inline double cpu_ms() {
  timespec t{}; clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);
  return t.tv_sec*1000.0+t.tv_nsec/1e6;
}
inline double ms_since(int64_t start) { return (now_ns()-start)/1e6; }
struct Candidate {
  Id id, root;
  double distance, uncertainty, age=0;
  std::vector<int> owners;
  std::optional<Id> parent;
  size_t depth=0, cross=0;
  int64_t queued=0;
};
inline Candidate candidate(py::handle row) {
  auto x=py::reinterpret_borrow<py::sequence>(row);
  require(x.size()==9,"Invalid native seed");
  Candidate c;
  c.id=x[0].cast<Id>();c.distance=x[1].cast<double>();c.uncertainty=x[2].cast<double>();
  c.owners=x[3].cast<std::vector<int>>();
  if(!x[4].is_none())c.parent=x[4].cast<Id>();
  c.depth=x[5].cast<size_t>();c.cross=x[6].cast<size_t>();c.root=x[7].cast<Id>();c.age=x[8].cast<double>();
  require(std::isfinite(c.distance)&&std::isfinite(c.uncertainty)&&c.uncertainty>=0&&!c.owners.empty(),"Invalid native candidate values");
  c.queued=now_ns();return c;
}
inline py::list row(const Candidate& c) {
  py::list x;x.append(c.id);x.append(c.distance);x.append(c.uncertainty);x.append(py::cast(c.owners));
  if(c.parent)x.append(*c.parent);else x.append(py::none());
  x.append(c.depth);x.append(c.cross);x.append(c.root);x.append(c.age);return x;
}
inline bool closer(const Candidate& a,const Candidate& b) {
  return a.distance<b.distance || (a.distance==b.distance && a.id<b.id);
}
struct Decision {
  std::string reason;int target=0;bool remote=false;
  double probability=0,wait=0;
  std::optional<double> draw,ur,ul;
};
struct Trace {
  std::vector<Id> ids;
  int64_t start=0,end=0;double cpu=0;
  std::vector<std::tuple<std::string,int64_t,int64_t>> spans;
  Stats stats;
};
struct Group {
  int node;std::vector<Candidate> candidates;size_t count=0;
  double cost=0,wait=0;
};

class Shard {
  int fd_=-1,sid_=0;
  size_t dim_=0,stride_=0,header_=0,degree_=0,code_bytes_=0,extra_bytes_=0;
  std::string epoch_;
  bool cosine_=false;
  py::object quantizer_owner_,counter_owner_,clock_domain_;
  py::list arrays_;
  Quantizer* quantizer_=nullptr;
  ExpansionCounters* counters_=nullptr;
  const uint8_t *codes_=nullptr,*extra_=nullptr;
  const float* factors_=nullptr;
  const uint32_t* center_ids_=nullptr;
  std::unordered_map<Id,size_t> locations_,code_locations_;
  std::vector<std::vector<int>> owners_;
public:
  Shard(py::object shard, Array<uint8_t> membership, py::object clock_domain):clock_domain_(clock_domain) {
    py::dict meta=shard.attr("meta");
    sid_=meta["shard_id"].cast<int>();dim_=meta["dimension"].cast<size_t>();
    stride_=meta["record_bytes"].cast<size_t>();header_=meta["header_bytes"].cast<size_t>();
    degree_=meta["max_degree"].cast<size_t>();epoch_=meta["epoch"].cast<std::string>();
    cosine_=meta["metric"].cast<std::string>()=="cosine";
    require(shard.attr("cache_bytes").cast<size_t>()==0,"Native search requires application cache disabled");
    auto ids=shard.attr("ids").cast<Array<Id>>();
    auto code_ids=shard.attr("code_ids").cast<Array<Id>>();
    require(membership.ndim()==2&&membership.shape(0)==code_ids.size(),"Native membership shape mismatch");
    auto codes=shard.attr("codes").cast<Array<uint8_t>>();
    auto extra=shard.attr("extra").cast<Array<uint8_t>>();
    auto factors=shard.attr("factors").cast<Array<float>>();
    auto centers=shard.attr("center_ids").cast<Array<uint32_t>>();
    arrays_.append(codes);arrays_.append(extra);arrays_.append(factors);arrays_.append(centers);
    codes_=codes.data();extra_=extra.data();factors_=factors.data();center_ids_=centers.data();
    code_bytes_=codes.shape(1);extra_bytes_=extra.shape(1);
    quantizer_owner_=shard.attr("quantizer");quantizer_=&quantizer_owner_.cast<Quantizer&>();
    counter_owner_=shard.attr("counters");counters_=&counter_owner_.cast<ExpansionCounters&>();
    locations_.reserve(ids.size());code_locations_.reserve(code_ids.size());owners_.resize(code_ids.size());
    for(ssize_t i=0;i<ids.size();++i)locations_.emplace(ids.data()[i],i);
    for(ssize_t i=0;i<code_ids.size();++i) {
      code_locations_.emplace(code_ids.data()[i],i);
      for(ssize_t n=0;n<membership.shape(1);++n)
        if(membership.data()[i*membership.shape(1)+n])owners_[i].push_back(int(n));
      require(!owners_[i].empty(),"Native code has no owner");
    }
    fd_=dup(shard.attr("fd").cast<int>());
    if(fd_<0)throw std::runtime_error("Cannot duplicate native index fd");
  }
  ~Shard(){if(fd_>=0)::close(fd_);}
  void close(){if(fd_>=0){::close(fd_);fd_=-1;}}

  py::dict run(py::dict state,py::dict config,Array<double> draws,
               std::shared_ptr<Control> control,py::object read_batch,
               py::object release_batch,py::function spawn,
               std::shared_ptr<dhcontrol::Ticket> completion=nullptr) {
    const int64_t native_entry_ns=now_ns();
    int64_t native_search_start_ns=0,native_loop_done_ns=0;
    int64_t native_gil_wait_start_ns=0,native_completion_enqueued_ns=0;
    require(fd_>=0,"Native search shard closed");
    require(state["epoch"].cast<std::string>()==epoch_,"Native epoch mismatch");
    auto visited=state["visited_nodes"].cast<std::vector<int>>();
    std::unordered_set<int> visited_set(visited.begin(),visited.end());
    require(!visited.empty()&&visited.back()==sid_&&visited.size()==visited_set.size(),"Invalid visited-node path");
    const std::string bytes=state["query_f32"].cast<std::string>();
    require(bytes.size()==dim_*sizeof(float),"Native query dimension mismatch");
    std::vector<float> query(dim_);std::memcpy(query.data(),bytes.data(),bytes.size());finite(query.data(),dim_);
    if(cosine_) {double norm=0;for(float v:query)norm+=double(v)*v;require(std::abs(std::sqrt(norm)-1)<=1.001e-5,"Cosine query must be normalized");}
    const auto cfg=config["search"].cast<py::dict>();const auto policy=config["scheduler"].cast<py::dict>();
    const auto peer=config["peer"].cast<py::dict>();const auto net=config["network"].cast<py::dict>();
    const size_t topk=cfg["top_k"].cast<size_t>(),frontier_limit=cfg["frontier_size"].cast<size_t>();
    const size_t beam=cfg["local_beam"].cast<size_t>(),remote_batch=net["batch_size"].cast<size_t>();
    const size_t stagnation_limit=cfg["stagnation_batches"].cast<size_t>();
    const bool exclude_self=cfg["exclude_self"].cast<bool>(),fallback_enabled=cfg["fallback_enabled"].cast<bool>();
    const size_t fallback_limit=state.contains("fallback_remaining")?state["fallback_remaining"].cast<size_t>():cfg["max_fallback_entries"].cast<size_t>();
    const size_t grant_total=state["work_grant"].cast<size_t>(),slots_total=state["branch_slots"].cast<size_t>();
    size_t remaining=grant_total,slots=slots_total,stagnation=0,fallback_used=0,draw_index=0;
    require(topk&&beam&&frontier_limit&&remote_batch&&grant_total,"Invalid native search budget");
    const double scale=state["scale"].cast<double>(),kappa=policy["kappa"].cast<double>();
    const double exploration=policy["exploration"].cast<double>(),max_wait=policy["max_wait_ms"].cast<double>();
    const double rtt=policy["remote_rtt_ms"].cast<double>(),bandwidth=policy["bandwidth_bytes_per_ms"].cast<double>();
    const double ewma=policy["ewma_weight"].cast<double>(),weight=policy["remote_weight"].cast<double>();
    const double temperature=policy["probability_temperature"].cast<double>(),child_fraction=peer["child_budget_fraction"].cast<double>();
    double service_per_node=policy["local_batch_ms"].cast<double>()/beam;
    require(std::isfinite(scale)&&scale>0&&max_wait>0&&bandwidth>0&&temperature>0,"Invalid native policy scale");
    const std::string mode=policy["mode"].cast<std::string>(),selection=policy["remote_selection"].cast<std::string>();
    const bool detailed=state.contains("trace_enabled")?state["trace_enabled"].cast<bool>():
        config["metrics"].cast<py::dict>()["detailed_trace"].cast<bool>();
    const bool lifecycle=detailed||(state.contains("lifecycle_trace_enabled")&&state["lifecycle_trace_enabled"].cast<bool>());
    std::optional<Id> query_id;if(!state["query_id"].is_none())query_id=state["query_id"].cast<Id>();
    const std::string branch_id=state["branch_id"].cast<std::string>();
    std::vector<Candidate> frontier,fallbacks,paths;
    for(auto key:{"seeds","remote_frontier"})if(state.contains(key))
      for(auto item:state[key])frontier.push_back(candidate(item));
    if(state.contains("fallback_entries"))for(auto item:state["fallback_entries"])fallbacks.push_back(candidate(item));
    auto initial_seen=state["seen"].cast<std::vector<Id>>();
    std::unordered_set<Id> seen(initial_seen.begin(),initial_seen.end());
    std::unordered_map<Id,double> results;
    std::vector<Id> expanded;
    std::vector<Decision> decisions;std::vector<Trace> traces;
    Stats stats;std::string stop_reason="frontier_exhausted";
    py::list children,events;
    bool can_forward=true;const bool async=!read_batch.is_none();
    require(!completion||!async,"Native control currently requires sync_pread");
    using V=dhwire::Value;
    V::Map native_report;
    V::Array native_children,native_events;
    V native_clock;
    if(completion) {
      for(auto key:{"branch_id","parent_branch_id","visited_nodes","work_grant","branch_slots"})
        native_report[key]=dhwire::from_python(state[key]);
      native_clock=dhwire::from_python(clock_domain_);
    }
    const int64_t started=now_ns();const double cpu_started=cpu_ms();
    const auto owner_thread=static_cast<uint64_t>(syscall(SYS_gettid));
    auto owner=[&](const std::vector<int>& owners) {
      if(std::find(owners.begin(),owners.end(),sid_)!=owners.end())return sid_;
      int best=std::numeric_limits<int>::max();
      if(can_forward)for(int n:owners)if(!visited_set.count(n))best=std::min(best,n);
      return best==std::numeric_limits<int>::max()?-1:best;
    };
    auto threshold=[&]() {
      std::vector<double> values;values.reserve(results.size());
      for(auto [id,d]:results)values.push_back(d);
      if(values.size()>=topk){std::nth_element(values.begin(),values.begin()+topk-1,values.end());return values[topk-1];}
      return std::max(scale,values.empty()?scale:*std::max_element(values.begin(),values.end()));
    };
    auto fallback=[&]() {
      if(!can_forward||!fallback_enabled||fallback_used>=fallback_limit||frontier.size()>=frontier_limit)return false;
      for(auto c:fallbacks)if(!seen.count(c.id)&&owner(c.owners)>=0) {
        seen.insert(c.id);c.queued=now_ns();frontier.push_back(c);++fallback_used;stagnation=0;return true;
      }
      return false;
    };
    auto score=[&](const Group& g,double t) {
      double gain=0;for(size_t i=0;i<g.count;++i){const auto& c=g.candidates[i];gain+=std::max((t-c.distance+kappa*c.uncertainty)/scale,0.0)+exploration;}
      return gain/g.cost*(1+g.wait/max_wait);
    };
    auto dropped=[&]() -> const char* {return can_forward?"visited_node_candidates_dropped":"remote_candidates_dropped_after_forward";};
    {
      py::gil_scoped_release unlock;
      native_search_start_ns=now_ns();
      control->check();
      auto prep_start=now_ns();auto prepared=quantizer_->prepare_native(query.data());
      stats["rabitq_prepare_ms"]=ms_since(prep_start);
      std::vector<char> raw(async?0:beam*stride_);
      while(remaining>0) {
        control->check();
        if(frontier.empty()&&!fallback())break;
        const auto now=now_ns();std::vector<Group> groups;
        for(auto it=frontier.begin();it!=frontier.end();) {
          const int target=owner(it->owners);
          if(target<0){stats[dropped()]++;it=frontier.erase(it);continue;}
          it->age=std::max(0.0,(now-it->queued)/1e6);
          auto g=std::find_if(groups.begin(),groups.end(),[&](const Group& group){return group.node==target;});
          if(g==groups.end()){groups.push_back(Group{target,{}});g=groups.end()-1;}
          g->candidates.push_back(*it);++it;
        }
        if(groups.empty())continue;
        size_t remote_count=0;for(auto& g:groups)if(g.node!=sid_)remote_count+=g.candidates.size();
        int local=-1;std::vector<int> eligible,remotes;
        for(size_t i=0;i<groups.size();++i) {
          auto& g=groups[i];std::sort(g.candidates.begin(),g.candidates.end(),closer);
          g.count=std::min({g.candidates.size(),remaining,g.node==sid_?beam:remote_batch});
          g.cost=service_per_node*g.count;
          if(g.node!=sid_)g.cost+=rtt+(bytes.size()+seen.size()*8+remote_count*96)/bandwidth;
          g.cost=std::max(g.cost,1e-9);
          for(size_t j=0;j<g.count;++j)g.wait=std::max(g.wait,g.candidates[j].age);
          if(g.node==sid_)local=i;
          if(g.node==sid_||slots){eligible.push_back(i);if(g.node!=sid_)remotes.push_back(i);}
        }
        if(eligible.empty()){stop_reason="branch_slots_exhausted";break;}
        int chosen=local;Decision decision;decision.reason="local_only";
        if(!remotes.empty() && !(local>=0&&remaining<2)) {
          if(local<0) {
            const double t=threshold();chosen=remotes.front();
            bool expired=false;
            for(int i:remotes)if(groups[i].wait>=max_wait) {
              if(!expired||groups[i].wait>groups[chosen].wait||
                 (groups[i].wait==groups[chosen].wait&&std::to_string(groups[i].node)<std::to_string(groups[chosen].node)))chosen=i;
              expired=true;
            }
            if(!expired)for(int i:remotes)if(score(groups[i],t)>score(groups[chosen],t)||
                (score(groups[i],t)==score(groups[chosen],t)&&std::to_string(groups[i].node)<std::to_string(groups[chosen].node)))chosen=i;
            decision.reason="local_exhausted";decision.probability=1;
          } else {
            auto expired=std::max_element(remotes.begin(),remotes.end(),[&](int a,int b){return groups[a].wait<groups[b].wait;});
            if(groups[*expired].wait>=max_wait){chosen=*expired;decision.reason="max_wait";decision.probability=1;}
            else if(mode=="reference") {
              chosen=eligible.front();for(int i:eligible)if(closer(groups[i].candidates[0],groups[chosen].candidates[0]))chosen=i;
              decision.reason="reference_order";decision.probability=groups[chosen].node!=sid_;
            } else if(mode!="local_first") {
              const double t=threshold();int best=remotes.front();for(int i:remotes)if(score(groups[i],t)>score(groups[best],t))best=i;
              const double ur=score(groups[best],t),ul=score(groups[local],t);decision.ur=ur;decision.ul=ul;
              const double z=weight>0?(std::log(weight)+std::log(ur)-std::log(ul))/temperature:-std::numeric_limits<double>::infinity();
              decision.probability=z>=0?1/(1+std::exp(-z)):std::exp(z)/(1+std::exp(z));
              bool remote;
              if(selection=="threshold"){remote=weight*ur>ul;decision.probability=remote;decision.reason="weighted_utility";}
              else {require(draw_index<size_t(draws.size()),"Native decision random stream exhausted");decision.draw=draws.data()[draw_index++];remote=*decision.draw<decision.probability;decision.reason="probability_draw";}
              chosen=remote?best:local;
            }
          }
        }
        if(chosen<0)break;
        auto& action=groups[chosen];decision.target=action.node;decision.remote=action.node!=sid_;decision.wait=action.wait;
        const std::string decision_id=branch_id+":"+std::to_string(decisions.size());decisions.push_back(decision);
        if(action.node!=sid_) {
          bool other=false;for(auto& g:groups)other|=g.node!=action.node;
          size_t grant=other?std::max(size_t(1),size_t(remaining*child_fraction)):remaining;
          if(local>=0)grant=std::min(grant,remaining-1);
          require(grant>0&&slots>0,"Invalid native child grant");
          const size_t child_slots=slots-1;slots=0;remaining-=grant;
          std::vector<Candidate> seeds,carried;
          for(size_t i=0;i<std::min({action.candidates.size(),remote_batch,frontier_limit});++i)seeds.push_back(action.candidates[i]);
          for(auto& seed:seeds)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& x){return x.id==seed.id;}),frontier.end());
          for(auto& c:frontier)if(std::find(c.owners.begin(),c.owners.end(),sid_)==c.owners.end()&&owner(c.owners)>=0)carried.push_back(c);
          {
            py::gil_scoped_acquire lock;
            py::dict payload;py::list seed_rows,carry_rows;
            for(auto& c:seeds)seed_rows.append(row(c));
            for(auto& c:carried)carry_rows.append(row(c));
            std::vector<Id> seen_sorted(seen.begin(),seen.end());std::sort(seen_sorted.begin(),seen_sorted.end());
            payload["seeds"]=seed_rows;payload["remote_frontier"]=carry_rows;payload["seen"]=py::cast(seen_sorted);
            payload["target_node"]=action.node;payload["work_grant"]=grant;payload["branch_slots"]=child_slots;
            payload["fallback_remaining"]=fallback_limit-fallback_used;payload["dispatch_decision_id"]=decision_id;
            payload["draws_used"]=draw_index;
            // The protocol adapter constructs UUID/ancestry and enqueues only
            // this approved dispatch. No local frontier returns to the head.
            py::dict descriptor=spawn(payload).cast<py::dict>();children.append(descriptor);
            py::dict event;event["decision_id"]=decision_id;event["target"]=action.node;event["child_branch_id"]=descriptor["branch_id"];
            event["work_grant"]=grant;event["local_expansions_before"]=expanded.size();event["candidate_count"]=seeds.size();
            event["carried_candidate_count"]=carried.size();event["elapsed_ms"]=ms_since(started);events.append(event);
            if(completion) {
              native_children.push_back(dhwire::from_python(descriptor));
              native_events.push_back(dhwire::from_python(event));
            }
          }
          can_forward=false;continue;
        }
        std::vector<Candidate> selected(action.candidates.begin(),action.candidates.begin()+action.count);
        std::vector<Id> batch;for(auto& c:selected)batch.push_back(c.id);
        for(auto id:batch)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){return c.id==id;}),frontier.end());
        const double old=results.size()>=topk?threshold():std::numeric_limits<double>::infinity();
        Stats work;Trace trace;trace.ids=batch;
        bool held=false,accounted=false;counters_->begin();
        try {
          if(async) {
            const auto wait_start=now_ns();
            {py::gil_scoped_acquire lock;
              auto reply=read_batch(py::cast(batch)).cast<py::tuple>();held=true;
              auto records=reply[0].cast<py::list>();require(records.size()==batch.size(),"Wrong native I/O batch size");
              // Allocate the second copy only after admission, and free it
              // before releasing the I/O reservation (2x record bytes).
              raw.resize(batch.size()*stride_);
              for(size_t i=0;i<batch.size();++i){py::bytes bytes=records[i].cast<py::bytes>();char* ptr=nullptr;Py_ssize_t len=0;
                if(PyBytes_AsStringAndSize(bytes.ptr(),&ptr,&len)<0)throw py::error_already_set();
                require(size_t(len)==stride_,"Short native async record");std::memcpy(raw.data()+i*stride_,ptr,stride_);}
              work=reply[1].cast<Stats>();
            }
            stats["query_io_wait_ms"]+=ms_since(wait_start);
          }
          control->check();trace.start=now_ns();const double service_cpu=cpu_ms();
          std::unordered_map<Id,Id> parents;
          std::unordered_map<Id,Candidate> selected_by_id;
          std::vector<std::pair<Id,double>> exact;
          for(size_t i=0;i<batch.size();++i) {
            control->check();const Id id=batch[i];selected_by_id.emplace(id,selected[i]);
            char* data=raw.data()+i*stride_;
            if(!async) {
              auto location=locations_.find(id);require(location!=locations_.end(),"Native expansion of nonlocal vertex");
              const auto t=now_ns();ssize_t count;
              do {count=::pread(fd_,data,stride_,header_+location->second*stride_);}while(count<0&&errno==EINTR);
              const auto end=now_ns();work["pread_ms"]+=(end-t)/1e6;work["pread_calls"]++;
              if(count<0)throw std::runtime_error(std::string("native pread: ")+strerror(errno));
              require(size_t(count)==stride_,"Short native node read");work["file_read_bytes"]+=stride_;
              if(detailed)trace.spans.emplace_back("pread",t,end);
            }
            const auto t=now_ns();double distance=0;
            for(size_t j=0;j<dim_;++j){float value;std::memcpy(&value,data+j*sizeof(float),sizeof(float));
              require(std::isfinite(value),"Nonfinite disk vector");const double delta=double(query[j])-value;distance+=delta*delta;}
            exact.emplace_back(id,distance);const auto end=now_ns();work["exact_distance_ms"]+=(end-t)/1e6;
            work["exact_distance_evaluations"]++;if(detailed)trace.spans.emplace_back("exact_distance",t,end);
            uint32_t degree;std::memcpy(&degree,data+dim_*4,4);
            require(degree<=degree_&&dim_*4+4+size_t(degree)*8<=stride_,"Invalid native node degree");
            work["scanned_edges"]+=degree;
            for(size_t j=0;j<degree;++j){Id neighbor;std::memcpy(&neighbor,data+dim_*4+4+j*8,8);parents.emplace(neighbor,id);}
          }
          std::vector<Id> neighbors;neighbors.reserve(parents.size());for(auto [id,parent]:parents)neighbors.push_back(id);
          std::sort(neighbors.begin(),neighbors.end());
          std::vector<std::pair<float,float>> estimates;estimates.reserve(neighbors.size());const auto quant_start=now_ns();
          for(Id id:neighbors) {
            const auto found=code_locations_.find(id);require(found!=code_locations_.end(),"Missing native neighbor code");const size_t p=found->second;
            auto value=quantizer_->score_native(prepared,codes_+p*code_bytes_,extra_bytes_?extra_+p*extra_bytes_:nullptr,factors_+3*p,center_ids_[p]);
            require(std::isfinite(value.first)&&std::isfinite(value.second),"Nonfinite native estimate");estimates.push_back(value);
          }
          const auto quant_end=now_ns();work["rabitq_ms"]=(quant_end-quant_start)/1e6;work["rabitq_evaluations"]=neighbors.size();
          if(detailed)trace.spans.emplace_back("rabitq",quant_start,quant_end);
          trace.end=now_ns();trace.cpu=cpu_ms()-service_cpu;work["service_ms"]=(trace.end-trace.start)/1e6;
          work["server_thread_cpu_ms"]=trace.cpu;
          if(held){std::vector<char>().swap(raw);py::gil_scoped_acquire lock;release_batch();held=false;}
          control->check();remaining-=batch.size();expanded.insert(expanded.end(),batch.begin(),batch.end());stats["batches"]++;
          for(auto [name,value]:work)if(name!="server_thread_cpu_ms")stats[name]+=value;
          const double measured=(work["service_ms"]+work["io_batch_wall_ms"])/batch.size();
          service_per_node=(1-ewma)*service_per_node+ewma*std::max(measured,1e-9);
          for(auto [id,distance]:exact) {paths.push_back(selected_by_id.at(id));if(!(exclude_self&&query_id&&id==*query_id))results[id]=distance;}
          for(size_t i=0;i<neighbors.size();++i) {
            Id id=neighbors[i];if(!seen.insert(id).second)continue;
            const auto& parent=selected_by_id.at(parents.at(id));const auto& owners=owners_[code_locations_.at(id)];
            const int target=owner(owners);if(target<0){stats[dropped()]++;continue;}
            Candidate c;c.id=id;c.distance=estimates[i].first;c.uncertainty=estimates[i].second;c.owners=owners;
            c.parent=parent.id;c.depth=parent.depth+1;c.cross=parent.cross+(target!=sid_);c.root=parent.root;c.queued=now_ns();frontier.push_back(std::move(c));
          }
          const bool improved=threshold()<old;stats["improved_topk_batches"]+=improved;stagnation=improved?0:stagnation+1;
          if(frontier.size()>frontier_limit){std::sort(frontier.begin(),frontier.end(),closer);stats["frontier_dropped"]+=frontier.size()-frontier_limit;frontier.resize(frontier_limit);}
          if(stagnation>=stagnation_limit)fallback();
          counters_->finish_native(true,work);
          accounted=true;
          if(detailed){trace.stats=std::move(work);traces.push_back(std::move(trace));}
        } catch(...) {
          if(held){std::vector<char>().swap(raw);py::gil_scoped_acquire lock;try{release_batch();}catch(...){}}
          if(!accounted)counters_->finish_native(false,{});
          throw;
        }
      }
      if(remaining==0)stop_reason="branch_work_credit_exhausted";
      native_loop_done_ns=now_ns();
      stats["server_thread_cpu_ms"]=cpu_ms()-cpu_started;
      stats["branch_wall_ms"]=ms_since(started);
      if(completion) {
        // Build the complete wire report before reacquiring the GIL. The
        // controller takes native ownership; Python never serializes it.
        std::vector<std::pair<Id,double>> ranked(results.begin(),results.end());
        std::sort(ranked.begin(),ranked.end(),[](auto a,auto b){return a.second<b.second||(a.second==b.second&&a.first<b.first);});
        if(ranked.size()>topk)ranked.resize(topk);
        auto ids_value=[](const auto& values){V::Array out;out.reserve(values.size());for(auto x:values)out.emplace_back(uint64_t(x));return V(std::move(out));};
        V::Array ids,distances,result_paths;std::unordered_set<Id> returned;
        for(auto [id,distance]:ranked){ids.emplace_back(id);distances.emplace_back(distance);returned.insert(id);}
        size_t max_depth=0,max_cross=0;
        for(auto& c:paths) {
          max_depth=std::max(max_depth,c.depth);max_cross=std::max(max_cross,c.cross);
          if(returned.count(c.id))result_paths.emplace_back(V::Map{
            {"id",V(c.id)},{"parent",c.parent?V(*c.parent):V()},
            {"depth",V(uint64_t(c.depth))},{"cross_shard_depth",V(uint64_t(c.cross))},
            {"root",V(c.root)},{"shard",V(int64_t(sid_))}});
        }
        V::Map all_stats;
        for(auto key:{"file_read_bytes","pread_calls","cache_hits","scanned_edges","rabitq_evaluations",
            "exact_distance_evaluations","pread_ms","rabitq_ms","exact_distance_ms","service_ms",
            "async_read_requests","io_batch_wall_ms","io_capacity_wait_ms","io_queue_wait_ms",
            "io_read_residence_ms","query_ready_queue_ms","query_compute_slice_wall_ms",
            "query_admission_queue_ms","query_executor_start_wait_ms","query_io_wait_ms","query_resume_count",
            "query_transfer_finalize_wait_ms","query_branch_wall_ms","query_state_other_ms","batches",
            "frontier_dropped","fallback_entries","improved_topk_batches","visited_node_candidates_dropped",
            "remote_candidates_dropped_after_forward"})all_stats[key]=V(0.);
        for(auto& [key,value]:stats)all_stats[key]=V(value);
        all_stats["expanded"]=V(uint64_t(expanded.size()));all_stats["fallback_entries"]=V(uint64_t(fallback_used));
        all_stats["max_expanded_depth"]=V(uint64_t(max_depth));all_stats["max_cross_shard_depth"]=V(uint64_t(max_cross));
        all_stats["pending_at_stop"]=V(uint64_t(frontier.size()));all_stats["stop_reason"]=V(stop_reason);
        all_stats["query_compute_slice_wall_ms"]=V(ms_since(started));
        native_report["node_id"]=V(int64_t(sid_));native_report["work_spent"]=V(uint64_t(expanded.size()));
        native_report["work_unused"]=V(uint64_t(remaining));native_report["children"]=V(std::move(native_children));
        native_report["ids"]=V(std::move(ids));native_report["squared_distances"]=V(std::move(distances));
        native_report["expanded_ids"]=ids_value(expanded);native_report["result_paths"]=V(std::move(result_paths));
        native_report["stats"]=V(std::move(all_stats));native_report["fork_events"]=V(std::move(native_events));
        V::Array decision_rows;
        for(size_t i=0;i<decisions.size();++i){auto& d=decisions[i];decision_rows.emplace_back(V::Map{
          {"decision_id",V(branch_id+":"+std::to_string(i))},{"reason",V(d.reason)},
          {"probability",V(d.probability)},{"draw",d.draw?V(*d.draw):V()},
          {"selected_node",V(int64_t(d.target))},{"remote",V(d.remote)},
          {"remote_utility",d.ur?V(*d.ur):V()},{"local_utility",d.ul?V(*d.ul):V()},
          {"selected_wait_ms",V(d.wait)}});}
        native_report["decisions"]=V(std::move(decision_rows));
        if(detailed){V::Array batches;for(auto& t:traces){V::Map work;for(auto& [key,value]:t.stats)work[key]=V(value);
          V::Array spans;for(auto& [kind,begin,end]:t.spans)spans.emplace_back(V::Array{V(kind),V(begin),V(end)});
          V::Map service{{"clock_domain",native_clock},{"start_ns",V(t.start)},{"end_ns",V(t.end)},
            {"spans",V(std::move(spans))},{"thread_cpu_ms",V(t.cpu)}};
          batches.emplace_back(V::Map{{"ids",ids_value(t.ids)},{"shard_id",V(int64_t(sid_))},
            {"service_trace",V(std::move(service))},{"stats",V(std::move(work))},{"io_trace",V()}});
        }native_report["local_batch_trace"]=V(std::move(batches));}
        native_report["compute_thread_id"]=V(owner_thread);native_report["compute_thread_migrations"]=V(int64_t(0));
        native_report["native_search_loop"]=V(true);native_report["decision_rng"]=V("numpy_pcg64_precomputed_v1");
        native_report["execution"]=V(V::Map{{"model",V("native")},{"io_backend",V("sync_pread")},
          {"fallback_reason",V()},{"query_parallelism",V("one_fixed_compute_thread_per_node_branch")},
          {"completion_backend",V("native")}});
        if(lifecycle)native_report["native_timestamps"]=V(V::Map{{"clock_domain",native_clock},
          {"native_entry_ns",V(native_entry_ns)},{"native_search_start_ns",V(native_search_start_ns)},
          {"native_loop_done_ns",V(native_loop_done_ns)},{"native_report_ready_ns",V(now_ns())}});
        completion->complete(V(std::move(native_report)));
        native_completion_enqueued_ns=now_ns();
      }
      native_gil_wait_start_ns=now_ns();
    }
    const int64_t native_gil_reacquired_ns=now_ns();
    if(completion) {
      py::dict report,timestamps;report["native_control_submitted"]=true;
      if(lifecycle){timestamps["clock_domain"]=clock_domain_;timestamps["native_entry_ns"]=native_entry_ns;
        timestamps["native_search_start_ns"]=native_search_start_ns;timestamps["native_loop_done_ns"]=native_loop_done_ns;
        timestamps["native_gil_wait_start_ns"]=native_gil_wait_start_ns;
        timestamps["native_completion_enqueued_ns"]=native_completion_enqueued_ns;
        timestamps["native_gil_reacquired_ns"]=native_gil_reacquired_ns;report["native_timestamps"]=timestamps;}
      return report;
    }
    std::vector<std::pair<Id,double>> ranked(results.begin(),results.end());
    std::sort(ranked.begin(),ranked.end(),[](auto a,auto b){return a.second<b.second||(a.second==b.second&&a.first<b.first);});
    if(ranked.size()>topk)ranked.resize(topk);
    py::dict report;for(auto key:{"branch_id","parent_branch_id","visited_nodes","work_grant","branch_slots"})report[key]=state[key];
    report["node_id"]=sid_;report["work_spent"]=expanded.size();report["work_unused"]=remaining;report["children"]=children;
    py::list ids,distances,result_paths;std::unordered_set<Id> returned;
    for(auto [id,distance]:ranked){ids.append(id);distances.append(distance);returned.insert(id);}
    size_t max_depth=0,max_cross=0;
    for(auto& c:paths) {
      max_depth=std::max(max_depth,c.depth);max_cross=std::max(max_cross,c.cross);
      if(returned.count(c.id)){py::dict p;p["id"]=c.id;p["parent"]=c.parent?py::cast(*c.parent):py::none();p["depth"]=c.depth;p["cross_shard_depth"]=c.cross;p["root"]=c.root;p["shard"]=sid_;result_paths.append(p);}
    }
    report["ids"]=ids;report["squared_distances"]=distances;report["expanded_ids"]=py::cast(expanded);report["result_paths"]=result_paths;
    py::dict all_stats=py::cast(stats);all_stats["expanded"]=expanded.size();all_stats["fallback_entries"]=fallback_used;
    all_stats["max_expanded_depth"]=max_depth;all_stats["max_cross_shard_depth"]=max_cross;
    all_stats["pending_at_stop"]=frontier.size();all_stats["stop_reason"]=stop_reason;report["stats"]=all_stats;
    report["fork_events"]=events;py::list decision_rows;
    for(size_t i=0;i<decisions.size();++i){auto& d=decisions[i];py::dict r;r["decision_id"]=branch_id+":"+std::to_string(i);
      r["reason"]=d.reason;r["probability"]=d.probability;r["draw"]=d.draw?py::cast(*d.draw):py::none();r["selected_node"]=d.target;
      r["remote"]=d.remote;r["remote_utility"]=d.ur?py::cast(*d.ur):py::none();r["local_utility"]=d.ul?py::cast(*d.ul):py::none();r["selected_wait_ms"]=d.wait;decision_rows.append(r);}
    report["decisions"]=decision_rows;
    if(detailed){py::list batches;for(auto& t:traces){py::dict r,service;
      service["clock_domain"]=clock_domain_;service["start_ns"]=t.start;service["end_ns"]=t.end;service["spans"]=py::cast(t.spans);service["thread_cpu_ms"]=t.cpu;
      r["ids"]=py::cast(t.ids);r["shard_id"]=sid_;r["service_trace"]=service;r["stats"]=py::cast(t.stats);r["io_trace"]=py::none();batches.append(r);}report["local_batch_trace"]=batches;}
    report["compute_thread_id"]=owner_thread;report["compute_thread_migrations"]=0;
    report["native_search_loop"]=true;report["decision_rng"]="numpy_pcg64_precomputed_v1";
    if(lifecycle){py::dict timestamps;timestamps["clock_domain"]=clock_domain_;
      timestamps["native_entry_ns"]=native_entry_ns;timestamps["native_search_start_ns"]=native_search_start_ns;
      timestamps["native_loop_done_ns"]=native_loop_done_ns;
      timestamps["native_gil_reacquired_ns"]=native_gil_reacquired_ns;
      timestamps["native_report_ready_ns"]=now_ns();
      report["native_timestamps"]=timestamps;}
    return report;
  }
};
} // namespace dhsearch

inline void register_native_search(py::module_& m) {
  py::register_exception<dhsearch::Cancelled>(m,"NativeSearchCancelled",PyExc_TimeoutError);
  py::class_<dhsearch::Control,std::shared_ptr<dhsearch::Control>>(m,"SearchControl")
    .def(py::init<double,double>()).def("renew",&dhsearch::Control::renew)
    .def("cancel",&dhsearch::Control::cancel).def("cancelled",&dhsearch::Control::cancelled);
  py::class_<dhsearch::Shard>(m,"SearchShard")
    .def(py::init<py::object,Array<uint8_t>,py::object>())
    .def("run",&dhsearch::Shard::run,py::arg("state"),py::arg("config"),py::arg("draws"),
         py::arg("control"),py::arg("read_batch"),py::arg("release_batch"),py::arg("spawn"),py::arg("completion")=nullptr)
    .def("close",&dhsearch::Shard::close);
}
