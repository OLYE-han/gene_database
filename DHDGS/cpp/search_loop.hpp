#pragma once
#include "read_metrics.hpp"
#include "replica_overlay.hpp"
#include "replica_directory.hpp"
#include "logical_region.hpp"
#include "decayed_hotness.hpp"
#include "search_control.hpp"
#include "native_control.hpp"
#include "value_utils.hpp"
#include <functional>
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
using V = dhwire::Value;
inline Candidate candidate(const V& value) {
  const auto& x=dhvalue::array(value);require(x.size()==9,"Invalid native seed");
  Candidate c;c.id=dhvalue::cast<Id>(x[0]);c.distance=dhvalue::number(x[1]);
  c.uncertainty=dhvalue::number(x[2]);c.owners=dhvalue::cast<std::vector<int>>(x[3]);
  if(!dhvalue::null(x[4]))c.parent=dhvalue::cast<Id>(x[4]);
  c.depth=dhvalue::cast<size_t>(x[5]);c.cross=dhvalue::cast<size_t>(x[6]);
  c.root=dhvalue::cast<Id>(x[7]);c.age=dhvalue::number(x[8]);
  require(std::isfinite(c.distance)&&std::isfinite(c.uncertainty)&&c.uncertainty>=0&&!c.owners.empty(),"Invalid native candidate values");
  c.queued=now_ns();return c;
}
inline V row(const Candidate& c) {
  return V::Array{V(c.id),V(c.distance),V(c.uncertainty),dhvalue::vector(c.owners),
    c.parent?V(*c.parent):V(),V(uint64_t(c.depth)),V(uint64_t(c.cross)),V(c.root),V(c.age)};
}
struct ReadReply { std::vector<std::vector<uint8_t>> records; Stats stats; };
using Read = std::function<ReadReply(const std::vector<Id>&)>;
using Release = std::function<void()>;
using Spawn = std::function<V(V)>;
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
  size_t node_count_=0,dim_=0,stride_=0,header_=0,degree_=0,code_bytes_=0,extra_bytes_=0;
  std::string epoch_;
  V replica_contract_;
  bool cosine_=false;
  py::object quantizer_owner_,counter_owner_,clock_domain_;
  V native_clock_;
  py::list arrays_;
  Quantizer* quantizer_=nullptr;
  ExpansionCounters* counters_=nullptr;
  const uint8_t *codes_=nullptr,*extra_=nullptr;
  const float* factors_=nullptr;
  const uint32_t* center_ids_=nullptr;
  std::unordered_map<Id,size_t> locations_,code_locations_;
  Id lookup_base_=0;
  std::vector<size_t> dense_locations_,dense_code_locations_;
  static constexpr size_t missing_location=std::numeric_limits<size_t>::max();
  size_t locate(Id id,bool code) const {
    if(!dense_code_locations_.empty()) {
      if(id<lookup_base_||id-lookup_base_>=dense_code_locations_.size())return missing_location;
      return (code?dense_code_locations_:dense_locations_)[size_t(id-lookup_base_)];
    }
    const auto& table=code?code_locations_:locations_;
    auto found=table.find(id);return found==table.end()?missing_location:found->second;
  }
  std::vector<std::vector<int>> owners_;
  using Overlays=std::unordered_map<std::string,std::shared_ptr<ReplicaOverlay>>;
  std::shared_ptr<const Overlays> overlays_=std::make_shared<const Overlays>();
  std::mutex overlay_write_;
  ReplicaDirectory peer_replicas_;
  std::shared_ptr<DecayedHotness> hotness_;
  std::unordered_map<std::string,std::weak_ptr<ReplicaOverlay>> retired_overlays_;
public:
  void configure_hotness(const V& config){
    std::shared_ptr<DecayedHotness> next;
    if(dhvalue::has(config,"hotness")&&dhvalue::get<bool>(dhvalue::at(config,"hotness"),"enabled"))next=std::make_shared<DecayedHotness>(dhvalue::at(config,"hotness"));
    std::atomic_store(&hotness_,std::move(next));
  }
  py::object hotness_snapshot()const{auto h=std::atomic_load(&hotness_);return dhwire::to_python(h?h->snapshot():V(dhvalue::M{{"enabled",false}}));}
  py::object hotness_metrics()const{auto h=std::atomic_load(&hotness_);return dhwire::to_python(h?h->metrics():V(dhvalue::M{{"enabled",false}}));}
  void replica_routes(py::object rows){peer_replicas_.update(dhwire::from_python(rows),now_ns(),node_count_);}
  py::object replica_counters()const{return dhwire::to_python(peer_replicas_.counters());}
  std::shared_ptr<void> pin_replica(const V& state) const {
    if(!dhvalue::has(state,"replica_version"))return {};
    auto all=std::atomic_load(&overlays_);
    auto it=all->find(dhvalue::get<std::string>(state,"replica_version"));
    require(it!=all->end(),"Replica version is not accepting queries");
    return it->second;
  }
  void install_replica(const std::string& version,py::object shard,Array<uint8_t> membership) {
    require(!version.empty()&&version.size()<=128,"Invalid replica version");
    py::dict m=shard.attr("meta");
    require(m["epoch"].cast<std::string>()==epoch_&&m["dimension"].cast<size_t>()==dim_,"Replica graph mismatch");
    for(const auto& [key,value]:replica_contract_.map())
      require(dhwire::pack(dhwire::from_python(m[py::str(key)]))==dhwire::pack(value),"Replica quantization contract mismatch");
    auto r=std::make_shared<ReplicaOverlay>();r->version=version;
    r->stride=m["record_bytes"].cast<size_t>();r->header=m["header_bytes"].cast<size_t>();r->degree=m["max_degree"].cast<size_t>();
    auto ids=shard.attr("ids").cast<Array<Id>>();auto code_ids=shard.attr("code_ids").cast<Array<Id>>();
    auto codes=shard.attr("codes").cast<Array<uint8_t>>();auto extra=shard.attr("extra").cast<Array<uint8_t>>();
    auto factors=shard.attr("factors").cast<Array<float>>();auto centers=shard.attr("center_ids").cast<Array<uint32_t>>();
    require(codes.ndim()==2&&extra.ndim()==2&&factors.ndim()==2&&factors.shape(1)==3&&
      codes.shape(0)==code_ids.size()&&extra.shape(0)==code_ids.size()&&factors.shape(0)==code_ids.size()&&
      centers.size()==code_ids.size()&&membership.ndim()==2&&membership.shape(0)==code_ids.size(),"Invalid replica arrays");
    require(size_t(codes.shape(1))==code_bytes_&&size_t(extra.shape(1))==extra_bytes_,"Replica encoding width mismatch");
    r->code_bytes=code_bytes_;r->extra_bytes=extra_bytes_;
    r->codes.assign(codes.data(),codes.data()+codes.size());r->extra.assign(extra.data(),extra.data()+extra.size());
    r->factors.assign(factors.data(),factors.data()+factors.size());r->centers.assign(centers.data(),centers.data()+centers.size());
    for(ssize_t i=0;i<ids.size();++i)require(r->records.emplace(ids.data()[i],i).second,"Duplicate replica record");
    r->sorted_record_ids.assign(ids.data(),ids.data()+ids.size());std::sort(r->sorted_record_ids.begin(),r->sorted_record_ids.end());
    r->owners.resize(code_ids.size());
    for(ssize_t i=0;i<code_ids.size();++i){require(r->locations.emplace(code_ids.data()[i],i).second,"Duplicate replica code");
      for(ssize_t n=0;n<membership.shape(1);++n)if(membership.data()[i*membership.shape(1)+n])r->owners[i].push_back(int(n));
      require(!r->owners[i].empty(),"Replica code lacks owners");}
    for(auto [id,p]:r->records)require(r->locations.count(id),"Replica record lacks code");
    const size_t bytes=py::hasattr(shard,"native_id_lookup_bytes")?shard.attr("native_id_lookup_bytes").cast<size_t>():0;
    if(bytes&&code_ids.size()){
      const auto lo=*std::min_element(code_ids.data(),code_ids.data()+code_ids.size());
      const auto hi=*std::max_element(code_ids.data(),code_ids.data()+code_ids.size());
      if(hi-lo<bytes/(2*sizeof(size_t))){r->lookup_base=lo;size_t span=size_t(hi-lo)+1;
        r->dense_records.assign(span,ReplicaOverlay::missing);r->dense_codes.assign(span,ReplicaOverlay::missing);
        for(auto [id,row]:r->records)r->dense_records[size_t(id-lo)]=row;
        for(auto [id,row]:r->locations)r->dense_codes[size_t(id-lo)]=row;}
    }
    r->fd=dup(shard.attr("fd").cast<int>());require(r->fd>=0,"Replica fd duplication failed");
    std::lock_guard<std::mutex> lock(overlay_write_);auto old=std::atomic_load(&overlays_);
    require(!old->count(version),"Replica version already installed");
    auto next=std::make_shared<Overlays>(*old);next->emplace(version,std::move(r));
    std::atomic_store(&overlays_,std::shared_ptr<const Overlays>(next));
  }
  void retire_replica(const std::string& version) {
    std::lock_guard<std::mutex> lock(overlay_write_);auto next=std::make_shared<Overlays>(*std::atomic_load(&overlays_));
    auto it=next->find(version);if(it!=next->end())retired_overlays_[version]=it->second;
    next->erase(version);std::atomic_store(&overlays_,std::shared_ptr<const Overlays>(next));
  }
  bool replica_released(const std::string& version){std::lock_guard<std::mutex> lock(overlay_write_);
    if(std::atomic_load(&overlays_)->count(version))return false;
    auto it=retired_overlays_.find(version);return it==retired_overlays_.end()||it->second.expired();}
  Shard(py::object shard, Array<uint8_t> membership, py::object clock_domain):clock_domain_(clock_domain) {
    native_clock_=dhwire::from_python(clock_domain);
    py::dict meta=shard.attr("meta");
    V::Map contract;for(const char* key:{"epoch","dimension","metric","bits","rotation_seed","rotation_sha256"})contract[key]=dhwire::from_python(meta[py::str(key)]);
    replica_contract_=std::move(contract);
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
    const size_t lookup_limit=py::hasattr(shard,"native_id_lookup_bytes")?shard.attr("native_id_lookup_bytes").cast<size_t>():0;
    if(lookup_limit&&ids.size()&&code_ids.size()) {
      lookup_base_=std::min(ids.data()[0],code_ids.data()[0]);
      const Id highest=std::max(ids.data()[ids.size()-1],code_ids.data()[code_ids.size()-1]);
      const Id delta=highest-lookup_base_;
      // Check before +1 and multiplication: arbitrary uint64 IDs stay safe.
      const size_t max_span=lookup_limit/(2*sizeof(size_t));
      if(delta<max_span) {
        const size_t span=size_t(delta)+1;
        dense_locations_.assign(span,missing_location);
        dense_code_locations_.assign(span,missing_location);
      }
    }
    if(dense_code_locations_.empty()){locations_.reserve(ids.size());code_locations_.reserve(code_ids.size());}
    owners_.resize(code_ids.size());node_count_=size_t(membership.shape(1));
    for(ssize_t i=0;i<ids.size();++i) {
      if(dense_locations_.empty())locations_.emplace(ids.data()[i],i);
      else dense_locations_[size_t(ids.data()[i]-lookup_base_)]=i;
    }
    for(ssize_t i=0;i<code_ids.size();++i) {
      if(dense_code_locations_.empty())code_locations_.emplace(code_ids.data()[i],i);
      else dense_code_locations_[size_t(code_ids.data()[i]-lookup_base_)]=i;
      for(ssize_t n=0;n<membership.shape(1);++n)
        if(membership.data()[i*membership.shape(1)+n])owners_[i].push_back(int(n));
      require(!owners_[i].empty(),"Native code has no owner");
    }
    fd_=dup(shard.attr("fd").cast<int>());
    if(fd_<0)throw std::runtime_error("Cannot duplicate native index fd");
  }
  ~Shard(){if(fd_>=0)::close(fd_);}
  void close(){auto h=std::atomic_load(&hotness_);if(h)h->close();if(fd_>=0){::close(fd_);fd_=-1;}}
  py::dict id_lookup_info() const {
    py::dict out;out["mode"]=dense_code_locations_.empty()?"hash":"dense";
    out["dense_bytes"]=(dense_locations_.size()+dense_code_locations_.size())*sizeof(size_t);
    out["id_base"]=lookup_base_;return out;
  }

  py::dict run(py::dict state,py::dict config,Array<double> draws,
               std::shared_ptr<Control> control,py::object read_batch,
               py::object release_batch,py::function spawn,
               std::shared_ptr<dhcontrol::Ticket> completion=nullptr) {
    V input=dhwire::from_python(state),cfg=dhwire::from_python(config),output;
    std::vector<double> randoms(draws.data(),draws.data()+draws.size());
    Read read;
    if(!read_batch.is_none())read=[&](const std::vector<Id>& ids){
      py::gil_scoped_acquire gil;
      auto reply=read_batch(py::cast(ids)).cast<py::tuple>();
      ReadReply out;out.stats=reply[1].cast<Stats>();
      for(auto item:reply[0]){auto bytes=py::cast<std::string>(item);out.records.emplace_back(bytes.begin(),bytes.end());}
      return out;
    };
    Release release=[&]{py::gil_scoped_acquire gil;release_batch();};
    Spawn forward=[&](V payload){py::gil_scoped_acquire gil;return dhwire::from_python(spawn(dhwire::to_python(payload)));};
    int64_t gil_wait;
    {py::gil_scoped_release gil;output=run_value(input,cfg,randoms,control,read,release,forward,completion);gil_wait=now_ns();}
    const auto acquired=now_ns();
    if(dhvalue::has(output,"native_timestamps")){
      auto& t=output.map().at("native_timestamps").map();
      t["native_gil_wait_start_ns"]=V(gil_wait);t["native_gil_reacquired_ns"]=V(acquired);
    }
    auto converted=dhwire::to_python(output).cast<py::dict>();
    if(!completion&&converted.contains("native_timestamps")){
      auto t=converted["native_timestamps"].cast<py::dict>();
      t["native_wire_report_ready_ns"]=t["native_report_ready_ns"];
      t["native_report_ready_ns"]=now_ns();
    }
    return converted;
  }

  V run_value(const V& state,const V& config,const std::vector<double>& draws,
               std::shared_ptr<Control> control,const Read& read_batch,
               const Release& release_batch,const Spawn& spawn,
               std::shared_ptr<dhcontrol::Ticket> completion=nullptr,
               std::shared_ptr<void> pinned={}) {
    auto overlay=std::static_pointer_cast<ReplicaOverlay>(pinned?pinned:pin_replica(state));
    require(!overlay||!read_batch,"Replica overlay requires synchronous native I/O");
    const size_t record_stride=overlay?std::max(stride_,overlay->stride):stride_;
    const int64_t native_entry_ns=now_ns();
    int64_t native_search_start_ns=0,native_loop_done_ns=0;
    int64_t native_completion_enqueued_ns=0;
    require(fd_>=0,"Native search shard closed");
    require(dhvalue::get<std::string>(state,"epoch")==epoch_,"Native epoch mismatch");
    auto visited=dhvalue::get<std::vector<int>>(state,"visited_nodes");
    std::unordered_set<int> visited_set(visited.begin(),visited.end());
    const bool revisits=dhvalue::optional<bool>(dhvalue::at(config,"peer"),"allow_node_revisit",false);
    require(!visited.empty()&&visited.back()==sid_&&(revisits||visited.size()==visited_set.size()),"Invalid visited-node path");
    const auto& bytes=std::get<V::Bytes>(dhvalue::at(state,"query_f32").data);
    require(bytes.size()==dim_*sizeof(float),"Native query dimension mismatch");
    std::vector<float> query(dim_);std::memcpy(query.data(),bytes.data(),bytes.size());finite(query.data(),dim_);
    if(cosine_) {double norm=0;for(float v:query)norm+=double(v)*v;require(std::abs(std::sqrt(norm)-1)<=1.001e-5,"Cosine query must be normalized");}
    const auto& cfg=dhvalue::at(config,"search");const auto& policy=dhvalue::at(config,"scheduler");
    const bool skip_seen_estimates=dhvalue::optional<bool>(cfg,"skip_seen_estimates",false);
    const auto& peer=dhvalue::at(config,"peer");const auto& net=dhvalue::at(config,"network");
    const bool logical=dhvalue::optional<bool>(peer,"logical_region_visits",false);
    const auto excluded=logical?dhregion::decode(state,dhvalue::get<size_t>(peer,"max_excluded_region_ids")):std::vector<Id>{};
    auto excluded_id=[&](Id id){return std::binary_search(excluded.begin(),excluded.end(),id);};
    auto region_path=dhregion::path(state);
    const int local_base_group=int(node_count_)+sid_;
    auto heat=std::atomic_load(&hotness_);
    auto heat_lane=heat&&!dhvalue::optional<bool>(state,"internal_validation",false)?heat->lane():std::shared_ptr<DecayedHotness::Lane>{};
    struct HeatFlush {std::shared_ptr<DecayedHotness::Lane> lane;bool force;~HeatFlush(){if(lane&&force)lane->flush();}} heat_flush{heat_lane,!completion};
    const size_t topk=dhvalue::get<size_t>(cfg,"top_k"),frontier_limit=dhvalue::get<size_t>(cfg,"frontier_size");
    const size_t beam=dhvalue::get<size_t>(cfg,"local_beam"),remote_batch=dhvalue::get<size_t>(net,"batch_size");
    const size_t stagnation_limit=dhvalue::get<size_t>(cfg,"stagnation_batches");
    const bool exclude_self=dhvalue::get<bool>(cfg,"exclude_self"),fallback_enabled=dhvalue::get<bool>(cfg,"fallback_enabled");
    const size_t fallback_limit=dhvalue::has(state,"fallback_remaining")?dhvalue::get<size_t>(state,"fallback_remaining"):dhvalue::get<size_t>(cfg,"max_fallback_entries");
    const size_t grant_total=dhvalue::get<size_t>(state,"work_grant"),slots_total=dhvalue::get<size_t>(state,"branch_slots");
    const size_t min_dwell=(visited.size()>1||bool(overlay))?dhvalue::optional<size_t>(peer,"min_expansions_after_cross",0):0;
    const bool dwell_escape=dhvalue::optional<bool>(peer,"cross_on_local_exhaustion",false);
    size_t remaining=grant_total,slots=slots_total,stagnation=0,fallback_used=0,draw_index=0;
    require(topk&&beam&&frontier_limit&&remote_batch&&grant_total,"Invalid native search budget");
    const double scale=dhvalue::get<double>(state,"scale"),kappa=dhvalue::get<double>(policy,"kappa");
    const double exploration=dhvalue::get<double>(policy,"exploration"),max_wait=dhvalue::get<double>(policy,"max_wait_ms");
    const double rtt=dhvalue::get<double>(policy,"remote_rtt_ms"),bandwidth=dhvalue::get<double>(policy,"bandwidth_bytes_per_ms");
    const double ewma=dhvalue::get<double>(policy,"ewma_weight"),weight=dhvalue::get<double>(policy,"remote_weight");
    const double temperature=dhvalue::get<double>(policy,"probability_temperature"),child_fraction=overlay?dhvalue::optional<double>(peer,"replica_child_budget_fraction",dhvalue::get<double>(peer,"child_budget_fraction")):dhvalue::get<double>(peer,"child_budget_fraction");
    double service_per_node=dhvalue::get<double>(policy,"local_batch_ms")/beam;
    require(std::isfinite(scale)&&scale>0&&max_wait>0&&bandwidth>0&&temperature>0,"Invalid native policy scale");
    const std::string mode=dhvalue::get<std::string>(policy,"mode"),selection=dhvalue::get<std::string>(policy,"remote_selection");
    const bool detailed=dhvalue::has(state,"trace_enabled")?dhvalue::get<bool>(state,"trace_enabled"):
        dhvalue::get<bool>(dhvalue::at(config,"metrics"),"detailed_trace");
    const bool lifecycle=detailed||(dhvalue::has(state,"lifecycle_trace_enabled")&&dhvalue::get<bool>(state,"lifecycle_trace_enabled"));
    std::optional<Id> query_id;if(!dhvalue::null(dhvalue::at(state,"query_id")))query_id=dhvalue::get<Id>(state,"query_id");
    const std::string branch_id=dhvalue::get<std::string>(state,"branch_id");
    std::vector<Candidate> frontier,fallbacks,paths;
    for(auto key:{"seeds","remote_frontier"})if(dhvalue::has(state,key))
      for(const auto& item:dhvalue::array(dhvalue::at(state,key)))frontier.push_back(candidate(item));
    if(dhvalue::has(state,"fallback_entries"))for(const auto& item:dhvalue::array(dhvalue::at(state,"fallback_entries")))fallbacks.push_back(candidate(item));
    auto initial_seen=dhvalue::get<std::vector<Id>>(state,"seen");
    auto ancestor_expanded=dhvalue::optional<std::vector<Id>>(state,"ancestor_expanded",{});
    std::unordered_set<Id> seen(initial_seen.begin(),initial_seen.end());
    if(revisits){seen.clear();seen.insert(ancestor_expanded.begin(),ancestor_expanded.end());
      frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){return seen.count(c.id);}),frontier.end());
      for(const auto& c:frontier)seen.insert(c.id);
    }
    std::unordered_map<Id,double> results;
    std::vector<Id> expanded;
    std::vector<Decision> decisions;std::vector<Trace> traces;
    Stats stats;std::string stop_reason="frontier_exhausted";
    if(logical){
      stats["region_exclusion_ids_inherited"]=excluded.size();
      frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){if(!excluded_id(c.id))return false;stats["region_excluded_candidates"]++;return true;}),frontier.end());
    }
    bool can_forward=true;const bool async=bool(read_batch);
    require(!completion||!async,"Native control currently requires sync_pread");
    V::Map native_report;
    V::Array native_children,native_events;
    V native_clock;
    {
      for(auto key:{"branch_id","parent_branch_id","visited_nodes","work_grant","branch_slots"})
        native_report[key]=dhvalue::at(state,key);
      native_clock=native_clock_;
      if(logical){native_report["logical_region"]=dhregion::current(state);native_report["visited_regions"]=dhvalue::vector(region_path);
        native_report["excluded_regions"]=dhvalue::has(state,"excluded_regions")?dhvalue::at(state,"excluded_regions"):V(dhvalue::A{});}
    }
    const int64_t started=now_ns();const double cpu_started=cpu_ms();
    const auto owner_thread=static_cast<uint64_t>(syscall(SYS_gettid));
    auto owner=[&](Id id,const std::vector<int>& owners) {
      if(logical&&excluded_id(id))return -1;
      if(overlay&&overlay->contains(id))return sid_;
      if(std::find(owners.begin(),owners.end(),sid_)!=owners.end())return logical&&overlay?(can_forward?local_base_group:-1):sid_;
      int best=std::numeric_limits<int>::max();
      if(can_forward)for(int n:owners)if(n!=sid_&&(revisits||!visited_set.count(n)))best=std::min(best,n);
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
      for(auto c:fallbacks)if(!seen.count(c.id)&&!excluded_id(c.id)&&owner(c.id,c.owners)>=0) {
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
      native_search_start_ns=now_ns();
      control->check();
      auto prep_start=now_ns();auto prepared=quantizer_->prepare_native(query.data());
      stats["rabitq_prepare_ms"]=ms_since(prep_start);
      std::vector<char> raw(async?0:beam*record_stride);
      while(remaining>0) {
        control->check();
        if(frontier.empty()&&!fallback())break;
        const auto now=now_ns();std::vector<Group> groups;
        for(auto it=frontier.begin();it!=frontier.end();) {
          const int target=owner(it->id,it->owners);
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
        bool dwell_blocked=!remotes.empty()&&expanded.size()<min_dwell;
        if(dwell_blocked&&local<0&&dwell_escape){stats["cross_dwell_exhaustion_escapes"]++;dwell_blocked=false;}
        if(dwell_blocked){stats["cross_dwell_blocked_decisions"]++;decision.reason="minimum_local_expansions";
          if(local<0){stop_reason="minimum_local_expansions_unmet";break;}}
        if(!dwell_blocked&&!remotes.empty() && !(local>=0&&remaining<2)) {
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
          const int destination=action.node==local_base_group?sid_:action.node;
          bool other=false;for(auto& g:groups)other|=g.node!=action.node;
          size_t grant=other?std::max(size_t(1),size_t(remaining*child_fraction)):remaining;
          if(local>=0)grant=std::min(grant,remaining-1);
          require(grant>0&&slots>0,"Invalid native child grant");
          const size_t child_slots=slots-1;slots=0;remaining-=grant;
          auto directory=peer_replicas_.snapshot();
          auto rule=directory->find(action.candidates.front().id);
          std::optional<ReplicaDirectory::Rule> replica;
          if(rule!=directory->end()){
            const auto& r=rule->second;uint64_t h=1469598103934665603ULL;
            for(unsigned char ch:decision_id){h^=ch;h*=1099511628211ULL;}
            double draw=double(h>>11)*(1.0/9007199254740992.0);
            const auto old_regions=dhvalue::optional<std::vector<std::string>>(state,"excluded_regions",{});
            if(r.source==size_t(destination)&&r.target!=size_t(sid_)&&(!logical||std::find(old_regions.begin(),old_regions.end(),r.version)==old_regions.end())&&(revisits||!visited_set.count(int(r.target)))&&
                uint64_t(now_ns())<r.expires&&draw<r.fraction*r.weight){
              replica=r;decision.target=int(r.target);decisions.back().target=decision.target;
              peer_replicas_.selected(r.version);
            }
          }
          decision.target=replica?int(replica->target):destination;decisions.back().target=decision.target;
          std::vector<Candidate> seeds,carried;
          for(size_t i=0;i<std::min({action.candidates.size(),remote_batch,frontier_limit});++i){
            auto c=action.candidates[i];
            if(replica){auto r=directory->find(c.id);if(r==directory->end()||r->second.version!=replica->version)continue;
              if(std::find(c.owners.begin(),c.owners.end(),int(replica->target))==c.owners.end())c.owners.push_back(int(replica->target));}
            seeds.push_back(std::move(c));
          }
          for(auto& seed:seeds)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& x){return x.id==seed.id;}),frontier.end());
          for(auto& c:frontier)if(owner(c.id,c.owners)!=sid_&&owner(c.id,c.owners)>=0)carried.push_back(c);
          {
            V::Array seed_rows,carry_rows;
            for(auto& c:seeds)seed_rows.push_back(row(c));
            for(auto& c:carried)carry_rows.push_back(row(c));
            std::vector<Id> seen_sorted(seen.begin(),seen.end());std::sort(seen_sorted.begin(),seen_sorted.end());
            V payload=V::Map{{"seeds",V(std::move(seed_rows))},{"remote_frontier",V(std::move(carry_rows))},
              {"seen",dhvalue::vector(seen_sorted)},{"target_node",V(int64_t(replica?replica->target:destination))},
              {"work_grant",V(uint64_t(grant))},{"branch_slots",V(uint64_t(child_slots))},
              {"fallback_remaining",V(uint64_t(fallback_limit-fallback_used))},
              {"dispatch_decision_id",V(decision_id)},{"draws_used",V(uint64_t(draw_index))}};
            if(revisits){auto history=ancestor_expanded;history.insert(history.end(),expanded.begin(),expanded.end());
              std::sort(history.begin(),history.end());history.erase(std::unique(history.begin(),history.end()),history.end());
              payload.map()["ancestor_expanded"]=dhvalue::vector(history);
              std::vector<Id> discovery=history;for(const auto& c:seeds)discovery.push_back(c.id);for(const auto& c:carried)discovery.push_back(c.id);
              payload.map()["seen"]=dhvalue::vector(discovery);
            }
            if(logical&&overlay){
              std::vector<Id> all;all.reserve(excluded.size()+overlay->sorted_record_ids.size());
              std::set_union(excluded.begin(),excluded.end(),overlay->sorted_record_ids.begin(),overlay->sorted_record_ids.end(),std::back_inserter(all));
              require(all.size()<=dhvalue::get<size_t>(peer,"max_excluded_region_ids"),"Logical exclusion capacity exceeded");
              payload.map()["excluded_region_ids"]=dhregion::encode(all);
              auto versions=dhvalue::optional<std::vector<std::string>>(state,"excluded_regions",{});versions.push_back(overlay->version);
              payload.map()["excluded_regions"]=dhvalue::vector(versions);
              stats["region_exclusion_ids_added"]+=all.size()-excluded.size();
              size_t done=0;for(auto id:expanded)done+=overlay->contains(id);
              stats["region_unexpanded_at_handoff"]+=overlay->records.size()-done;
              stats["region_handoffs"]++;
            }
            if(replica){payload.map()["replica_version"]=replica->version;payload.map()["replica_access_kind"]="peer";payload.map()["replica_fallback_source"]=uint64_t(destination);
              stats["peer_replica_selected"]++;}
            stats["physical_node_revisits"]+=visited_set.count(replica?int(replica->target):destination);
            if(destination==sid_&&!replica)stats["same_host_region_handoffs"]++;
            V descriptor=spawn(std::move(payload));
            native_events.emplace_back(V::Map{{"decision_id",V(decision_id)},{"target",V(int64_t(replica?replica->target:destination))},
              {"child_branch_id",dhvalue::at(descriptor,"branch_id")},{"work_grant",V(uint64_t(grant))},
              {"local_expansions_before",V(uint64_t(expanded.size()))},{"candidate_count",V(uint64_t(seeds.size()))},
              {"carried_candidate_count",V(uint64_t(carried.size()))},{"elapsed_ms",V(ms_since(started))}});
            native_children.push_back(std::move(descriptor));
          }
          can_forward=false;continue;
        }
        std::vector<Candidate> selected(action.candidates.begin(),action.candidates.begin()+action.count);
        std::vector<Id> batch;for(auto& c:selected)batch.push_back(c.id);
        for(auto id:batch)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){return c.id==id;}),frontier.end());
        batch.erase(std::remove_if(batch.begin(),batch.end(),[&](Id id){
          if(control->claim(id))return false;
          stats["duplicate_local_expansions_avoided"]++;return true;
        }),batch.end());
        if(batch.empty())continue;
        // Selected candidates may have been removed by a concurrent local
        // branch; preserve ID-to-parent alignment for the surviving records.
        selected.erase(std::remove_if(selected.begin(),selected.end(),[&](const Candidate& c){return std::find(batch.begin(),batch.end(),c.id)==batch.end();}),selected.end());
        const double old=results.size()>=topk?threshold():std::numeric_limits<double>::infinity();
        Stats work;Trace trace;trace.ids=batch;
        bool held=false,accounted=false;counters_->begin();
        try {
          if(async) {
            const auto wait_start=now_ns();
            {auto reply=read_batch(batch);held=true;
              require(reply.records.size()==batch.size(),"Wrong native I/O batch size");
              raw.resize(batch.size()*stride_);
              for(size_t i=0;i<batch.size();++i){require(reply.records[i].size()==stride_,"Short native async record");
                std::memcpy(raw.data()+i*stride_,reply.records[i].data(),stride_);}
              work=std::move(reply.stats);
            }
            stats["query_io_wait_ms"]+=ms_since(wait_start);
          }
          control->check();trace.start=now_ns();const double service_cpu=cpu_ms();
          std::unordered_map<Id,Id> parents;
          std::unordered_map<Id,Candidate> selected_by_id;
          std::vector<std::pair<Id,double>> exact;
          for(size_t i=0;i<batch.size();++i) {
            control->check();const Id id=batch[i];selected_by_id.emplace(id,selected[i]);
            char* data=raw.data()+i*record_stride;
            if(!async) {
              size_t location=locate(id,false),read_stride=stride_,read_header=header_;int read_fd=fd_;
              if((location==missing_location||logical)&&overlay){auto pos=overlay->locate(id);
                if(pos!=ReplicaOverlay::missing){location=pos;read_stride=overlay->stride;read_header=overlay->header;read_fd=overlay->fd;stats["replica_record_reads"]++;}}
              require(location!=missing_location,"Native expansion of nonlocal vertex");
              const auto t=now_ns();ssize_t count;
              do {count=::pread(read_fd,data,read_stride,read_header+location*read_stride);}while(count<0&&errno==EINTR);
              const auto end=now_ns();work["pread_ms"]+=(end-t)/1e6;work["pread_calls"]++;
              if(count<0)throw std::runtime_error(std::string("native pread: ")+strerror(errno));
              require(size_t(count)==read_stride,"Short native node read");work["file_read_bytes"]+=read_stride;
              if(detailed)trace.spans.emplace_back("pread",t,end);
            }
            const auto t=now_ns();double distance=0;
            for(size_t j=0;j<dim_;++j){float value;std::memcpy(&value,data+j*sizeof(float),sizeof(float));
              require(std::isfinite(value),"Nonfinite disk vector");const double delta=double(query[j])-value;distance+=delta*delta;}
            exact.emplace_back(id,distance);const auto end=now_ns();work["exact_distance_ms"]+=(end-t)/1e6;
            work["exact_distance_evaluations"]++;if(detailed)trace.spans.emplace_back("exact_distance",t,end);
            uint32_t degree;std::memcpy(&degree,data+dim_*4,4);
            require(degree<=(overlay?std::max(degree_,overlay->degree):degree_)&&dim_*4+4+size_t(degree)*8<=record_stride,"Invalid native node degree");
            work["scanned_edges"]+=degree;
            for(size_t j=0;j<degree;++j){Id neighbor;std::memcpy(&neighbor,data+dim_*4+4+j*8,8);parents.emplace(neighbor,id);}
          }
          std::vector<Id> neighbors;neighbors.reserve(parents.size());
          stats["rabitq_neighbor_candidates"]+=parents.size();
          for(auto [id,parent]:parents){
            if(logical&&excluded_id(id)){stats["region_excluded_candidates"]++;continue;}
            // Such candidates are already ignored below. Their estimate is
            // immutable for this query, so recomputing it cannot affect search.
            if(skip_seen_estimates&&seen.count(id)){stats["rabitq_skipped_seen"]++;continue;}
            neighbors.push_back(id);
          }
          std::sort(neighbors.begin(),neighbors.end());
          std::vector<std::pair<float,float>> estimates;estimates.reserve(neighbors.size());const auto quant_start=now_ns();
          for(Id id:neighbors) {
            const size_t p=locate(id,true);std::pair<float,float> value;
            if(p!=missing_location)value=quantizer_->score_native(prepared,codes_+p*code_bytes_,extra_bytes_?extra_+p*extra_bytes_:nullptr,factors_+3*p,center_ids_[p]);
            else {require(bool(overlay),"Missing native neighbor code");const size_t r=overlay->locate(id,true);require(r!=ReplicaOverlay::missing,"Missing replica neighbor code");
              value=quantizer_->score_native(prepared,overlay->codes.data()+r*code_bytes_,extra_bytes_?overlay->extra.data()+r*extra_bytes_:nullptr,overlay->factors.data()+3*r,overlay->centers[r]);}
            require(std::isfinite(value.first)&&std::isfinite(value.second),"Nonfinite native estimate");estimates.push_back(value);
          }
          const auto quant_end=now_ns();work["rabitq_ms"]=(quant_end-quant_start)/1e6;work["rabitq_evaluations"]=neighbors.size();
          if(detailed)trace.spans.emplace_back("rabitq",quant_start,quant_end);
          trace.end=now_ns();trace.cpu=cpu_ms()-service_cpu;work["service_ms"]=(trace.end-trace.start)/1e6;
          work["server_thread_cpu_ms"]=trace.cpu;
          if(held){std::vector<char>().swap(raw);release_batch();held=false;}
          control->check();remaining-=batch.size();expanded.insert(expanded.end(),batch.begin(),batch.end());stats["batches"]++;
          if(heat_lane){const auto t=now_ns();heat_lane->record(batch,uint64_t(t));stats["hotness_record_ms"]+=ms_since(t);stats["hotness_expansions_recorded"]+=batch.size();}
          for(auto [name,value]:work)if(name!="server_thread_cpu_ms")stats[name]+=value;
          const double measured=(work["service_ms"]+work["io_batch_wall_ms"])/batch.size();
          service_per_node=(1-ewma)*service_per_node+ewma*std::max(measured,1e-9);
          for(auto [id,distance]:exact) {paths.push_back(selected_by_id.at(id));if(!(exclude_self&&query_id&&id==*query_id))results[id]=distance;}
          for(size_t i=0;i<neighbors.size();++i) {
            Id id=neighbors[i];if(!seen.insert(id).second)continue;
            const auto& parent=selected_by_id.at(parents.at(id));const auto p=locate(id,true);
            auto owners=p!=missing_location?owners_[p]:overlay->owners.at(overlay->locate(id,true));
            if(overlay&&overlay->contains(id)&&std::find(owners.begin(),owners.end(),sid_)==owners.end())owners.push_back(sid_);
            const int target=owner(id,owners);if(target<0){stats[dropped()]++;continue;}
            Candidate c;c.id=id;c.distance=estimates[i].first;c.uncertainty=estimates[i].second;c.owners=owners;
            c.parent=parent.id;c.depth=parent.depth+1;c.cross=parent.cross+(target!=sid_);c.root=parent.root;c.queued=now_ns();frontier.push_back(std::move(c));
          }
          const bool improved=threshold()<old;stats["improved_topk_batches"]+=improved;stagnation=improved?0:stagnation+1;
          if(frontier.size()>frontier_limit){std::sort(frontier.begin(),frontier.end(),closer);stats["frontier_dropped"]+=frontier.size()-frontier_limit;frontier.resize(frontier_limit);}
          if(stagnation>=stagnation_limit)fallback();
          const auto counter_start=now_ns();
          counters_->finish_native(true,work);
          accounted=true;
          stats["counter_update_ms"]+=ms_since(counter_start);
          if(detailed){trace.stats=std::move(work);traces.push_back(std::move(trace));}
        } catch(...) {
          if(held){std::vector<char>().swap(raw);try{release_batch();}catch(...){}}
          if(!accounted)counters_->finish_native(false,{});
          throw;
        }
      }
      if(remaining==0)stop_reason="branch_work_credit_exhausted";
      if(heat_lane&&!completion){const auto t=now_ns();heat_lane->flush();stats["hotness_record_ms"]+=ms_since(t);}
      native_loop_done_ns=now_ns();
      stats["server_thread_cpu_ms"]=cpu_ms()-cpu_started;
      stats["branch_wall_ms"]=ms_since(started);
      {
        // Produce a native report for either transport without Python APIs.
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
        if(dhvalue::optional<bool>(state,"replica_unavailable_fallback",false))native_report["replica_unavailable_fallback"]=true;
        if(overlay){native_report["replica_version"]=V(overlay->version);
          native_report["replica_access_kind"]=V(dhvalue::optional<std::string>(state,"replica_access_kind",visited.size()==1?"entry":"peer"));
          all_stats["replica_local_expansions"]=V(uint64_t(expanded.size()));
        }
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
        if(completion){
          V::Map timestamps;
          if(lifecycle)timestamps=native_report.at("native_timestamps").map();
          completion->complete(V(std::move(native_report)));native_completion_enqueued_ns=now_ns();
          V::Map out{{"native_control_submitted",V(true)}};
          if(lifecycle){timestamps["native_completion_enqueued_ns"]=V(native_completion_enqueued_ns);out["native_timestamps"]=V(std::move(timestamps));}
          return out;
        }
        return V(std::move(native_report));
      }
    }
  }

};
} // namespace dhsearch

inline void register_native_search(py::module_& m) {
  py::register_exception<dhsearch::Cancelled>(m,"NativeSearchCancelled",PyExc_TimeoutError);
  py::class_<dhsearch::Control,std::shared_ptr<dhsearch::Control>>(m,"SearchControl")
    .def(py::init<double,double>()).def("renew",&dhsearch::Control::renew)
    .def("cancel",&dhsearch::Control::cancel).def("cancelled",&dhsearch::Control::cancelled);
  py::class_<dhsearch::Shard,std::shared_ptr<dhsearch::Shard>>(m,"SearchShard")
    .def(py::init<py::object,Array<uint8_t>,py::object>())
    .def("id_lookup_info",&dhsearch::Shard::id_lookup_info)
    .def("replica_routes",&dhsearch::Shard::replica_routes)
    .def("replica_counters",&dhsearch::Shard::replica_counters)
    .def("hotness_snapshot",&dhsearch::Shard::hotness_snapshot)
    .def("hotness_metrics",&dhsearch::Shard::hotness_metrics)
    .def("configure_hotness",[](dhsearch::Shard& s,py::object c){s.configure_hotness(dhwire::from_python(c));})
    .def("install_replica",&dhsearch::Shard::install_replica)
    .def("retire_replica",&dhsearch::Shard::retire_replica)
    .def("replica_released",&dhsearch::Shard::replica_released)
    .def("run",&dhsearch::Shard::run,py::arg("state"),py::arg("config"),py::arg("draws"),
         py::arg("control"),py::arg("read_batch"),py::arg("release_batch"),py::arg("spawn"),py::arg("completion")=nullptr)
    .def("close",&dhsearch::Shard::close);
}
