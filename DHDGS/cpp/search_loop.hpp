#pragma once
#include "read_metrics.hpp"
#include "exact_distance.hpp"
#include "pcg64_compat.hpp"
#include "replica_overlay.hpp"
#include "replica_load.hpp"
#include "replica_directory.hpp"
#include "logical_region.hpp"
#include "replica_pair_scope.hpp"
#include "decayed_hotness.hpp"
#include "search_control.hpp"
#include "search_resume.hpp"
#include "remote_pull_policy.hpp"
#include "native_control.hpp"
#include "value_utils.hpp"
#include "one_sided.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
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
  struct LoopFrame {
    V state,config,reply;
    std::vector<double> draws;
    size_t remaining=0,slots=0,stagnation=0,fallback_used=0,draw_index=0,pull_rounds=0,pull_bytes=0;
    RemotePullPolicy remote_policy;
    bool pull_failed=false,can_forward=true,refining=false,finishing=false;
    std::string finish_reason,sync_mode;
    size_t sync_last=0;
    std::vector<Id> refine_ids;
    std::unordered_map<Id,Candidate> deferred;
    double service_per_node=0,cpu_total=0,slot_wall=0;
    int64_t started=0,native_entry=0,search_start=0,pull_begin=0,pull_suspend_ns=0;
    uint64_t first_thread=0,last_thread=0,migrations=0,resumes=0;
    std::vector<Candidate> frontier,fallbacks,paths;
    std::unordered_set<Id> seen;
    std::unordered_map<Id,double> results;
    std::vector<Id> expanded;
    std::vector<Decision> decisions;
    std::vector<Trace> traces;
    Stats stats;
    V::Array pull_events,pull_gate_events,children,events,sync_events;
    std::shared_ptr<ReplicaOverlay> codes;
    std::unordered_map<Id,std::vector<uint8_t>> records;
    dhgrowth::Entries demand_batch;
    Quantizer::Prepared prepared;
    std::vector<Group> groups;
    Group action;
    int local=-1;
  };
  int fd_=-1,sid_=0;
  size_t node_count_=0,dim_=0,stride_=0,header_=0,degree_=0,code_bytes_=0,extra_bytes_=0;
  std::string epoch_,centers_sha_;
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
  std::shared_ptr<dhonesided::Image> export_one_sided(const V& cfg)const {
    struct MappedImage:dhonesided::Image {~MappedImage(){if(records)munmap(const_cast<void*>(records),record_bytes);}};
    auto image=std::make_shared<MappedImage>();
    struct stat st{};require(!fstat(fd_,&st)&&st.st_size>0,"Cannot size source registration");
    size_t span=0;Id base=UINT64_MAX,last=0;
    if(!dense_code_locations_.empty()){base=lookup_base_;span=dense_code_locations_.size();}
    else {for(const auto& item:code_locations_){base=std::min(base,item.first);last=std::max(last,item.first);}
      require(base!=UINT64_MAX&&last-base<UINT64_MAX,"Empty/overflowing source ID directory");span=size_t(last-base)+1;}
    const size_t width=8+code_bytes_+extra_bytes_+12+4+node_count_,cap=dhvalue::get<size_t>(cfg,"max_registered_bytes");
    require(span<=dhvalue::get<size_t>(cfg,"max_directory_bytes")/16&&size_t(st.st_size)<=cap&&
      span*16<=cap-size_t(st.st_size)&&owners_.size()<=(cap-size_t(st.st_size)-span*16)/width,"Source image capacity exceeded");
    image->directory.assign(span*2,UINT64_MAX);image->codes.resize(owners_.size()*width);
    image->base=base;image->epoch=epoch_;image->header=header_;image->stride=stride_;image->dimension=dim_;image->code_width=width;
    for(size_t i=0;i<span;++i){Id id=base+i;auto row=locate(id,false),c=locate(id,true);image->directory[i*2]=row;image->directory[i*2+1]=c;
      if(c==missing_location)continue;require(c<owners_.size(),"Invalid exported code row");auto* dst=image->codes.data()+c*width;
      std::memcpy(dst,&id,8);dst+=8;std::memcpy(dst,codes_+c*code_bytes_,code_bytes_);dst+=code_bytes_;
      if(extra_bytes_)std::memcpy(dst,extra_+c*extra_bytes_,extra_bytes_);dst+=extra_bytes_;
      std::memcpy(dst,factors_+c*3,12);dst+=12;std::memcpy(dst,center_ids_+c,4);dst+=4;for(auto owner:owners_[c])dst[owner]=1;
    }
    // Some deployed kernels/providers request writable GUP pins even for a
    // remote-read-only MR. Private COW permits that without modifying the file.
    // It may consume a private RAM copy; account it separately from page cache.
    const bool private_cow=dhvalue::optional<std::string>(cfg,"record_mapping","private_cow")=="private_cow";
    auto* address=mmap(nullptr,size_t(st.st_size),PROT_READ|(private_cow?PROT_WRITE:0),private_cow?MAP_PRIVATE:MAP_SHARED,fd_,0);require(address!=MAP_FAILED,"Source mmap failed");
    image->records=address;image->record_bytes=size_t(st.st_size);return image;
  }
  V read_records(const std::vector<Id>& ids,size_t max_bytes)const {
    const auto start=now_ns();const double cpu=cpu_ms();
    require(!ids.empty()&&ids.size()<=1024&&stride_<=max_bytes/ids.size(),"Invalid pull record capacity");
    V::Bytes records(ids.size()*stride_);std::set<Id> dependencies;double pread_ms=0;
    for(size_t i=0;i<ids.size();++i){auto row=locate(ids[i],false);require(row!=missing_location,"Pull of non-source record");auto* dst=records.data()+i*stride_;
      const auto t=now_ns();size_t done=0;while(done<stride_){auto n=::pread(fd_,dst+done,stride_-done,header_+row*stride_+done);if(n<0&&errno==EINTR)continue;require(n>0,"Short source pull read");done+=size_t(n);}pread_ms+=ms_since(t);
      uint32_t d;std::memcpy(&d,dst+dim_*4,4);require(d<=degree_&&dim_*4+4+size_t(d)*8<=stride_,"Invalid pulled degree");
      for(size_t j=0;j<d;++j){Id n;std::memcpy(&n,dst+dim_*4+4+j*8,8);dependencies.insert(n);}
    }
    // Binary rows: ID, code, extra code, factors[3], center, membership[N].
    const size_t width=8+code_bytes_+extra_bytes_+12+4+node_count_;
    require(dependencies.size()<=(max_bytes-records.size())/width,"Pull dependency byte capacity exceeded");
    V::Bytes codes(dependencies.size()*width);size_t i=0;
    for(auto id:dependencies){auto p=locate(id,true);require(p!=missing_location,"Missing source pull code");auto* dst=codes.data()+i++*width;
      std::memcpy(dst,&id,8);dst+=8;std::memcpy(dst,codes_+p*code_bytes_,code_bytes_);dst+=code_bytes_;
      if(extra_bytes_)std::memcpy(dst,extra_+p*extra_bytes_,extra_bytes_);dst+=extra_bytes_;
      std::memcpy(dst,factors_+p*3,12);dst+=12;std::memcpy(dst,center_ids_+p,4);dst+=4;
      for(auto n:owners_[p])dst[n]=1;
    }
    return V::Map{{"accepted",true},{"epoch",epoch_},{"record_stride",uint64_t(stride_)},{"code_stride",uint64_t(width)},{"ids",dhvalue::vector(ids)},
      {"records",std::move(records)},{"codes",std::move(codes)},{"source_read_ms",pread_ms},
      {"source_cpu_ms",cpu_ms()-cpu},{"source_service_ms",ms_since(start)}};
  }
  std::shared_ptr<dhreplica::Prepared> prepare_replica_directory(const std::string& version,const std::string& directory,
      py::object metadata,const std::string& centers_sha,const std::vector<uint64_t>& entries,
      size_t lookup_bytes,size_t warm_bytes,std::shared_ptr<dhreplica::Cancel> stop){
    auto m=dhwire::from_python(metadata);require(!version.empty()&&version.size()<=96&&bool(stop),"Invalid replica preparation");
    require(centers_sha==centers_sha_,"Replica centers contract mismatch");
    require(dhvalue::get<std::string>(m,"epoch")==epoch_&&dhvalue::get<size_t>(m,"dimension")==dim_,"Replica graph mismatch");
    for(const auto& [key,value]:replica_contract_.map())require(dhwire::pack(dhvalue::at(m,key))==dhwire::pack(value),"Replica quantization contract mismatch");
    py::gil_scoped_release release;
    return dhreplica::load(version,directory,m,centers_sha,dim_,node_count_,code_bytes_,extra_bytes_,entries,lookup_bytes,warm_bytes,stop);
  }
  void publish_replica(const std::shared_ptr<dhreplica::Prepared>& prepared){
    require(bool(prepared)&&prepared->epoch==epoch_,"Invalid prepared replica");prepared->stop->check();
    std::lock_guard<std::mutex> lock(overlay_write_);prepared->stop->check();auto old=std::atomic_load(&overlays_);
    const auto& version=prepared->overlay->version;
    require(!old->count(version)&&!retired_overlays_.count(version),"Replica version already installed/retired");
    auto next=std::make_shared<Overlays>(*old);next->emplace(version,prepared->overlay);std::atomic_store(&overlays_,std::shared_ptr<const Overlays>(next));
  }
  void configure_hotness(const V& config){
    std::shared_ptr<DecayedHotness> next;
    if(dhvalue::has(config,"hotness")&&dhvalue::get<bool>(dhvalue::at(config,"hotness"),"enabled"))next=std::make_shared<DecayedHotness>(dhvalue::at(config,"hotness"));
    std::atomic_store(&hotness_,std::move(next));
  }
  py::object hotness_snapshot()const{auto h=std::atomic_load(&hotness_);return dhwire::to_python(h?h->snapshot():V(dhvalue::M{{"enabled",false}}));}
  py::object hotness_metrics()const{auto h=std::atomic_load(&hotness_);return dhwire::to_python(h?h->metrics():V(dhvalue::M{{"enabled",false}}));}
  void replica_routes(py::object rows){peer_replicas_.update(dhwire::from_python(rows),now_ns(),node_count_);}
  py::object replica_counters()const{return dhwire::to_python(peer_replicas_.counters());}
  void configure_replica_growth(const std::string& version,py::object settings,size_t source_bytes,size_t source,const std::string& path){
    auto cfg=dhwire::from_python(settings);cfg.map()["target_identity"]=uint64_t(sid_);auto all=std::atomic_load(&overlays_);auto it=all->find(version);require(it!=all->end(),"Missing replica for growth");
    auto overlay=it->second;py::gil_scoped_release release;
    require(!std::atomic_load(&overlay->growth),"Replica growth already configured");
    auto growth=std::make_shared<dhgrowth::Cache>(overlay,std::move(cfg),source_bytes,source,epoch_,dim_,node_count_,path);
    std::atomic_store(&overlay->growth,std::move(growth));
  }
  py::object demand_growth_metrics()const{V::Array out;auto all=std::atomic_load(&overlays_);for(const auto& [id,o]:*all)if(auto g=std::atomic_load(&o->growth))out.push_back(g->metrics());return dhwire::to_python(out);}
  void seal_demand_growth(){py::gil_scoped_release release;auto all=std::atomic_load(&overlays_);for(const auto& [id,o]:*all)if(auto g=std::atomic_load(&o->growth))g->seal();}
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
    std::shared_ptr<ReplicaOverlay> retired;
    {
      std::lock_guard<std::mutex> lock(overlay_write_);auto next=std::make_shared<Overlays>(*std::atomic_load(&overlays_));
      auto it=next->find(version);if(it!=next->end()){retired=it->second;retired_overlays_[version]=retired;}
      next->erase(version);std::atomic_store(&overlays_,std::shared_ptr<const Overlays>(next));
    }
    // Maintenance must finish while both its immutable core and this Shard
    // are alive. Never join under the directory lock or on a CQ callback.
    if(retired)if(auto g=std::atomic_load(&retired->growth))g->close();
  }
  bool replica_released(const std::string& version){std::lock_guard<std::mutex> lock(overlay_write_);
    if(std::atomic_load(&overlays_)->count(version))return false;
    auto it=retired_overlays_.find(version);if(it==retired_overlays_.end())return true;
    if(!it->second.expired())return false;retired_overlays_.erase(it);return true;}
  Shard(py::object shard, Array<uint8_t> membership, py::object clock_domain):clock_domain_(clock_domain) {
    native_clock_=dhwire::from_python(clock_domain);
    py::dict meta=shard.attr("meta");
    centers_sha_=dhreplica::digest(py::str(shard.attr("directory")).cast<std::string>()+"/centers.npy",dhreplica::Cancel(300));
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
  ~Shard(){close();}
  void close(){
    std::vector<std::shared_ptr<ReplicaOverlay>> pinned;
    {
      std::lock_guard<std::mutex> lock(overlay_write_);
      for(const auto& [id,o]:*std::atomic_load(&overlays_))pinned.push_back(o);
      for(const auto& [id,w]:retired_overlays_)if(auto o=w.lock())pinned.push_back(std::move(o));
    }
    for(const auto& o:pinned)if(auto g=std::atomic_load(&o->growth))g->close();
    auto h=std::atomic_load(&hotness_);if(h)h->close();if(fd_>=0){::close(fd_);fd_=-1;}
  }
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
               std::shared_ptr<void> pinned={},
               const std::function<V(int,const std::vector<Id>&)>& fetch={},
               std::shared_ptr<LoopFrame> frame={}) {
    dhruntime::DecisionRandomStream policy_random(draws,dhvalue::get<uint64_t>(state,"decision_seed"));
    const int64_t slice_start=now_ns();const double slice_cpu=cpu_ms();
    const bool sync_resume=frame&&!frame->sync_mode.empty();
    bool finishing=frame&&frame->finishing;
    bool resume_action=bool(frame)&&!finishing&&(!sync_resume||frame->sync_mode=="claim");
    bool resume_refine=frame&&frame->refining;
    bool resume_pull=resume_action&&!resume_refine&&!sync_resume;
    bool resume_dispatch=(resume_action&&resume_refine)||(sync_resume&&frame->sync_mode=="claim");
    auto overlay=std::static_pointer_cast<ReplicaOverlay>(pinned?pinned:pin_replica(state));
    auto growth=overlay?std::atomic_load(&overlay->growth):std::shared_ptr<dhgrowth::Cache>{};
    std::unordered_map<Id,std::shared_ptr<dhgrowth::Entry>> demand_pins;
    if(frame)for(auto& e:frame->demand_batch)demand_pins.emplace(e->id,e);
    require(!overlay||!read_batch,"Replica overlay requires synchronous native I/O");
    const size_t record_stride=overlay?std::max(stride_,overlay->stride):stride_;
    const int64_t native_entry_ns=frame?frame->native_entry:now_ns();
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
    const bool candidate_sync=dhpair::active(peer,state);
    const bool pair_scoped=dhpair::scoped(peer);
    auto sync_target=[&](size_t node){return dhpair::target(peer,state,size_t(sid_),node);};
    auto sync_region=[&](size_t node){return dhpair::region(peer,state,node);};
    const bool nonblocking_start=candidate_sync&&dhvalue::optional<bool>(dhvalue::at(peer,"candidate_sync"),"nonblocking_start",false);
    bool delivery_required=false;size_t pending_starts=0;
    const size_t sync_interval=candidate_sync?dhvalue::get<size_t>(dhvalue::at(peer,"candidate_sync"),"interval_expansions"):10;
    size_t sync_last=frame?frame->sync_last:0;
    std::string claimed_branch;
    const bool logical=dhvalue::optional<bool>(peer,"logical_region_visits",false);
    const bool region_tasks=dhvalue::optional<bool>(peer,"region_tasks",false);
    const V pull_cfg=dhvalue::has(peer,"replica_pull")?dhvalue::at(peer,"replica_pull"):V(V::Map{});
    const V remote_cfg=dhvalue::has(peer,"remote_pull")?dhvalue::at(peer,"remote_pull"):V(V::Map{});
    const bool remote_first=dhvalue::optional<bool>(remote_cfg,"enabled",false)&&(!pair_scoped||(candidate_sync&&bool(overlay)));
    const bool general_pull=remote_first&&dhvalue::get<bool>(remote_cfg,"pull_enabled");
    // In continuous mode its explicit READ switch also governs replica roots.
    // Otherwise a disabled READ ablation would fall through to legacy tail
    // reads merely because an overlay is installed.
    const bool pull_enabled=!growth&&(remote_first?general_pull:bool(overlay)&&!pair_scoped)&&fetch&&region_tasks&&dhvalue::optional<bool>(pull_cfg,"enabled",false);
    const size_t switch_after=remote_first?dhvalue::get<size_t>(remote_cfg,"switch_after_batches"):0;
    RemotePullPolicy remote_policy;
    const int replica_source=overlay?int(dhvalue::has(state,"replica_fallback_source")?dhvalue::get<size_t>(state,"replica_fallback_source"):dhvalue::get<size_t>(state,"original_entry_node")):-1;
    bool pull_failed=false;size_t pull_rounds=0,pull_bytes=0;
    const V tail_policy=dhvalue::has(pull_cfg,"tail_policy")?dhvalue::at(pull_cfg,"tail_policy"):V(V::Map{});
    const bool tail_stop=!remote_first&&dhvalue::optional<bool>(tail_policy,"enabled",false);
    const V lazy_cfg=dhvalue::has(pull_cfg,"lazy_rerank")?dhvalue::at(pull_cfg,"lazy_rerank"):V(V::Map{});
    const bool lazy=!remote_first&&pull_enabled&&dhvalue::optional<bool>(lazy_cfg,"enabled",false);
    std::unordered_map<Id,Candidate> deferred;
    std::vector<Id> refine_ids;
    auto code_owner=frame?frame->codes:std::make_shared<ReplicaOverlay>();
    auto& pulled_codes=*code_owner;std::unordered_map<Id,std::vector<uint8_t>> pulled_records;
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
    Stats stats;V::Array pull_events,pull_gate_events,sync_events;std::string stop_reason="frontier_exhausted";
    if(logical){
      stats["region_exclusion_ids_inherited"]=excluded.size();
      frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){if(!excluded_id(c.id))return false;stats["region_excluded_candidates"]++;return true;}),frontier.end());
    }
    bool can_forward=true;const bool async=bool(read_batch);
    auto inbox=control->inbox();auto visits=control->regions();
    auto visit_snapshot=visits?visits->snapshot():std::shared_ptr<const RegionVisits::Snapshot>{};
    auto receive_candidates=[&](){
      if(!inbox)return;
      const auto wait_before=inbox->receive_wait_ns.load(),count_before=inbox->timed_consumed.load();
      for(const auto& row:inbox->take()){
        auto c=candidate(row);
        if(std::find(expanded.begin(),expanded.end(),c.id)!=expanded.end())continue;
        if(std::find_if(frontier.begin(),frontier.end(),[&](const auto& x){return x.id==c.id;})!=frontier.end())continue;
        c.queued=now_ns();seen.insert(c.id);frontier.push_back(std::move(c));stats["region_candidates_merged"]++;
      }
      if(candidate_sync){stats["candidate_receive_to_consume_ms"]+=(inbox->receive_wait_ns.load()-wait_before)/1e6;stats["candidate_timed_consumed"]+=inbox->timed_consumed.load()-count_before;}
      if(frontier.size()>frontier_limit){std::sort(frontier.begin(),frontier.end(),closer);stats["frontier_dropped"]+=frontier.size()-frontier_limit;frontier.resize(frontier_limit);}
    };
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
      if(region_tasks){native_report["region_task_mode"]=true;
        native_report["region_executor_id"]=dhvalue::at(state,"region_executor_id");
        native_report["region_merged_admission"]=dhvalue::at(state,"region_merged_admission");}
    }
    const int64_t started=frame?frame->started:now_ns();
    const auto owner_thread=static_cast<uint64_t>(syscall(SYS_gettid));
    auto owner=[&](Id id,const std::vector<int>& owners) {
      if(logical&&excluded_id(id))return -1;
      if(overlay&&overlay->contains(id))return sid_;
      if(growth){auto it=demand_pins.find(id);if(it!=demand_pins.end())return sid_;if(auto e=growth->acquire(id)){demand_pins.emplace(id,std::move(e));return sid_;}}
      if(std::find(owners.begin(),owners.end(),sid_)!=owners.end())return logical&&overlay&&!candidate_sync?(can_forward?local_base_group:-1):sid_;
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
      native_search_start_ns=frame?frame->search_start:now_ns();
      control->check();
      auto prep_start=now_ns();auto prepared=frame?std::move(frame->prepared):quantizer_->prepare_native(query.data());
      stats["rabitq_prepare_ms"]=ms_since(prep_start);
      if(frame) {
        deferred=std::move(frame->deferred);refine_ids=std::move(frame->refine_ids);stop_reason=frame->finish_reason;
        remaining=frame->remaining;slots=frame->slots;stagnation=frame->stagnation;fallback_used=frame->fallback_used;draw_index=frame->draw_index;
        remote_policy=std::move(frame->remote_policy);pull_rounds=frame->pull_rounds;pull_bytes=frame->pull_bytes;pull_failed=frame->pull_failed;can_forward=frame->can_forward;service_per_node=frame->service_per_node;
        frontier=std::move(frame->frontier);fallbacks=std::move(frame->fallbacks);paths=std::move(frame->paths);seen=std::move(frame->seen);
        results=std::move(frame->results);expanded=std::move(frame->expanded);decisions=std::move(frame->decisions);traces=std::move(frame->traces);stats=std::move(frame->stats);
        sync_events=std::move(frame->sync_events);
        pull_events=std::move(frame->pull_events);pull_gate_events=std::move(frame->pull_gate_events);native_children=std::move(frame->children);native_events=std::move(frame->events);pulled_records=std::move(frame->records);
        ++frame->resumes;if(frame->last_thread!=owner_thread)++frame->migrations;frame->last_thread=owner_thread;
        for(const char* key:{"pull_dispatch_queue_ms","pull_network_ms","pull_resume_queue_ms"})
          if(dhvalue::has(frame->reply,key))stats[key]+=dhvalue::get<double>(frame->reply,key);
        if(!sync_resume)stats[dhvalue::optional<bool>(frame->reply,"pull_resume_on_main",false)?"pull_main_resume_count":"pull_tail_resume_count"]++;
        else {stats["candidate_sync_resumes"]++;stats["candidate_sync_park_ms"]+=dhvalue::optional<double>(frame->reply,"park_ms",0.);stats["candidate_sync_resume_queue_ms"]+=dhvalue::optional<double>(frame->reply,"resume_queue_ms",0.);}
        const bool exclude_pause=dhvalue::has(pull_cfg,"async")&&dhvalue::optional<bool>(dhvalue::at(pull_cfg,"async"),"exclude_pause_from_age",false);
        if(exclude_pause&&frame->pull_suspend_ns) {
          const auto resumed=dhvalue::optional<int64_t>(frame->reply,"pull_resume_enter_ns",slice_start);
          const auto pause=std::max(int64_t(0),resumed-frame->pull_suspend_ns);
          // Only candidates already live before suspension are shifted. Inbox
          // arrivals and later discoveries receive fresh timestamps below.
          for(auto& c:frontier)c.queued+=pause;
          stats["pull_policy_pause_ms"]+=pause/1e6;
        }
      }
      auto checkpoint=[&](PullRequest& request,std::vector<Group> groups,Group action,int local,int64_t begin,bool refining,bool ending) {
        // This callback only inspects code directories while the query is
        // suspended. The fixed overlay and previous code rows stay pinned
        // through cancellation and I/O completion; no candidate mutation.
        if(dhvalue::has(pull_cfg,"one_sided")&&dhvalue::get<bool>(dhvalue::at(pull_cfg,"one_sided"),"enabled")&&
           (dhvalue::optional<bool>(dhvalue::at(pull_cfg,"one_sided"),"missing_codes_only",false)||
            dhvalue::optional<bool>(dhvalue::at(pull_cfg,"one_sided"),"measure_code_availability",false)))
          request.code_lookup=[this,overlay,code_owner,pinned](Id id)->unsigned{
            if(locate(id,true)!=missing_location)return 1;
            if(overlay&&overlay->locate(id,true)!=ReplicaOverlay::missing)return 2;
            if(code_owner->locate(id,true)!=ReplicaOverlay::missing)return 3;
            return 0;
          };
        if(!frame){frame=std::make_shared<LoopFrame>();frame->state=state;frame->config=config;frame->draws=draws;frame->started=started;frame->native_entry=native_entry_ns;frame->search_start=native_search_start_ns;frame->first_thread=frame->last_thread=owner_thread;}
        frame->sync_last=sync_last;frame->sync_mode.clear();
        frame->remaining=remaining;frame->slots=slots;frame->stagnation=stagnation;frame->fallback_used=fallback_used;frame->draw_index=draw_index;
        frame->remote_policy=remote_policy;frame->pull_rounds=pull_rounds;frame->pull_bytes=pull_bytes;frame->pull_failed=pull_failed;frame->can_forward=can_forward;frame->service_per_node=service_per_node;
        frame->frontier=std::move(frontier);frame->fallbacks=std::move(fallbacks);frame->paths=std::move(paths);frame->seen=std::move(seen);
        frame->results=std::move(results);frame->expanded=std::move(expanded);frame->decisions=std::move(decisions);frame->traces=std::move(traces);frame->stats=std::move(stats);
        frame->sync_events=std::move(sync_events);
        frame->pull_events=std::move(pull_events);frame->pull_gate_events=std::move(pull_gate_events);frame->children=std::move(native_children);frame->events=std::move(native_events);
        frame->codes=code_owner;frame->records=std::move(pulled_records);frame->prepared=std::move(prepared);frame->groups=std::move(groups);frame->action=std::move(action);frame->local=local;frame->pull_begin=begin;
        frame->cpu_total+=cpu_ms()-slice_cpu;frame->slot_wall+=ms_since(slice_start);
        request.resume=[this,frame,control,spawn,completion,pinned,fetch](V reply){frame->reply=std::move(reply);run_value(frame->state,frame->config,frame->draws,control,{},[]{},spawn,completion,pinned,fetch,frame);};
        frame->pull_suspend_ns=now_ns();
        frame->refining=refining;frame->finishing=ending;frame->refine_ids=refine_ids;frame->deferred=std::move(deferred);frame->finish_reason=stop_reason;
        request.mode=refining?"vectors":lazy?"adjacency":"full";
      };
      auto pause_sync=[&](const std::string& kind,V request,const std::vector<Group>& gs,Group act,int loc) {
        PullRequest parked;checkpoint(parked,gs,std::move(act),loc,now_ns(),false,false);
        frame->sync_mode=kind;frame->finishing=false;
        CandidatePause pause;pause.request=std::move(request);pause.request.map()["kind"]=kind;
        pause.resume=[this,frame,control,spawn,completion,pinned,fetch](V reply){
          frame->finishing=dhvalue::optional<bool>(reply,"commit",false);frame->reply=std::move(reply);
          run_value(frame->state,frame->config,frame->draws,control,{},[]{},spawn,completion,pinned,fetch,frame);
        };throw pause;
      };
      auto send_existing=[&](bool force) {
        if(!candidate_sync||!control->candidate_send)return;
        if(!force&&expanded.size()-sync_last<sync_interval)return;
        sync_last=expanded.size();if(visits)visit_snapshot=visits->snapshot();
        std::map<int,V::Array> batches;
        const auto exhausted=control->candidate_exhausted_targets?control->candidate_exhausted_targets():std::set<int>{};
        double bound=std::numeric_limits<double>::infinity();
        if(frontier.size()>=frontier_limit&&!frontier.empty())bound=std::max_element(frontier.begin(),frontier.end(),closer)->distance;
        for(auto it=frontier.begin();it!=frontier.end();) {
          int n=owner(it->id,it->owners);
          if(n>=0&&n!=sid_&&sync_target(size_t(n))&&visit_snapshot&&(visit_snapshot->opened.count(sync_region(n))||visit_snapshot->pending.count(sync_region(n)))){
            if(exhausted.count(n))stats["candidate_sync_known_exhausted_skipped"]++;else batches[n].push_back(row(*it));it=frontier.erase(it);
          }else ++it;
        }
        for(auto& [n,rows]:batches){stats["candidate_sync_candidates_sent"]+=rows.size();stats["candidate_sync_batches"]++;
          control->candidate_send(n,V::Map{{"rows",std::move(rows)},{"admission_bound",std::isfinite(bound)?V(bound):V()},
            {"bound_valid",true},{"expansions",uint64_t(expanded.size())}});}
      };
      if(sync_resume){
        if(dhvalue::optional<bool>(frame->reply,"budget_terminal",false))stats["candidate_budget_terminal"]++;
        if(lifecycle)sync_events.emplace_back(V::Map{{"kind",frame->sync_mode},{"parked_ns",frame->pull_suspend_ns},{"ready_ns",dhvalue::at(frame->reply,"candidate_ready_ns")},{"resume_ns",slice_start},{"commit",finishing}});
        if(dhvalue::has(frame->reply,"error"))throw std::runtime_error(dhvalue::get<std::string>(frame->reply,"error"));
        if(frame->sync_mode=="claim"){
          const bool won=dhvalue::get<bool>(frame->reply,"claimed");
          if(won)claimed_branch=dhvalue::get<std::string>(frame->reply,"branch_id");
          else {visits->open(dhregion::base(frame->action.node));resume_action=false;resume_dispatch=false;send_existing(true);}
        }
      }
      auto install_refine=[&](V reply) {
        require(!dhvalue::has(reply,"pull_error"),"Deferred exact vector read failed");
        require(dhvalue::get<bool>(reply,"accepted")&&dhvalue::get<std::string>(reply,"epoch")==epoch_&&
          dhvalue::get<std::vector<Id>>(reply,"ids")==refine_ids&&dhvalue::get<std::string>(reply,"record_mode")=="vectors","Deferred vector identity mismatch");
        const auto start=now_ns();const double cpu=cpu_ms();
        const auto stride=dhvalue::get<size_t>(reply,"record_stride");const auto& data=std::get<V::Bytes>(dhvalue::at(reply,"records").data);
        require(stride==overlay->stride&&data.size()==refine_ids.size()*stride,"Deferred vector layout mismatch");
        pull_bytes+=refine_ids.size()*dim_*4;require(pull_bytes<=dhvalue::get<size_t>(pull_cfg,"max_bytes"),"Deferred vector byte limit");
        for(size_t i=0;i<refine_ids.size();++i){const auto id=refine_ids[i];
          const double distance=exact_squared_l2(query.data(),data.data()+i*stride,dim_);
          if(!(exclude_self&&query_id&&id==*query_id))results[id]=distance;paths.push_back(deferred.at(id));deferred.erase(id);
        }
        stats["pull_record_bytes"]+=refine_ids.size()*dim_*4;
        stats["lazy_refined_vectors"]+=refine_ids.size();stats["lazy_refine_batches"]++;
        stats["exact_distance_evaluations"]+=refine_ids.size();stats["lazy_refine_cpu_ms"]+=cpu_ms()-cpu;
        stats["exact_distance_ms"]+=ms_since(start);stats["pull_wire_bytes"]+=dhvalue::get<size_t>(reply,"wire_bytes");
        for(const auto& [key,value]:reply.map())if(key.rfind("one_sided_",0)==0&&key.substr(key.size()-3)!="_ns")
          if(std::holds_alternative<double>(value.data)||std::holds_alternative<uint64_t>(value.data))stats[key]+=dhvalue::cast<double>(value);
        refine_ids.clear();
      };
      if(resume_refine){install_refine(std::move(frame->reply));stats["pull_wait_ms"]+=ms_since(frame->pull_begin);stats["lazy_refine_wait_ms"]+=ms_since(frame->pull_begin);resume_refine=false;}
      auto refine=[&](const std::vector<Group>& groups,const Group& action,int local,bool ending) {
        while(!deferred.empty()) {
          const auto cap=dhvalue::get<size_t>(lazy_cfg,"max_vectors");
          if(size_t(stats["lazy_refined_vectors"])>=cap){stats["lazy_unrefined_vertices"]+=deferred.size();deferred.clear();break;}
          std::vector<Candidate> selected;for(const auto& [id,c]:deferred)selected.push_back(c);
          const double w=dhvalue::get<double>(lazy_cfg,"uncertainty_weight");
          std::sort(selected.begin(),selected.end(),[&](const auto& a,const auto& b){const double x=a.distance-w*a.uncertainty,y=b.distance-w*b.uncertainty;return x<y||(x==y&&a.id<b.id);});
          if(results.size()>=topk&&selected.front().distance-w*selected.front().uncertainty>=threshold()){
            stats["lazy_unrefined_vertices"]+=deferred.size();deferred.clear();break;
          }
          const auto count=std::min({selected.size(),cap-size_t(stats["lazy_refined_vectors"]),dhvalue::get<size_t>(pull_cfg,"batch_records")});
          refine_ids.clear();for(size_t i=0;i<count;++i)refine_ids.push_back(selected[i].id);
          const auto begin=now_ns();
          try{auto reply=fetch(replica_source,refine_ids);install_refine(std::move(reply));stats["pull_wait_ms"]+=ms_since(begin);}
          catch(PullRequest& request){send_existing(true);checkpoint(request,groups,action,local,begin,true,ending);throw;}
        }
      };
      auto settle_starts=[&]{
        if(!nonblocking_start||!control->candidate_settlements)return;
        auto settled=control->candidate_settlements();pending_starts=dhvalue::get<size_t>(settled,"pending");
        for(const auto& reply:std::get<V::Array>(dhvalue::at(settled,"rows").data)){
          const auto bid=dhvalue::get<std::string>(reply,"branch_id");
          if(!dhvalue::get<bool>(reply,"accepted")){
            auto it=std::find_if(native_children.begin(),native_children.end(),[&](const V& x){return dhvalue::get<std::string>(x,"branch_id")==bid;});
            require(it!=native_children.end(),"Unknown pending branch refund");
            remaining+=dhvalue::get<size_t>(*it,"work_grant");slots+=1+dhvalue::get<size_t>(*it,"branch_slots");
            stats["candidate_start_refunded_budget"]+=dhvalue::get<size_t>(*it,"work_grant");native_children.erase(it);
            native_events.erase(std::remove_if(native_events.begin(),native_events.end(),[&](const V& x){return dhvalue::get<std::string>(x,"child_branch_id")==bid;}),native_events.end());
            stats["candidate_start_conflicts"]++;
          }else stats["candidate_start_accepted"]++;
        }
      };
      std::vector<char> raw(async?0:beam*record_stride);
      while(!finishing) {
        if(growth&&!resume_action){demand_pins.clear();if(frame)frame->demand_batch.clear();}
        settle_starts();if(!remaining)break;
        control->check();
        std::vector<Group> groups;int local=-1;Group action;Decision decision;std::string decision_id;
        if(resume_action){groups=std::move(frame->groups);local=frame->local;action=std::move(frame->action);decision=decisions.back();decision_id=branch_id+":"+std::to_string(decisions.size()-1);}
        else {
        receive_candidates();if(visits)visit_snapshot=visits->snapshot();
        send_existing(false);
        if(tail_stop&&stats.count("tail_start_expanded")) {
          const auto done=expanded.size()-size_t(stats.at("tail_start_expanded"));
          stats["tail_policy_expanded"]=done;
          bool stop=done>=dhvalue::get<size_t>(tail_policy,"max_expansions");
          const auto stable=dhvalue::get<size_t>(tail_policy,"stable_batches");
          if(!stop&&stable&&done>=dhvalue::get<size_t>(tail_policy,"min_expansions")&&stagnation>=stable&&results.size()>=topk&&!frontier.empty()) {
            double best=std::numeric_limits<double>::infinity();
            for(const auto& c:frontier)best=std::min(best,c.distance-dhvalue::get<double>(tail_policy,"uncertainty_weight")*c.uncertainty);
            stop=best>=threshold()-dhvalue::get<double>(tail_policy,"improvement_margin")*scale;
            if(stop)stats["tail_policy_gain_stops"]++;
          }
          if(stop){stats["tail_policy_stops"]++;stats["tail_policy_unused_credit"]+=remaining;stop_reason="tail_policy_stop";break;}
        }
        if(frontier.empty()&&!fallback())break;
        const auto now=now_ns();
        for(auto it=frontier.begin();it!=frontier.end();) {
          const int target=owner(it->id,it->owners);
          if(target<0){stats[dropped()]++;it=frontier.erase(it);continue;}
          const size_t physical_target=target==local_base_group?sid_:size_t(target);
          if(region_tasks&&target!=sid_&&visit_snapshot&&!visit_snapshot->permits(physical_target,dhregion::current(state))){
            ++it;stats["source_reserved_candidate_checks"]++;continue;
          }
          if(candidate_sync&&target!=sid_&&sync_target(physical_target)&&visit_snapshot&&(visit_snapshot->opened.count(sync_region(physical_target))||visit_snapshot->pending.count(sync_region(physical_target)))){++it;continue;}
          it->age=std::max(0.0,(now-it->queued)/1e6);
          auto g=std::find_if(groups.begin(),groups.end(),[&](const Group& group){return group.node==target;});
          if(g==groups.end()){groups.push_back(Group{target,{}});g=groups.end()-1;}
          g->candidates.push_back(*it);++it;
        }
        if(candidate_sync&&groups.empty()&&!frontier.empty()){send_existing(true);receive_candidates();if(frontier.empty())continue;}
        if(groups.empty()){if(region_tasks&&!frontier.empty()){stop_reason="source_reserved";break;}continue;}
        size_t remote_count=0;for(auto& g:groups)if(g.node!=sid_)remote_count+=g.candidates.size();
        std::vector<int> eligible,remotes;
        for(size_t i=0;i<groups.size();++i) {
          auto& g=groups[i];std::sort(g.candidates.begin(),g.candidates.end(),closer);
          g.count=std::min({g.candidates.size(),remaining,g.node==sid_?beam:remote_batch});
          if(tail_stop&&stats.count("tail_start_expanded"))g.count=std::min(g.count,dhvalue::get<size_t>(tail_policy,"max_expansions")-(expanded.size()-size_t(stats.at("tail_start_expanded"))));
          g.cost=service_per_node*g.count;
          if(g.node!=sid_)g.cost+=rtt+(bytes.size()+seen.size()*8+remote_count*96)/bandwidth;
          g.cost=std::max(g.cost,1e-9);
          for(size_t j=0;j<g.count;++j)g.wait=std::max(g.wait,g.candidates[j].age);
          if(g.node==sid_)local=i;
          if(g.node==sid_||slots||(general_pull&&(!pair_scoped||g.node==replica_source)&&!remote_policy.switch_to_push(g.node,switch_after))){eligible.push_back(i);if(g.node!=sid_)remotes.push_back(i);}
        }
        // A replica's source competes with other remote actions through the
        // regular expansion policy; there is no source-specific residence gate.
        if(eligible.empty()){stop_reason="branch_slots_exhausted";break;}
        int chosen=local;decision.reason="local_only";
        bool dwell_blocked=!remote_first&&!remotes.empty()&&expanded.size()<min_dwell;
        if(dwell_blocked&&local<0&&dwell_escape){stats["cross_dwell_exhaustion_escapes"]++;dwell_blocked=false;}
        if(dwell_blocked){stats["cross_dwell_blocked_decisions"]++;decision.reason="minimum_local_expansions";
          if(local<0){stop_reason="minimum_local_expansions_unmet";break;}}
        bool pair_distance=false;if(remote_first&&pair_scoped){int best=eligible.front();for(int i:eligible)if(closer(groups[i].candidates[0],groups[best].candidates[0]))best=i;pair_distance=groups[best].node==replica_source;}
        if(remote_first&&(!pair_scoped||pair_distance)) {
          chosen=eligible.front();for(int i:eligible)if(closer(groups[i].candidates[0],groups[chosen].candidates[0]))chosen=i;
          decision.reason="distance_first";decision.probability=groups[chosen].node!=sid_;
          // Keep batching, but never skip a better candidate merely because it
          // resides in another group. All distances use the same RaBitQ order.
          auto& best=groups[chosen];
          for(int i:eligible)if(i!=chosen)
            while(best.count>1&&closer(groups[i].candidates[0],best.candidates[best.count-1]))--best.count;
        } else if(!dwell_blocked&&!remotes.empty() && !(local>=0&&remaining<2)) {
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
              else {if(draw_index>=draws.size())stats["decision_random_extended_draws"]++;decision.draw=policy_random.at(draw_index++);remote=*decision.draw<decision.probability;decision.reason="probability_draw";}
              chosen=remote?best:local;
            }
          }
        }
        if(chosen<0)break;
        action=groups[chosen];decision.target=action.node;decision.remote=action.node!=sid_;decision.wait=action.wait;
        decision_id=branch_id+":"+std::to_string(decisions.size());decisions.push_back(decision);
        }
        const size_t decision_remaining=remaining;
        if(growth&&!resume_dispatch&&action.node==replica_source){
          if(resume_pull){
            stats["demand_wait_ms"]+=ms_since(frame->pull_begin);
            if(!dhvalue::has(frame->reply,"pull_error")&&dhvalue::optional<bool>(frame->reply,"accepted",false)){
              require(frame->demand_batch.size()==dhvalue::get<std::vector<Id>>(frame->reply,"ids").size(),"Demand completion count mismatch");
              for(auto& e:frame->demand_batch)demand_pins[e->id]=e;
              action.node=sid_;action.count=frame->demand_batch.size();stats["demand_resume_batches"]++;
            }else{stats["demand_fallbacks"]++;pull_failed=true;}
            resume_pull=false;
          }else if(!pull_failed&&growth->learning()){
            auto snap=visits?visits->snapshot():std::shared_ptr<const RegionVisits::Snapshot>{};
            const auto source_region=dhregion::base(replica_source);
            if(!snap||(!snap->opened.count(source_region)&&!snap->pending.count(source_region))){
              const auto& gcfg=dhvalue::at(peer,"demand_growth");size_t n=std::min(action.count,dhvalue::get<size_t>(gcfg,"read_batch_records"));
              std::vector<Id> ids;std::vector<std::vector<Id>> proofs;
              for(size_t i=0;i<n;++i){const auto& c=action.candidates[i];ids.push_back(c.id);std::vector<Id> path{c.id};auto parent=c.parent;
                while(parent&&path.size()<dhvalue::get<size_t>(gcfg,"max_path_records")){
                  if(std::find(path.begin(),path.end(),*parent)!=path.end())break;path.push_back(*parent);
                  if(overlay->contains(*parent)||growth->contains_durable(*parent))break;
                  auto it=std::find_if(paths.begin(),paths.end(),[&](const Candidate& x){return x.id==*parent;});
                  if(it==paths.end()){parent.reset();break;}parent=it->parent;
                }
                if(!overlay->contains(path.back())&&!growth->contains_durable(path.back()))path.clear();else std::reverse(path.begin(),path.end());proofs.push_back(std::move(path));
              }
              const auto begin=now_ns();
              try{if(!growth->can_request(ids)){stats["demand_fast_rejections"]++;throw std::runtime_error("Demand cache full");}fetch(replica_source,ids);throw std::runtime_error("Demand READ must suspend");}
              catch(PullRequest& request){
                demand_pins.clear();checkpoint(request,std::move(groups),std::move(action),local,begin,false,false);
                auto lookup=[this,weak=std::weak_ptr<ReplicaOverlay>(overlay)](Id id)->unsigned{if(locate(id,true)!=missing_location)return 1;auto core=weak.lock();if(core&&core->locate(id,true)!=ReplicaOverlay::missing)return 2;return 0;};
                request.demand_start=[growth,ids,proofs,frame,lookup](dhgrowth::Read read,std::function<void(V)> done){
                  growth->request(ids,proofs,std::move(read),lookup,[frame,ids,done=std::move(done)](dhgrowth::Entries entries,std::string error)mutable{
                    frame->demand_batch=std::move(entries);V reply=V::Map{{"accepted",error.empty()},{"ids",dhvalue::vector(ids)},{"wire_bytes",uint64_t(0)},{"one_sided",true},{"demand_shared",true}};
                    if(!error.empty())reply.map()["pull_error"]=error;done(std::move(reply));
                  });
                };throw;
              }catch(const std::exception&){stats["demand_fallbacks"]++;pull_failed=true;}
            }else stats["demand_source_already_started"]++;
          }
        }
        bool pulled=false;
        const int pull_source=general_pull&&!pair_scoped?action.node:replica_source;
        const bool general_action=general_pull&&action.node!=sid_&&(!pair_scoped||action.node==replica_source);
        const bool sequence_push=general_action&&remote_policy.switch_to_push(action.node,switch_after);
        if(general_action&&!resume_pull){
          stats["remote_first_actions"]++;
          if(sequence_push)stats[remote_policy.disabled.count(action.node)?"remote_pull_disabled_pushes":"remote_pull_streak_pushes"]++;
        }
        // This observation follows graph-policy approval. Child credit is split
        // later and must not be mistaken for credit at the pull gate.
        if(!remote_first&&!resume_dispatch&&action.node==replica_source&&!resume_pull) {
          std::string reason="attempt";
          if(!pull_enabled)reason="disabled";
          else if(pull_failed)reason="prior_failure";
          else if(remaining>dhvalue::get<size_t>(pull_cfg,"max_remaining"))reason="remaining";
          else if(pull_rounds>=dhvalue::get<size_t>(pull_cfg,"max_rounds"))reason="rounds";
          else if(pull_bytes+(lazy?dhvalue::get<size_t>(lazy_cfg,"max_vectors")*dim_*4:0)+dhvalue::get<size_t>(pull_cfg,"batch_bytes")>dhvalue::get<size_t>(pull_cfg,"max_bytes"))reason="bytes";
          stats["pull_gate_"+reason]++;
          pull_gate_events.emplace_back(V::Map{{"decision_id",decision_id},{"remaining_before",uint64_t(remaining)},
            {"local_expanded_before",uint64_t(expanded.size())},{"reason",reason},
            {"graph_reason",decision.reason},{"rounds_before",uint64_t(pull_rounds)},
            {"bytes_before",uint64_t(pull_bytes)},{"local_candidates",uint64_t(local>=0?groups[local].candidates.size():0)},
            {"source_candidates",uint64_t(action.candidates.size())}});
        }
        // The graph policy already approved this remote action. A bounded data
        // access alternative changes the compute location, not search credit.
        if(!resume_dispatch&&action.node==pull_source&&action.node!=sid_&&pull_enabled&&(!general_pull?!pull_failed:!sequence_push)){
          if(!resume_pull)stats["pull_approved_actions"]++;
          if(resume_pull||((general_pull||remaining<=dhvalue::get<size_t>(pull_cfg,"max_remaining"))&&pull_rounds<dhvalue::get<size_t>(pull_cfg,"max_rounds")&&
             pull_bytes+(lazy?dhvalue::get<size_t>(lazy_cfg,"max_vectors")*dim_*4:0)+dhvalue::get<size_t>(pull_cfg,"batch_bytes")<=dhvalue::get<size_t>(pull_cfg,"max_bytes"))){
            const size_t n=std::min({action.count,dhvalue::get<size_t>(pull_cfg,"batch_records"),tail_stop&&!stats.count("tail_start_expanded")?dhvalue::get<size_t>(tail_policy,"max_expansions"):action.count});std::vector<Id> ids;
            for(size_t i=0;i<n;++i)ids.push_back(action.candidates[i].id);
            const auto begin=resume_pull?frame->pull_begin:now_ns();if(!resume_pull)++pull_rounds;std::string pull_error;
            V::Map pull_timing;
            if(resume_pull&&lifecycle)for(const auto* key:{"pull_queued_ns","pull_sent_ns","pull_ready_ns","pull_resume_enter_ns","pull_resume_on_main"})
              if(dhvalue::has(frame->reply,key))pull_timing[key]=dhvalue::at(frame->reply,key);
            try{
              V reply=resume_pull?std::move(frame->reply):fetch(pull_source,ids);
              if(dhvalue::has(reply,"pull_error"))throw std::runtime_error(dhvalue::get<std::string>(reply,"pull_error"));
              stats["pull_wire_bytes"]+=dhvalue::get<size_t>(reply,"wire_bytes");
              if(!dhvalue::get<bool>(reply,"accepted"))throw std::runtime_error("Pull source active");
              require(dhvalue::get<std::string>(reply,"epoch")==epoch_&&dhvalue::get<std::vector<Id>>(reply,"ids")==ids,"Pull record identity mismatch");
              const auto install_start=now_ns();const auto install_cpu=cpu_ms();
              const auto stride=dhvalue::get<size_t>(reply,"record_stride");const auto& data=std::get<V::Bytes>(dhvalue::at(reply,"records").data);const auto& codes=std::get<V::Bytes>(dhvalue::at(reply,"codes").data);
              require(stride==(overlay?overlay->stride:stride_)&&data.size()==ids.size()*stride,"Pull record layout mismatch");
              const size_t width=8+code_bytes_+extra_bytes_+12+4+node_count_;
              require(codes.size()%width==0&&data.size()+codes.size()<=dhvalue::get<size_t>(pull_cfg,"batch_bytes"),"Pull dependency layout/bound mismatch");
              const bool adjacency=dhvalue::optional<std::string>(reply,"record_mode","full")=="adjacency";
              require(!lazy||adjacency,"Lazy navigation requires adjacency-only data");
              pull_bytes+=(adjacency?ids.size()*(stride-dim_*4):data.size())+codes.size();
              for(size_t i=0;i<ids.size();++i)pulled_records[ids[i]]=std::vector<uint8_t>(data.begin()+i*stride,data.begin()+(i+1)*stride);
              for(size_t off=0;off<codes.size();off+=width){const auto* src=codes.data()+off;Id id;std::memcpy(&id,src,8);src+=8;
                if(pulled_codes.locations.count(id))continue;size_t row=pulled_codes.locations.size();pulled_codes.locations[id]=row;
                pulled_codes.codes.insert(pulled_codes.codes.end(),src,src+code_bytes_);src+=code_bytes_;
                pulled_codes.extra.insert(pulled_codes.extra.end(),src,src+extra_bytes_);src+=extra_bytes_;
                for(size_t j=0;j<3;++j){float v;std::memcpy(&v,src,4);src+=4;require(std::isfinite(v),"Nonfinite pull factor");pulled_codes.factors.push_back(v);}
                uint32_t center;std::memcpy(&center,src,4);src+=4;pulled_codes.centers.push_back(center);
                std::vector<int> owners;for(size_t j=0;j<node_count_;++j)if(src[j])owners.push_back(int(j));require(!owners.empty(),"Missing pull ownership");pulled_codes.owners.push_back(std::move(owners));
              }
              stats["pull_source_queue_ms"]+=dhvalue::optional<double>(reply,"source_queue_ms",0.);stats["pull_source_cpu_ms"]+=dhvalue::get<double>(reply,"source_cpu_ms");stats["pull_source_read_ms"]+=dhvalue::get<double>(reply,"source_read_ms");
              stats["pull_install_ms"]+=ms_since(install_start);stats["pull_install_cpu_ms"]+=cpu_ms()-install_cpu;
              for(const auto& [key,value]:reply.map())if(key.rfind("one_sided_",0)==0&&key.size()>3&&key.substr(key.size()-3)!="_ns")
                if(std::holds_alternative<double>(value.data)||std::holds_alternative<uint64_t>(value.data))stats[key]+=dhvalue::cast<double>(value);
              if(lifecycle)for(const char* key:{"one_sided_submit_ns","one_sided_assigned_ns","one_sided_records_ready_ns","one_sided_codes_ready_ns","one_sided_result_ready_ns","one_sided_auth_begin_ns","one_sided_auth_ready_ns"})
                if(dhvalue::has(reply,key))pull_timing[key]=dhvalue::at(reply,key);
              stats["pull_source_service_ms"]+=dhvalue::get<double>(reply,"source_service_ms");stats["pull_record_bytes"]+=adjacency?ids.size()*(stride-dim_*4):data.size();stats["pull_code_bytes"]+=codes.size();
              if(stats["pull_batches"]==0&&dhvalue::optional<bool>(pull_cfg,"retry_pruned_candidates",false)) {
                // A regular source handoff rebuilds discovery from expanded
                // IDs and its live frontier. Permit only previously pruned,
                // unexpanded candidates to be rediscovered at tail entry.
                const auto before=seen.size();seen.clear();
                seen.insert(ancestor_expanded.begin(),ancestor_expanded.end());
                seen.insert(expanded.begin(),expanded.end());
                for(const auto& candidate:frontier)seen.insert(candidate.id);
                stats["pull_seen_reopened"]+=before>seen.size()?before-seen.size():0;
                stats["pull_seen_resets"]++;
              }
              if(tail_stop&&!stats.count("tail_start_expanded"))stats["tail_start_expanded"]=expanded.size();
              stats["pull_batches"]++;stats["pull_records"]+=ids.size();
              if(general_pull){remote_policy.success(pull_source);stats["remote_pull_max_streak"]=std::max(stats["remote_pull_max_streak"],double(remote_policy.consecutive));}
              action.node=sid_;action.count=n;pulled=true;
            }catch(PullRequest& request){
              send_existing(true);checkpoint(request,std::move(groups),std::move(action),local,begin,false,false);
              throw;
            }catch(const std::exception& e){if(general_pull)remote_policy.failure(pull_source);else pull_failed=true;stats["pull_fallbacks"]++;pull_error=e.what();}
            pull_events.emplace_back(V::Map{{"remaining_before",uint64_t(remaining)},{"source",int64_t(pull_source)},
              {"local_expanded_before",uint64_t(expanded.size())},{"records",uint64_t(ids.size())},{"success",pulled},
              {"wait_ms",ms_since(begin)},{"start_ns",begin},{"error",pull_error}});
            if(lifecycle){pull_timing["install_done_ns"]=now_ns();for(auto& [key,value]:pull_timing)pull_events.back().map()[key]=std::move(value);}
            stats["pull_wait_ms"]+=ms_since(begin);control->check();
          }else stats["pull_limit_rejections"]++;
        }
        resume_action=false;resume_pull=false;resume_dispatch=false;
        if(action.node!=sid_) {
          if(remote_first&&(!pair_scoped||action.node==replica_source)){stats["remote_first_pushes"]++;remote_policy.failure(action.node);
            if(!slots){stop_reason="branch_slots_exhausted";break;}}
          if(lazy&&!deferred.empty())refine(groups,action,local,false);
          const int destination=action.node==local_base_group?sid_:action.node;
          if(candidate_sync&&sync_target(size_t(destination))&&!nonblocking_start&&claimed_branch.empty()) {
            send_existing(true);
            pause_sync("claim",V::Map{{"target",int64_t(destination)},{"claim_id",decision_id}},groups,action,local);
          }
          bool other=false;for(auto& g:groups)other|=g.node!=action.node;
          size_t grant=other?std::max(size_t(1),size_t(remaining*child_fraction)):remaining;
          if(local>=0&&remaining>1)grant=std::min(grant,remaining-1);
          require(grant>0&&slots>0,"Invalid native child grant");
          const size_t child_slots=region_tasks?size_t((slots-1)*dhvalue::optional<double>(peer,"fanout_child_slot_fraction",.5)):slots-1;
          slots-=1+child_slots;remaining-=grant;
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
          for(auto& c:frontier)if(owner(c.id,c.owners)!=sid_&&owner(c.id,c.owners)>=0&&(!region_tasks||owner(c.id,c.owners)==action.node))carried.push_back(c);
          if(region_tasks){
            if(carried.size()+seeds.size()>frontier_limit)carried.resize(frontier_limit-seeds.size());
            for(const auto& c:carried)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const auto& x){return x.id==c.id;}),frontier.end());
          }
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
            if(logical&&overlay&&!region_tasks){
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
            if(candidate_sync&&sync_target(size_t(destination))){payload.map()["claimed_branch_id"]=claimed_branch;claimed_branch.clear();}
            V descriptor=spawn(std::move(payload));
            native_events.emplace_back(V::Map{{"decision_id",V(decision_id)},{"target",V(int64_t(replica?replica->target:destination))},
              {"child_branch_id",dhvalue::at(descriptor,"branch_id")},{"work_grant",V(uint64_t(grant))},
              {"remaining_before",V(uint64_t(decision_remaining))},{"remaining_after",V(uint64_t(remaining))},
              {"local_expansions_before",V(uint64_t(expanded.size()))},{"candidate_count",V(uint64_t(seeds.size()))},
              {"carried_candidate_count",V(uint64_t(carried.size()))},{"elapsed_ms",V(ms_since(started))},
              {"dispatch_submitted_ns",V(now_ns())},{"search_elapsed_ms",V(ms_since(native_search_start_ns))}});
            native_children.push_back(std::move(descriptor));
          }
          can_forward=region_tasks;continue;
        }
        std::vector<Candidate> selected(action.candidates.begin(),action.candidates.begin()+action.count);
        std::vector<Id> batch;for(auto& c:selected)batch.push_back(c.id);
        for(auto id:batch)frontier.erase(std::remove_if(frontier.begin(),frontier.end(),[&](const Candidate& c){return c.id==id;}),frontier.end());
        batch.erase(std::remove_if(batch.begin(),batch.end(),[&](Id id){
          if(control->claim(id))return false;
          stats["duplicate_local_expansions_avoided"]++;return true;
        }),batch.end());
        if(batch.empty())continue;
        if(remote_first&&!pulled)remote_policy.local();
        if(!async&&raw.size()<batch.size()*record_stride)raw.resize(batch.size()*record_stride);
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
          std::unordered_map<Id,std::shared_ptr<ReplicaOverlay>> demand_banks;
          std::vector<std::pair<Id,double>> exact;
          for(size_t i=0;i<batch.size();++i) {
            control->check();const Id id=batch[i];selected_by_id.emplace(id,selected[i]);
            char* data=raw.data()+i*record_stride;
            if(growth&&demand_pins.count(id)){
              auto e=demand_pins.at(id);const auto t=now_ns();growth->read(e,data,record_stride);demand_banks.emplace(id,e->codes);
              if(e->file){work["pread_ms"]+=ms_since(t);work["pread_calls"]++;work["file_read_bytes"]+=record_stride;stats["demand_durable_expansions"]++;}
              else stats["demand_cache_expansions"]++;
            }
            else if(pulled){auto it=pulled_records.find(id);require(it!=pulled_records.end(),"Missing pulled record");std::memcpy(data,it->second.data(),it->second.size());stats["pull_expanded"]++;}
            else if(!async) {
              size_t location=locate(id,false),read_stride=stride_,read_header=header_;int read_fd=fd_;
              if((location==missing_location||logical)&&overlay){auto pos=overlay->locate(id);
                if(pos!=ReplicaOverlay::missing){location=pos;read_stride=overlay->stride;read_header=overlay->header;read_fd=overlay->fd;stats["replica_record_reads"]++;}}
              if(overlay)stats[read_fd==overlay->fd?"replica_overlay_expansions":"replica_host_base_expansions"]++;
              require(location!=missing_location,"Native expansion of nonlocal vertex");
              const auto t=now_ns();ssize_t count;
              do {count=::pread(read_fd,data,read_stride,read_header+location*read_stride);}while(count<0&&errno==EINTR);
              const auto end=now_ns();work["pread_ms"]+=(end-t)/1e6;work["pread_calls"]++;
              if(count<0)throw std::runtime_error(std::string("native pread: ")+strerror(errno));
              require(size_t(count)==read_stride,"Short native node read");work["file_read_bytes"]+=read_stride;
              if(detailed)trace.spans.emplace_back("pread",t,end);
            }
            if(lazy&&pulled){deferred[id]=selected[i];stats["lazy_navigation_vertices"]++;}
            else {
            const auto t=now_ns();const double distance=exact_squared_l2(query.data(),data,dim_);
            exact.emplace_back(id,distance);const auto end=now_ns();work["exact_distance_ms"]+=(end-t)/1e6;
            work["exact_distance_evaluations"]++;if(detailed)trace.spans.emplace_back("exact_distance",t,end);
            }
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
            else {const auto* owner=overlay?overlay.get():&pulled_codes;size_t r=owner->locate(id,true);if(r==ReplicaOverlay::missing&&demand_banks.count(parents.at(id))){owner=demand_banks.at(parents.at(id)).get();r=owner->locate(id,true);}if(r==ReplicaOverlay::missing){owner=&pulled_codes;r=owner->locate(id,true);}require(r!=ReplicaOverlay::missing,"Missing replica/pull neighbor code");
              value=quantizer_->score_native(prepared,owner->codes.data()+r*code_bytes_,extra_bytes_?owner->extra.data()+r*extra_bytes_:nullptr,owner->factors.data()+3*r,owner->centers[r]);}
            require(std::isfinite(value.first)&&std::isfinite(value.second),"Nonfinite native estimate");estimates.push_back(value);
          }
          const auto quant_end=now_ns();work["rabitq_ms"]=(quant_end-quant_start)/1e6;work["rabitq_evaluations"]=neighbors.size();
          if(detailed)trace.spans.emplace_back("rabitq",quant_start,quant_end);
          trace.end=now_ns();trace.cpu=cpu_ms()-service_cpu;work["service_ms"]=(trace.end-trace.start)/1e6;
          work["server_thread_cpu_ms"]=trace.cpu;
          if(held){std::vector<char>().swap(raw);release_batch();held=false;}
          control->check();remaining-=batch.size();expanded.insert(expanded.end(),batch.begin(),batch.end());stats["batches"]++;
          if(growth)for(auto id:batch)if(demand_pins.count(id))growth->expanded(demand_pins.at(id));
          if(heat_lane){const auto t=now_ns();heat_lane->record(batch,uint64_t(t));stats["hotness_record_ms"]+=ms_since(t);stats["hotness_expansions_recorded"]+=batch.size();}
          for(auto [name,value]:work)if(name!="server_thread_cpu_ms")stats[name]+=value;
          const double measured=(work["service_ms"]+work["io_batch_wall_ms"])/batch.size();
          service_per_node=(1-ewma)*service_per_node+ewma*std::max(measured,1e-9);
          for(auto [id,distance]:exact) {paths.push_back(selected_by_id.at(id));if(!(exclude_self&&query_id&&id==*query_id))results[id]=distance;}
          for(size_t i=0;i<neighbors.size();++i) {
            Id id=neighbors[i];if(!seen.insert(id).second)continue;
            const auto& parent=selected_by_id.at(parents.at(id));const auto p=locate(id,true);
            auto owners=p!=missing_location?owners_[p]:overlay&&overlay->locate(id,true)!=ReplicaOverlay::missing?overlay->owners.at(overlay->locate(id,true)):demand_banks.count(parents.at(id))?demand_banks.at(parents.at(id))->owners.at(demand_banks.at(parents.at(id))->locate(id,true)):pulled_codes.owners.at(pulled_codes.locate(id,true));
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
      if(nonblocking_start&&!finishing&&control->candidate_pending_starts&&control->candidate_pending_starts()){
        // If an admission settles after the last safe point, park/resume rather
        // than finalizing stale credit. The runtime also tests queued receipts.
        send_existing(true);
        pause_sync("settle",V::Map{{"remaining",uint64_t(remaining)},{"expanded",uint64_t(expanded.size())},{"children",native_children}}, {},Group{},-1);
      }
      const bool sole_root=candidate_sync&&dhvalue::null(dhvalue::at(state,"parent_branch_id"))&&native_children.empty();
      if(candidate_sync&&!finishing&&control->candidate_finish&&remaining==0){send_existing(true);delivery_required=control->candidate_finish();finishing=true;stats["candidate_early_results"]++;}
      if(candidate_sync&&!finishing&&!sole_root){send_existing(true);
        pause_sync("idle",V::Map{{"remaining",uint64_t(remaining)},{"expanded",uint64_t(expanded.size())},{"children",native_children}}, {},Group{},-1);}
      if(stats.count("tail_start_expanded"))stats["tail_policy_expanded"]=expanded.size()-size_t(stats.at("tail_start_expanded"));
      if(remaining==0)stop_reason="branch_work_credit_exhausted";
      if(lazy&&!deferred.empty())refine({},Group{},-1,true);
      if(inbox&&!candidate_sync){V::Array pending;for(const auto& c:frontier)pending.push_back(row(c));inbox->offer(pending);
        stats["region_frontier_saved"]=pending.size();}
      if(heat_lane&&!completion){const auto t=now_ns();heat_lane->flush();stats["hotness_record_ms"]+=ms_since(t);}
      native_loop_done_ns=now_ns();
      stats["server_thread_cpu_ms"]=(frame?frame->cpu_total:0)+cpu_ms()-slice_cpu;
      stats["pull_resume_count"]=frame?frame->resumes:0;
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
        all_stats["query_compute_slice_wall_ms"]=V((frame?frame->slot_wall:0)+ms_since(slice_start));
        native_report["node_id"]=V(int64_t(sid_));native_report["work_spent"]=V(uint64_t(expanded.size()));
        native_report["work_unused"]=V(uint64_t(remaining));native_report["children"]=V(std::move(native_children));
        native_report["ids"]=V(std::move(ids));native_report["squared_distances"]=V(std::move(distances));
        native_report["expanded_ids"]=ids_value(expanded);native_report["result_paths"]=V(std::move(result_paths));
        if(dhvalue::optional<bool>(state,"replica_unavailable_fallback",false))native_report["replica_unavailable_fallback"]=true;
        if(overlay){native_report["replica_version"]=V(overlay->version);
          native_report["replica_access_kind"]=V(dhvalue::optional<std::string>(state,"replica_access_kind",visited.size()==1?"entry":"peer"));
          all_stats["replica_local_expansions"]=V(uint64_t(expanded.size()));
        }
        if(pair_scoped&&candidate_sync){for(auto k:{"replica_pair_source","replica_pair_host","replica_pair_version"})native_report[k]=dhvalue::at(state,k);all_stats["replica_pair_branch"]=1.;}
        native_report["stats"]=V(std::move(all_stats));native_report["fork_events"]=V(std::move(native_events));native_report["pull_events"]=std::move(pull_events);
        native_report["pull_gate_events"]=std::move(pull_gate_events);
        if(candidate_sync)native_report["candidate_pause_events"]=std::move(sync_events);
        if(delivery_required)native_report["candidate_delivery_required"]=true;
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
        native_report["compute_thread_id"]=V(frame?frame->first_thread:owner_thread);native_report["compute_thread_migrations"]=V(frame?frame->migrations:uint64_t(0));
        native_report["native_search_loop"]=V(true);native_report["decision_rng"]=V("numpy_pcg64_precomputed_v1");
        native_report["execution"]=V(V::Map{{"model",V("native")},{"io_backend",V("sync_pread")},
          {"fallback_reason",V()},{"query_parallelism",V(frame?"one_serial_compute_thread_per_resume":"one_fixed_compute_thread_per_node_branch")},
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
  dhgrowth::bind(m);
  py::class_<dhreplica::Cancel,std::shared_ptr<dhreplica::Cancel>>(m,"ReplicaCancel")
    .def(py::init<double>()).def("cancel",&dhreplica::Cancel::cancel);
  py::class_<dhreplica::Prepared,std::shared_ptr<dhreplica::Prepared>>(m,"PreparedReplica")
    .def_readonly("timings",&dhreplica::Prepared::timings);
  py::register_exception<dhsearch::Cancelled>(m,"NativeSearchCancelled",PyExc_TimeoutError);
  py::class_<dhsearch::Control,std::shared_ptr<dhsearch::Control>>(m,"SearchControl")
    .def(py::init<double,double>()).def("renew",&dhsearch::Control::renew)
    .def("cancel",&dhsearch::Control::cancel).def("cancelled",&dhsearch::Control::cancelled);
  py::class_<dhsearch::Shard,std::shared_ptr<dhsearch::Shard>>(m,"SearchShard")
    .def(py::init<py::object,Array<uint8_t>,py::object>())
    .def("id_lookup_info",&dhsearch::Shard::id_lookup_info)
    .def("replica_routes",&dhsearch::Shard::replica_routes)
    .def("replica_counters",&dhsearch::Shard::replica_counters)
    .def("configure_replica_growth",&dhsearch::Shard::configure_replica_growth)
    .def("demand_growth_metrics",&dhsearch::Shard::demand_growth_metrics)
    .def("seal_demand_growth",&dhsearch::Shard::seal_demand_growth)
    .def("hotness_snapshot",&dhsearch::Shard::hotness_snapshot)
    .def("hotness_metrics",&dhsearch::Shard::hotness_metrics)
    .def("configure_hotness",[](dhsearch::Shard& s,py::object c){s.configure_hotness(dhwire::from_python(c));})
    .def("install_replica",&dhsearch::Shard::install_replica)
    .def("prepare_replica_directory",&dhsearch::Shard::prepare_replica_directory)
    .def("publish_replica",&dhsearch::Shard::publish_replica)
    .def("retire_replica",&dhsearch::Shard::retire_replica)
    .def("replica_released",&dhsearch::Shard::replica_released)
    .def("run",&dhsearch::Shard::run,py::arg("state"),py::arg("config"),py::arg("draws"),
         py::arg("control"),py::arg("read_batch"),py::arg("release_batch"),py::arg("spawn"),py::arg("completion")=nullptr)
    .def("close",&dhsearch::Shard::close);
}
