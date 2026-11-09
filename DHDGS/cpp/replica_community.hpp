#pragma once
#include <chrono>
#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <cerrno>
#include <unistd.h>
#include "replica_planning_graph.hpp"

// Source-local, complete directed balls. No query work or graph mutation here.
namespace dhcommunity {
using Id=uint64_t;
using Set=std::set<Id>;
struct Limit:std::runtime_error{using std::runtime_error::runtime_error;};
struct Region { Set ids,seeds,hot,portals; size_t edges=0,exits=0,codes=0,bytes=0,core_rows=0,added_balls=0;double heat=0,score=0; };
struct Planner {
 int fd;size_t dim,stride,header,maxdeg,maxrows,maxbytes,maxcodes,percode,fixed;
 size_t edge_limit,cache_limit,scans=0,cache_bytes=0,reads=0;
 dhplanning::RowView rows;
 std::shared_ptr<dhplanning::Graph> shared;
 std::unordered_map<Id,std::vector<Id>> cache;
 std::map<Id,double> hot;
 std::unordered_set<Id> route_ids;double generalization=0;
 std::chrono::steady_clock::time_point deadline;
 double price,total_heat=0;
 void check(){if(shared)shared->check();if(std::chrono::steady_clock::now()>deadline)throw Limit("community planning deadline");}
 void init_rows(const Array<Id>& source,const py::dict& meta){
  if(meta.contains("_planning_graph")){
   shared=meta["_planning_graph"].cast<std::shared_ptr<dhplanning::Graph>>();shared->validate(fd,source,meta);
   rows.value=shared->rows;
  }else{
   py::gil_scoped_release release;rows.reserve(source.size());
   for(ssize_t i=0;i<source.size();++i){if((i&4095)==0)check();require(rows.emplace(source.data()[i],i).second,"Duplicate community source ID");}
  }
 }
 template<class Container> void prefetch(const Container& ids){if(shared)shared->prefetch(ids.begin(),ids.end());}
 void read(void* ptr,size_t bytes,uint64_t off){size_t n=0;while(n<bytes){ssize_t k=::pread(fd,(char*)ptr+n,bytes-n,off+n);if(k<0&&errno==EINTR)continue;if(k<=0)throw std::runtime_error("community short adjacency read");n+=k;}}
 const std::vector<Id>& adj(Id id){
  if(shared){check();auto& out=shared->adjacency(id);cache_bytes=shared->used();if(cache_bytes>cache_limit)throw Limit("community adjacency memory budget");return out;}
  check();auto cached=cache.find(id);if(cached!=cache.end())return cached->second;
  auto row=rows.find(id);if(row==rows.end())throw std::invalid_argument("community nonlocal record");
  const uint64_t off=header+uint64_t(row->second)*stride+4*dim;uint32_t d=0;read(&d,4,off);
  if(d>maxdeg||4*dim+4+uint64_t(d)*8>stride)throw std::invalid_argument("community invalid degree");
  const size_t cost=sizeof(Id)*d+128;
  if(cache_bytes+cost>cache_limit)throw Limit("community adjacency memory budget");
  std::vector<Id> v(d);if(d)read(v.data(),d*8,off+4);++reads;cache_bytes+=cost;
  return cache.emplace(id,std::move(v)).first->second;
 }
 void scan(){check();if(++scans>edge_limit)throw Limit("community edge scan budget");}
 void scan_many(size_t n){check();if(scans>edge_limit||n>edge_limit-scans)throw Limit("community edge scan budget");scans+=n;}
 Region closure(const Set& seeds,int hops,bool trigger){
  Region r;r.seeds=seeds;std::unordered_map<Id,int> best;std::deque<std::pair<Id,int>> q;
  for(auto s:seeds){if(!rows.count(s))throw std::invalid_argument("community seed nonlocal");q.emplace_back(s,hops);}
  while(!q.empty()){
   check();auto [v,left]=q.front();q.pop_front();
   if(trigger&&hot.count(v)&&r.hot.insert(v).second){r.seeds.insert(v);left=hops;}
   auto it=best.find(v);if(it!=best.end()&&it->second>=left)continue;
   best[v]=left;r.ids.insert(v);
   if(r.ids.size()>maxrows)throw Limit("community complete ball exceeds row budget");
   if(left)for(auto n:adj(v)){scan();if(rows.count(n))q.emplace_back(n,left-1);}
  }
  return r;
 }
 void evaluate(Region& r){
  std::unordered_set<Id> codes(r.ids.begin(),r.ids.end());r.edges=r.exits=0;r.heat=0;
  auto ahead=r.ids.begin();size_t remaining=0;
  for(auto v:r.ids){if(shared&&remaining==0){shared->prefetch(ahead,r.ids.end());remaining=std::min(shared->batch(),r.ids.size());}if(shared){++ahead;--remaining;}
   if(hot.count(v))r.heat+=hot.at(v);for(auto n:adj(v)){scan();++r.edges;r.exits+=!r.ids.count(n);codes.insert(n);}}
  r.portals.clear();
  for(auto v:r.ids)if(route_ids.count(v)){
   bool complete=true;for(auto n:adj(v)){scan();if(rows.count(n)&&!r.ids.count(n)){complete=false;break;}}
   if(complete)r.portals.insert(v);
  }
  r.codes=codes.size();size_t cb=r.codes*percode+fixed;
  r.bytes=header+r.ids.size()*(stride+8)+cb;
  if(cb>maxcodes||r.bytes>maxbytes)throw Limit("community complete dependency byte budget");
  double retention=r.edges?1.-double(r.exits)/r.edges:0.;
  // Explicit structural proxy, NOT calibrated saved CPU or theoretical recall.
  double hot_coverage=total_heat?r.heat/total_heat:0;
  double local_coverage=route_ids.empty()?0.:double(r.portals.size())/route_ids.size();
  r.score=((1-generalization)*hot_coverage+generalization*local_coverage)*retention-price*double(r.bytes)/maxbytes;
 }
};
inline py::dict plan(int fd,Array<Id> source, std::vector<Id> hotids,std::vector<double> weights,
                     py::dict cfg,py::dict meta,std::vector<Id> verifyids={},std::vector<Id> verifyseeds={},int verifyh=0,std::vector<Id> routeids={},std::vector<Id> verifyentries={}){
 auto z=[&](const char* key){return cfg[key].cast<size_t>();};
 require(source.ndim()==1&&hotids.size()==weights.size()&&!hotids.empty(),"Invalid community inputs");
 const int hmax=cfg["community_hops"].cast<int>();
 const int hmin=cfg.contains("community_min_hops")?cfg["community_min_hops"].cast<int>():1;
 require(hmin>=1&&hmin<=hmax,"Invalid minimum complete hops");
 const size_t starts=z("community_seed_starts"),rounds=z("community_ppr_rounds"),anchors=z("community_ppr_candidates"),iters=z("community_ppr_iterations"),ppr_vertices=z("community_ppr_vertices");
 const double alpha=cfg["community_ppr_restart"].cast<double>(),tol=cfg["community_ppr_tolerance"].cast<double>();
 const bool trigger=cfg["community_hot_closure"].cast<bool>();
 require(hmax>=1&&hmax<=8&&alpha>0&&alpha<1&&tol>0&&starts&&iters&&anchors,"Invalid community parameters");
 Planner p;
 p.generalization=cfg.contains("community_generalization_weight")?cfg["community_generalization_weight"].cast<double>():0.;
 require(p.generalization>=0&&p.generalization<=1,"Invalid generalization weight");
 p.fd=fd;p.dim=meta["dimension"].cast<size_t>();p.stride=meta["record_bytes"].cast<size_t>();p.header=meta["header_bytes"].cast<size_t>();p.maxdeg=meta["max_degree"].cast<size_t>();
 p.maxrows=z("max_region_rows");p.maxbytes=z("max_region_bytes");p.maxcodes=z("max_code_bytes");
 p.percode=meta["community_code_bytes"].cast<size_t>();p.fixed=meta["community_fixed_bytes"].cast<size_t>();
 p.edge_limit=z("community_max_edge_scans");p.cache_limit=z("community_adjacency_bytes");p.price=cfg["community_byte_price"].cast<double>();
 require(p.stride>=4*p.dim+4&&p.maxrows&&p.maxbytes&&p.percode,"Invalid community layout/budgets");
 Region best;int besth=0;size_t rejected=0,ppr_steps=0,complete_cores=0,refined_cores=0;double outmass=0,residual=0;std::vector<std::string> reasons;
 auto begin=std::chrono::steady_clock::now();p.deadline=begin+std::chrono::milliseconds(z("community_max_plan_ms"));
 p.init_rows(source,meta);
 {
 py::gil_scoped_release release;
 for(auto id:routeids){if(!p.rows.count(id)||!p.route_ids.insert(id).second)throw std::invalid_argument("Invalid local routing portal");}
 require(!p.route_ids.empty()||p.generalization==0,"Generalization requires routing portals");
 for(size_t i=0;i<hotids.size();++i){if(!std::isfinite(weights[i])||weights[i]<=0||!p.rows.count(hotids[i])||p.hot.count(hotids[i]))throw std::invalid_argument("Invalid community hotspot");p.hot[hotids[i]]=weights[i];p.total_heat+=weights[i];}
 if(!verifyids.empty()||!verifyseeds.empty()){
  require(verifyh>0&&verifyh<=8&&!verifyids.empty()&&!verifyseeds.empty(),"Invalid community certificate");
  Set ids(verifyids.begin(),verifyids.end());if(ids.size()!=verifyids.size())throw std::invalid_argument("Duplicate community members");
  best=p.closure(Set(verifyseeds.begin(),verifyseeds.end()),verifyh,trigger);
  if(best.ids!=ids)throw std::invalid_argument("Community member completeness mismatch");p.evaluate(best);besth=verifyh;
  // Final seeds alone can describe disconnected balls. Independently ensure
  // every member is reachable from an eligible hot routing entry in R.
  Set reached;std::deque<Id> pending;
  const auto& roots=verifyentries.empty()?hotids:verifyentries;
  for(auto id:roots){if(!verifyentries.empty()&&!best.portals.count(id))throw std::invalid_argument("Routing entry lacks complete local neighborhood");if(best.ids.count(id)){reached.insert(id);pending.push_back(id);}}
  while(!pending.empty()){
   p.check();auto id=pending.front();pending.pop_front();
   for(auto n:p.adj(id)){p.scan();if(best.ids.count(n)&&reached.insert(n).second)pending.push_back(n);}
  }
  if(reached!=best.ids)throw std::invalid_argument("Community members unreachable from routing entries");
 }else{
  std::vector<Id> ordered=hotids;std::sort(ordered.begin(),ordered.end(),[&](Id a,Id b){return p.hot[a]!=p.hot[b]?p.hot[a]>p.hot[b]:a<b;});
  struct Candidate {Region r;int hops;Id root;};std::vector<Candidate> cores;
  auto exhausted=[&](){return std::chrono::steady_clock::now()>p.deadline||p.scans>=p.edge_limit;};
  // Compare complete cores first. Expensive PPR for one seed must not hide
  // a better core that could have been fully enumerated within the budget.
  for(int h=hmax;h>=hmin&&!exhausted();--h)for(size_t si=0;si<std::min(starts,ordered.size())&&!exhausted();++si){
   try{
    auto r=p.closure(Set{ordered[si]},h,trigger);p.evaluate(r);
    r.core_rows=r.ids.size();++complete_cores;
    if(!besth||r.score>best.score){best=r;besth=h;}
    bool duplicate=false;for(auto& c:cores)if(c.hops==h&&c.r.ids==r.ids){duplicate=true;break;}
    if(!duplicate)cores.push_back({std::move(r),h,ordered[si]});
   }catch(const Limit& e){++rejected;if(reasons.size()<16)reasons.push_back(e.what());}
  }
  std::stable_sort(cores.begin(),cores.end(),[](const Candidate& a,const Candidate& b){return a.r.score>b.r.score;});
  for(auto& candidate:cores){
   if(!rounds||exhausted())break;
   try{
    auto r=candidate.r;const int h=candidate.hops;++refined_cores;
    for(size_t round=0;round<rounds;++round){
     Set exploration=r.ids; // One complete local shell, within explicit memory/row bound.
     for(auto v:r.ids)for(auto n:p.adj(v)){p.scan();if(p.rows.count(n))exploration.insert(n);if(exploration.size()>ppr_vertices)throw Limit("community PPR exploration budget");}
     std::vector<Id> verts(exploration.begin(),exploration.end());std::unordered_map<Id,size_t> loc;for(size_t i=0;i<verts.size();++i)loc[verts[i]]=i;
     // Resolve IDs once, not on every PPR iteration. This is a compact view
     // of the original outgoing adjacency; external targets map to OUT.
     if(verts.size()>=UINT32_MAX)throw Limit("community PPR index capacity");
     std::vector<size_t> offsets;std::vector<uint32_t> targets;offsets.reserve(verts.size()+1);offsets.push_back(0);
     auto workspace=[&](size_t capacity){return (verts.size()+1)*(sizeof(size_t)+3*sizeof(double)+64)+capacity*sizeof(uint32_t);};
     for(auto v:verts){const auto& a=p.adj(v);p.scan_many(a.size());
      const size_t needed=targets.size()+a.size();
      const size_t capacity=needed>targets.capacity()?std::max(needed,2*targets.capacity()):targets.capacity();
      const size_t temporary=capacity>targets.capacity()?targets.capacity()*sizeof(uint32_t):0;
      if(p.cache_bytes+workspace(capacity)+temporary>p.cache_limit)throw Limit("community PPR workspace memory budget");
      if(capacity>targets.capacity())targets.reserve(capacity);
      for(auto n:a){auto it=loc.find(n);targets.push_back(it==loc.end()?uint32_t(verts.size()):uint32_t(it->second));}
      offsets.push_back(targets.size());
      if(p.cache_bytes+workspace(targets.capacity())>p.cache_limit)throw Limit("community PPR workspace memory budget");
     }
     std::vector<double> restart(verts.size()+1),prob(verts.size()+1),next(prob.size());size_t out=verts.size();double norm=0;
     for(auto v:r.seeds){double w=p.hot.count(v)?std::sqrt(p.hot[v]):0.;restart[loc.at(v)]=w;norm+=w;}
     if(!norm){restart[loc.at(candidate.root)]=norm=1;}
     for(auto& v:restart)v/=norm;prob=restart;
     for(size_t iteration=0;iteration<iters;++iteration){
      for(size_t i=0;i<next.size();++i)next[i]=alpha*restart[i];next[out]+=(1-alpha)*prob[out];
      for(size_t i=0;i<verts.size();++i){const size_t degree=offsets[i+1]-offsets[i];p.scan_many(degree);if(!degree){next[i]+=(1-alpha)*prob[i];continue;}
       double mass=(1-alpha)*prob[i]/degree;for(size_t j=offsets[i];j<offsets[i+1];++j)next[targets[j]]+=mass;}
      residual=0;for(size_t i=0;i<prob.size();++i)residual+=std::abs(next[i]-prob[i]);prob.swap(next);++ppr_steps;if(residual<=tol)break;
     }
     outmass=prob[out];std::vector<Id> frontier;for(auto v:verts)if(!r.ids.count(v))frontier.push_back(v);
     std::sort(frontier.begin(),frontier.end(),[&](Id a,Id b){double x=prob[loc.at(a)],y=prob[loc.at(b)];return x!=y?x>y:a<b;});
     Region improvement=r;
     for(size_t j=0;j<std::min(anchors,frontier.size());++j){
      try{Set seeds=r.seeds;seeds.insert(frontier[j]);auto trial=p.closure(seeds,h,trigger);p.evaluate(trial);trial.core_rows=r.core_rows;trial.added_balls=r.added_balls+1;if(trial.score>improvement.score)improvement=std::move(trial);}
      catch(const Limit& e){++rejected;if(reasons.size()<16)reasons.push_back(e.what());p.check();}
     }
     if(improvement.ids==r.ids)break;r=std::move(improvement);if(r.score>best.score){best=r;besth=h;}
    }
   }catch(const Limit& e){++rejected;if(reasons.size()<16)reasons.push_back(e.what());}
   // Keep only complete, evaluated candidates; never return a partial traversal.
   if(exhausted())break;
  }
  if(!besth)throw std::invalid_argument("No complete community fits planning/capacity budgets");
  if(best.score<=0)throw std::invalid_argument("No positive community structural proxy score");
 }
 }
 py::dict stats;stats["rows"]=best.ids.size();stats["code_rows"]=best.codes;stats["estimated_bytes"]=best.bytes;
 stats["edges"]=best.edges;stats["exit_edges"]=best.exits;stats["outgoing_ratio"]=best.edges?double(best.exits)/best.edges:1.;stats["covered_hot_weight"]=best.heat;
 stats["score"]=best.score;stats["score_kind"]=p.route_ids.empty()?"hot_work_retention_minus_byte_price_proxy":"hot_work_and_local_portal_retention_proxy";stats["safe_routing_entries"]=best.portals.size();
 stats["completed_cores"]=complete_cores;stats["refined_cores"]=refined_cores;
 stats["selected_core_rows"]=best.core_rows;stats["selected_ppr_added_balls"]=best.added_balls;
 stats["adjacency_reads"]=p.reads;stats["edge_scans"]=p.scans;stats["adjacency_cache_bytes"]=p.cache_bytes;
 stats["rejected_candidates"]=rejected;stats["rejection_reasons"]=reasons;stats["ppr_steps"]=ppr_steps;stats["last_ppr_out_mass"]=outmass;stats["last_ppr_residual"]=residual;
 stats["planning_seconds"]=std::chrono::duration<double>(std::chrono::steady_clock::now()-begin).count();
 std::vector<Id> entries;if(p.route_ids.empty()){for(auto id:hotids)if(best.ids.count(id))entries.push_back(id);}else entries.assign(best.portals.begin(),best.portals.end());std::sort(entries.begin(),entries.end());
 py::dict result;result["ids"]=std::vector<Id>(best.ids.begin(),best.ids.end());result["entries"]=entries;result["seeds"]=std::vector<Id>(best.seeds.begin(),best.seeds.end());result["hops"]=besth;result["stats"]=stats;return result;
}
inline void bind(py::module_& m){dhplanning::bind(m);m.def("replica_community_plan",&plan,py::arg("fd"),py::arg("ids"),py::arg("hot_ids"),py::arg("weights"),py::arg("settings"),py::arg("meta"),py::arg("verify_ids")=std::vector<Id>{},py::arg("verify_seeds")=std::vector<Id>{},py::arg("verify_hops")=0,py::arg("route_ids")=std::vector<Id>{},py::arg("verify_entry_ids")=std::vector<Id>{});}
}
