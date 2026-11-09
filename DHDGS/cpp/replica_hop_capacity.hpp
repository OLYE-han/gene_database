#pragma once
#include "replica_hop_expand.hpp"

namespace dhcapacity {
using dhcommunity::Id;
using dhcommunity::Set;

inline dhcommunity::Planner planner(int fd, const py::dict& cfg, const py::dict& meta) {
  dhcommunity::Planner p;
  auto c = [&](const char* k) { return cfg[k].cast<size_t>(); };
  auto m = [&](const char* k) { return meta[k].cast<size_t>(); };
  p.fd=fd; p.dim=m("dimension"); p.stride=m("record_bytes");
  p.header=m("header_bytes"); p.maxdeg=m("max_degree");
  p.maxrows=c("max_region_rows"); p.maxbytes=c("max_region_bytes");
  p.maxcodes=c("max_code_bytes"); p.percode=m("community_code_bytes");
  p.fixed=m("community_fixed_bytes"); p.edge_limit=c("community_max_edge_scans");
  p.cache_limit=c("community_adjacency_bytes"); p.price=0;
  p.deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(c("community_max_plan_ms"));
  return p;
}

// Breadth-ordered complete balls, with incremental shared-dependency accounting.
// No record of a rejected ball is committed. The same attached-ball verifier
// used by closure cuts independently replays the resulting certificate.
inline py::dict breadth(int fd, Array<Id> source, std::vector<Id> core,
                        py::dict cfg, py::dict meta, py::object admitted=py::none(), size_t batch_rows=1024) {
  require(batch_rows>0 && batch_rows<=65536,"Invalid admission batch size");
  auto p=planner(fd,cfg,meta);
  require(!core.empty() && core.size()<=p.maxrows,"Invalid capacity core row bound");
  const auto hc=cfg["hop_expansion"].cast<py::dict>();
  const size_t maxanchors=hc["rounds"].cast<size_t>()*hc["candidates"].cast<size_t>();
  dhcommunity::Region region;
  std::vector<Id> selected;
  size_t rejected=0,considered=0;
  const auto started=std::chrono::steady_clock::now();
  // Soft deadline stops only between complete admitted balls. Leave the
  // hard shared deadline for final accounting and independent verification.
  const bool soft=cfg.contains("expansion_soft_ms");
  const auto soft_end=started+std::chrono::milliseconds(soft?cfg["expansion_soft_ms"].cast<size_t>():0);
  bool stopped_early=false;
  p.init_rows(source,meta);
  {
    py::gil_scoped_release release;
    for(auto v:core) {
      require(p.rows.count(v),"Nonlocal capacity core");
      require(region.ids.insert(v).second,"Duplicate capacity core");
    }
    p.evaluate(region);
    require(region.bytes<=p.maxbytes && region.codes*p.percode+p.fixed<=p.maxcodes,"Capacity core exceeds budget");
    std::vector<Id> pending;
    auto flush=[&](bool force) {
      if(admitted.is_none())return;
      while(pending.size()>=batch_rows || (force&&!pending.empty())) {
        size_t n=std::min(batch_rows,pending.size());
        std::vector<Id> batch(pending.begin(),pending.begin()+n);
        {py::gil_scoped_acquire acquire;admitted(batch);}
        pending.erase(pending.begin(),pending.begin()+n);
        p.check();
      }
    };
    if(!admitted.is_none()) {pending=core;std::sort(pending.begin(),pending.end());flush(true);}
    std::unordered_set<Id> members(core.begin(),core.end()),codes(members);
    std::unordered_map<Id,size_t> depth;
    using Item=std::pair<size_t,Id>;
    std::priority_queue<Item,std::vector<Item>,std::greater<Item>> frontier;
    for(auto v:core)depth[v]=0;
    auto discover=[&](Id v,size_t d) {
      if(!p.rows.count(v)||members.count(v))return;
      auto it=depth.find(v);
      if(it==depth.end()||d<it->second) {depth[v]=d;frontier.emplace(d,v);}
    };
    for(auto v:core)for(auto n:p.adj(v)) {
      p.scan();codes.insert(n);discover(n,1);
    }
    size_t bytes=region.bytes;
    while(!frontier.empty() && selected.size()<maxanchors) {
      p.check();
      if(soft && std::chrono::steady_clock::now()>=soft_end){stopped_early=true;break;}
      if(bytes>p.maxbytes || p.maxbytes-bytes<p.stride+8)break;
      const auto [d,a]=frontier.top();frontier.pop();
      if(members.count(a)||depth.at(a)!=d)continue;
      ++considered;
      Set add{a};
      for(auto v:p.adj(a)) {
        p.scan();if(p.rows.count(v)&&!members.count(v))add.insert(v);
      }
      if(members.size()+add.size()>p.maxrows ||
         add.size()>(p.maxbytes-bytes)/(p.stride+8)) {++rejected;continue;}
      p.prefetch(add);
      std::unordered_set<Id> newcodes;
      for(auto v:add) {
        if(!codes.count(v))newcodes.insert(v);
        for(auto n:p.adj(v)) {p.scan();if(!codes.count(n))newcodes.insert(n);}
      }
      const size_t delta=add.size()*(p.stride+8)+newcodes.size()*p.percode;
      if(delta>p.maxbytes-bytes ||
         (codes.size()+newcodes.size())*p.percode+p.fixed>p.maxcodes) {++rejected;continue;}
      // Commit an entire ball atomically at the planning level.
      bytes+=delta;codes.insert(newcodes.begin(),newcodes.end());selected.push_back(a);
      for(auto v:add) {
        members.insert(v);region.ids.insert(v);
        auto it=depth.find(v);const size_t proposed=v==a?d:d+1;
        if(it==depth.end()||proposed<it->second)depth[v]=proposed;
      }
      if(!admitted.is_none()) {pending.insert(pending.end(),add.begin(),add.end());flush(false);}
      for(auto v:add)for(auto n:p.adj(v)) {p.scan();discover(n,depth.at(v)+1);}
    }
    p.evaluate(region);
    require(region.bytes==bytes && region.codes==codes.size(),"Incremental capacity accounting mismatch");
    flush(true);
  }
  py::dict stats;
  stats["core_rows"]=core.size();stats["rows"]=region.ids.size();stats["code_rows"]=region.codes;
  stats["estimated_bytes"]=region.bytes;stats["edges"]=region.edges;stats["exit_edges"]=region.exits;
  stats["selected_balls"]=selected.size();stats["candidates"]=considered;stats["rejections"]=rejected;
  stats["soft_deadline_stopped"]=stopped_early;
  stats["edge_scans"]=p.scans;stats["adjacency_reads"]=p.reads;
  stats["planning_seconds"]=std::chrono::duration<double>(std::chrono::steady_clock::now()-started).count();
  py::dict result;result["ids"]=std::vector<Id>(region.ids.begin(),region.ids.end());
  result["anchors"]=selected;result["stats"]=stats;return result;
}

inline py::dict inspect(int fd,Array<Id> source,std::vector<Id> core,std::vector<Id> ids,
                        std::vector<Id> potential_entries,py::dict cfg,py::dict meta) {
  auto p=planner(fd,cfg,meta);size_t maxdistance=0,shells=0;bool closed=false;
  std::vector<Id> safe;
  p.init_rows(source,meta);
  {
    py::gil_scoped_release release;
    std::unordered_set<Id> members(ids.begin(),ids.end()),seen;
    std::deque<std::pair<Id,size_t>> queue;
    for(auto v:core) {require(members.count(v),"Capacity core missing");if(seen.insert(v).second)queue.emplace_back(v,0);}
    while(!queue.empty()) {
      auto [v,d]=queue.front();queue.pop_front();maxdistance=std::max(maxdistance,d);
      for(auto n:p.adj(v)) {p.scan();if(members.count(n)&&seen.insert(n).second)queue.emplace_back(n,d+1);}
    }
    require(seen.size()==members.size(),"Capacity members unreachable from core");
    seen=std::unordered_set<Id>(core.begin(),core.end());std::vector<Id> layer=core;
    for(;;) {
      bool missing=false;std::vector<Id> next;
      for(auto v:layer)for(auto n:p.adj(v)) {
        p.scan();if(!p.rows.count(n))continue;
        if(!members.count(n)){missing=true;break;}
        if(seen.insert(n).second)next.push_back(n);
      }
      if(missing)break;
      if(next.empty()){closed=true;break;}
      ++shells;layer=std::move(next);
    }
    for(auto v:potential_entries)if(members.count(v)) {
      bool full=true;for(auto n:p.adj(v)){p.scan();if(p.rows.count(n)&&!members.count(n)){full=false;break;}}
      if(full)safe.push_back(v);
    }
  }
  py::dict result;result["complete_shells_beyond_core"]=shells;result["max_core_distance"]=maxdistance;
  result["source_closure_complete"]=closed;result["potential_safe_entries"]=safe.size();
  result["safe_entries"]=safe;return result;
}
inline void bind(py::module_& m) {
  m.def("replica_hop_breadth_expand",&breadth,py::arg("fd"),py::arg("source"),py::arg("core"),py::arg("cfg"),py::arg("meta"),py::arg("admitted")=py::none(),py::arg("batch_rows")=1024);
  m.def("replica_hop_capacity_inspect",&inspect);
}
}
