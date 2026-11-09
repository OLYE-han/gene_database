#pragma once
#include "replica_community.hpp"
#include <queue>

// Budgeted maximum-weight closure on attached complete balls. The cut solves
// the stated additive prize surrogate, not ANN recall or query-level coverage.
namespace dhhop {
using dhcommunity::Id;using dhcommunity::Set;
struct Flow {
 struct E {int to,rev;double cap;};
 std::vector<std::vector<E>> g;std::vector<int> level,pos;
 explicit Flow(size_t n):g(n),level(n),pos(n){}
 void edge(int u,int v,double c){E a{v,int(g[v].size()),c},b{u,int(g[u].size()),0};g[u].push_back(a);g[v].push_back(b);}
 double push(int u,int t,double f){if(u==t)return f;for(int& i=pos[u];i<int(g[u].size());++i){auto& e=g[u][i];if(e.cap>1e-10&&level[e.to]==level[u]+1){double x=push(e.to,t,std::min(f,e.cap));if(x>1e-10){e.cap-=x;g[e.to][e.rev].cap+=x;return x;}}}return 0;}
 std::vector<bool> cut(int s,int t,const std::function<void()>& check){
  for(;;){check();std::fill(level.begin(),level.end(),-1);std::queue<int> q;q.push(s);level[s]=0;
   while(!q.empty()){int u=q.front();q.pop();for(auto& e:g[u])if(e.cap>1e-10&&level[e.to]<0){level[e.to]=level[u]+1;q.push(e.to);}}
   if(level[t]<0)break;std::fill(pos.begin(),pos.end(),0);while(push(s,t,1e100)>1e-10)check();
  }
  std::vector<bool> out(g.size());for(size_t i=0;i<g.size();++i)out[i]=level[i]>=0;return out;
 }
};
inline py::dict expand(int fd,Array<Id> source,std::vector<Id> coreids,
 std::vector<Id> weightids,std::vector<double> weights,py::dict cfg,py::dict meta,
 std::vector<Id> verifyanchors={},bool verify=false){
 require(!coreids.empty()&&weightids.size()==weights.size(),"Invalid hop expansion inputs");
 auto z=[&](const char* k){return cfg[k].cast<size_t>();};
 dhcommunity::Planner p;p.fd=fd;p.dim=meta["dimension"].cast<size_t>();p.stride=meta["record_bytes"].cast<size_t>();p.header=meta["header_bytes"].cast<size_t>();p.maxdeg=meta["max_degree"].cast<size_t>();
 p.maxrows=z("max_region_rows");p.maxbytes=z("max_region_bytes");p.maxcodes=z("max_code_bytes");p.percode=meta["community_code_bytes"].cast<size_t>();p.fixed=meta["community_fixed_bytes"].cast<size_t>();
 p.edge_limit=z("community_max_edge_scans");p.cache_limit=z("community_adjacency_bytes");p.price=0;
 const auto b=std::chrono::steady_clock::now();p.deadline=b+std::chrono::milliseconds(z("community_max_plan_ms"));
 const auto c=cfg["hop_expansion"].cast<py::dict>();const size_t rounds=c["rounds"].cast<size_t>(),limit=c["candidates"].cast<size_t>(),steps=c["cut_steps"].cast<size_t>();
 const double mix=c["history_weight"].cast<double>();require(rounds&&limit&&steps&&mix>=0&&mix<=1,"Invalid hop expansion parameters");
 Set original(coreids.begin(),coreids.end());require(original.size()==coreids.size(),"Duplicate hop core");
 require(original.size()<=p.maxrows,"Hop core row budget exceeded");
 dhcommunity::Region region;region.ids=original;std::vector<Id> selected;size_t cuts=0,rejected=0,candidate_count=0;double covered_weight=0;
 {
 p.init_rows(source,meta);
 py::gil_scoped_release release;

 for(auto v:original)require(p.rows.count(v),"Nonlocal hop core");p.evaluate(region);
 std::map<Id,double> heat;double norm=0;for(size_t i=0;i<weightids.size();++i){require(p.rows.count(weightids[i])&&std::isfinite(weights[i])&&weights[i]>=0,"Invalid hop work weight");heat[weightids[i]]+=weights[i];norm+=weights[i];}
 if(verify){
  Set attachable=region.ids;for(auto u:region.ids)for(auto n:p.adj(u)){p.scan();attachable.insert(n);}
  for(auto v:verifyanchors){require(p.rows.count(v)&&attachable.count(v),"Hop ball is not attached to preceding region");auto ball=p.closure(Set{v},1,false);
   for(auto u:ball.ids)if(region.ids.insert(u).second)for(auto n:p.adj(u)){p.scan();attachable.insert(n);}
   selected.push_back(v);
  }require(region.ids.size()<=p.maxrows,"Hop certificate row budget exceeded");p.evaluate(region);
 }
 else for(size_t round=0;round<rounds;++round){
  try{
   // A frontier anchor always has a real incoming edge from the current core.
   std::map<Id,double> boundary;std::unordered_set<Id> oldcodes(region.ids.begin(),region.ids.end());
   for(auto u:region.ids){const auto& adj=p.adj(u);double mass=(norm?mix*heat[u]/norm:0)+(1-mix)/region.ids.size();
    for(auto v:adj){p.scan();oldcodes.insert(v);if(p.rows.count(v)&&!region.ids.count(v))boundary[v]+=mass/std::max(size_t(1),adj.size());}}
   if(boundary.empty())break;
   std::vector<Id> anchors;for(auto [v,w]:boundary)anchors.push_back(v);
   auto rank=[&](Id v){return boundary[v]+(norm?mix*heat[v]/norm:0);};
   std::sort(anchors.begin(),anchors.end(),[&](Id a,Id b){double x=rank(a),y=rank(b);return x!=y?x>y:a<b;});if(anchors.size()>limit)anchors.resize(limit);
   std::vector<Set> balls;std::map<Id,size_t> occurrences;
   for(auto v:anchors){auto ball=p.closure(Set{v},1,false);Set added;std::set_difference(ball.ids.begin(),ball.ids.end(),region.ids.begin(),region.ids.end(),std::inserter(added,added.end()));for(auto id:added)++occurrences[id];balls.push_back(std::move(added));}
   candidate_count+=anchors.size();std::vector<double> prize;double total=0;
   for(size_t i=0;i<anchors.size();++i){double value=boundary[anchors[i]];for(auto id:balls[i])value+=norm?mix*heat[id]/norm/occurrences[id]:0;prize.push_back(value);total+=value;}
   if(total<=0)break;for(auto& v:prize)v/=total;
   // Nodes: S,T, anchors, newly required records, newly required codes. Shared
   // dependencies are paid once. No absent original graph edge is invented.
   const size_t firstrecord=2+anchors.size();std::map<Id,int> rec,code;
   for(auto [id,count]:occurrences)rec[id]=int(firstrecord+rec.size());
   size_t firstcode=firstrecord+rec.size();std::vector<std::pair<int,int>> deps;
   auto addcode=[&](int r,Id id){if(oldcodes.count(id))return;auto it=code.emplace(id,int(firstcode+code.size())).first;deps.emplace_back(r,it->second);};
   for(auto [id,r]:rec){addcode(r,id);for(auto v:p.adj(id)){p.scan();addcode(r,v);}}
   require(firstcode+code.size()<size_t(INT_MAX),"Hop flow graph too large");
   // Explicit accounting includes residual edges; fail to last complete round.
   size_t implication=deps.size();for(const auto& ball:balls)implication+=ball.size();
   if(p.cache_bytes+implication*64+(firstcode+code.size())*128>p.cache_limit)throw dhcommunity::Limit("hop flow workspace memory budget");
   auto solve=[&](double price){p.check();Flow flow(firstcode+code.size());const double inf=2+price*double(p.maxbytes)/1048576.;
    for(size_t i=0;i<anchors.size();++i){flow.edge(0,int(2+i),prize[i]);for(auto v:balls[i])flow.edge(int(2+i),rec.at(v),inf);}
    for(auto [id,r]:rec)flow.edge(r,1,price*(p.stride+8)/1048576.);
    for(auto [id,r]:code)flow.edge(r,1,price*p.percode/1048576.);
    for(auto [u,v]:deps)flow.edge(u,v,inf);auto reachable=flow.cut(0,1,[&]{p.check();});++cuts;
    std::vector<Id> chosen;for(size_t i=0;i<anchors.size();++i)if(reachable[2+i])chosen.push_back(anchors[i]);return chosen;
   };
   dhcommunity::Region best=region;std::vector<Id> bestanchors;double bestprize=-1;
   auto trial=[&](double price){auto chosen=solve(price);auto r=region;double gain=0;
    std::unordered_set<Id> take(chosen.begin(),chosen.end());for(size_t i=0;i<anchors.size();++i)if(take.count(anchors[i])){r.ids.insert(balls[i].begin(),balls[i].end());gain+=prize[i];}
    if(r.ids.size()>p.maxrows){++rejected;return false;}
    try{p.evaluate(r);}catch(const dhcommunity::Limit&){++rejected;p.check();return false;}
    if(gain>bestprize){best=std::move(r);bestanchors=std::move(chosen);bestprize=gain;}return true;
   };
   double lo=0,hi=1.;if(!trial(0)){while(!trial(hi)){hi*=2;if(hi>1e12)throw dhcommunity::Limit("hop cut price range");}for(size_t i=0;i<steps;++i){double mid=(lo+hi)/2;if(trial(mid))hi=mid;else lo=mid;}}
   if(best.ids==region.ids)break;region=std::move(best);selected.insert(selected.end(),bestanchors.begin(),bestanchors.end());
  }catch(const dhcommunity::Limit&){++rejected;break;}
 }
 for(auto id:region.ids)covered_weight+=heat[id];
 }
 py::dict stats;stats["core_rows"]=original.size();stats["rows"]=region.ids.size();stats["code_rows"]=region.codes;stats["estimated_bytes"]=region.bytes;stats["edges"]=region.edges;stats["exit_edges"]=region.exits;stats["covered_work_weight"]=covered_weight;stats["selected_balls"]=selected.size();stats["candidates"]=candidate_count;stats["flow_cuts"]=cuts;stats["rejections"]=rejected;stats["edge_scans"]=p.scans;stats["adjacency_reads"]=p.reads;stats["planning_seconds"]=std::chrono::duration<double>(std::chrono::steady_clock::now()-b).count();
 py::dict out;out["ids"]=std::vector<Id>(region.ids.begin(),region.ids.end());out["anchors"]=selected;out["stats"]=stats;return out;
}
inline void bind(py::module_& m){m.def("replica_hop_expand",&expand,py::arg("fd"),py::arg("source_ids"),py::arg("core_ids"),py::arg("weight_ids"),py::arg("weights"),py::arg("settings"),py::arg("meta"),py::arg("verify_anchors")=std::vector<Id>{},py::arg("verify")=false);}
}
