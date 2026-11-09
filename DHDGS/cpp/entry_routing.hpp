#pragma once
// First-hop placement only. No new search branches, graph changes or budget grants.
#include "value_utils.hpp"
#include <atomic>
#include <cmath>
#include <memory>
#include <vector>
#include <algorithm>
#include <stdexcept>

namespace dhentry {
using namespace dhvalue;
struct Options {
  std::string mode="fixed";
  bool telemetry=false;
  uint64_t poll_ms=20,ttl_ms=100;
  double temperature_ms=5.,prior_ms=5.,cpu_threshold=.9,cpu_penalty_ms=2.;
  static Options parse(const V& v) {
    Options o;
    o.mode=optional<std::string>(v,"mode","fixed");
    o.telemetry=optional<bool>(v,"telemetry",false);
    o.poll_ms=optional<uint64_t>(v,"poll_ms",20);o.ttl_ms=optional<uint64_t>(v,"ttl_ms",100);
    o.temperature_ms=optional<double>(v,"temperature_ms",5.);o.prior_ms=optional<double>(v,"prior_ms",5.);
    o.cpu_threshold=optional<double>(v,"cpu_threshold",.9);o.cpu_penalty_ms=optional<double>(v,"cpu_penalty_ms",2.);
    if((o.mode!="fixed"&&o.mode!="least_queue"&&o.mode!="cost")||
       (!o.telemetry&&o.mode!="fixed")||o.poll_ms<1||o.ttl_ms<o.poll_ms||o.ttl_ms>60000||
       !std::isfinite(o.temperature_ms)||o.temperature_ms<=0||!std::isfinite(o.prior_ms)||o.prior_ms<=0||
       !std::isfinite(o.cpu_threshold)||o.cpu_threshold<0||!std::isfinite(o.cpu_penalty_ms)||o.cpu_penalty_ms<0)
      throw std::invalid_argument("Invalid entry_routing options");
    return o;
  }
};
struct Load {
  uint64_t received=0,sampled=0,completed=0,cpu_ns=0,wall_ns=0;
  size_t queued=0,active=0,workers=1;
  double service_ms=5.,util=0,rpc_ms=0;
};
struct Cost {
  struct Work {std::atomic<uint64_t> cpu_ns{0},visits{0};};
  std::atomic<uint64_t> samples{0};
  std::vector<std::unique_ptr<Work>> nodes;
  explicit Cost(size_t n){for(size_t i=0;i<n;++i)nodes.emplace_back(new Work);}
  void observe(const V& reports) {
    std::vector<uint64_t> cpu(nodes.size(),0);std::vector<bool> visited(nodes.size(),false);
    for(const auto& r:array(reports)){if(optional<std::string>(r,"region_admission","")=="source_reserved_not_started")continue;auto i=get<size_t>(r,"node_id");if(i>=nodes.size())continue;
      const auto& stats=at(r,"stats");double ms=optional<double>(stats,"server_thread_cpu_ms",0.);
      if(std::isfinite(ms)&&ms>=0)cpu[i]+=uint64_t(ms*1e6);visited[i]=true;}
    for(size_t i=0;i<nodes.size();++i){nodes[i]->cpu_ns+=cpu[i];if(visited[i])++nodes[i]->visits;}
    ++samples;
  }
};
struct Node {
  std::shared_ptr<const Load> load=std::make_shared<const Load>();
  std::atomic<uint64_t> outstanding{0},selected{0},polls{0},errors{0},poll_ns{0};
  std::atomic<bool> polling{false};
};
struct Ticket {
  std::shared_ptr<Node> node;
  explicit Ticket(std::shared_ptr<Node> n):node(std::move(n)){++node->outstanding;}
  ~Ticket(){--node->outstanding;}
};
struct Candidate {size_t target;std::shared_ptr<Cost> cost;};
struct Choice {int index=-1;double score=0,age_ms=0;size_t queued=0;bool stale=false;};
class Policy {
public:
  std::vector<std::shared_ptr<Node>> nodes;
  std::vector<std::shared_ptr<Cost>> sources;
  std::shared_ptr<const Options> options=std::make_shared<const Options>();
  std::atomic<uint64_t> choices{0},stale_fallbacks{0},selection_ns{0};
  void init(size_t n,const V& v){options=std::make_shared<const Options>(Options::parse(v));
    for(size_t i=0;i<n;++i){nodes.emplace_back(new Node);sources.emplace_back(new Cost(n));}}
  void configure(const V& v){auto next=Options::parse(v),old=*std::atomic_load(&options);
    if(next.telemetry!=old.telemetry||next.poll_ms!=old.poll_ms)throw std::invalid_argument("Telemetry settings require restart");
    std::atomic_store(&options,std::make_shared<const Options>(next));}
  void update(size_t i,const V& value,uint64_t received,double rpc_ms){
    auto& n=*nodes.at(i);auto old=std::atomic_load(&n.load);auto v=std::make_shared<Load>();
    v->received=received;v->sampled=get<uint64_t>(value,"sampled_ns");
    v->cpu_ns=get<uint64_t>(value,"cpu_ns");v->wall_ns=get<uint64_t>(value,"wall_ns");v->completed=get<uint64_t>(value,"completed");
    v->workers=get<size_t>(value,"workers");v->active=get<size_t>(value,"active");v->queued=get<size_t>(value,"queued");
    if(!v->workers||rpc_ms<0||!std::isfinite(rpc_ms))throw std::invalid_argument("Invalid routing load");
    v->rpc_ms=rpc_ms;v->service_ms=old->service_ms;v->util=old->util;
    if(old->sampled&&v->sampled>old->sampled&&v->cpu_ns>=old->cpu_ns&&v->wall_ns>=old->wall_ns&&v->completed>=old->completed){
      const auto count=v->completed-old->completed;
      if(count)v->service_ms=.8*old->service_ms+.2*(v->wall_ns-old->wall_ns)/1e6/count;
      v->util=.8*old->util+.2*double(v->cpu_ns-old->cpu_ns)/(v->sampled-old->sampled)/v->workers;
    }
    std::atomic_store(&n.load,std::shared_ptr<const Load>(v));++n.polls;n.poll_ns+=uint64_t(rpc_ms*1e6);
  }
  double backlog(size_t i,const Load& l)const{
    // Two views of the same backlog: never ADD queued and outstanding roots.
    // Outstanding remains until final report, so this is a conservative proxy,
    // not an exact count of unacknowledged or runnable work.
    double outstanding=nodes[i]->outstanding.load();
    return std::max(double(l.queued),std::max(0.,outstanding-l.workers))*l.service_ms/l.workers;
  }
  Choice choose(const std::vector<Candidate>& candidates,uint64_t now,double draw) {
    auto o=std::atomic_load(&options);Choice out;++choices;
    if(candidates.empty()||o->mode=="fixed")return out;
    std::vector<std::shared_ptr<const Load>> loads;for(auto& n:nodes)loads.push_back(std::atomic_load(&n->load));
    auto fresh=[&](size_t i){const auto& l=*loads.at(i);return l.received&&now>=l.received&&(now-l.received)/1e6+l.rpc_ms<=o->ttl_ms;};
    if(!fresh(candidates[0].target)){++stale_fallbacks;out.stale=true;return out;}
    std::vector<double> scores(candidates.size(),INFINITY);double best=INFINITY;
    for(size_t j=0;j<candidates.size();++j){const auto& c=candidates[j];if(!fresh(c.target))continue;const auto& l=*loads[c.target];
      double score=double(l.queued);
      if(o->mode=="cost"){
        score=backlog(c.target,l)+l.rpc_ms*.5;
        auto count=c.cost?c.cost->samples.load():0;
        if(count){for(size_t k=0;k<nodes.size();++k){auto& w=*c.cost->nodes[k];
          score+=double(w.cpu_ns.load())/count/1e6;
          if(k!=c.target&&w.visits.load())score+=double(w.visits.load())/count*(fresh(k)?backlog(k,*loads[k]):o->ttl_ms);}}
        else score+=o->prior_ms;
        score+=o->cpu_penalty_ms*std::clamp(l.util-o->cpu_threshold,0.,1.);
      }
      scores[j]=score;best=std::min(best,score);
    }
    double sum=0;std::vector<double> weights;for(auto s:scores){double w=std::isfinite(s)?
      (o->mode=="least_queue"?double(s==best):std::exp(std::max(-60.,-(s-best)/o->temperature_ms))):0.;weights.push_back(w);sum+=w;}
    double position=std::clamp(draw,0.,std::nextafter(1.,0.))*sum;
    for(size_t j=0;j<weights.size();++j)if(weights[j]>0){if(position<weights[j]){out.index=int(j);break;}position-=weights[j];}
    if(out.index<0){++stale_fallbacks;out.stale=true;return out;}
    const auto& l=*loads[candidates[out.index].target];out.score=scores[out.index];out.queued=l.queued;out.age_ms=(now-l.received)/1e6+l.rpc_ms;
    return out;
  }
  V metrics(uint64_t now)const{A rows;for(size_t i=0;i<nodes.size();++i){auto& n=*nodes[i];auto l=std::atomic_load(&n.load);
    rows.emplace_back(M{{"node",uint64_t(i)},{"queued",uint64_t(l->queued)},{"active",uint64_t(l->active)},{"workers",uint64_t(l->workers)},
      {"snapshot_age_ms",l->received?V((now-l->received)/1e6+l->rpc_ms):V()},{"rpc_ms",l->rpc_ms},{"service_ms",l->service_ms},{"util",l->util},
      {"outstanding_roots",n.outstanding.load()},{"selected",n.selected.load()},{"polls",n.polls.load()},{"errors",n.errors.load()},{"poll_ns",n.poll_ns.load()}});}
    return M{{"mode",std::atomic_load(&options)->mode},{"choices",choices.load()},{"stale_fallbacks",stale_fallbacks.load()},
      {"selection_ns",selection_ns.load()},{"nodes",std::move(rows)}};}
};
}
