#pragma once
#include <map>
#include <unordered_set>
#include <unordered_map>

namespace dhplan {
// Match Python 3.12 float sum's compensated accumulation; incremental += in
// the reference planner remains ordinary double addition.
struct Sum {double hi=0,lo=0;void add(double x){double t=hi+x;lo+=std::abs(hi)>=std::abs(x)?(hi-t)+x:(x-t)+hi;hi=t;}double value()const{return hi+lo;}};
struct Branch{size_t node;double cpu;std::vector<uint64_t> ids;};
struct Observation{uint64_t id,entry;double score;std::vector<Branch> branches;};
inline py::dict candidate(py::list observations,size_t source,size_t target,py::dict settings,py::object heat){
  std::vector<Observation> rows;rows.reserve(observations.size());
  for(auto handle:observations){auto q=py::reinterpret_borrow<py::dict>(handle);
    if(q["source"].cast<size_t>()!=source||!q["replica"].is_none()||q["query_id"].is_none())continue;
    Observation o{q["query_id"].cast<uint64_t>(),q["entry"].cast<uint64_t>(),q["score"].cast<double>(),{}};
    for(auto h:q["branches"]){auto b=py::reinterpret_borrow<py::dict>(h);auto node=b["node"].cast<size_t>();o.branches.push_back({node,b["cpu_ms"].cast<double>(),node==source?b["expanded_ids"].cast<std::vector<uint64_t>>():std::vector<uint64_t>{}});}
    rows.push_back(std::move(o));}
  const size_t min_entry=settings["min_entry_queries"].cast<size_t>(),min_group=settings["min_group_queries"].cast<size_t>(),max_core=settings["max_core_rows"].cast<size_t>();
  const double coverage=settings["min_coverage"].cast<double>(),portals=settings.contains("portal_budget_fraction")?settings["portal_budget_fraction"].cast<double>():0.;
  const std::string policy=settings.contains("selection_policy")?settings["selection_policy"].cast<std::string>():"entry_worksets";
  const bool weighted=!heat.is_none()&&policy!="cpu_worksets";
  std::unordered_map<uint64_t,double> scores;if(weighted)scores=heat.cast<std::unordered_map<uint64_t,double>>();
  std::vector<uint64_t> ids,entries,training;size_t ranked_count=0,attached=0;double min_score=0,max_score=0,cpu=0;
  {
    py::gil_scoped_release release;
    struct Group {uint64_t entry;std::vector<size_t> observations;std::unordered_map<uint64_t,size_t> positions;std::unordered_set<uint64_t> ids;double source_cpu=0,benefit=0;};
    std::map<uint64_t,Group> groups;
    for(size_t i=0;i<rows.size();++i){auto& q=rows[i];auto& g=groups[q.entry];g.entry=q.entry;
      auto [it,inserted]=g.positions.emplace(q.id,g.observations.size());if(inserted)g.observations.push_back(i);else g.observations[it->second]=i;}
    std::vector<Group*> ranked;
    for(auto& [entry,g]:groups){g.ids.insert(entry);double total=0.;
      for(auto i:g.observations){Sum sum;for(auto& b:rows[i].branches){sum.add(b.cpu);if(b.node==source){g.ids.insert(b.ids.begin(),b.ids.end());g.source_cpu+=b.cpu;}}total+=sum.value();}
      if(g.observations.size()<min_entry||g.source_cpu==0||g.source_cpu/std::max(total,1e-9)<coverage)continue;
      double benefit=g.source_cpu;if(weighted){Sum sum;std::vector<uint64_t> ordered(g.ids.begin(),g.ids.end());std::sort(ordered.begin(),ordered.end());for(auto id:ordered){auto it=scores.find(id);if(it!=scores.end())sum.add(it->second);}benefit=sum.value();}
      if(benefit<=0)continue;g.benefit=benefit/g.ids.size();ranked.push_back(&g);}
    ranked_count=ranked.size();std::sort(ranked.begin(),ranked.end(),[](auto a,auto b){return a->benefit!=b->benefit?a->benefit>b->benefit:a->entry<b->entry;});
    std::unordered_set<uint64_t> selected,chosen;std::vector<size_t> used;
    const size_t cap=std::max(size_t(1),size_t(max_core*(1-portals)));
    for(auto g:ranked){size_t added=0;for(auto id:g->ids)added+=!selected.count(id);if(selected.size()+added>cap)continue;
      selected.insert(g->ids.begin(),g->ids.end());chosen.insert(g->entry);used.insert(used.end(),g->observations.begin(),g->observations.end());}
    if(portals&&!selected.empty()){
      std::vector<Group*> all;for(auto& [entry,g]:groups)all.push_back(&g);
      // Python portal ordering sums branch CPU directly, without per-query grouping.
      for(auto g:all){Sum sum;for(auto i:g->observations)for(auto& b:rows[i].branches)if(b.node==source)sum.add(b.cpu);g->source_cpu=sum.value();}
      std::sort(all.begin(),all.end(),[](auto a,auto b){return a->source_cpu!=b->source_cpu?a->source_cpu>b->source_cpu:a->entry<b->entry;});
      for(auto g:all){if(chosen.count(g->entry))continue;if(!selected.count(g->entry)&&selected.size()>=max_core)continue;
        selected.insert(g->entry);chosen.insert(g->entry);used.insert(used.end(),g->observations.begin(),g->observations.end());++attached;}}
    std::set<uint64_t> qids;min_score=std::numeric_limits<double>::infinity();max_score=-min_score;
    Sum sum;for(auto i:used){const auto& q=rows[i];qids.insert(q.id);min_score=std::min(min_score,q.score);max_score=std::max(max_score,q.score);for(auto& b:q.branches)if(b.node==source)sum.add(b.cpu);}cpu=sum.value();
    if(qids.size()<min_group)throw std::invalid_argument("Insufficient independent, entry-eligible hotspot traces");
    ids.assign(selected.begin(),selected.end());entries.assign(chosen.begin(),chosen.end());training.assign(qids.begin(),qids.end());
    std::sort(ids.begin(),ids.end());std::sort(entries.begin(),entries.end());
  }
  double slack=std::max((max_score-min_score)*.25,1e-6);
  py::dict result;result["source"]=source;result["target"]=target;result["entries"]=entries;result["ids"]=ids;result["training_query_ids"]=training;
  result["min_score"]=min_score-slack;result["max_score"]=max_score+slack;result["sampled_cpu_ms"]=cpu;result["selection_policy"]=policy;result["candidate_entries"]=ranked_count;result["attached_portals"]=attached;return result;
}
}
