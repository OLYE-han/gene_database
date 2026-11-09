// Only termination metadata reaches the head; candidate payloads stay peer-to-peer.
struct SyncProbe {uint64_t epoch;size_t pending=0;bool failed=false;std::map<std::string,V> states;std::set<std::string> rejected;};
V sync_command(const std::shared_ptr<Query>& q,const std::string& op,const std::string& bid,const V& row,uint64_t epoch){
  return M{{"version",uint64_t(1)},{"op",op},{"query_request_id",q->id},{"token",q->token},
    {"epoch",at(q->state,"epoch")},{"signature",at(q->state,"signature")},{"branch_id",bid},
    {"sync_version",at(row,"sync_version")},{"probe_epoch",epoch}};
}
void finish_sync_probe(const std::shared_ptr<Query>& q,const std::shared_ptr<SyncProbe>& probe) {
  const bool abort=probe->failed;probe->pending=probe->states.size();
  add(q->route_profile,abort?"candidate_quiescence_aborts":"candidate_quiescence_commits",1);
  for(auto& [bid,row]:probe->states){
    auto op=abort?"peer_candidate_abort":"peer_candidate_commit";
    if(!leases->submit(endpoints.at(get<size_t>(row,"node_id")),sync_command(q,op,bid,row,probe->epoch),
      [this,q,probe,abort,bid](dhwire::Result reply){
        std::lock_guard<std::mutex> lock(q->mu);record_rpc(q->control,reply);
        if(!reply.ok||!optional<bool>(reply.response,"ok",false))q->error="Candidate termination command failed";
        if(abort&&probe->rejected.count(bid)){
          auto i=q->sync_idle.find(bid);
          if(i!=q->sync_idle.end()&&get<uint64_t>(i->second,"sync_version")<=get<uint64_t>(probe->states.at(bid),"sync_version"))q->sync_idle.erase(i);
        }
        if(--probe->pending==0){q->sync_probing=false;
          add(q->route_profile,"candidate_quiescence_ms",(stamp()-q->sync_probe_start)/1e6);
          if(abort)maybe_sync_probe(q);
        }
        maybe_merge(q);
      })) {q->error="Candidate termination queue full";maybe_merge(q);}
  }
}
void maybe_sync_probe(const std::shared_ptr<Query>& q) { // q->mu held
  if(q->sync_probing||q->finished||q->merging||!q->error.empty())return;
  std::vector<std::string> todo{q->root};std::set<std::string> visited;
  while(!todo.empty()){
    auto bid=todo.back();todo.pop_back();if(!visited.insert(bid).second)return;
    auto report=q->reports.find(bid);auto idle=q->sync_idle.find(bid);
    if(report==q->reports.end()&&idle==q->sync_idle.end())return;
    const V& state=report!=q->reports.end()?report->second:idle->second;
    if(report!=q->reports.end()&&optional<bool>(report->second,"candidate_delivery_required",false)&&!q->sync_delivered.count(bid))return;
    for(auto& child:array(at(state,"children")))todo.push_back(get<std::string>(child,"branch_id"));
  }
  if(visited.size()!=q->sync_idle.size()+q->reports.size()||q->sync_idle.empty())return;
  auto probe=std::make_shared<SyncProbe>();probe->epoch=++q->sync_epoch;probe->states=q->sync_idle;probe->pending=probe->states.size();
  q->sync_probing=true;q->sync_probe_start=stamp();add(q->route_profile,"candidate_quiescence_probes",1);
  for(auto& [bid,row]:probe->states){
    if(!leases->submit(endpoints.at(get<size_t>(row,"node_id")),sync_command(q,"peer_candidate_prepare",bid,row,probe->epoch),
      [this,q,probe,bid](dhwire::Result reply){
        std::lock_guard<std::mutex> lock(q->mu);record_rpc(q->control,reply);
        bool ready=reply.ok&&optional<bool>(reply.response,"ok",false)&&get<bool>(at(reply.response,"result"),"prepared");
        if(!ready){probe->failed=true;probe->rejected.insert(bid);}
        if(--probe->pending==0)finish_sync_probe(q,probe);
      })) {q->error="Candidate prepare queue full";maybe_merge(q);}
  }
}
dhwire::Reply receive_candidate_idle(V message,size_t bytes) {
  check(has(at(config,"peer"),"candidate_sync")&&get<bool>(at(at(config,"peer"),"candidate_sync"),"enabled"),"Candidate sync disabled");
  std::shared_ptr<Query> q;{std::lock_guard<std::mutex> lock(mu);auto i=queries.find(get<std::string>(message,"query_request_id"));check(i!=queries.end(),"Unknown candidate query");q=i->second;}
  std::lock_guard<std::mutex> lock(q->mu);check(get<std::string>(message,"token")==q->token,"Invalid candidate token");
  if(!q->finished&&!q->merging&&!q->reports.count(get<std::string>(message,"branch_id"))){
    auto bid=get<std::string>(message,"branch_id");const auto node=get<size_t>(message,"node_id");check(node<node_count,"Invalid candidate node");track_node(q,node);
    auto i=q->sync_idle.find(bid);if(i==q->sync_idle.end()||get<uint64_t>(message,"sync_version")>=get<uint64_t>(i->second,"sync_version"))q->sync_idle[bid]=std::move(message);
    maybe_sync_probe(q);
  }
  add(q->control,"rpc_count",1);add(q->control,"request_bytes",double(bytes));
  dhwire::Reply out{M{{"ok",true}}};out.sent=[q](uint64_t,size_t bytes){std::lock_guard<std::mutex> lock(q->mu);add(q->control,"response_bytes",double(bytes));};return out;
}

dhwire::Reply receive_candidate_delivery(V message,size_t bytes) {
  std::shared_ptr<Query> q;{std::lock_guard<std::mutex> lock(mu);auto i=queries.find(get<std::string>(message,"query_request_id"));check(i!=queries.end(),"Unknown candidate query");q=i->second;}
  std::lock_guard<std::mutex> lock(q->mu);check(get<std::string>(message,"token")==q->token,"Invalid delivery token");
  const auto bid=get<std::string>(message,"branch_id");const auto node=get<size_t>(message,"node_id");check(node<node_count&&!bid.empty()&&bid.size()<=256,"Invalid delivery identity");
  if(get<std::string>(message,"op")=="candidate_delivery_failed"){
    if(!q->finished&&!q->merging){q->error=get<std::string>(message,"error");maybe_merge(q);}return dhwire::Reply{M{{"ok",true}}};
  }
  check(q->sync_delivered.count(bid)||q->sync_delivered.size()<node_count,"Too many delivery closures");
  if(!q->finished&&!q->merging&&q->sync_delivered.insert(bid).second){q->sync_delivery_metrics[bid]=at(message,"metrics");add(q->route_profile,"candidate_delivery_close_ms",optional<double>(message,"close_ms",0));track_node(q,node);q->trace["delivery_done_ns:"+bid]=stamp();maybe_sync_probe(q);maybe_merge(q);}
  add(q->control,"rpc_count",1);add(q->control,"request_bytes",double(bytes));
  return dhwire::Reply{M{{"ok",true}}};
}
