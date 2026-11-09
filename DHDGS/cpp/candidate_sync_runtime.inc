void fail_candidate(const std::shared_ptr<Record>& r,const std::string& error) {
  r->control->cancel();r->ticket->fail(error);cancel_candidate(r);
  auto message=candidate_header(r,"candidate_delivery_failed");message.map()["node_id"]=uint64_t(sid);message.map()["branch_id"]=at(r->state,"branch_id");message.map()["error"]=error;
  const auto& ep=array(at(r->state,"head_endpoint"));
  client->submit({cast<std::string>(ep[0]),cast<uint16_t>(ep[1])},message,[](dhwire::Result){});
}

// Network progress threads only enqueue bounded completion work in this mode.
void candidate_complete(const std::shared_ptr<Record>& r,dhwire::Result reply,std::function<void(dhwire::Result)> fn) {
  const auto queued=stamp();
  auto work=[this,r,reply=std::move(reply),fn=std::move(fn),queued]()mutable{
    const auto begun=stamp();
    r->ticket->network_event(M{{"candidate_callback_queue_ms",(begun-queued)/1e6},
      {"candidate_wire_to_callback_ms",reply.done_ns&&queued>=reply.done_ns?(queued-reply.done_ns)/1e6:0.}});
    try{fn(std::move(reply));}catch(const std::exception& e){fail_candidate(r,e.what());}
  };
  if(candidate_workers){if(!candidate_workers->submit(std::move(work)))fail_candidate(r,"Candidate completion queue full");}
  else work();
}
bool finish_candidate_search(const std::shared_ptr<Record>& r) {
  std::lock_guard<std::mutex> lock(r->sync->mu);auto& s=*r->sync;
  check(!s.starts_pending&&s.settlements.empty(),"Unsettled start at completion");
  auto late=r->actor->inbox->take();for(const auto& row:late)if(!r->control->claimed(cast<uint64_t>(array(row)[0]))&&s.missed.insert(cast<uint64_t>(array(row)[0])).second)++sync_exhausted;
  s.phase=3;s.remaining=0;s.search_done_ns=stamp();s.delivery_required=s.pending!=0;++s.version;
  return s.delivery_required;
}
void notify_candidate_delivery(const std::shared_ptr<Record>& r) {
  {std::lock_guard<std::mutex> lock(r->sync->mu);auto& s=*r->sync;
    if(!s.delivery_required||s.delivery_notified||s.pending)return;s.delivery_notified=true;}
  auto message=candidate_header(r,"candidate_delivery_done");
  {std::lock_guard<std::mutex> lock(r->sync->mu);message.map()["metrics"]=r->sync->delivery_metrics;message.map()["close_ms"]=(stamp()-r->sync->search_done_ns)/1e6;}message.map()["node_id"]=uint64_t(sid);message.map()["branch_id"]=at(r->state,"branch_id");
  const auto& e=array(at(r->state,"head_endpoint"));
  candidate_rpc(r,{cast<std::string>(e[0]),cast<uint16_t>(e[1])},message,[this,r](dhwire::Result reply){
    if(!reply.ok||!optional<bool>(reply.response,"ok",false))fail_candidate(r,"Candidate delivery closure failed: "+reply.error);
  });
}
// Query admission mutex held. A losing first start contributes candidates,
// never a second task or a second grant. Preserve the same retry receipt.
V merge_start_candidates(const std::shared_ptr<Record>& proposed) {
  auto c=proposed->context;const auto& state=proposed->state;const auto actor=c->canonical_actor;
  auto claim=c->starts.find(sid);std::shared_ptr<Record> r;
  if(actor){std::lock_guard<std::mutex> lock(mu);r=records.at(get<std::string>(state,"query_request_id")+"\n"+actor->id);}
  std::unique_lock<std::mutex> state_lock;if(r)state_lock=std::unique_lock<std::mutex>(r->sync->mu);
  auto& receipts=r?r->sync->receipts:claim->second.receipts;
  const auto key="start:"+get<std::string>(state,"branch_id");auto old=receipts.find(key);
  if(old!=receipts.end()){check(get<std::string>(old->second,"digest")==candidate_digest(state),"Conflicting first-start retry");return old->second;}
  check(!r||r->sync->phase<2||(r->sync->phase==3&&!r->sync->remaining),"First-start target frozen");
  A rows=array(at(state,"seeds"));for(const auto& x:array(at(state,"remote_frontier")))rows.push_back(x);
  auto inbox=r?r->actor->inbox:claim->second.inbox;uint64_t missed=0;
  const bool terminal=r&&!r->sync->remaining&&(r->sync->phase==3||(r->sync->phase==1&&!r->sync->waiting_starts))&&!r->sync->starts_pending&&r->sync->settlements.empty();
  if(terminal){for(const auto& row:rows)missed+=!r->control->claimed(cast<uint64_t>(array(row)[0]))&&r->sync->missed.insert(cast<uint64_t>(array(row)[0])).second;sync_exhausted+=missed;}
  else inbox->offer(rows,true);
  V answer=M{{"accepted",true},{"start_accepted",false},{"branch_id",r?r->actor->id:claim->second.id},{"terminal_budget",terminal},{"budget_exhausted",missed},{"digest",candidate_digest(state)}};
  receipts.emplace(key,answer);++sync_received;
  bool wake=false;
  if(r){++r->sync->version;if(r->sync->phase==1&&!r->sync->waiting_starts&&r->sync->remaining&&!inbox->empty()){
    r->sync->phase=0;r->continuation=std::move(r->sync->resume);wake=true;++sync_reactivated;}}
  if(state_lock.owns_lock())state_lock.unlock();
  if(wake)resume_candidate(r,M{});
  return answer;
}
void start_candidate_branch(const std::shared_ptr<Transfer>& t,V descriptor,size_t attempt=0) {
  auto r=t->record;
  auto callback=[this,t,r,descriptor,attempt](dhwire::Result response)mutable{
    candidate_complete(r,std::move(response),[this,t,r,descriptor,attempt](dhwire::Result reply)mutable{
      const bool ok=reply.ok&&optional<bool>(reply.response,"ok",false)&&optional<bool>(at(reply.response,"result"),"accepted",false);
      if(!ok&&attempt<retries&&!r->control->cancelled()){start_candidate_branch(t,std::move(descriptor),attempt+1);return;}
      if(!ok){fail_candidate(r,"Atomic first-start failed: "+reply.error);return;}
      const auto& result=at(reply.response,"result");const bool accepted=optional<bool>(result,"start_accepted",true);
      if(accepted){r->ticket->child(descriptor);r->ticket->transfer_done(M{},"");}
      r->ticket->network_event(M{{"peer_rpc_count",1.},{"peer_candidate_start_rpc_count",1.},{"peer_request_bytes",double(reply.request_bytes)},{"peer_response_bytes",double(reply.response_bytes)},
        {"candidate_start_rpc_count",1.},{"candidate_start_ack_ms",(reply.done_ns-reply.queued_ns)/1e6},{"candidate_start_conflict",accepted?0.:1.}});
      bool wake=false;
      {std::lock_guard<std::mutex> lock(r->sync->mu);auto& s=*r->sync;check(s.starts_pending>0,"Unbalanced start acknowledgement");--s.starts_pending;
        s.settlements.push_back(M{{"branch_id",at(descriptor,"branch_id")},{"accepted",accepted}});
        if(optional<bool>(result,"terminal_budget",false))s.exhausted_targets.insert(int(t->node));
        if(!accepted)s.remaining+=get<size_t>(descriptor,"work_grant");
        if(s.waiting_starts&&s.phase==1){s.waiting_starts=false;s.phase=0;r->continuation=std::move(s.resume);wake=true;}
      }
      if(wake)resume_candidate(r,M{});
    });
  };
  if(!client->submit(endpoints.at(t->node),t->message,callback)){dhwire::Result failure;failure.error="First-start outbox full";callback(std::move(failure));}
}
std::string candidate_digest(V value) {
  value.map().erase("request_id");auto bytes=dhwire::pack(value);unsigned char hash[SHA256_DIGEST_LENGTH];
  SHA256(bytes.data(),bytes.size(),hash);std::string out;const char* hex="0123456789abcdef";
  for(auto x:hash){out+=hex[x>>4];out+=hex[x&15];}return out;
}
void cancel_candidate(const std::shared_ptr<Record>& r) {
  if(!r->sync)return;std::function<void(V)> release;bool parked=false;
  {std::lock_guard<std::mutex> lock(r->sync->mu);
    if(r->sync->phase==1||r->sync->phase==2){r->sync->phase=3;release=std::move(r->sync->resume);parked=true;}}
  if(parked){r->status=2;r->search_context.reset();finish_region(r);}
}
// Included inside DataRuntime::Impl. All mailbox/phase locks are query-local.
V candidate_header(const std::shared_ptr<Record>& r,const std::string& op) {
  M m{{"version",uint64_t(1)},{"op",op}};
  for(auto k:{"query_request_id","token","epoch","signature"})m[k]=at(r->state,k);
  V out(std::move(m));dhpair::copy(r->state,out);return out;
}
void resume_candidate(const std::shared_ptr<Record>& r,V reply,bool final=false) {
  reply.map()["candidate_ready_ns"]=stamp();
  reply.map()["park_ms"]=(stamp()-r->candidate_park_at)/1e6;
  r->candidate_reply=std::move(reply);r->candidate_resume=true;r->candidate_ready=stamp();
  if(final){execute_record(r);return;} // Commit has no graph work; caller is a control handler.
  auto task=[this,r]{execute_record(r);};
  if(!(shared_compute?workers->submit_tail(task):workers->submit(task))){r->continuation={};r->ticket->fail("Candidate resume queue full");r->status=2;finish_region(r);}
}
void candidate_rpc(const std::shared_ptr<Record>& r,dhwire::Endpoint endpoint,V message,
                   std::function<void(dhwire::Result)> done,size_t attempt=0) {
  auto callback=[this,r,endpoint,message,done,attempt](dhwire::Result reply)mutable{
    if(!reply.ok&&attempt<retries&&!r->control->cancelled()){
      candidate_rpc(r,endpoint,std::move(message),std::move(done),attempt+1);return;
    }
    candidate_complete(r,std::move(reply),done);
  };
  if(!client->submit(endpoint,message,callback)){
    dhwire::Result reply;reply.error="Candidate control outbox full";callback(std::move(reply));
  }
}
void notify_candidate_idle(const std::shared_ptr<Record>& r) {
  V message;bool terminal=false;{
    std::lock_guard<std::mutex> lock(r->sync->mu);
    if(r->sync->phase!=1||r->sync->waiting_starts||r->sync->pending)return;
    if(!r->sync->remaining&&optional<bool>(at(at(config,"peer"),"candidate_sync"),"early_budget_report",true)){
      r->sync->phase=3;r->continuation=std::move(r->sync->resume);terminal=true;
    }
    message=candidate_header(r,"candidate_idle");
    auto& m=message.map();m["node_id"]=uint64_t(sid);m["branch_id"]=at(r->state,"branch_id");
    m["sync_version"]=r->sync->version;m["children"]=at(r->sync->idle,"children");
    m["parent_branch_id"]=at(r->state,"parent_branch_id");
  }
  if(terminal){resume_candidate(r,M{{"commit",true},{"budget_terminal",true}},true);return;}
  const auto& e=array(at(r->state,"head_endpoint"));
  candidate_rpc(r,{cast<std::string>(e[0]),cast<uint16_t>(e[1])},message,[r](dhwire::Result result){
    if(!result.ok||!optional<bool>(result.response,"ok",false))r->ticket->fail("Candidate idle notification failed: "+result.error);
  });
}
// sync->mu held. One in-flight packet per (branch,target); subsequent deltas
// remain in the query mailbox and are coalesced up to the existing wire bound.
// No query worker waits for an ACK, and no candidate is discarded.
V next_candidate_packet(const std::shared_ptr<Record>& r,size_t node) {
  auto& s=*r->sync;auto& out=s.outgoing.at(node);
  const auto cap=get<size_t>(at(at(config,"peer"),"candidate_sync"),"max_records");
  auto message=candidate_header(r,"peer_candidate_delta");auto& m=message.map();
  m["sender_branch"]=at(r->state,"branch_id");m["sequence"]=++s.sequence;
  for(auto k:{"admission_bound","bound_valid","expansions"})m[k]=at(out.metadata,k);
  A rows;rows.reserve(std::min(cap,out.rows.size()));
  while(rows.size()<cap&&!out.rows.empty()){rows.push_back(std::move(out.rows.front()));out.rows.pop_front();}
  m["rows"]=std::move(rows);++sync_sent;return message;
}
void send_candidate_packet(const std::shared_ptr<Record>& r,size_t node,V message,size_t attempt=0) {
  if(!client->submit(endpoints.at(node),message,[this,r,node,message,attempt](dhwire::Result response)mutable{
    candidate_complete(r,std::move(response),[this,r,node,message,attempt](dhwire::Result reply)mutable{
    bool retry=!reply.ok||!optional<bool>(reply.response,"ok",false);
    if(!retry&&optional<bool>(at(reply.response,"result"),"retry",false))retry=true;
    if(retry&&attempt<retries&&!r->control->cancelled()){send_candidate_packet(r,node,std::move(message),attempt+1);return;}
    M packet_metrics{{"peer_rpc_count",1.},{"peer_candidate_sync_rpc_count",1.},{"peer_request_bytes",double(reply.request_bytes)},{"peer_response_bytes",double(reply.response_bytes)},{"candidate_sync_rpc_count",1.},{"candidate_sync_tx_bytes",double(reply.request_bytes)},
      {"candidate_sync_rx_bytes",double(reply.response_bytes)},{"candidate_sync_ack_ms",(reply.done_ns-reply.queued_ns)/1e6},
      {"candidate_sync_completion_after_wire_ms",reply.done_ns?(stamp()-reply.done_ns)/1e6:0.}};
    r->ticket->network_event(packet_metrics);
    if(!retry){auto& result=at(reply.response,"result");
      if(optional<bool>(result,"terminal_budget",false)){std::lock_guard<std::mutex> lock(r->sync->mu);r->sync->exhausted_targets.insert(int(node));}
      packet_metrics["candidate_sync_budget_exhausted"]=double(optional<size_t>(result,"budget_exhausted",0));
      packet_metrics["candidate_sync_pruned"]=double(optional<size_t>(result,"pruned",0));
      r->ticket->network_event(M{{"candidate_sync_budget_exhausted",packet_metrics["candidate_sync_budget_exhausted"]},{"candidate_sync_pruned",packet_metrics["candidate_sync_pruned"]}});
    }
    V next;
    {std::lock_guard<std::mutex> lock(r->sync->mu);for(const auto& [k,v]:packet_metrics)if(k.rfind("candidate_sync_",0)==0)add(r->sync->delivery_metrics,k,number(v));
      auto out=r->sync->outgoing.find(node);
      if(!retry&&out!=r->sync->outgoing.end()&&!out->second.rows.empty()&&!r->control->cancelled())next=next_candidate_packet(r,node);
      else {--r->sync->pending;if(out!=r->sync->outgoing.end()){out->second.busy=false;out->second.rows.clear();}}
    }
    if(retry)fail_candidate(r,"Candidate delivery failed; no silent candidate loss");
    else if(!null(next))send_candidate_packet(r,node,std::move(next));
    else {notify_candidate_delivery(r);notify_candidate_idle(r);}
    });
  })){
    {std::lock_guard<std::mutex> lock(r->sync->mu);--r->sync->pending;}
    fail_candidate(r,"Candidate outbox capacity exceeded");
  }
}
void send_candidates(const std::shared_ptr<Record>& r,size_t node,V payload) {
  const auto& cfg=at(at(config,"peer"),"candidate_sync");
  const auto cap=get<size_t>(cfg,"max_records");
  auto rows=std::move(payload.map().at("rows").data);auto& candidates=std::get<A>(rows);
  if(optional<bool>(cfg,"coalesce_pending",false)){
    V next;const auto count=candidates.size();
    {std::lock_guard<std::mutex> lock(r->sync->mu);auto& s=*r->sync;check(s.phase==0,"Send from non-running branch");
      auto& out=s.outgoing[node];check(out.rows.size()+count<=optional<size_t>(cfg,"max_buffered_records",4096),"Candidate coalescing mailbox full");
      out.metadata=std::move(payload);for(auto& row:candidates)out.rows.push_back(std::move(row));
      if(!out.busy&&!out.rows.empty()){out.busy=true;++s.pending;next=next_candidate_packet(r,node);}
    }
    if(!null(next))send_candidate_packet(r,node,std::move(next));
    return;
  }
  for(size_t off=0;off<candidates.size();off+=cap){
    V message=candidate_header(r,"peer_candidate_delta");auto& m=message.map();
    m["sender_branch"]=at(r->state,"branch_id");m["rows"]=A(candidates.begin()+off,candidates.begin()+std::min(candidates.size(),off+cap));
    m["admission_bound"]=at(payload,"admission_bound");m["bound_valid"]=at(payload,"bound_valid");m["expansions"]=at(payload,"expansions");
    {std::lock_guard<std::mutex> lock(r->sync->mu);check(r->sync->phase==0,"Send from non-running branch");
      m["sequence"]=++r->sync->sequence;++r->sync->pending;}
    ++sync_sent;send_candidate_packet(r,node,std::move(message));
  }
}
void park_candidate(const std::shared_ptr<Record>& r) {
  r->candidate_park_at=stamp();r->candidate_park=false;auto request=std::move(r->candidate_request);
  if(get<std::string>(request,"kind")=="claim"){
    auto message=candidate_header(r,"peer_candidate_claim");message.map()["owner"]=at(request,"claim_id");
    const auto target=get<size_t>(request,"target");++sync_claims;
    candidate_rpc(r,endpoints.at(target),message,[this,r,target](dhwire::Result reply){
      V answer;if(reply.ok&&optional<bool>(reply.response,"ok",false))answer=at(reply.response,"result");
      else answer=M{{"error","Unique-branch reservation failed: "+reply.error}};
      r->ticket->network_event(M{{"peer_rpc_count",1.},{"peer_candidate_claim_rpc_count",1.},{"peer_request_bytes",double(reply.request_bytes)},{"peer_response_bytes",double(reply.response_bytes)},{"candidate_claim_rpc_count",1.},{"candidate_claim_ms",(reply.done_ns-reply.queued_ns)/1e6}});
      if(has(answer,"claimed")&&!get<bool>(answer,"claimed")){++sync_claim_conflicts;r->context->visits->open(dhpair::region(at(config,"peer"),r->state,target));}
      resume_candidate(r,std::move(answer));
    });
    return;
  }
  bool wake=false;
  {
    std::lock_guard<std::mutex> lock(r->sync->mu);auto& s=*r->sync;
    s.waiting_starts=get<std::string>(request,"kind")=="settle";
    s.remaining=get<size_t>(request,"remaining");s.idle=std::move(request);s.resume=std::move(r->continuation);s.phase=1;++s.version;
    if(s.waiting_starts){if(!s.settlements.empty()||!s.starts_pending){s.waiting_starts=false;s.phase=0;r->continuation=std::move(s.resume);wake=true;}}
    else if(!s.remaining){auto late=r->actor->inbox->take();for(auto& row:late)if(!r->control->claimed(cast<uint64_t>(array(row)[0]))&&s.missed.insert(cast<uint64_t>(array(row)[0])).second)++sync_exhausted;}
    else if(!r->actor->inbox->empty()){s.phase=0;r->continuation=std::move(s.resume);wake=true;++sync_reactivated;}
  }
  if(wake)resume_candidate(r,M{});else notify_candidate_idle(r);
}
dhwire::Reply handle_candidate(const V& request) {
  check(candidate_sync&&(!dhpair::scoped(at(config,"peer"))||dhpair::member(request)||has(request,"branch_id")),"Candidate protocol disabled or outside replica pair");
  check(get<std::string>(request,"epoch")==get<std::string>(meta,"epoch")&&get<std::string>(request,"signature")==signature,"Candidate configuration mismatch");
  std::shared_ptr<QueryContext> c;{std::lock_guard<std::mutex> lock(mu);check(!stopping,"Runtime closed");c=context(request);}
  const auto op=get<std::string>(request,"op");V result;std::shared_ptr<Record> r;bool wake=false,commit=false;
  {
    std::lock_guard<std::mutex> admission(c->admission_mu);
    auto actor=c->canonical_actor;auto claim=c->starts.find(sid);
    if(op=="peer_candidate_delta"&&!actor&&claim==c->starts.end()&&optional<bool>(at(at(config,"peer"),"candidate_sync"),"nonblocking_start",false)){
      QueryContext::Claim x;x.inbox=std::make_shared<dhsearch::RegionInbox>(get<size_t>(at(config,"search"),"frontier_size"));claim=c->starts.emplace(sid,std::move(x)).first;
    }
    if(op=="peer_candidate_claim"){
      const auto owner=get<std::string>(request,"owner");check(owner.size()<=512,"Invalid claim identity");
      if(actor)result=M{{"claimed",false},{"branch_id",actor->id}};
      else if(claim!=c->starts.end()&&!claim->second.id.empty())result=M{{"claimed",claim->second.owner==owner},{"branch_id",claim->second.id}};
      else {if(claim==c->starts.end()){QueryContext::Claim next;next.inbox=std::make_shared<dhsearch::RegionInbox>(get<size_t>(at(config,"search"),"frontier_size"));claim=c->starts.emplace(sid,std::move(next)).first;}
        claim->second.owner=owner;claim->second.id=identifier();result=M{{"claimed",true},{"branch_id",claim->second.id}};c->visits->announce(dhpair::region(at(config,"peer"),request,sid));}
    }else {
      if(actor) {std::lock_guard<std::mutex> lock(mu);r=records.at(get<std::string>(request,"query_request_id")+"\n"+actor->id);}
      if(op=="peer_candidate_delta"){
        check(bool(r)||claim!=c->starts.end(),"Candidate target has no reserved branch");
        const auto& rows=array(at(request,"rows"));check(rows.size()<=get<size_t>(at(at(config,"peer"),"candidate_sync"),"max_records"),"Candidate packet bound exceeded");
        const auto key=get<std::string>(request,"sender_branch")+":"+std::to_string(get<size_t>(request,"sequence"));
        std::unique_lock<std::mutex> state;if(r)state=std::unique_lock<std::mutex>(r->sync->mu);
        auto& receipts=r?r->sync->receipts:claim->second.receipts;auto prior=receipts.find(key);
        if(prior!=receipts.end()){check(get<std::string>(prior->second,"digest")==candidate_digest(request),"Conflicting candidate retry");result=prior->second;}
        else if(r&&r->sync->phase>=2&&!(r->sync->phase==3&&!r->sync->remaining))result=M{{"retry",r->sync->phase==2},{"terminal",r->sync->phase==3}};
        else {
          auto inbox=r?r->actor->inbox:claim->second.inbox;uint64_t missed=0,pruned=inbox->pruned.load();
          if(r&&r->sync->remaining==0&&(r->sync->phase==3||(r->sync->phase==1&&!r->sync->waiting_starts))&&!r->sync->starts_pending&&r->sync->settlements.empty()){for(auto& row:rows)missed+=!r->control->claimed(cast<uint64_t>(array(row)[0]))&&r->sync->missed.insert(cast<uint64_t>(array(row)[0])).second;sync_exhausted+=missed;}
          else inbox->offer(rows,true);
          result=M{{"accepted",true},{"terminal_budget",bool(r&&r->sync->remaining==0&&(r->sync->phase==3||(r->sync->phase==1&&!r->sync->waiting_starts))&&!r->sync->starts_pending&&r->sync->settlements.empty())},{"budget_exhausted",missed},{"pruned",inbox->pruned.load()-pruned},{"sequence",at(request,"sequence")}};
          check(receipts.size()<16*(get<size_t>(at(config,"search"),"max_expansions")+endpoints.size()),"Candidate sequence capacity exceeded");
          result.map()["digest"]=candidate_digest(request);receipts.emplace(key,result);++sync_received;
          if(r){++r->sync->version;if(r->sync->phase==1&&!r->sync->waiting_starts&&r->sync->remaining&&!inbox->empty()){
            r->sync->phase=0;r->continuation=std::move(r->sync->resume);wake=true;++sync_reactivated;}}
        }
      }else {
        check(bool(r)&&get<std::string>(request,"branch_id")==get<std::string>(r->state,"branch_id"),"Unknown canonical branch");
        std::lock_guard<std::mutex> state(r->sync->mu);auto& s=*r->sync;const auto epoch=get<uint64_t>(request,"probe_epoch");
        if(op=="peer_candidate_prepare"){
          bool ready=(s.phase==1||s.phase==2)&&!s.waiting_starts&&!s.starts_pending&&s.pending==0&&s.version==get<uint64_t>(request,"sync_version")&&r->actor->inbox->empty();
          if(ready){s.phase=2;s.frozen_epoch=epoch;}result=M{{"prepared",ready}};
        }else if(op=="peer_candidate_abort"){
          if(s.phase==2&&s.frozen_epoch==epoch)s.phase=1;
          result=M{{"idle",s.phase==1&&s.pending==0},{"sync_version",s.version}};
        }else if(op=="peer_candidate_commit"){
          check(s.phase==3||(s.phase==2&&s.frozen_epoch==epoch),"Commit without prepare");
          if(s.phase!=3){s.phase=3;r->continuation=std::move(s.resume);commit=true;}result=M{{"committed",true}};
        }else throw std::invalid_argument("Unknown candidate operation");
      }
    }
  }
  if(wake)resume_candidate(r,M{});
  else if(r&&op=="peer_candidate_delta")notify_candidate_idle(r);
  if(commit)resume_candidate(r,M{{"commit",true}},true);
  return dhwire::Reply{M{{"ok",true},{"result",std::move(result)}}};
}
