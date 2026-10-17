"""Data-node search branch. No router RPC is used for graph expansion."""
from __future__ import annotations

import math
import time
import uuid

import numpy as np

from .scheduler import Action, Candidate, Policy, remote_probability

COUNTERS = ('file_read_bytes','pread_calls','cache_hits','scanned_edges','rabitq_evaluations',
            'exact_distance_evaluations','pread_ms','rabitq_ms','exact_distance_ms','service_ms',
            'async_read_requests','io_batch_wall_ms','io_capacity_wait_ms','io_queue_wait_ms',
            'io_read_residence_ms','query_ready_queue_ms','query_compute_slice_wall_ms',
            'query_io_wait_ms','query_resume_count','query_transfer_finalize_wait_ms',
            'query_branch_wall_ms','query_state_other_ms')


def branch_steps(state, shard, directory, c, spawn, cancelled):
    """Only this actor mutates this branch. Child grants are deducted before send.

    spawn is an asynchronous peer transfer enqueue, never a head scheduling RPC.
    Branch manifests seal the child list, allowing the head to detect termination
    without receiving the frontier, visited set or per-batch candidates.
    """
    started = time.perf_counter()
    sid = shard.meta['shard_id']
    visited_nodes = list(state['visited_nodes'])
    if not visited_nodes or visited_nodes[-1]!=sid or len(set(visited_nodes))!=len(visited_nodes):
        raise ValueError('Invalid visited-node path')
    visited_set = set(visited_nodes)
    can_forward = True  # Exactly one successor owns the right to visit new nodes.
    cfg, policy_cfg = c['search'], c['scheduler']
    q = np.frombuffer(state['query_f32'],dtype='<f4')
    remaining = state['work_grant']
    slots = state['branch_slots']
    frontier = {int(row[0]): list(row[1:]) for row in [*state['seeds'],*state.get('remote_frontier',[])]}
    # item: distance, uncertainty, owners, parent, depth, cross_depth, root, age_ms
    seen = set(map(int,state['seen']))
    results, expanded_ids, paths, children, events = {}, [], [], [], []
    stats = {key:0 for key in COUNTERS}
    stats.update(batches=0,frontier_dropped=0,fallback_entries=0,improved_topk_batches=0,
                 visited_node_candidates_dropped=0,remote_candidates_dropped_after_forward=0)
    service_per_node = policy_cfg['local_batch_ms']/cfg['local_beam']
    policy = Policy(policy_cfg['kappa'],policy_cfg['exploration'],policy_cfg['max_wait_ms'])
    rng = np.random.default_rng(state["decision_seed"])
    decisions = []
    stagnation = 0
    queued_at = {gid:time.perf_counter() for gid in frontier}
    fallback_entries = list(state.get('fallback_entries',[]))
    fallback_used = 0
    traces = []
    stop_reason = 'frontier_exhausted'

    def owner_for(owners):
        if sid in owners: return sid  # Local replicas never require a return RPC.
        available = set(owners)-visited_set
        return min(available) if can_forward and available else None

    def threshold():
        ranked = sorted(results.values())
        return ranked[cfg['top_k']-1] if len(ranked)>=cfg['top_k'] else max(state['scale'],max(ranked,default=state['scale']))

    def fallback():
        nonlocal fallback_used, stagnation
        if not can_forward or not cfg['fallback_enabled'] or fallback_used>=state.get('fallback_remaining',cfg['max_fallback_entries']):
            return False
        if len(frontier)>=cfg['frontier_size']: return False
        for row in fallback_entries:
            gid = row[0]
            if gid not in seen and owner_for(row[3]) is not None:
                seen.add(gid)
                frontier[gid] = list(row[1:])
                queued_at[gid] = time.perf_counter()
                fallback_used += 1
                stagnation = 0
                return True
        return False

    while remaining > 0:
        if cancelled():
            raise TimeoutError('Query cancelled, deadline reached or head lease expired')
        if not frontier and not fallback():
            break
        now = time.perf_counter()
        groups = {}
        for gid, item in list(frontier.items()):
            item[7] = max(0.,(now-queued_at[gid])*1000)
            owner = owner_for(item[2])
            if owner is None:
                key='visited_node_candidates_dropped' if can_forward else 'remote_candidates_dropped_after_forward'
                stats[key]+=1
                frontier.pop(gid);queued_at.pop(gid,None)
                continue
            groups.setdefault(owner,[]).append((item[0],gid))
        if not groups:
            continue
        for values in groups.values(): values.sort()
        actions = []
        for target,values in groups.items():
            limit = cfg['local_beam'] if target==sid else c['network']['batch_size']
            batch = values[:min(limit,remaining)]
            cost = service_per_node*len(batch)
            if target != sid:
                # Account for sending query + bounded candidate/visited state, not just IDs.
                byte_estimate = len(state['query_f32'])+len(seen)*8+sum(len(v) for k,v in groups.items() if k!=sid)*96
                cost += policy_cfg['remote_rtt_ms']+byte_estimate/policy_cfg['bandwidth_bytes_per_ms']
            actions.append(Action(str(target),target!=sid,
                tuple(Candidate(gid,d,max(0.,frontier[gid][1])) for d,gid in batch),
                max(cost,1e-9),max(frontier[gid][7] for _,gid in batch)))
        local_action = next((a for a in actions if not a.remote),None)
        eligible = [a for a in actions if not a.remote or slots>0]
        if not eligible:
            stop_reason='branch_slots_exhausted'; break
        remote_actions=[a for a in eligible if a.remote]
        reason='local_only'; probability=0.; draw=None; ur=None; ul=None
        if not remote_actions or (local_action is not None and remaining<2):
            action=local_action
        elif local_action is None:
            action=policy.choose(remote_actions,threshold(),state['scale'])
            reason='local_exhausted';probability=1.
        elif any(a.wait_ms>=policy_cfg['max_wait_ms'] for a in remote_actions):
            action=max(remote_actions,key=lambda a:a.wait_ms)
            reason='max_wait';probability=1.
        elif policy_cfg['mode']=='reference':
            action=min(eligible,key=lambda a:(a.candidates[0].estimated_distance,a.candidates[0].global_id))
            reason='reference_order';probability=float(action.remote)
        elif policy_cfg['mode']=='local_first':
            action=local_action
        else:
            t=threshold()
            best=max(remote_actions,key=lambda a:policy.score(a,t,state['scale']))
            ur,ul=policy.score(best,t,state['scale']),policy.score(local_action,t,state['scale'])
            probability=remote_probability(ur,ul,policy_cfg['remote_weight'],policy_cfg['probability_temperature'])
            if policy_cfg['remote_selection']=='threshold':
                use_remote=policy_cfg['remote_weight']*ur>ul
                probability=float(use_remote)
                reason='weighted_utility'
            else:
                draw=float(rng.random());use_remote=draw<probability
                reason='probability_draw'
            action=best if use_remote else local_action
        if action is None: break
        decision_id=f"{state['branch_id']}:{len(decisions)}"
        decisions.append({'decision_id':decision_id,'reason':reason,'probability':probability,'draw':draw,
                          'selected_node':int(action.key),'remote':action.remote,
                          'remote_utility':ur,'local_utility':ul,'selected_wait_ms':action.wait_ms})
        target = int(action.key)
        if target != sid:
            other_work = any(owner!=target for owner in groups)
            grant = max(1,int(remaining*c['peer']['child_budget_fraction'])) if other_work else remaining
            if local_action is not None: grant=min(grant,remaining-1)
            if grant<=0: action=local_action; target=sid
            else:
                child_slots = slots-1
                slots = 0
                remaining -= grant
                seeds=[]
                for _,gid in groups[target][:min(c['network']['batch_size'],cfg['frontier_size'])]:
                    item=frontier.pop(gid)
                    queued_at.pop(gid,None)
                    seeds.append([gid,*item])
                child_id=str(uuid.uuid4())
                # Carry pending work for unvisited nodes. Parent-local work stays only here.
                carried=[[gid,*item] for gid,item in frontier.items()
                         if sid not in item[2] and set(item[2])-visited_set]
                child={**state,'branch_id':child_id,'parent_branch_id':state['branch_id'],
                    'work_grant':grant,'branch_slots':child_slots,'seeds':seeds,'seen':sorted(seen),
                    'remote_frontier':carried,
                    'fallback_entries':fallback_entries,
                    'fallback_remaining':state.get('fallback_remaining',cfg['max_fallback_entries'])-fallback_used,
                    'visited_nodes':[*visited_nodes,target],
                    'ancestry':[*state.get('ancestry',[]),
                        {'branch_id':state['branch_id'],'node_id':sid}],
                    'dispatch_decision_id':decision_id,
                    'target_node':target,'decision_seed':int(rng.integers(0,2**32))}
                descriptor={'decision_id':decision_id,'branch_id':child_id,'node_id':target,'work_grant':grant,
                            'branch_slots':child_slots,'parent_branch_id':state['branch_id']}
                # The descriptor is irrevocable even if the transfer acknowledgement is lost.
                children.append(descriptor)
                events.append({'decision_id':decision_id,'target':target,'child_branch_id':child_id,'work_grant':grant,
                               'local_expansions_before':len(expanded_ids),'candidate_count':len(seeds),
                               'carried_candidate_count':len(carried),
                               'elapsed_ms':(time.perf_counter()-started)*1000})
                # Transfer the sole forwarding right; this node now performs local work only.
                can_forward = False
                spawn(child,descriptor)
                continue
        batch=[gid for _,gid in groups[sid][:min(cfg['local_beam'],remaining)]]
        selected={gid:frontier.pop(gid) for gid in batch}
        for gid in batch: queued_at.pop(gid,None)
        old=threshold() if len(results)>=cfg['top_k'] else math.inf
        # This is the ONLY suspension point. No decision/remote dispatch can run
        # until this local round is read and consumed by the same generator.
        reply=yield (q,batch,state['epoch'])
        if cancelled():
            raise TimeoutError('Query cancelled while waiting for local I/O')
        remaining-=len(batch)
        expanded_ids.extend(batch)
        stats['batches']+=1
        for key in COUNTERS: stats[key]+=reply['stats'].get(key,0)
        measured=(reply['stats']['service_ms']+reply['stats'].get('io_batch_wall_ms',0))/len(batch)
        w=policy_cfg['ewma_weight']
        service_per_node=(1-w)*service_per_node+w*max(measured,1e-9)
        for gid,d in reply['expanded']:
            item=selected[gid]
            paths.append({'id':gid,'parent':item[3],'depth':item[4],
                          'cross_shard_depth':item[5], 'root':item[6],'shard':sid})
            if not (cfg['exclude_self'] and gid==state['query_id']): results[gid]=d
        parents=dict(reply['parents'])
        for gid,d,u in reply['neighbors']:
            if gid in seen: continue
            seen.add(gid)
            parent=parents[gid]
            p=selected[parent]
            owners=directory[gid]
            target=owner_for(owners)
            if target is None:
                key='visited_node_candidates_dropped' if can_forward else 'remote_candidates_dropped_after_forward'
                stats[key]+=1
                continue
            frontier[gid]=[d,u,list(owners),parent,p[4]+1,p[5]+int(target!=sid),p[6],0.]
            queued_at[gid]=time.perf_counter()
        improved=threshold()<old
        stats['improved_topk_batches']+=int(improved)
        stagnation=0 if improved else stagnation+1
        if len(frontier)>cfg['frontier_size']:
            retained=sorted(frontier,key=lambda gid:(frontier[gid][0],gid))[:cfg['frontier_size']]
            stats['frontier_dropped']+=len(frontier)-len(retained)
            frontier={gid:frontier[gid] for gid in retained}
            queued_at={gid:queued_at[gid] for gid in retained}
        if stagnation>=cfg['stagnation_batches']: fallback()
        if c['metrics']['detailed_trace']:
            traces.append({'ids':batch,'shard_id':sid,'service_trace':reply['service_trace'],
                           'stats':reply['stats'],'io_trace':reply.get('io_trace')})
    if remaining==0: stop_reason='branch_work_credit_exhausted'
    ranked=sorted(results.items(),key=lambda pair:(pair[1],pair[0]))[:cfg['top_k']]
    # Top-k of each branch is sufficient for an exact merge of the evaluated candidate union.
    stats.update(expanded=len(expanded_ids),fallback_entries=fallback_used,
        server_thread_cpu_ms=0.,  # Set by the driver, summing actual CPU slices.
        
        branch_wall_ms=(time.perf_counter()-started)*1000,
        max_expanded_depth=max((p['depth'] for p in paths),default=0),
        max_cross_shard_depth=max((p['cross_shard_depth'] for p in paths),default=0),
        pending_at_stop=len(frontier),stop_reason=stop_reason)
    report={'branch_id':state['branch_id'],'parent_branch_id':state['parent_branch_id'],
        'visited_nodes':visited_nodes,
        'node_id':sid,'work_grant':state['work_grant'],'branch_slots':state['branch_slots'],
        'work_spent':len(expanded_ids),'work_unused':remaining,'children':children,
        'ids':[gid for gid,_ in ranked],'squared_distances':[d for _,d in ranked],
        'expanded_ids':expanded_ids,'result_paths':[p for p in paths if p['id'] in dict(ranked)],
        'stats':stats,'fork_events':events,'decisions':decisions}
    if c['metrics']['detailed_trace']: report['local_batch_trace']=traces
    return report


def run_branch(state, shard, directory, c, spawn, cancelled):
    """Synchronous driver of the very same decisions used by resumable execution."""
    steps=branch_steps(state,shard,directory,c,spawn,cancelled)
    cpu=time.thread_time();reply=None
    try:
        while True:
            q,ids,epoch=steps.send(reply)
            reply=shard.expand(q,ids,epoch)
    except StopIteration as finished:
        report=finished.value
        report['stats']['server_thread_cpu_ms']=(time.thread_time()-cpu)*1000
        return report
    finally:
        steps.close()
