"""Entry routing and lifecycle only; autonomous branch search lives on data nodes."""
from __future__ import annotations

import time
import uuid

import numpy as np

from .reference_search import Engine as ReferenceEngine
from .partition import prepare
from .peer import HeadSession, BranchRegistry, exchange, execution_signature
from .peer_search import run_branch, COUNTERS


class Engine(ReferenceEngine):
    def search(self,vector,query_id=None):
        if self.c['search']['execution']=='reference':
            return super().search(vector,query_id)
        started=time.perf_counter();cpu=time.thread_time()
        request_id=str(uuid.uuid4())
        session=None
        try:
            c,cfg=self.c,self.c['search']
            if query_id is not None and (type(query_id) is not int or not 0<=query_id<2**64):
                raise ValueError('query_id must be uint64 or null')
            raw=np.asarray(vector)
            if raw.shape!=(self.manifest['dimension'],):raise ValueError('Wrong search query dimension')
            query=prepare(raw[None,:],c['data']['metric'])[0].astype(np.float32)
            if cfg['top_k']>self.manifest['rows']-int(cfg['exclude_self'] and query_id in self.owners):
                raise ValueError('top_k exceeds eligible rows')
            with self.router_lock:
                entries=list(map(int,self.router.search(query,min(c['routing']['entry_candidates'],
                    self.manifest['route_samples']),c['routing']['ef_search'])))
            positions=np.array([self.route_positions[gid] for gid in entries],dtype=np.int64)
            b=self.route_bank
            ds,us=self.quantizer.score(query,b['codes'][positions],b['extra'][positions],b['factors'][positions],b['center_ids'][positions])
            home=self.owners[entries[0]][0]
            # Entries sent once. The head never sees live frontier/visited state.
            seeds=[[gid,float(d),float(u),list(self.owners[gid]),None,0,0,gid,0.]
                   for gid,d,u in zip(entries,ds,us)]
            routing_ms=(time.perf_counter()-started)*1000
            root_id,token=str(uuid.uuid4()),str(uuid.uuid4())
            state={'query_request_id':request_id,'branch_id':root_id,'parent_branch_id':None,
                   'token':token,'entry_node':home,'target_node':home,'epoch':self.manifest['epoch'],
                   'signature':execution_signature(c),'query_f32':query.astype('<f4').tobytes(),'query_id':query_id,
                   'work_grant':cfg['max_expansions'],'branch_slots':c['peer']['max_branches']-1,
                   'seeds':[seeds[0]],'seen':[entries[0]],'fallback_entries':seeds[1:],
                   'visited_nodes':[home],'remote_frontier':[],
                   'fallback_remaining':cfg['max_fallback_entries'],
                   'scale':max(float(np.median(np.abs(ds))),float(np.finfo(np.float32).eps)),
                   'ancestry':[],'decision_seed':c['seed']}
            deadline=time.monotonic()+c['peer']['query_timeout_seconds']
            transfer={'rpc_count':0,'request_bytes':0,'response_bytes':0}
            execution_started=time.perf_counter()
            if c['network']['transport']=='tcp':
                session=HeadSession(c,request_id,token,root_id,home)
                state['head_endpoint']=session.endpoint
                node=c['cluster']['nodes'][home]
                _,transfer=exchange([node['host'],node['port']],{'version':1,'op':'peer_submit','state':state},
                                    c['network']['max_message_bytes'],c['peer']['handoff_timeout_seconds'])
                reports=session.wait(deadline)
                involved=sorted(session.registry.nodes)
            else:
                reports=self._local_branches(state,deadline)
                involved=sorted({r['node_id'] for r in reports})
            execution_ms=(time.perf_counter()-execution_started)*1000
            merged={}
            for report in reports:
                for gid,d in zip(report['ids'],report['squared_distances']):
                    if not np.isfinite(d) or d<0: raise ValueError('Invalid branch result distance')
                    if gid in merged and not np.isclose(merged[gid],d,rtol=1e-6,atol=1e-8):
                        raise ValueError('Replica exact distance mismatch')
                    merged[gid]=min(d,merged.get(gid,float('inf')))
            ranked=sorted(merged.items(),key=lambda p:(p[1],p[0]))[:cfg['top_k']]
            stats={key:sum(r['stats'][key] for r in reports) for key in COUNTERS}
            for key in ('expanded','batches','fallback_entries','frontier_dropped','improved_topk_batches','server_thread_cpu_ms',
                        'visited_node_candidates_dropped','remote_candidates_dropped_after_forward'):
                stats[key]=sum(r['stats'][key] for r in reports)
            ids=[gid for r in reports for gid in r['expanded_ids']]
            per_shard={sid:{} for sid in range(len(c['cluster']['nodes']))}
            for r in reports:
                for key in (*COUNTERS,'expanded','batches','server_thread_cpu_ms'):
                    per_shard[r['node_id']][key]=per_shard[r['node_id']].get(key,0)+r['stats'][key]
            peer_rpc=sum(r.get('peer_transfer',{}).get('peer_rpc_count',0) for r in reports)
            peer_tx=sum(r.get('peer_transfer',{}).get('peer_request_bytes',0) for r in reports)
            peer_rx=sum(r.get('peer_transfer',{}).get('peer_response_bytes',0) for r in reports)
            stats.update(peer_execution_model=c['peer']['execution_model'],
                io_backends=sorted({r.get('execution',{}).get('io_backend','sync_pread') for r in reports}),
                per_shard=per_shard,unique_expanded=len(set(ids)),duplicate_expansions=len(ids)-len(set(ids)),
                branch_count=len(reports),branch_unused_credit=sum(r['work_unused'] for r in reports),
                visited_nodes=max((r['visited_nodes'] for r in reports),key=len),
                node_revisit_count=len(reports)-len({r['node_id'] for r in reports}),
                router_submit_rpc_count=transfer['rpc_count'],peer_branch_rpc_count=peer_rpc,
                router_expand_rpc_count=0,
                rpc_count=transfer['rpc_count']+peer_rpc,
                request_bytes=transfer['request_bytes']+peer_tx,response_bytes=transfer['response_bytes']+peer_rx,
                wire_bytes=transfer['request_bytes']+peer_tx+transfer['response_bytes']+peer_rx,
                peer_request_bytes=peer_tx,peer_response_bytes=peer_rx,
                rpc_retries=sum(max(0,r.get('peer_transfer',{}).get('peer_rpc_count',0)-len(r['children'])) for r in reports),
                shards_visited=sorted({r['node_id'] for r in reports}),keepalive_nodes=involved,
                expansions_by_shard={sid:v.get('expanded',0) for sid,v in per_shard.items()},
                max_expanded_depth=max(r['stats']['max_expanded_depth'] for r in reports),
                max_discovery_depth=None,max_cross_shard_depth=max(r['stats']['max_cross_shard_depth'] for r in reports),
                graph_rounds=None,server_to_server_migrations=0,server_to_server_branches=len(reports)-1,
                exact_reranked=stats['exact_distance_evaluations'],transport=c['network']['transport'],
                execution='peer_single_visit_parallel_local',stop_reason='all_branch_manifests_complete',
                branch_stop_reasons=[r['stats']['stop_reason'] for r in reports],
                pending_at_stop=sum(r['stats']['pending_at_stop'] for r in reports),
                coordinator_thread_cpu_ms=(time.thread_time()-cpu)*1000,
                pure_network_latency_fraction=None,physical_ssd_latency_fraction=None,
                result_paths=[p for r in reports for p in r['result_paths'] if p['id'] in dict(ranked)])
            if stats['expanded']>cfg['max_expansions']:raise ValueError('Global budget exceeded')
            # Cleanup is part of API latency; keep it separate from search and merge.
            if session is not None:
                session.close()
                control=dict(session.control)
                control_by_kind=session.control_by_kind
                session=None
            else:
                control={'rpc_count':0,'request_bytes':0,'response_bytes':0,'errors':0}
                control_by_kind={}
            stats.update(control_rpc_count=control['rpc_count'],control_request_bytes=control['request_bytes'],
                         control_response_bytes=control['response_bytes'],control_errors=control['errors'],
                         control_by_kind=control_by_kind,
                         control_wire_bytes=control['request_bytes']+control['response_bytes'],
                         all_protocol_bytes=stats['wire_bytes']+control['request_bytes']+control['response_bytes'],
                         all_protocol_rpc_count=stats['rpc_count']+control['rpc_count'])
            latency=(time.perf_counter()-started)*1000
            parts={'routing_wall_ms':routing_ms,'peer_execution_and_control_wait_ms':execution_ms,
                   'head_merge_and_lifecycle_ms':max(0.,latency-routing_ms-execution_ms)}
            stats.update(latency_ms=latency,latency_breakdown_ms=parts,
                         latency_breakdown_fraction={k:v/latency for k,v in parts.items()},
                         metrics_complete=stats['rpc_retries']==0 and control['errors']==0)
            return {'status':'ok','request_id':request_id,'ids':[gid for gid,_ in ranked],
                'squared_distances':[d for _,d in ranked],'epoch':self.manifest['epoch'],
                'metric':self.manifest['metric'],'stats':stats,'branch_reports':reports}
        except Exception as exc:
            exc.dhdgs_metrics={'request_id':request_id,'stats':{'latency_ms':(time.perf_counter()-started)*1000,
                                 'metrics_complete':False}}
            raise
        finally:
            if session is not None:session.close()

    def _local_branches(self,state,deadline):
        """In-process execution simulator, explicitly not a network measurement."""
        registry=BranchRegistry(state['query_request_id'],state['token'],state['branch_id'],state['target_node'],
            state['work_grant'],state['branch_slots'],len(self.c['cluster']['nodes']))
        pending=[state];reports=[]
        while pending:
            item=pending.pop(0);sid=item['target_node']
            def spawn(child,descriptor): pending.append(child)
            report=run_branch(item,self.transport.shards[sid],self.owners,self.c,spawn,
                              lambda:time.monotonic()>=deadline)
            reports.append(report)
            registry.update({'query_request_id':state['query_request_id'],'token':state['token'],
                'node_id':sid,'involved_nodes':[sid], 'reports':[report]})
        if not registry.complete():raise ValueError('Incomplete local branch tree')
        return reports
