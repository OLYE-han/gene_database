"""Entry routing and lifecycle only; autonomous branch search lives on data nodes."""
from __future__ import annotations

import time
import threading
import uuid

import numpy as np

from .reference_search import Engine as ReferenceEngine
from .partition import prepare
from .peer import HeadSession, BranchRegistry, exchange, execution_signature
from .peer_search import run_branch, COUNTERS
from .tracing import trace_lifecycle


class Engine(ReferenceEngine):
    def __new__(cls,c):
        if c['peer'].get('head_lanes',{}).get('enabled',False):
            from .head_lanes import LaneEngine
            return LaneEngine(c)
        return super().__new__(cls)

    def apply_configuration(self,c,router=None):
        self.c=c
        if router is not None:self.router=router

    def __init__(self,c):
        from .runtime import apply_process_runtime
        self.runtime_settings=apply_process_runtime(c)
        super().__init__(c)
        self.head_hub=None
        self.head_flow=None
        if c['search']['execution']=='peer' and c['network']['transport']=='tcp' and c['peer']['communication']['mode']=='optimized':
            from .head_hub import acquire_hub
            self.head_hub=acquire_hub(c)
            if c['peer'].get('head_direct',{}).get('enabled',False):
                from .head_direct import DirectFlow
                self.head_flow=DirectFlow(c)
            elif c['peer'].get('head_pipeline',{}).get('enabled',False):
                from .head_pipeline import PipelineFlow
                self.head_flow=PipelineFlow(c)
            elif c['peer'].get('head_query_workers',0):
                from .head_flow import QueryFlow
                self.head_flow=QueryFlow(c['peer']['head_query_workers'],c['peer']['max_active_queries'])

    def close(self):
        if self.head_flow is not None:self.head_flow.close();self.head_flow=None
        if self.head_hub is not None:
            from .head_hub import release_hub
            release_hub(self.head_hub);self.head_hub=None
        super().close()

    def search(self,vector,query_id=None):
        if self.head_flow is not None:
            return self.search_async(vector,query_id).result()
        steps=self._search_steps(vector,query_id)
        try:next(steps)
        except StopIteration as done:return done.value
        raise RuntimeError('Synchronous search unexpectedly suspended')

    def search_async(self,vector,query_id=None):
        if self.head_flow is None:raise RuntimeError('Async head flow is not enabled')
        offered=time.monotonic_ns()
        if any(self.c['peer'].get(k,{}).get('enabled',False) for k in ('head_pipeline','head_direct')):
            timing={}
            return self.head_flow.submit(self._search_steps(vector,query_id,offered,timing),timing)
        return self.head_flow.submit(self._search_steps(vector,query_id,offered))

    def _search_steps(self,vector,query_id=None,offered_ns=None,pipeline_timing=None):
        if self.c['search']['execution']=='reference':
            return super().search(vector,query_id)
        stage_enter=time.monotonic_ns()
        started=time.perf_counter() if offered_ns is None else offered_ns/1e9
        cpu=time.thread_time();prior_cpu_ms=0.
        head_trace={'search_enter_ns':time.monotonic_ns()} if trace_lifecycle(self.c, query_id) else None
        if head_trace is not None and offered_ns is not None:
            head_trace.update(search_enter_ns=offered_ns,head_stage_start_ns=stage_enter)
        request_id=str(uuid.uuid4())
        session=None
        direct=self.c['peer'].get('head_direct',{}).get('enabled',False)
        dispatch_guard=threading.Lock();dispatch_closed=False
        try:
            c,cfg=self.c,self.c['search']
            if query_id is not None and (type(query_id) is not int or not 0<=query_id<2**64):
                raise ValueError('query_id must be uint64 or null')
            raw=np.asarray(vector)
            if raw.shape!=(self.manifest['dimension'],):raise ValueError('Wrong search query dimension')
            query=prepare(raw[None,:],c['data']['metric'])[0].astype(np.float32)
            if cfg['top_k']>self.manifest['rows']-int(cfg['exclude_self'] and query_id in self.owners):
                raise ValueError('top_k exceeds eligible rows')
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
            if head_trace is not None:head_trace['routing_done_ns']=time.monotonic_ns()
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
            if '_cluster_revision' in c:
                state['cluster_revision']=c['_cluster_revision']
            transfer={'rpc_count':0,'request_bytes':0,'response_bytes':0}
            execution_started=time.perf_counter()
            if c['network']['transport']=='tcp':
                node=c['cluster']['nodes'][home]
                if direct:
                    session=HeadSession(c,request_id,token,root_id,home,self.head_hub)
                    state['head_endpoint']=session.endpoint
                    operation=self.head_hub.submit_query(session,state,[node['host'],node['port']],deadline)
                    pipeline_timing['distribution_send_done_ns']=session.receiver_metrics['entry_send_done_ns']
                elif pipeline_timing is not None:
                    pipeline_timing['dispatch_enqueued_ns']=time.monotonic_ns()
                    def dispatch():
                        nonlocal session
                        with dispatch_guard:
                            if dispatch_closed:raise RuntimeError('Query cancelled before dispatch')
                            pipeline_timing['dispatch_started_ns']=time.monotonic_ns()
                            session=HeadSession(c,request_id,token,root_id,home,self.head_hub)
                            state['head_endpoint']=session.endpoint
                        return self.head_hub.submit_query(session,state,[node['host'],node['port']],deadline)
                    operation=self.head_hub.enqueue(dispatch)
                else:
                    session=HeadSession(c,request_id,token,root_id,home,self.head_hub,
                                        deferred=self.head_flow is not None)
                    state['head_endpoint']=session.endpoint
                    if self.head_flow is not None:
                        operation=self.head_hub.submit_query(session,state,[node['host'],node['port']],deadline)
                if self.head_flow is not None:
                    prior_cpu_ms=(time.thread_time()-cpu)*1000
                    response=yield operation
                    cpu=time.thread_time()
                    session.receiver_metrics['head_stage_resume_ns']=time.monotonic_ns()
                    transfer=response['counts']
                    if head_trace is not None:
                        head_trace.update(entry_submit_enter_ns=response['enter_ns'],
                            entry_submit_return_ns=response['return_ns'])
                    reports=list(session.registry.reports.values())
                else:
                    if head_trace is not None:head_trace['entry_submit_enter_ns']=time.monotonic_ns()
                    _,transfer=session.exchange([node['host'],node['port']],{'version':1,'op':'peer_submit','state':state},
                                        c['network']['max_message_bytes'],c['peer']['handoff_timeout_seconds'])
                    if head_trace is not None:head_trace['entry_submit_return_ns']=time.monotonic_ns()
                    reports=session.wait(deadline)
                if head_trace is not None:head_trace['all_reports_received_ns']=time.monotonic_ns()
                involved=sorted(session.registry.nodes)
            else:
                reports=self._local_branches(state,deadline)
                involved=sorted({r['node_id'] for r in reports})
            execution_ms=(time.perf_counter()-execution_started)*1000
            if direct:
                session.close(success=True)
                pipeline_timing['result_enqueued_ns']=time.monotonic_ns()
                packet=dict(config=c,epoch=self.manifest['epoch'],reports=reports,transfer=transfer,
                    involved=involved,request_id=request_id,query_id=query_id,trace=head_trace,
                    control=dict(session.control),control_by_kind=session.control_by_kind,
                    control_error_details=list(session.control_error_details),receiver=dict(session.receiver_metrics),
                    timing=dict(pipeline_timing),route_cpu_ms=prior_cpu_ms)
                session=None
                result=yield self.head_flow.merge(packet)
                stamp=time.monotonic_ns();result['stats']['head_direct']['result_received_ns']=stamp
                result['stats']['result_return_ipc_ms']=(stamp-result['stats']['head_direct']['result_finished_ns'])/1e6
                result['stats']['latency_ms']=(stamp-pipeline_timing['submitted_ns'])/1e6
                parts=result['stats']['latency_breakdown_ms'];parts['result_return_ipc_ms']=result['stats']['result_return_ipc_ms']
                result['stats']['latency_breakdown_fraction']={k:v/result['stats']['latency_ms'] for k,v in parts.items()}
                if 'head_timestamps' in result['stats']:result['stats']['head_timestamps']['search_return_ns']=stamp
                return result
            from .head_merge import aggregate_reports
            ranked,stats=aggregate_reports(c,reports,transfer,involved)
            stats['coordinator_thread_cpu_ms']=prior_cpu_ms+(time.thread_time()-cpu)*1000
            if self.head_flow is not None:
                stats['head_stage_queue_ms']=(stage_enter-offered_ns)/1e6
                stats['head_stage_resume_queue_ms']=(session.receiver_metrics['head_stage_resume_ns']-
                                                    session.receiver_metrics['query_ready_ns'])/1e6
            if pipeline_timing is not None:
                pipeline_timing['merge_finished_ns']=time.monotonic_ns()
                stats['head_pipeline']=dict(pipeline_timing)
                for label,a,b in (('receive_queue','receive_enqueued','receive_started'),
                                  ('route_queue','route_enqueued','route_started'),
                                  ('dispatch_queue','dispatch_enqueued','dispatch_started'),
                                  ('merge_queue','merge_enqueued','merge_started')):
                    stats['head_'+label+'_ms']=(pipeline_timing[b+'_ns']-pipeline_timing[a+'_ns'])/1e6
            stats['entry_rpc']=dict(transfer)
            if stats['expanded']>cfg['max_expansions']:raise ValueError('Global budget exceeded')
            # Cleanup is part of API latency; keep it separate from search and merge.
            if session is not None:
                session.close(success=True)
                control=dict(session.control)
                control_by_kind=session.control_by_kind
                control_error_details=list(session.control_error_details)
                stats['head_receiver']=dict(getattr(session,'receiver_metrics',{}))
                session=None
            else:
                control={'rpc_count':0,'request_bytes':0,'response_bytes':0,'errors':0}
                control_by_kind={}
                control_error_details=[]
            stats.update(control_rpc_count=control['rpc_count'],control_request_bytes=control['request_bytes'],
                         control_response_bytes=control['response_bytes'],control_errors=control['errors'],
                         control_by_kind=control_by_kind,
                         control_error_details=control_error_details,
                         control_wire_bytes=control['request_bytes']+control['response_bytes'],
                         all_protocol_bytes=stats['wire_bytes']+control['request_bytes']+control['response_bytes'],
                         all_protocol_rpc_count=stats['rpc_count']+control['rpc_count'])
            latency=(time.perf_counter()-started)*1000
            parts={'routing_wall_ms':routing_ms,'peer_execution_and_control_wait_ms':execution_ms,
                   'head_merge_and_lifecycle_ms':max(0.,latency-routing_ms-execution_ms)}
            stats.update(latency_ms=latency,latency_breakdown_ms=parts,
                         latency_breakdown_fraction={k:v/latency for k,v in parts.items()},
                         metrics_complete=stats['rpc_retries']==0 and control['errors']==0)
            if head_trace is not None:
                head_trace['search_return_ns']=time.monotonic_ns()
                stats['head_timestamps']=head_trace
            return {'status':'ok','request_id':request_id,'ids':[gid for gid,_ in ranked],
                'squared_distances':[d for _,d in ranked],'epoch':self.manifest['epoch'],
                'metric':self.manifest['metric'],'stats':stats,'branch_reports':reports}
        except Exception as exc:
            exc.dhdgs_metrics={'request_id':request_id,'stats':{'latency_ms':(time.perf_counter()-started)*1000,
                                 'metrics_complete':False}}
            raise
        finally:
            with dispatch_guard:dispatch_closed=True
            if session is not None:session.close()

    def _local_branches(self,state,deadline):
        """In-process execution simulator, explicitly not a network measurement."""
        registry=BranchRegistry(state['query_request_id'],state['token'],state['branch_id'],state['target_node'],
            state['work_grant'],state['branch_slots'],len(self.c['cluster']['nodes']))
        pending=[state];reports=[]
        while pending:
            item=pending.pop(0);sid=item['target_node']
            def spawn(child,descriptor): pending.append(child)
            if self.c['peer']['execution_model']=='native':
                from .native_search import run_native
                from .index import native
                report=run_native(item,self.transport.shards[sid],self.owners,self.c,spawn,
                                  lambda:time.monotonic()>=deadline,
                                  control=native().SearchControl(deadline,deadline))
            else:
                report=run_branch(item,self.transport.shards[sid],self.owners,self.c,spawn,
                                  lambda:time.monotonic()>=deadline)
            reports.append(report)
            registry.update({'query_request_id':state['query_request_id'],'token':state['token'],
                'node_id':sid,'involved_nodes':[sid], 'reports':[report]})
        if not registry.complete():raise ValueError('Incomplete local branch tree')
        return reports
