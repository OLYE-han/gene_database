"""Offline replica dispatch accounting from completed query reports.

No runtime locks or callbacks. Frequencies use observed successful replica-hit
requests; a sampled input never implies a full-population event count.
"""
from collections import Counter
import math


def distribution(values):
    n=sum(values.values())
    if not n:return dict(count=0,mean=None,min=None,p50=None,p95=None,p99=None,max=None)
    def quantile(p):
        rank=math.ceil(p*n);seen=0
        for value,count in sorted(values.items()):
            seen+=count
            if seen>=rank:return value
    return dict(count=n,mean=sum(v*c for v,c in values.items())/n,
                min=min(values),p50=quantile(.5),p95=quantile(.95),p99=quantile(.99),max=max(values))


class ReplicaBranchMetrics:
    def __init__(self,replica_sources=None):
        self.replica_sources=replica_sources or {}
        self.counts=Counter();self.groups={};self.node_groups={}
        self.request_keys=set();self.sampled=False

    @staticmethod
    def _new_group():
        return dict(events=0,queries=0,executed_queries=0,child_reports=0,
                    child_work=0,child_cpu_ms=0.,child_cpu_reports=0,
                    remaining=Counter(),granted=Counter(),local_expansions=Counter(),
                    per_hit_event_counts=Counter())

    def _source(self,b,q):
        for key in ['replica_source_node','replica_fallback_source','replica_pair_source']:
            if key in b:return int(b[key])
        version=b['replica_version']
        if version in self.replica_sources:return int(self.replica_sources[version])
        # original_entry_node only identifies the source of the root replica;
        # a peer-routed replica may copy a different shard.
        if b.get('parent_branch_id')is None and 'original_entry_node'in q.get('stats',{}):
            return int(q['stats']['original_entry_node'])
        return None

    def add(self,record,dataset='input'):
        """Return this request's direct and downstream dispatch rows."""
        if not isinstance(record,dict):
            self.counts['non_query_records']+=1;return []
        if record.get('kind')=='window':
            self.counts['window_queries']+=record['count'];return []
        if record.get('kind') not in [None,'sample']:
            self.counts['non_query_records']+=1;return []
        q=record.get('result',record)
        if not isinstance(q,dict):
            self.counts['failed_records']+=1;return []
        if record.get('transport_ok',True)is False or q.get('status','ok')!='ok':
            self.counts['failed_records']+=1;return []
        branches=q.get('branch_reports',q.get('branches'))
        if branches is None:
            self.counts['records_without_branches']+=1;return []
        self.sampled|=record.get('kind')=='sample'
        key=record.get('job',q.get('request_id'))
        if key is None:key='row:'+str(self.counts['queries'])
        ident=(dataset,str(key))
        if ident in self.request_keys:raise ValueError('Duplicate request identity: '+repr(ident))
        self.request_keys.add(ident);self.counts['queries']+=1
        by_id={b['branch_id']:b for b in branches}
        if len(by_id)!=len(branches):raise ValueError('Duplicate branch report')
        replicas=[b for b in branches if 'replica_version'in b]
        if not replicas:return []
        self.counts['replica_hit_queries']+=1
        sources={b['branch_id']:self._source(b,q)for b in replicas}
        if any(v is None for v in sources.values()):self.counts['queries_with_unknown_replica_source']+=1
        self.counts['replica_reports_without_fork_events']+=sum('fork_events'not in b for b in replicas)

        def origin(b):
            seen=set()
            while b is not None:
                bid=b['branch_id']
                if bid in seen:raise ValueError('Branch ancestry cycle')
                seen.add(bid)
                if bid in sources:return b
                parent=b.get('parent_branch_id');b=by_id.get(parent)
            return None

        rows=[];query_groups=Counter();query_nodes=Counter();executed=set();executed_nodes=set()
        event_ids=set()
        for b in branches:
            root=origin(b)
            if root is None:continue
            source=sources[root['branch_id']];host=int(root['node_id'])
            scope='direct'if b is root else 'downstream'
            for index,event in enumerate(b.get('fork_events',[])):
                event_id=(b['branch_id'],event.get('decision_id',str(index)))
                if event_id in event_ids:raise ValueError('Duplicate fork event')
                event_ids.add(event_id)
                target=int(event['target']);child=by_id.get(event.get('child_branch_id'))
                if child is not None and int(child['node_id'])!=target:raise ValueError('Child target mismatch')
                if child is not None and child.get('parent_branch_id')!=b['branch_id']:
                    raise ValueError('Child parent mismatch')
                role='host_base'if target==host else 'unknown_source'if source is None else 'source'if target==source else 'third_node'
                work=None if child is None else child.get('work_spent')
                cpu=None if child is None else child.get('stats',{}).get('server_thread_cpu_ms')
                row=dict(dataset=dataset,request=key,replica_version=root['replica_version'],
                    replica_host=host,replica_source=source,scope=scope,from_node=int(b['node_id']),
                    target=target,target_role=role,cross_server=int(b['node_id'])!=target,
                    branch_id=b['branch_id'],child_branch_id=event.get('child_branch_id'),
                    remaining_before=event.get('remaining_before'),child_grant=event.get('work_grant'),
                    remaining_after=event.get('remaining_after'),local_expansions_before=event.get('local_expansions_before'),
                    parent_search_elapsed_ms=event.get('search_elapsed_ms'),child_report_present=child is not None,
                    child_work=work,child_cpu_ms=cpu)
                rows.append(row)
                # Per request/category count: multiple replicas or targets in
                # one query count once for frequency, but retain all events.
                for table,key2,queries,active in [(self.groups,(scope,role),query_groups,executed),
                    (self.groups,('any',role),query_groups,executed),
                    (self.node_groups,(scope,source,host,target),query_nodes,executed_nodes)]:
                    g=table.setdefault(key2,self._new_group());g['events']+=1;queries[key2]+=1
                    for field,hist in [('remaining_before','remaining'),('child_grant','granted'),('local_expansions_before','local_expansions')]:
                        value=row[field]
                        if value is not None:
                            if int(value)!=value or value<0:raise ValueError('Invalid search credit/count')
                            g[hist][int(value)]+=1
                    if child is not None:g['child_reports']+=1
                    if work is not None:g['child_work']+=work
                    if cpu is not None:g['child_cpu_ms']+=cpu;g['child_cpu_reports']+=1
                    if work is not None and work>0:active.add(key2)
        for table,queries,active in [(self.groups,query_groups,executed),(self.node_groups,query_nodes,executed_nodes)]:
            for key2,count in queries.items():
                g=table[key2];g['queries']+=1;g['per_hit_event_counts'][count]+=1
                g['executed_queries']+=key2 in active
        return rows

    def summary(self):
        hits=self.counts['replica_hit_queries']
        def describe(g):
            out={k:v for k,v in g.items()if not isinstance(v,Counter)}
            out.update(query_fraction_per_hit=g['queries']/hits if hits else None,
                       events_per_hit=g['events']/hits if hits else None,
                       executed_query_fraction_per_hit=g['executed_queries']/hits if hits else None)
            out['remaining_before']=distribution(g['remaining']);out['child_grant']=distribution(g['granted'])
            out['local_expansions_before']=distribution(g['local_expansions'])
            counts=g['per_hit_event_counts'].copy()
            if hits>g['queries']:counts[0]=hits-g['queries']
            out['events_per_hit_distribution']=dict(sorted(counts.items()))
            return out
        groups=[]
        for scope in ['direct','downstream','any']:
            for role in ['source','third_node','host_base','unknown_source']:
                g=self.groups.get((scope,role),self._new_group())
                groups.append(dict(scope=scope,target_role=role,**describe(g)))
        return dict(counts=dict(self.counts),sampled=self.sampled,
            source_classification_complete=not self.counts['queries_with_unknown_replica_source'],
            replica_event_fields_complete=not self.counts['replica_reports_without_fork_events'],
            measurement='observed query records only; no population extrapolation',
            frequency_denominator='observed successful replica-hit requests',
            event_semantics='fork_events dispatch records; neither network retries nor RDMA READs',
            budget_semantics='remaining_before is parent branch credit before split; child_grant is transferred credit, not a new global budget',
            groups=groups,nodes=[dict(scope=k[0],source=k[1],host=k[2],target=k[3],**describe(g))
                                for k,g in sorted(self.node_groups.items(),key=lambda item:str(item[0]))])
