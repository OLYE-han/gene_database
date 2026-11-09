"""Build-local immutable adjacency and verification reuse, never a publish token."""
import copy
import hashlib
import json
import time
from collections import OrderedDict
from .index import native


def limited_settings(settings):
    """Every sequential native stage consumes the same monotonic budget."""
    deadline=settings.get('_planning_deadline')
    if deadline is None:return settings
    left=deadline-time.monotonic()
    if left<=0:raise TimeoutError('End-to-end planning deadline')
    out=dict(settings,community_max_plan_ms=min(settings['community_max_plan_ms'],max(1,int(left*1000))))
    if '_expansion_stop_at' in settings:
        out['expansion_soft_ms']=max(0,int((settings['_expansion_stop_at']-time.monotonic())*1000))
    return out


class PlanningContext:
    def __init__(self, shard, settings, pipeline, deadline):
        from .community import metadata
        self.shard, self.deadline = shard, deadline
        self.base = metadata(shard)
        self.graph = native().ReplicaPlanningGraph(
            shard.fd, shard.ids, self.base,
            min(settings['community_adjacency_bytes'], pipeline['cache_bytes']),
            pipeline['prefetch_rows'], pipeline['io_threads'], pipeline['queue_reads'])
        self.memoize = pipeline['memoize_verification']
        self.limit = pipeline['verification_cache_entries']
        self.memo = OrderedDict()
        self.hits = self.calls = 0
        self.saved_seconds = 0.
        self.closed = False
        self.final_stats = None

    def check(self):
        if self.closed:
            raise RuntimeError('Planning context closed')
        if time.monotonic() > self.deadline:
            self.graph.cancel()
            raise TimeoutError('Partition planning deadline')
        self.graph.validate(self.shard.fd, self.shard.ids, self.shard.meta)

    def metadata(self):
        self.check()
        return dict(self.base, _planning_graph=self.graph)

    def balls(self, roots, hops, settings, edge_limit=None):
        self.check()
        return self.graph.balls(roots, hops, settings['max_region_rows'],
                                edge_limit or settings['community_max_edge_scans'],
                                min(self.deadline-time.monotonic(), settings['community_max_plan_ms']/1000))

    def verified(self, kind, plan, settings, execute):
        self.check()
        self.calls += 1
        if not self.memoize:
            return execute()
        # Reuse only identical claims and resource limits from this pinned index.
        # Per-call time allowance may shrink; the session deadline still applies.
        cfg = {k:v for k,v in settings.items() if k not in ('community_max_plan_ms','_planning_deadline','expansion_soft_ms','_expansion_stop_at')}
        claim = {k:plan[k] for k in ('ids', 'entries', 'region_proof')}
        if 'certified_entries' in plan:claim['certified_entries']=plan['certified_entries']
        key = hashlib.sha256(json.dumps([kind, claim, cfg], sort_keys=True).encode()).digest()
        if key in self.memo:
            self.hits += 1
            result, elapsed = self.memo[key]
            self.saved_seconds += elapsed
            self.memo.move_to_end(key)
            return copy.deepcopy(result)
        started = time.monotonic()
        result = execute()
        self.check()
        self.memo[key] = (copy.deepcopy(result), time.monotonic()-started)
        if len(self.memo) > self.limit:
            self.memo.popitem(last=False)
        return result

    def stats(self):
        if self.final_stats is not None:
            return dict(self.final_stats)
        return dict(self.graph.stats(), verification_calls=self.calls,
                    verification_cache_hits=self.hits,
                    reused_verification_previous_seconds=self.saved_seconds)

    def cancel(self):
        graph = self.graph
        if graph is not None:
            graph.cancel()

    def close(self):
        if not self.closed:
            self.final_stats = self.stats()
            self.graph.close()
            self.graph = None  # Release cached records after the owner has joined all reads.
            self.memo.clear()
            self.closed = True
