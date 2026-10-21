"""FIFO RPC capacity adaptation: resources for approved requests only."""
from collections import deque
import threading
import time
from .peer_rpc import FairConnectionPool


class AdaptiveConnectionPool(FairConnectionPool):
    def __init__(self,settings,max_endpoints):
        self.settings=settings;self.maximum=settings['connections_max']
        self.limits={};self.changes={};self.idle_since={}
        self.events=deque(maxlen=settings['history_size']);self.peak={}
        self.halt=threading.Event()
        super().__init__(settings['connections_min'],max_endpoints)
        self.monitor=threading.Thread(target=self._monitor,name='dhdgs-rpc-capacity',daemon=True)
        self.monitor.start()

    def _endpoint_limit(self,endpoint):
        return self.limits.get(endpoint,self.limit)

    def _monitor(self):
        s=self.settings;period=s['sample_interval_ms']/1000
        while not self.halt.wait(period):
            discarded=[];now=time.monotonic()
            with self.cv:
                if self.closed:return
                known=set(self.entries)|set(self.pending)
                for endpoint in list(self.limits):
                    if endpoint not in known:
                        self.limits.pop(endpoint);self.changes.pop(endpoint,None);self.idle_since.pop(endpoint,None);self.peak.pop(endpoint,None)
                for endpoint,entry in self.entries.items():
                    old=self._endpoint_limit(endpoint);new=old;reason=None
                    pending=self.pending.get(endpoint,())
                    oldest=now-pending[0]['queued_at'] if pending else 0.
                    if pending or entry['borrowed']:self.idle_since[endpoint]=now
                    else:self.idle_since.setdefault(endpoint,now)
                    if now-self.changes.get(endpoint,0)>=s['cooldown_ms']/1000:
                        if pending and oldest>s['target_wait_ms']/1000 and entry['borrowed']>=old:
                            new=min(self.maximum,old+s['growth_step']);reason='connection_wait'
                        elif not pending and not entry['borrowed'] and now-self.idle_since[endpoint]>=s['downscale_idle_ms']/1000:
                            new=max(self.limit,old-1);reason='idle'
                    if new!=old:
                        self.limits[endpoint]=new;self.changes[endpoint]=now
                        self.peak[endpoint]=max(new,self.peak.get(endpoint,self.limit))
                        self.events.append(dict(monotonic_ns=time.monotonic_ns(),endpoint=list(endpoint),
                            old=old,target=new,reason=reason,queued=len(pending),
                            oldest_wait_ms=oldest*1000,borrowed=entry['borrowed']))
                        # Shrink only idle sockets; borrowed reservations are
                        # never revoked. Do no socket work under the pool lock.
                        excess=max(0,len(entry['idle'])+entry['borrowed']-new)
                        for _ in range(min(excess,len(entry['idle']))):discarded.append(entry['idle'].pop())
                discarded.extend(self._grant_locked())
            self._dispose(discarded)

    def metrics(self):
        with self.cv:
            return dict(min_connections=self.limit,max_connections=self.maximum,
                endpoints=[dict(endpoint=list(endpoint),limit=self._endpoint_limit(endpoint),
                    peak_limit=self.peak.get(endpoint,self.limit),borrowed=entry['borrowed'],idle=len(entry['idle']),
                    waiting=len(self.pending.get(endpoint,()))) for endpoint,entry in self.entries.items()],
                events=list(self.events),closed=self.closed)

    def close(self):
        self.halt.set()
        super().close()
        if threading.current_thread() is not self.monitor:self.monitor.join()
