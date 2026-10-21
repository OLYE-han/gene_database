"""Bounded persistent RPC connections; one request at a time per borrowed socket."""
from collections import OrderedDict, deque
import select
import socket
import threading
import time
from .transport import send, receive


def rpc_exchange(endpoint, message, limit, timeout, pool=None):
    start=time.monotonic();deadline=start+timeout
    counts=dict(request_bytes=0,response_bytes=0,rpc_count=1,pool_wait_ms=0.,
                connect_ms=0.,send_ms=0.,response_wait_ms=0.,connections_created=0,connections_reused=0)
    counts['rpc_start_ns']=time.monotonic_ns()
    sock=None;entry=None;healthy=False
    try:
        if pool is not None:
            sock,entry=pool.borrow(tuple(endpoint),deadline)
            counts['pool_wait_ms']=(time.monotonic()-start)*1000
        counts['pool_acquired_ns']=time.monotonic_ns()
        if sock is None:
            t=time.monotonic();sock=socket.create_connection(tuple(endpoint),max(.001,deadline-t))
            counts['connect_ms']=(time.monotonic()-t)*1000;counts['connections_created']=1
            if pool is not None and getattr(pool,'tcp_nodelay',False):
                sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        else:counts['connections_reused']=1
        t=time.monotonic();sock.settimeout(max(.001,deadline-t))
        counts['request_bytes']=send(sock,message,limit);counts['send_ms']=(time.monotonic()-t)*1000
        counts['send_done_ns']=time.monotonic_ns()
        t=time.monotonic();sock.settimeout(max(.001,deadline-t))
        response,counts['response_bytes']=receive(sock,limit)
        counts['response_wait_ms']=(time.monotonic()-t)*1000
        counts['response_received_ns']=time.monotonic_ns()
        healthy=True
        if not response.get('ok'):raise RuntimeError(response.get('error','Peer request failed'))
        counts['wall_ms']=(time.monotonic()-start)*1000
        return response,counts
    except Exception as exc:
        counts['wall_ms']=(time.monotonic()-start)*1000
        exc.rpc_metrics=counts
        raise
    finally:
        counts['rpc_finally_enter_ns']=time.monotonic_ns()
        try:
            if pool is not None and entry is not None:
                detail=pool.release(entry,sock,healthy)
                if detail:counts.update(detail)
            elif sock is not None:sock.close()
        finally:
            counts['rpc_release_done_ns']=time.monotonic_ns()
            counts['pool_release_ms']=(counts['rpc_release_done_ns']-counts['rpc_finally_enter_ns'])/1e6
            counts['wall_including_release_ms']=(time.monotonic()-start)*1000


class ConnectionPool:
    def __init__(self,per_endpoint,max_endpoints,probe_mode='locked'):
        if probe_mode not in ('locked','reserved'):raise ValueError('Invalid RPC pool probe mode')
        self.probe_mode=probe_mode
        self.limit=per_endpoint;self.max_endpoints=max_endpoints
        self.cv=threading.Condition();self.entries=OrderedDict();self.closed=False

    def borrow(self,endpoint,deadline):
        if self.probe_mode=='reserved':return self._borrow_reserved(endpoint,deadline)
        return self._borrow_locked(endpoint,deadline)

    def _borrow_locked(self,endpoint,deadline):
        with self.cv:
            while True:
                if self.closed:raise RuntimeError('RPC pool closed')
                entry=self.entries.get(endpoint)
                if entry is None:
                    if len(self.entries)>=self.max_endpoints:
                        idle=next((key for key,e in self.entries.items() if e['borrowed']==0),None)
                        if idle is not None:
                            old=self.entries.pop(idle)
                            for sock in old['idle']:sock.close()
                        else:
                            left=deadline-time.monotonic()
                            if left<=0:raise TimeoutError('RPC endpoint capacity wait expired')
                            self.cv.wait(left);continue
                    entry={'endpoint':endpoint,'borrowed':0,'idle':[]}
                    self.entries[endpoint]=entry
                self.entries.move_to_end(endpoint)
                if entry['borrowed']<self.limit:
                    entry['borrowed']+=1
                    while entry['idle']:
                        sock=entry['idle'].pop()
                        # An idle RPC stream must have no unread data. Readability
                        # means EOF or a protocol violation; never submit a new
                        # query on a connection already closed by the server.
                        try:
                            readable,_,errors=select.select([sock],[],[sock],0)
                            if not readable and not errors:return sock,entry
                        except (OSError,ValueError):pass
                        sock.close()
                    return None,entry
                left=deadline-time.monotonic()
                if left<=0:raise TimeoutError('RPC connection pool wait expired')
                self.cv.wait(left)

    def release(self,entry,sock,healthy):
        if self.probe_mode=='reserved':return self._release_reserved(entry,sock,healthy)
        with self.cv:
            entry['borrowed']-=1
            if sock is not None:
                if healthy and not self.closed:entry['idle'].append(sock)
                else:sock.close()
            self.cv.notify_all()

    def close(self):
        if self.probe_mode=='reserved':return self._close_reserved()
        with self.cv:
            self.closed=True
            for entry in self.entries.values():
                for sock in entry['idle']:sock.close()
                entry['idle'].clear()
            self.cv.notify_all()


    @staticmethod
    def _close_sockets(sockets):
        # Drain even if one close raises. No caller holds the pool condition.
        error=None
        while sockets:
            sock=sockets.pop()
            try:sock.close()
            except BaseException as exc:
                if error is None:error=exc
        if error is not None:raise error

    def _borrow_reserved(self,endpoint,deadline):
        discarded=[]
        # Reserve once, before probing. This pins the endpoint against eviction
        # and counts the candidate against the per-endpoint in-flight limit.
        with self.cv:
            while True:
                if self.closed:raise RuntimeError('RPC pool closed')
                entry=self.entries.get(endpoint)
                if entry is None:
                    if len(self.entries)>=self.max_endpoints:
                        idle=next((key for key,e in self.entries.items() if e['borrowed']==0),None)
                        if idle is None:
                            left=deadline-time.monotonic()
                            if left<=0:raise TimeoutError('RPC endpoint capacity wait expired')
                            self.cv.wait(left);continue
                        old=self.entries.pop(idle)
                        discarded=old['idle'];old['idle']=[]
                    entry={'endpoint':endpoint,'borrowed':0,'idle':[]}
                    self.entries[endpoint]=entry
                self.entries.move_to_end(endpoint)
                if entry['borrowed']<self.limit:
                    entry['borrowed']+=1
                    sock=entry['idle'].pop() if entry['idle'] else None
                    break
                left=deadline-time.monotonic()
                if left<=0:raise TimeoutError('RPC connection pool wait expired')
                self.cv.wait(left)
        try:
            self._close_sockets(discarded)
            while True:
                healthy=sock is None
                if sock is not None:
                    try:
                        readable,_,errors=select.select([sock],[],[sock],0)
                        healthy=not readable and not errors
                    except (OSError,ValueError):pass
                if healthy:
                    # close() may have run while select released the GIL. An
                    # already-returned borrowed socket remains owned by caller;
                    # an unreturned reservation must instead fail and unwind.
                    with self.cv:
                        if self.closed:raise RuntimeError('RPC pool closed')
                    return sock,entry
                rejected=sock;sock=None
                self._close_sockets([rejected])
                with self.cv:
                    if self.closed:raise RuntimeError('RPC pool closed')
                    sock=entry['idle'].pop() if entry['idle'] else None
        except BaseException:
            with self.cv:
                entry['borrowed']-=1
                self.cv.notify_all()
            if sock is not None:discarded.append(sock)
            # Preserve the original failure while still closing all detached
            # descriptors and releasing the reservation exactly once.
            try:self._close_sockets(discarded)
            except BaseException:pass
            raise

    def _release_reserved(self,entry,sock,healthy):
        discarded=[]
        entered=time.monotonic_ns()
        with self.cv:
            acquired=time.monotonic_ns()
            entry['borrowed']-=1
            if sock is not None:
                if healthy and not self.closed:entry['idle'].append(sock)
                else:discarded.append(sock)
            self.cv.notify_all()
        self._close_sockets(discarded)
        return {'pool_release_lock_wait_ms':(acquired-entered)/1e6}

    def _close_reserved(self):
        discarded=[]
        with self.cv:
            self.closed=True
            for entry in self.entries.values():
                discarded.extend(entry['idle']);entry['idle']=[]
            self.cv.notify_all()
        self._close_sockets(discarded)


class FairConnectionPool(ConnectionPool):
    """Per-target FIFO reservation with one private wakeup per granted caller.

    The registry lock protects ownership, not network I/O. A release transfers
    capacity to a specific waiter *before* waking it, so a new caller cannot
    steal that slot. Full targets do not block available unrelated targets.
    Socket probing and closing always happen outside the registry lock.
    """
    def __init__(self,per_endpoint,max_endpoints):
        super().__init__(per_endpoint,max_endpoints,'reserved')
        if per_endpoint<1 or max_endpoints<1:raise ValueError('Pool capacities must be positive')
        self.pending=OrderedDict()

    @staticmethod
    def _dispose(sockets):
        # Detached idle sockets no longer own capacity. A close error must not
        # strand already-granted reservations belonging to other callers.
        for sock in sockets:
            try:sock.close()
            except OSError:pass

    def _endpoint_limit(self,endpoint):
        return self.limit

    def _grant_locked(self):
        discarded=[]
        for endpoint in list(self.pending):
            queue=self.pending[endpoint]
            while queue:
                waiter=queue[0]
                if waiter['deadline']<=time.monotonic():
                    queue.popleft();waiter['error']=TimeoutError('RPC connection pool wait expired')
                    waiter['event'].set();continue
                entry=self.entries.get(endpoint)
                if entry is None:
                    if len(self.entries)>=self.max_endpoints:
                        victim=next((key for key,value in self.entries.items()
                                     if not value['borrowed']),None)
                        if victim is None:break
                        old=self.entries.pop(victim);discarded.extend(old['idle']);old['idle']=[]
                    entry={'endpoint':endpoint,'borrowed':0,'idle':[]}
                    self.entries[endpoint]=entry
                if entry['borrowed']>=self._endpoint_limit(endpoint):break
                queue.popleft();entry['borrowed']+=1
                self.entries.move_to_end(endpoint)
                waiter['entry']=entry
                waiter['sock']=entry['idle'].pop() if entry['idle'] else None
                waiter['event'].set()
            if not queue:self.pending.pop(endpoint,None)
        return discarded

    def borrow(self,endpoint,deadline):
        waiter=dict(event=threading.Event(),deadline=deadline,entry=None,sock=None,error=None,queued_at=time.monotonic())
        with self.cv:
            if self.closed:raise RuntimeError('RPC pool closed')
            self.pending.setdefault(endpoint,deque()).append(waiter)
            discarded=self._grant_locked()
        entry=sock=None
        try:
            self._dispose(discarded)
            waiter['event'].wait(max(0.,deadline-time.monotonic()))
            with self.cv:
                if waiter['entry'] is None:
                    queue=self.pending.get(endpoint)
                    if queue is not None and waiter in queue:
                        queue.remove(waiter)
                        if not queue:self.pending.pop(endpoint)
                    error=waiter['error'] or TimeoutError('RPC connection pool wait expired')
                    raise error
                entry,sock=waiter['entry'],waiter['sock']
            while True:
                with self.cv:
                    if self.closed:raise RuntimeError('RPC pool closed')
                    if time.monotonic()>=deadline:raise TimeoutError('RPC connection pool wait expired')
                if sock is None:return None,entry
                try:
                    readable,_,errors=select.select([sock],[],[sock],0)
                    healthy=not readable and not errors
                except (OSError,ValueError):healthy=False
                if healthy:
                    with self.cv:
                        if self.closed:raise RuntimeError('RPC pool closed')
                        if time.monotonic()>=deadline:raise TimeoutError('RPC connection pool wait expired')
                    return sock,entry
                rejected=sock;sock=None;rejected.close()
                with self.cv:sock=entry['idle'].pop() if entry['idle'] else None
        except BaseException:
            # Includes interruption while waiting, and close/deadline racing
            # with a grant. Reclaim exactly one assigned reservation, if any.
            with self.cv:
                queue=self.pending.get(endpoint)
                if queue is not None and waiter in queue:
                    queue.remove(waiter)
                    if not queue:self.pending.pop(endpoint)
                if entry is None and waiter['entry'] is not None:
                    entry,sock=waiter['entry'],waiter['sock']
            if entry is not None:self.release(entry,sock,False)
            raise

    def release(self,entry,sock,healthy):
        discarded=[];entered=time.monotonic_ns()
        with self.cv:
            acquired=time.monotonic_ns()
            entry['borrowed']-=1
            if sock is not None:
                if healthy and not self.closed:entry['idle'].append(sock)
                else:discarded.append(sock)
            if not self.closed:discarded.extend(self._grant_locked())
        self._dispose(discarded)
        return {'pool_release_lock_wait_ms':(acquired-entered)/1e6}

    def close(self):
        discarded=[]
        with self.cv:
            self.closed=True
            for entry in self.entries.values():
                discarded.extend(entry['idle']);entry['idle']=[]
            for queue in self.pending.values():
                for waiter in queue:
                    waiter['error']=RuntimeError('RPC pool closed');waiter['event'].set()
            self.pending.clear()
        self._dispose(discarded)
