"""Initialization/management adapters; the data RPC hot path stays in C++."""

from .index import native
from .native_search import prepare_shard
from .peer import execution_signature


def enabled(c):
    return c.get('native_runtime', {}).get('data_enabled', False)


class NativePeer:
    def __init__(self, c, shard):
        if c['storage']['io']['backend'] != 'sync_pread':
            raise ValueError('Native runtime currently requires sync_pread')
        prepare_shard(shard, None)
        self.runtime = native().NativeDataRuntime(c, shard.meta, execution_signature(c), shard.native_search)

    def metrics(self):
        return self.runtime.metrics()

    def close(self):
        self.runtime.close()


def make_peer(c, shard):
    if enabled(c):
        return NativePeer(c, shard)
    from .peer import PeerRuntime
    return PeerRuntime(c, shard)


class NativeServer:
    """socketserver-compatible lifecycle, without Python per-request dispatch."""
    def __init__(self, c, sid, peer, admin, close_owner):
        self.transport = native().NativeManagedTransport(c, sid, peer.runtime, admin)
        self.server_address = (c['cluster']['nodes'][sid]['host'], self.transport.port)
        self.close_owner = close_owner
        self.closed = False

    def configure(self, peer, accepting, revision=''):
        self.transport.configure(peer.runtime, accepting, revision)

    def metrics(self):
        return self.transport.metrics()

    def serve_forever(self, poll_interval=.5):
        self.transport.wait_shutdown()

    def shutdown(self):
        self.transport.shutdown()

    def server_close(self):
        if self.closed:
            return
        self.closed = True
        self.shutdown()
        # close releases the GIL while the admin worker drains.
        self.transport.close()
        self.close_owner()


def standalone_server(c, sid):
    from .index import Shard, shard_path
    from .metrics import ResourceSampler
    from .runtime import apply_process_runtime
    from .gc_profile import snapshot
    apply_process_runtime(c)
    worker = Shard(shard_path(c, sid), c['storage']['cache_bytes_per_shard'],
                   c['metrics'].get('expansion_counter_stripes',1),c['storage'].get('native_id_lookup_bytes',0))
    peer = None
    sampler = ResourceSampler(c['metrics']['resource_sample_interval_ms'], c['metrics']['resource_max_samples'])
    try:
        meta = worker.meta
        if (meta['shard_id'], meta['dimension'], meta['metric'], meta['bits'], meta['header_bytes']) != (
                sid, c['data']['dimension'], c['data']['metric'], c['quantization']['bits'], c['storage']['page_bytes']):
            raise ValueError('Server configuration does not match shard')
        peer = NativePeer(c, worker)
        def admin(request):
            if request.get('version') != 1 or request.get('op') != 'metrics':
                raise ValueError('Unsupported native management request')
            metrics = worker.metrics()
            metrics.update(peer_lifecycle=peer.metrics(), resource_samples=list(sampler.samples), gc_profile=snapshot())
            return dict(ok=True, request_id=request.get('request_id'), result=metrics)
        def close():
            peer.close()
            sampler.finish()
            worker.close()
        server = NativeServer(c, sid, peer, admin, close)
        server.configure(peer, True)
        sampler.start()
        return server
    except BaseException:
        if peer is not None:
            peer.close()
        worker.close()
        raise
