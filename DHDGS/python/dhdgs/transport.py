"""Bounded binary MessagePack frames over persistent TCP (trusted cluster only)."""
from __future__ import annotations

import socket
import socketserver
import struct
import threading
import time
import uuid

import msgpack
import numpy as np

from .index import Shard, shard_path
from .metrics import ResourceSampler


def _read_exact(sock, count):
    data = bytearray()
    while len(data) < count:
        block = sock.recv(count-len(data))
        if not block:
            raise ConnectionError("Connection closed during frame")
        data.extend(block)
    return bytes(data)


def receive(sock, limit):
    length = struct.unpack("!I", _read_exact(sock, 4))[0]
    if length == 0 or length > limit:
        raise ValueError("Invalid RPC frame size")
    value = msgpack.unpackb(_read_exact(sock, length), raw=False, strict_map_key=True)
    if not isinstance(value, dict):
        raise ValueError("RPC message must be a mapping")
    return value, length+4


def send(sock, value, limit):
    body = msgpack.packb(value, use_bin_type=True)
    if not body or len(body) > limit:
        raise ValueError("RPC exceeds configured max_message_bytes")
    sock.sendall(struct.pack("!I", len(body))+body)
    return len(body)+4


class LocalTransport:
    def __init__(self, c):
        self.shards = {}
        try:
            for node in c["cluster"]["nodes"]:
                self.shards[node["id"]] = Shard(shard_path(c, node["id"]), c["storage"]["cache_bytes_per_shard"], c['metrics'].get('expansion_counter_stripes',1),c['storage'].get('native_id_lookup_bytes',0))
        except Exception:
            self.close()
            raise

    def expand(self, sid, query, ids, epoch, query_request_id=None):
        started = time.perf_counter_ns()
        result = self.shards[sid].expand(query, ids, epoch)
        ended = time.perf_counter_ns()
        result.update(client_start_ns=started, client_end_ns=ended)
        result.update(query_request_id=query_request_id, rpc_request_id=None)
        result["stats"].update(rpc_count=0, wire_bytes=0, request_bytes=0, response_bytes=0,
                               rpc_retries=0, action_ms=(ended-started)/1e6)
        return result

    def metrics(self):
        return {sid: shard.metrics() for sid, shard in self.shards.items()}

    def close(self):
        for shard in self.shards.values():
            shard.close()


class TCPTransport:
    def __init__(self, c):
        self.c = c
        self.nodes = c["cluster"]["nodes"]
        self.sockets = {}
        self.locks = [threading.Lock() for _ in self.nodes]

    def expand(self, sid, query, ids, epoch, query_request_id=None):
        cfg = self.c["network"]
        request_id = str(uuid.uuid4())
        request = {"version": 1, "op": "expand", "request_id": request_id, "query_request_id":query_request_id,
                   "epoch": epoch, "ids": list(map(int, ids)),
                   "query_f32": np.asarray(query, dtype="<f4").tobytes()}
        started = time.perf_counter_ns()
        request_bytes, response_bytes = 0, 0
        with self.locks[sid]:
            for attempt in range(cfg["max_retries"]+1):
                try:
                    if sid not in self.sockets:
                        node = self.nodes[sid]
                        self.sockets[sid] = socket.create_connection((node["host"], node["port"]), cfg["timeout_seconds"])
                    sock = self.sockets[sid]
                    request_bytes += send(sock, request, cfg["max_message_bytes"])
                    response, size = receive(sock, cfg["max_message_bytes"])
                    response_bytes += size
                    if response.get("request_id") != request_id:
                        raise ValueError("RPC request ID mismatch")
                    if not response.get("ok"):
                        raise ValueError(f"Shard {sid} rejected expansion: {response.get('error')}")
                    result = response["result"]
                    if result["epoch"] != epoch or result["shard_id"] != sid:
                        raise ValueError("RPC shard/epoch mismatch")
                    ended = time.perf_counter_ns()
                    result.update(client_start_ns=started, client_end_ns=ended)
                    result.update(query_request_id=query_request_id, rpc_request_id=request_id)
                    result["stats"].update(rpc_count=attempt+1, wire_bytes=request_bytes+response_bytes,
                        request_bytes=request_bytes, response_bytes=response_bytes, rpc_retries=attempt,
                        action_ms=(ended-started)/1e6)
                    return result
                except (OSError, ConnectionError):
                    sock = self.sockets.pop(sid, None)
                    if sock is not None:
                        sock.close()
                    if attempt == cfg["max_retries"]:
                        raise

    def metrics(self):
        result = {}
        cfg = self.c["network"]
        for node in self.nodes:
            with socket.create_connection((node["host"],node["port"]),cfg["timeout_seconds"]) as sock:
                rid = str(uuid.uuid4())
                send(sock, {"version":1,"op":"metrics","request_id":rid},cfg["max_message_bytes"])
                response, _ = receive(sock,cfg["max_message_bytes"])
                if response.get("request_id") != rid or not response.get("ok"):
                    raise ValueError("Invalid metrics response")
                result[node["id"]] = response["result"]
        return result

    def close(self):
        for sock in self.sockets.values():
            sock.close()
        self.sockets.clear()


def create_server(c, sid):
    if not 0 <= sid < len(c["cluster"]["nodes"]):
        raise ValueError("Invalid node ID")
    from .cpp_runtime import enabled, standalone_server
    if enabled(c):
        return standalone_server(c, sid)
    from .runtime import apply_process_runtime
    apply_process_runtime(c)
    worker = Shard(shard_path(c, sid), c["storage"]["cache_bytes_per_shard"], c['metrics'].get('expansion_counter_stripes',1),c['storage'].get('native_id_lookup_bytes',0))
    if (worker.meta["shard_id"] != sid or worker.meta["dimension"] != c["data"]["dimension"]
            or worker.meta["metric"] != c["data"]["metric"]
            or worker.meta["bits"] != c["quantization"]["bits"]
            or worker.meta["header_bytes"] != c["storage"]["page_bytes"]):
        worker.close()
        raise ValueError("Server configuration does not match shard")
    limit = c["network"]["max_message_bytes"]
    max_batch = max(c["network"]["batch_size"], c["search"]["local_beam"])
    sampler = ResourceSampler(c["metrics"]["resource_sample_interval_ms"],
                              c["metrics"]["resource_max_samples"])
    from .peer import PeerRuntime
    peer = PeerRuntime(c,worker) if c['search']['execution']=='peer' else None

    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(c["network"]["timeout_seconds"])
            while True:
                try:
                    request, _ = receive(self.request, limit)
                    received_ns=time.monotonic_ns()
                except (ConnectionError, OSError, ValueError, msgpack.UnpackException):
                    return
                request_id = request.get("request_id")
                try:
                    op=request.get('op')
                    if request.get('version')==1 and op in ('peer_submit','peer_lease','peer_cancel'):
                        if peer is None: raise ValueError('Peer execution is disabled')
                        trace=None
                        if op=='peer_submit':
                            accept_enter_ns=time.monotonic_ns()
                            result=peer.accept(request['state'])
                            state=request['state']
                            trace=peer.records[(state['query_request_id'],state['branch_id'])].get('execution_timestamps')
                            if trace is not None and not result.get('duplicate'):
                                trace.update(receiver_message_ready_ns=received_ns,
                                    receiver_accept_enter_ns=accept_enter_ns,
                                    receiver_accept_return_ns=time.monotonic_ns())
                            else:trace=None
                        else: result=peer.lease(request['query_request_id'],request['token'],op=='peer_cancel')
                        if trace is not None:trace['receiver_ack_send_enter_ns']=time.monotonic_ns()
                        send(self.request,{'ok':True,'result':result},limit)
                        if trace is not None:trace['receiver_ack_send_return_ns']=time.monotonic_ns()
                        continue
                    if request.get("version") != 1 or op not in ("expand", "metrics") or not isinstance(request_id, str):
                        raise ValueError("Unsupported request")
                    if request["op"] == "metrics":
                        result = worker.metrics()
                        from .gc_profile import snapshot as gc_snapshot
                        result['gc_profile'] = gc_snapshot()
                        if peer is not None: result['peer_lifecycle']=peer.metrics()
                        result["resource_samples"] = list(sampler.samples)
                        send(self.request, {"ok":True,"request_id":request_id,"result":result},limit)
                        continue
                    if peer is not None:
                        raise ValueError('Router-driven expansion disabled in peer execution')
                    ids = request["ids"]
                    if (not isinstance(ids, list) or not 1 <= len(ids) <= max_batch
                            or any(type(i) is not int or not 0 <= i < 2**64 for i in ids)):
                        raise ValueError("Invalid expansion ID batch")
                    raw = request["query_f32"]
                    if not isinstance(raw, bytes) or len(raw) != worker.meta["dimension"]*4:
                        raise ValueError("Invalid query byte length")
                    query = np.frombuffer(raw, dtype="<f4")
                    result = worker.expand(query, ids, request["epoch"])
                    response = {"ok": True, "request_id": request_id, "result": result}
                except (ValueError, KeyError, TypeError, RuntimeError, OSError) as exc:
                    response = {"ok": False, "request_id": request_id, "error": str(exc)}
                try:
                    send(self.request, response, limit)
                except (OSError, ValueError):
                    return

    class Server(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True
        request_queue_size = c['network'].get('listen_backlog',5)

        def server_close(self):
            super().server_close()
            sampler.finish()
            if peer is not None: peer.close()
            worker.close()

    node = c["cluster"]["nodes"][sid]
    try:
        server = Server((node["host"], node["port"]), Handler)
        sampler.start()
        return server
    except Exception:
        if peer is not None: peer.close()
        worker.close()
        raise
