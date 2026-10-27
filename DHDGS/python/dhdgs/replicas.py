"""Bounded immutable subgraph packaging and authenticated maintenance transport.

The native query path only reads published overlays. File construction, RDMA
bulk transfer and warming use a separate service and never decide search forks.
"""
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import shutil
import struct
import threading
import time

import numpy as np

from .data import load_complete_npy
from .index import Shard, MAGIC, HEADER, native

FILES = frozenset(('nodes.bin', 'ids.npy', 'code_ids.npy', 'code_membership.npy',
                  'codes.npy', 'extra.npy', 'factors.npy', 'center_ids.npy',
                  'centers.npy', 'rotation.bin', 'manifest.json', 'entries.json'))
CONTRACT = ('epoch', 'dimension', 'metric', 'bits', 'rotation_seed', 'rotation_sha256')


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.hexdigest()


def ident(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,96}', value):
        raise ValueError('Invalid replica identity')
    return value


def maintenance_config(c):
    out = copy.deepcopy(c)
    b = c['load_balance']
    p = out['native_runtime']['network_module']
    p.update(enabled=True, send_min_reactors=b['maintenance_min_reactors'],
             receive_min_reactors=b['maintenance_min_reactors'],
             send_max_reactors=b['maintenance_max_reactors'],
             receive_max_reactors=b['maintenance_max_reactors'],
             handlers_per_receive_reactor=1,
             connections_per_endpoint_per_reactor=b['maintenance_connections'],
             min_connections_per_endpoint_per_reactor=0,
             send_queue_capacity=b['inflight_chunks'] * 4,
             receive_queue_capacity_per_reactor=b['inflight_chunks'] * 4,
             max_receive_connections=b['maintenance_connections'] * b['maintenance_max_reactors'])
    out['network']['timeout_seconds'] = b['operation_timeout_s']
    return out


def region_quality(ids, adjacency, entries, settings):
    """Capped uniform-walk residence, computed once at build time.

    This is a structural prior, not a search-recall guarantee or a conversion
    between graph hops and best-first expansions. Shadow probes measure actual
    residence separately. The head stores scores only for HNSW entry IDs.
    """
    location = {int(v):i for i,v in enumerate(ids)}
    src, dst, degrees = [], [], np.zeros(len(ids), dtype='float64')
    for gid, neighbors in adjacency.items():
        i = location[gid]; degrees[i] = len(neighbors)
        for n in neighbors:
            j = location.get(int(n))
            if j is not None: src.append(i); dst.append(j)
    src, dst = np.asarray(src,dtype='int64'), np.asarray(dst,dtype='int64')
    return quality_arrays(ids,entries,settings,src,dst,degrees)


def quality_arrays(ids,entries,settings,src,dst,degrees):
    location={int(v):i for i,v in enumerate(ids)}
    src=np.asarray(src,dtype='int64');dst=np.asarray(dst,dtype='int64')
    steps = settings.get('quality_walk_steps',8)
    survival = np.ones(len(ids)); residence = np.ones(len(ids))
    # Exiting the copied region is absorbing. Dead ends also terminate a walk.
    for _ in range(steps-1):
        survival = np.bincount(src,weights=survival[dst],minlength=len(ids))/np.maximum(degrees,1)
        residence += survival
    score = residence/steps
    floor = settings.get('entry_weight_floor',.1)
    weight = floor+(1-floor)*score**settings.get('entry_weight_power',1.)
    ranked = sorted(range(len(ids)),key=lambda i:(-weight[i],int(ids[i])))[:settings.get('max_peer_entries',2048)]
    return dict(walk_steps=steps,mean_expected_walk_steps=float(np.mean(residence)),
        entry_weights={str(v):float(weight[location[int(v)]]) for v in entries},
        peer_entries=[[int(ids[i]),float(weight[i])] for i in ranked],
        internal_edge_fraction=len(src)/max(1,float(degrees.sum())))


def read_records(shard, ids, batch_bytes=1048576):
    """Bounded coalesced reads in physical-row order, preserving requested order."""
    stride=shard.meta['record_bytes'];header=shard.meta['header_bytes']
    ids=list(map(int,ids));i=0
    while i<len(ids):
        row=shard.location[ids[i]];j=i+1
        while j<len(ids) and (j-i+1)*stride<=batch_bytes and shard.location[ids[j]]==row+j-i:j+=1
        raw=os.pread(shard.fd,(j-i)*stride,header+row*stride)
        if len(raw)!=(j-i)*stride:raise ValueError('Short replica batch read')
        for k in range(i,j):yield ids[k],memoryview(raw)[(k-i)*stride:(k-i+1)*stride]
        i=j


def package(shard, ids, entries, directory, settings, cancelled=lambda: False):
    """Copy original records; include EVERY adjacent vertex's estimate bank."""
    directory = Path(directory)
    ids = np.asarray(sorted(set(map(int, ids)) | set(map(int, entries))), dtype='uint64')
    m = dict(shard.meta)
    if not len(ids) or len(ids) > settings['max_region_rows']:
        raise ValueError('Replica row budget exceeded/empty')
    if any(int(v) not in shard.location for v in ids):
        raise ValueError('Replica contains a nonlocal record')
    core_rows = len(ids)
    use_native=settings.get('native_replica_pack',False)
    selected, frontier = set(map(int, ids)), set(map(int, ids))
    # Preserve original edges. Add source-local exit records as a bounded halo,
    # ranked by how many selected vertices lead to them (ID breaks ties).
    for _ in range(0 if use_native else settings['halo_hops']):
        counts = Counter()
        for gid,raw in read_records(shard,sorted(frontier)):
            if cancelled(): raise RuntimeError('Replica halo cancelled')
            if len(raw) != m['record_bytes']: raise ValueError('Short halo read')
            degree, = struct.unpack_from('<I', raw, 4 * m['dimension'])
            if degree > m['max_degree'] or 4*m['dimension']+4+8*degree > len(raw):
                raise ValueError('Invalid halo adjacency')
            counts.update(int(v) for v in np.frombuffer(raw, '<u8', degree, 4*m['dimension']+4)
                          if int(v) not in selected and int(v) in shard.location)
        room = settings['max_region_rows'] - len(selected)
        frontier = set(v for v, _ in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:room])
        selected.update(frontier)
        if not frontier: break
    ids = np.asarray(sorted(selected), dtype='uint64')
    need = m['header_bytes'] + (settings['max_region_rows'] if use_native else len(ids)) * m['record_bytes']
    directory.parent.mkdir(parents=True, exist_ok=True)
    if need > settings['max_region_bytes'] or shutil.disk_usage(directory.parent).free < need + settings['reserve_disk_bytes']:
        raise ValueError('Insufficient replica space budget')
    directory.mkdir(exist_ok=False)
    ghost = set(map(int, ids)); adjacency = {}
    try:
        packed=None
        if use_native:
            if cancelled():raise RuntimeError('Replica build cancelled')
            prefix=HEADER.pack(MAGIC,1,m['dimension'],len(ids),m['record_bytes'])
            prefix+=bytes(m['header_bytes']-len(prefix))
            packed=native().replica_extract(shard.fd,shard.ids,ids,m['dimension'],m['record_bytes'],m['header_bytes'],
                m['max_degree'],settings['halo_hops'],settings['max_region_rows'],str(directory/'nodes.bin'),prefix)
            ids,code_ids=packed['ids'],packed['code_ids']
            if cancelled():raise RuntimeError('Replica build cancelled')
        else:
            with (directory / 'nodes.bin').open('xb') as f:
                header = HEADER.pack(MAGIC, 1, m['dimension'], len(ids), m['record_bytes'])
                f.write(header + bytes(m['header_bytes'] - len(header)))
                for gid,raw in read_records(shard,ids):
                    if cancelled(): raise RuntimeError('Replica build cancelled')
                    if len(raw) != m['record_bytes']: raise ValueError('Short source replica read')
                    degree, = struct.unpack_from('<I', raw, 4 * m['dimension'])
                    if degree > m['max_degree'] or 4 * m['dimension'] + 4 + 8 * degree > len(raw):
                        raise ValueError('Invalid source adjacency')
                    adjacency[int(gid)] = np.frombuffer(raw, dtype='<u8', count=degree, offset=4 * m['dimension'] + 4).copy()
                    ghost.update(adjacency[int(gid)].tolist())
                    f.write(raw)
                f.flush()
            code_ids = np.asarray(sorted(ghost), dtype='uint64')
        per_code = shard.codes.shape[1] + shard.extra.shape[1] + 12 + 4 + 8 + len(shard.meta.get('node_ids', []))
        if len(code_ids) * max(per_code, 32) > settings['max_code_bytes']:
            raise ValueError('Replica code dependency budget exceeded')
        positions = np.asarray([shard.code_location[int(v)] for v in code_ids], dtype='int64')
        np.save(directory / 'ids.npy', ids)
        np.save(directory / 'code_ids.npy', code_ids)
        for key in ('codes', 'extra', 'factors', 'center_ids'):
            np.save(directory / (key + '.npy'), getattr(shard, key)[positions])
        membership = load_complete_npy(shard.directory / 'code_membership.npy')
        np.save(directory / 'code_membership.npy', np.asarray(membership[positions]))
        for name in ('centers.npy', 'rotation.bin'):
            shutil.copyfile(shard.directory / name, directory / name)
        m.update(rows=len(ids), code_rows=len(code_ids))
        (directory / 'manifest.json').write_text(json.dumps(m))
        (directory / 'entries.json').write_text(json.dumps(sorted(set(map(int, entries)))))
        files = {p.name: {'bytes': p.stat().st_size, 'sha256': digest(p)} for p in directory.iterdir()}
        total = sum(f['bytes'] for f in files.values())
        code_bytes = total - files['nodes.bin']['bytes']
        if total > settings['max_region_bytes'] or code_bytes > settings['max_code_bytes']:
            raise ValueError('Actual replica byte budget exceeded')
        quality = quality_arrays(ids,entries,settings,packed['src'],packed['dst'],packed['degrees']) if packed is not None else region_quality(ids,adjacency,entries,settings)
        return dict(quality=quality,files=files, bytes=total, rows=len(ids), core_rows=core_rows, halo_rows=len(ids)-core_rows, code_rows=len(code_ids),
                    contract={k: m[k] for k in CONTRACT},
                    centers_sha256=files['centers.npy']['sha256'],
                    entries=sorted(set(map(int, entries))))
    except BaseException:
        shutil.rmtree(directory)
        raise


class ReplicaService:
    def __init__(self, owner):
        self.owner, self.c = owner, owner.c
        self.b = self.c['load_balance']
        self.root = owner.state_file.parent / 'replicas'
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.states = {}
        self.stop = threading.Event()
        self.jobs = ThreadPoolExecutor(max_workers=1, thread_name_prefix='replica-build')
        self.netc = maintenance_config(self.c)
        self.client = native().MaintenanceClient(self.netc)
        n = self.c['cluster']['nodes'][owner.sid]
        self.server = native().MaintenanceServer(self.netc, n['host'], n['port'] + self.b['port_offset'], self.handle)

    def rpc(self, node, op, **fields):
        request = dict(op=op, token=self.owner.token, **fields)
        result = self.client.request(node['host'], node['port'] + self.b['port_offset'], request)
        reply = result['response']
        if not reply.get('ok'): raise RuntimeError(reply.get('error', 'Replica RPC failed'))
        return reply['result']

    def event(self, state, phase, **extra):
        with self.lock:
            state.update(phase=phase, **extra)
            state['events'].append(dict(phase=phase, monotonic_ns=time.monotonic_ns(), **extra))
            public = {k: v for k, v in state.items() if k not in ('fds', 'blocks', 'plan', 'future', 'lock', 'cancel')}
            tmp = self.root / (state['id'] + '.json.tmp')
            tmp.write_text(json.dumps(public, indent=2))
            tmp.replace(self.root / (state['id'] + '.json'))

    def handle(self, request):
        if not isinstance(request.get('token'), str) or not hmac.compare_digest(request['token'], self.owner.token):
            raise ValueError('Invalid replica management token')
        op = request['op']
        if op == 'hotness':
            return dict(ok=True,result=self.owner.worker.native_search.hotness_snapshot())
        if op == 'hotness_reset':
            q=self.owner.peer.runtime.network_metrics()['compute_queue']
            if q['active'] or q['queued']:raise ValueError('Hotness reset requires idle compute workers')
            self.owner.worker.native_search.configure_hotness(self.c)
            return dict(ok=True,result=dict(reset=True))
        if op == 'load':
            metrics = self.owner.peer.runtime.network_metrics()
            return dict(ok=True, result=dict(node=self.owner.sid, time_ns=time.monotonic_ns(),
                runtime=metrics, meta=self.owner.worker.meta,
                hotness=self.owner.worker.native_search.hotness_metrics(),
                free_bytes=shutil.disk_usage(self.root).free,
                centers_sha256=digest(self.owner.worker.directory / 'centers.npy')))
        if op == 'routes':
            self.owner.worker.native_search.replica_routes(request['routes'])
            return dict(ok=True,result=dict(updated=True))
        if op == 'counters':
            return dict(ok=True,result=dict(peer_selected=self.owner.worker.native_search.replica_counters()))
        version = ident(request['id'])
        if op == 'source_start':
            with self.lock:
                if version in self.states:
                    if self.states[version].get('plan') != request['plan']:
                        raise ValueError('Conflicting source retry')
                    return dict(ok=True, result=self.status(version))
                if any(s['phase'] in ('BUILDING', 'TRANSFERRING') for s in self.states.values()):
                    raise RuntimeError('A source build is already running')
                state = dict(id=version, role='source', phase='BUILDING', events=[], plan=request['plan'])
                self.states[version] = state
                self.event(state, 'BUILDING')
                state['future'] = self.jobs.submit(self.send, state)
            return dict(ok=True, result=self.status(version))
        if op == 'status': return dict(ok=True, result=self.status(version))
        if op == 'prepare': return dict(ok=True, result=self.prepare(version, request['manifest']))
        if op == 'chunk': return dict(ok=True, result=self.chunk(version, request))
        if op == 'commit': return dict(ok=True, result=self.commit(version))
        if op == 'warm':
            with self.lock:
                s = self.states[version]
                if s['phase'] == 'VERIFIED':
                    self.event(s, 'WARMING')
                    s['future'] = self.jobs.submit(self.warm, s)
                elif s['phase'] not in ('WARMING', 'READY'): raise ValueError('Replica not verified')
            return dict(ok=True, result=self.status(version))
        if op == 'retire':
            s = self.states[version]
            with s['lock']:
                s['cancel'].set()
                self.owner.worker.native_search.retire_replica(version)
                self.event(s, 'DRAINING')
            return dict(ok=True, result=self.status(version))
        if op == 'reap':
            if not self.owner.worker.native_search.replica_released(version):
                return dict(ok=True, result=dict(reclaimed=False))
            with self.lock:
                s = self.states[version]
                if s['phase']=='RETIRED':return dict(ok=True,result=dict(reclaimed=True))
                if s.get('future') is not None and not s['future'].done():
                    return dict(ok=True, result=dict(reclaimed=False))
                if s['phase'] not in ('DRAINING', 'FAILED'): raise ValueError('Replica is still accepting')
                for fd in s.pop('fds', {}).values(): os.close(fd)
                path = self.root / version
                if path.exists(): shutil.rmtree(path)
                self.event(s, 'RETIRED')
            return dict(ok=True, result=dict(reclaimed=True))
        raise ValueError('Unsupported replica operation')

    def status(self, version):
        with self.lock:
            s = self.states[version]
            return {k: v for k, v in s.items() if k not in ('fds', 'blocks', 'plan', 'future', 'lock', 'cancel')}

    def prepare(self, version, manifest):
        m = self.owner.worker.meta
        if manifest['contract'] != {k: m[k] for k in CONTRACT}:
            raise ValueError('Replica graph/quantization contract mismatch')
        if manifest['centers_sha256'] != digest(self.owner.worker.directory / 'centers.npy'):
            raise ValueError('Replica centers mismatch')
        if set(manifest['files']) != FILES: raise ValueError('Unexpected replica files')
        total = sum(f['bytes'] for f in manifest['files'].values())
        if any(type(f['bytes']) is not int or f['bytes'] <= 0 for f in manifest['files'].values()):
            raise ValueError('Invalid replica file length')
        fraction=self.b.get('max_target_shard_fraction',float('inf'))
        shard_bytes=self.owner.worker.meta['header_bytes']+self.owner.worker.meta['rows']*self.owner.worker.meta['record_bytes']
        if total != manifest['bytes'] or total > self.b['max_region_bytes'] or total>fraction*shard_bytes:
            raise ValueError('Replica byte budget exceeded')
        with self.lock:
            if version in self.states:
                if self.states[version].get('manifest') != manifest: raise ValueError('Conflicting replica retry')
                return self.status(version)
            active = sum(s.get('role') == 'target' and s['phase'] not in ('RETIRED', 'FAILED') for s in self.states.values())
            if active >= self.b['max_replicas']: raise ValueError('Replica capacity exceeded')
            used=sum(x.get('manifest',{}).get('bytes',0) for x in self.states.values() if x.get('role')=='target' and x['phase'] not in ('RETIRED','FAILED'))
            if used+total>self.b.get('max_total_replica_bytes',self.b['max_region_bytes']*self.b['max_replicas']):
                raise ValueError('Total target replica byte budget exceeded')
            if shutil.disk_usage(self.root).free < total + self.b['reserve_disk_bytes']:
                raise ValueError('Replica destination lacks space')
            path = self.root / version
            path.mkdir(exist_ok=False)
            s = dict(id=version, role='target', phase='TRANSFERRING', events=[], manifest=manifest,
                     fds={}, blocks={}, lock=threading.Lock(), cancel=threading.Event(), received_bytes=0)
            self.states[version] = s
            try:
                for name, info in manifest['files'].items():
                    fd = os.open(path / name, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                    s['fds'][name] = fd
                    os.posix_fallocate(fd, 0, info['bytes'])
                self.event(s, 'TRANSFERRING')
            except BaseException as e:
                for fd in s['fds'].values(): os.close(fd)
                s['fds'].clear()
                self.event(s, 'FAILED', error=str(e))
                shutil.rmtree(path)
                raise
        return self.status(version)

    def chunk(self, version, request):
        s = self.states[version]
        with s['lock']:
            if s['phase'] != 'TRANSFERRING': raise ValueError('Replica not receiving')
            name, offset, raw = request['file'], request['offset'], request['data']
            if name not in FILES or type(offset) is not int or not isinstance(raw, bytes):
                raise ValueError('Invalid replica chunk')
            length = s['manifest']['files'][name]['bytes']
            if offset < 0 or offset % self.b['chunk_bytes'] or len(raw) != min(self.b['chunk_bytes'], length - offset) or not raw:
                raise ValueError('Invalid replica chunk range')
            checksum = hashlib.sha256(raw).hexdigest()
            if checksum != request['sha256']: raise ValueError('Replica chunk checksum mismatch')
            key = (name, offset)
            if key in s['blocks']:
                if s['blocks'][key] != checksum: raise ValueError('Conflicting replica chunk')
                return dict(duplicate=True)
            view = memoryview(raw)
            written = 0
            while written < len(raw):
                n = os.pwrite(s['fds'][name], view[written:], offset + written)
                if n <= 0: raise OSError('Short replica write')
                written += n
            s['blocks'][key] = checksum
            s['received_bytes'] += len(raw)
            return dict(duplicate=False, bytes=len(raw))

    def commit(self, version):
        s = self.states[version]
        with s['lock']:
            if s['phase'] in ('VERIFIED', 'WARMING', 'READY'): return self.status(version)
            if s['phase'] != 'TRANSFERRING' or s['received_bytes'] != s['manifest']['bytes']:
                raise ValueError('Replica transfer incomplete')
            for name, info in s['manifest']['files'].items():
                if digest(self.root / version / name) != info['sha256']:
                    raise ValueError('Replica file checksum mismatch')
                os.fsync(s['fds'][name])
            for fd in s['fds'].values(): os.close(fd)
            s['fds'].clear()
            self.event(s, 'VERIFIED')
        return self.status(version)

    def warm(self, s):
        shard = None
        try:
            started = time.monotonic()
            shard = Shard(self.root / s['id'], 0, id_lookup_max_bytes=self.b.get('replica_id_lookup_bytes',0))
            # Eagerly validate every record's degree and code dependency before
            # publication. This is also a bounded sequential SSD/cache warm pass.
            entries = set(s['manifest']['entries'])
            if not entries <= set(shard.location): raise ValueError('Replica entry missing')
            if self.b.get('native_replica_pack',False):
                read_bytes=native().replica_validate_records(shard.fd,shard.ids,shard.code_ids,
                    shard.meta['dimension'],shard.meta['record_bytes'],shard.meta['header_bytes'],shard.meta['max_degree'])
            else:
                read_bytes = 0
                for gid,raw in read_records(shard,shard.ids):
                    if self.stop.is_set() or s['cancel'].is_set() or time.monotonic() - started > self.b['operation_timeout_s']:
                        raise TimeoutError('Replica warm timeout/cancelled')
                    if len(raw) != shard.meta['record_bytes']: raise ValueError('Replica short warm read')
                    degree, = struct.unpack_from('<I', raw, shard.meta['dimension'] * 4)
                    if degree > shard.meta['max_degree'] or shard.meta['dimension'] * 4 + 4 + degree * 8 > len(raw):
                        raise ValueError('Invalid replica degree')
                    if any(int(v) not in shard.code_location for v in np.frombuffer(raw, '<u8', degree, shard.meta['dimension'] * 4 + 4)):
                        raise ValueError('Replica has a missing neighbor code')
                    read_bytes += len(raw)
            warmed = 0
            for gid in list(entries) + [int(v) for v in shard.ids if int(v) not in entries]:
                if self.stop.is_set() or s['cancel'].is_set(): raise RuntimeError('Replica warm cancelled')
                if warmed + shard.meta['record_bytes'] > self.b['warm_bytes']: break
                os.pread(shard.fd, shard.meta['record_bytes'], shard.meta['header_bytes'] + shard.location[gid] * shard.meta['record_bytes'])
                warmed += shard.meta['record_bytes']
            membership = np.asarray(load_complete_npy(shard.directory / 'code_membership.npy'), dtype='uint8')
            with s['lock']:
                if self.stop.is_set() or s['cancel'].is_set(): raise RuntimeError('Replica publication cancelled')
                self.owner.worker.native_search.install_replica(s['id'], shard, membership)
                self.event(s, 'READY', validation_read_bytes=read_bytes, warm_read_bytes=warmed, warm_seconds=time.monotonic() - started)
        except BaseException as e:
            self.event(s, 'FAILED', error=str(e))
        finally:
            if shard: shard.close()

    def send(self, s):
        path = self.root / (s['id'] + '_source')
        try:
            p = s['plan']
            manifest = package(self.owner.worker, p['ids'], p['entries'], path, self.b, self.stop.is_set)
            target = self.c['cluster']['nodes'][p['target']]
            self.rpc(target, 'prepare', id=s['id'], manifest=manifest)
            self.event(s, 'TRANSFERRING', manifest=manifest)
            begin, total = time.monotonic(), 0
            next_send, check_at, pressure = begin, begin, False
            checks, throttled_bytes = 0, 0
            pending = deque()
            with ThreadPoolExecutor(max_workers=self.b['inflight_chunks']) as pool:
                for name in sorted(manifest['files']):
                    with (path / name).open('rb') as f:
                        offset = 0
                        while raw := f.read(self.b['chunk_bytes']):
                            if self.stop.is_set(): raise RuntimeError('Replica transfer cancelled')
                            if time.monotonic()-begin > self.b['operation_timeout_s']:
                                raise TimeoutError('Replica transfer deadline exceeded')
                            if len(pending) >= self.b['inflight_chunks']: pending.popleft().result()
                            total += len(raw)
                            now = time.monotonic()
                            if now >= check_at:
                                local = self.owner.peer.runtime.network_metrics()['compute_queue']['queued']
                                remote = self.rpc(target, 'load')['runtime']['compute_queue']['queued']
                                pressure = max(local if self.b.get('copy_pressure_source_queue',True) else 0, remote) >= self.b['copy_pressure_queue_tasks']
                                checks += 1; check_at = time.monotonic()+self.b['copy_pressure_check_ms']/1000
                            factor = self.b['copy_pressure_rate_factor'] if pressure else 1.
                            if pressure: throttled_bytes += len(raw)
                            next_send = max(next_send, now) + len(raw)/(self.b['copy_mib_per_s']*1048576*factor)
                            delay = next_send-time.monotonic()
                            if delay > 0 and self.stop.wait(delay): raise RuntimeError('Replica transfer cancelled')
                            pending.append(pool.submit(self.rpc, target, 'chunk', id=s['id'], file=name,
                                offset=offset, data=raw, sha256=hashlib.sha256(raw).hexdigest()))
                            offset += len(raw)
                for future in pending: future.result()
            self.rpc(target, 'commit', id=s['id'])
            self.rpc(target, 'warm', id=s['id'])
            self.event(s, 'SENT', sent_bytes=total, transfer_seconds=time.monotonic() - begin,
                       foreground_pressure_checks=checks, throttled_bytes=throttled_bytes)
        except BaseException as e:
            self.event(s, 'FAILED', error=str(e))
        finally:
            if path.exists(): shutil.rmtree(path)

    def close(self):
        self.stop.set()
        self.client.close()
        self.jobs.shutdown(wait=True)
        self.server.close()
        for s in self.states.values():
            for fd in s.get('fds', {}).values(): os.close(fd)
            s.get('fds', {}).clear()
