"""Persistent HNSW head, coordinated live configuration, and result collection."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo
import gzip
import hashlib
import json
import socket
import threading
import time
import uuid
from pathlib import Path

import msgpack
import numpy as np
import queue

from .managed_node import clean_config, hot_values, patched, revision, save_json
from .transport import send, receive
from .search import Engine
from .metrics import distribution, export_query_csv, summarize_query_timings, summarize_per_query_timings, export_resource_csvs


def admin_rpc(node,token,op,timeout=30.,limit=8388608,**fields):
    rid=uuid.uuid4().hex
    message=dict(version=1,op=op,request_id=rid,token=token,**fields)
    # Identity and transaction fields are unchanged across a lost-ACK retry.
    for attempt in range(2):
        try:
            with socket.create_connection((node['host'],node['port']),timeout) as sock:
                sock.settimeout(timeout);send(sock,message,limit);reply,_=receive(sock,limit)
            if reply.get('request_id')!=rid:raise ValueError('Management reply identity mismatch')
            if not reply.get('ok'):raise RuntimeError(f"Node {node['id']}: {reply.get('error')}")
            return reply['result']
        except (OSError,ConnectionError):
            if attempt:raise


class ClusterHead:
    def __init__(self,c,settings,token,truth=None):
        self.settings=settings;self.token=token;self.truth=truth
        self.root=Path(settings['results_directory']);self.root.mkdir(parents=True,exist_ok=True)
        self.state_file=self.root/'head_state.json';self.c=clean_config(c)
        if self.state_file.exists():self.c=patched(self.c,json.loads(self.state_file.read_text())['settings'])
        self.c['_cluster_revision']=revision(self.c)
        self.engine=Engine(self.c)
        self.op_lock=threading.Lock();self.benchmark_lock=threading.Lock();self.cv=threading.Condition();self.active=0
        self.busy=None;self.ready=False;self.error=None

    def rpc(self,node,op,**fields):
        return admin_rpc(node,self.token,op,self.settings['command_timeout_seconds'],
                         self.c['network']['max_message_bytes'],**fields)

    def nodes(self):return self.c['cluster']['nodes']

    def status(self):
        nodes=[]
        for n in self.nodes():
            try:nodes.append(self.rpc(n,'admin_status'))
            except Exception as exc:nodes.append(dict(node_id=n['id'],error=str(exc)))
        hub=self.engine.head_hub
        receivers=hub.status() if hasattr(hub,'status') else []
        return dict(ready=self.ready,busy=self.busy,active_queries=self.active,revision=revision(self.c),
                    benchmark_active=self.benchmark_lock.locked(),
                    head_receivers=receivers,
                    native_head=(self.engine.runtime.metrics() if self.c.get('native_runtime',{}).get('head_enabled',False) else None),
                    head_pipeline=(self.engine.head_flow.ingress.status() if hasattr(self.engine.head_flow,'ingress') else None),
                    error=self.error,settings=hot_values(self.c),nodes=nodes)

    @contextmanager
    def exclusive(self,operation,require_ready=True):
        if not self.op_lock.acquire(blocking=False):raise RuntimeError('Another control operation is running')
        try:
            with self.cv:
                if require_ready and not self.ready:raise RuntimeError('Cluster is not ready; run recover')
                self.busy=operation
                if not self.cv.wait_for(lambda:self.active==0,self.settings['drain_timeout_seconds']):
                    raise RuntimeError('Timed out draining head queries')
            yield
        finally:
            with self.cv:self.busy=None;self.cv.notify_all()
            self.op_lock.release()

    def quiescent(self):
        deadline=time.monotonic()+self.settings['drain_timeout_seconds']
        while True:
            states=[self.rpc(n,'admin_status') for n in self.nodes()]
            if all(s['quiescent'] for s in states):return states
            if time.monotonic()>=deadline:raise RuntimeError('Data nodes did not drain; configuration not changed')
            time.sleep(.02)

    def configure(self,patch,recover=False):
        with self.exclusive('recover' if recover else 'configure',require_ready=not recover):
            candidate=patched(self.c,patch)
            # Native Router fixes ef on its first search to permit lock-free
            # concurrent reads. Replace only the head's HNSW object while idle.
            router=self.engine.router
            if candidate['routing']['ef_search']!=self.c['routing']['ef_search'] and not candidate['peer'].get('head_lanes',{}).get('enabled',False):
                from .index import native
                m=self.engine.manifest;r=candidate['routing']
                router=native().Router(m['dimension'],m['route_samples'],r['hnsw_m'],r['ef_construction'],candidate['seed'])
                router.load(str(Path(candidate['paths']['output'])/'index/router.hnsw'))
            self.ready=False;tx=uuid.uuid4().hex;log=self.root/'transactions'/f'{tx}.json'
            journal=dict(transaction=tx,phase='draining',target_revision=revision(candidate),patch=patch)
            save_json(log,journal)
            try:
                states=self.quiescent()
                for n,s in zip(self.nodes(),states):
                    if s['epoch']!=self.engine.manifest['epoch']:raise ValueError('Data node index epoch mismatch')
                    self.rpc(n,'admin_pause')
                    if s.get('transaction'):
                        if not recover:raise RuntimeError('Pending transaction requires recover')
                        self.rpc(n,'admin_abort',transaction=s['transaction']['id'])
                states=self.quiescent()
                for n,s in zip(self.nodes(),states):
                    self.rpc(n,'admin_prepare',transaction=tx,expected_revision=s['revision'],patch=hot_values(candidate))
                journal['phase']='prepared';save_json(log,journal)
                for n in self.nodes():self.rpc(n,'admin_commit',transaction=tx)
                # Last fully chosen head configuration is the recovery target.
                # Before this write: recover old. After this write: recover new.
                save_json(self.state_file,dict(settings=hot_values(candidate),revision=revision(candidate)))
                candidate['_cluster_revision']=revision(candidate)
                self.c=candidate;self.engine.apply_configuration(candidate,router)
                journal['phase']='committed';save_json(log,journal)
                for n in self.nodes():self.rpc(n,'admin_finalize',transaction=tx)
                for n in self.nodes():self.rpc(n,'admin_resume',revision=revision(candidate))
                states=[self.rpc(n,'admin_status') for n in self.nodes()]
                if not all(s['accepting'] and s['revision']==revision(candidate) for s in states):
                    raise RuntimeError('Cluster configuration verification failed')
                self.ready=True;self.error=None;journal.update(phase='complete',nodes=states)
                save_json(log,journal);return dict(revision=revision(candidate),nodes=states,transaction=tx)
            except Exception as exc:
                self.error=str(exc);journal.update(phase='failed',error=self.error);save_json(log,journal)
                # No new query may enter a partially updated cluster. Recovery
                # resolves pending node transactions against head_state.json.
                raise

    def query(self,vector,query_id=None):
        with self.cv:
            if not self.ready or self.busy:raise RuntimeError('Head is paused or not ready')
            self.active+=1
        try:
            result=self.engine.search(vector,query_id);result['config_revision']=revision(self.c);return result
        finally:
            with self.cv:self.active-=1;self.cv.notify_all()

    def query_async(self,vector,query_id=None):
        with self.cv:
            if not self.ready or self.busy:raise RuntimeError('Head is paused or not ready')
            self.active+=1
        try:future=self.engine.search_async(vector,query_id)
        except BaseException:
            with self.cv:self.active-=1;self.cv.notify_all()
            raise
        def finished(f):
            try:
                if not f.cancelled() and f.exception() is None:f.result()['config_revision']=revision(self.c)
            finally:
                with self.cv:self.active-=1;self.cv.notify_all()
        future.add_done_callback(finished)
        return future

    def pull(self):
        def node_metrics(n):
            meta=self.rpc(n,'admin_snapshot');pieces=[]
            for offset in range(0,meta['bytes'],262144):
                reply=self.rpc(n,'admin_chunk',snapshot_id=meta['snapshot_id'],offset=offset,count=262144)
                pieces.append(reply['data'])
            raw=b''.join(pieces)
            if len(raw)!=meta['bytes'] or hashlib.sha256(raw).hexdigest()!=meta['sha256']:
                raise ValueError('Incomplete metrics snapshot')
            return n['id'],msgpack.unpackb(raw,raw=False,strict_map_key=False)
        with ThreadPoolExecutor(max_workers=len(self.nodes())) as pool:return dict(pool.map(node_metrics,self.nodes()))

    def new_run(self,kind):
        name=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%dT%H%M%S')+'_'+kind+'_'+uuid.uuid4().hex[:8]
        path=self.root/'runs'/name;path.mkdir(parents=True)
        save_json(path/'configuration.json',dict(config=clean_config(self.c),revision=revision(self.c)))
        return path

    def collect(self):
        with self.exclusive('collect'):
            self.quiescent();path=self.new_run('collect');data=self.pull()
            with gzip.open(path/'nodes.json.gz','wt') as f:json.dump(data,f,allow_nan=False)
            return dict(directory=str(path),nodes=list(data),revision=revision(self.c))

    def stop_nodes(self):
        with self.exclusive('stop_nodes',require_ready=False):
            self.quiescent();self.ready=False
            results=[]
            for n in self.nodes():results.append(self.rpc(n,'admin_shutdown'))
            return results

    def benchmark(self,options):
        allowed={'queries','concurrency','warmup'}
        if not isinstance(options,dict) or set(options)-allowed:raise ValueError('Unknown benchmark options')
        opts={**self.settings['benchmark'],**options}
        count,concurrency,warmup=[opts[k] for k in ('queries','concurrency','warmup')]
        if any(type(v)is not int for v in (count,concurrency,warmup)) or not 1<=count<=self.settings['max_queries'] or not 1<=concurrency<=self.settings['max_client_threads'] or not 0<=warmup<=self.settings['max_queries']:
            raise ValueError('Invalid benchmark limits')
        if self.truth is None:raise RuntimeError('No validated ground truth configured')
        if not self.benchmark_lock.acquire(blocking=False):
            raise RuntimeError('A benchmark capture or report is already running')
        try:
            with self.exclusive('benchmark'):
                if getattr(self.engine,'disk_mode',False):
                    return self._capture_disk_benchmark(opts)
                captured=self._capture_benchmark(opts)
            # All query/snapshot work is complete. Ordinary queries and safe
            # configuration transactions can proceed while this report runs.
            return self._finish_benchmark(captured)
        finally:
            self.benchmark_lock.release()

    def _capture_disk_benchmark(self,opts):
        """Native retrieval into RAM, then deferred output; return without analysis."""
        from .metrics import process_snapshot
        count,concurrency,warmup=[opts[k] for k in ('queries','concurrency','warmup')]
        path=self.new_run('disk_benchmark');save_json(path/'benchmark_options.json',opts)
        try:
            selection=json.loads(Path(self.settings['query_selection']).read_text())['validation'][:count]
            if len(selection)!=count:raise ValueError('Not enough validation queries')
            vectors=np.load(self.c['query']['vectors'],mmap_mode='r');ids=np.load(self.c['query']['ids'],mmap_mode='r')
            x=np.ascontiguousarray(vectors[selection],dtype=np.float64)
            qids=np.ascontiguousarray(ids[selection],dtype=np.uint64)
            truth=[self.truth(int(row),self.c['search']['top_k']) for row in selection]
            if warmup:
                picks=np.arange(warmup)%count
                self.engine.run_to_disk(x[picks],qids[picks],path/'warmup',min(concurrency,warmup))
            self.quiescent();before=self.pull();head_before=[process_snapshot()]
            disk=self.engine.run_to_disk(x,qids,path/'results',concurrency)
            head_after=[process_snapshot()]
            save_json(path/'disk_manifest.json',disk)
            self.quiescent();after=self.pull()
            rev=revision(self.c)
            settings=self.c['benchmark_results']
            save_json(path/'capture_context.json',dict(settings=settings,selection=selection,truth=truth,
                query_ids=qids.tolist(),config_revision=rev,max_message_bytes=self.c['network']['max_message_bytes'],
                offered_ns=disk['offered_ns']))
            save_json(path/'report_context.json',dict(count=count,concurrency=concurrency,offered_ns=disk['offered_ns'],
                config_revision=rev,max_expansions=self.c['search']['max_expansions'],before=before,after=after,
                head_before=head_before,head_after=head_after,receiver_roles={},report_receiver_pids=[],
                build_profile=self.engine.manifest.get('build_profile'),capture=disk))
            return dict(directory=str(path),offline_report_pending=True,query_count=count,
                completion=disk['completion'],result_sink=disk,
                raw_completion_qps=count/((disk['last_completion_ns']-disk['offered_ns'])/1e9))
        except BaseException as exc:
            save_json(path/'failed.json',dict(error=str(exc),revision=revision(self.c)));raise

    def _capture_benchmark(self,opts):
        count,concurrency,warmup=[opts[k] for k in ('queries','concurrency','warmup')]
        path=self.new_run('benchmark');save_json(path/'benchmark_options.json',opts)
        spool=None
        try:
            selection=json.loads(Path(self.settings['query_selection']).read_text())['validation'][:count]
            if len(selection)!=count:raise ValueError('Not enough validation queries')
            vectors=np.load(self.c['query']['vectors'],mmap_mode='r');ids=np.load(self.c['query']['ids'],mmap_mode='r')
            truth=[self.truth(int(row),self.c['search']['top_k']) for row in selection]
            benchmark_revision=revision(self.c)
            for i in range(warmup):
                row=selection[i%count];self.engine.search(vectors[row],int(ids[row]))
            from .benchmark_archive import CaptureArchive
            import yaml
            settings=self.c.get('benchmark_results')
            if settings is None:
                defaults=Path(__file__).resolve().parents[2]/'configs/dhdgs.yaml'
                settings=yaml.safe_load(defaults.read_text())['benchmark_results']
            capture_context=dict(settings=settings,selection=selection,truth=truth,
                query_ids=[int(ids[row]) for row in selection],config_revision=benchmark_revision,
                max_message_bytes=self.c['network']['max_message_bytes'])
            self.quiescent();before=self.pull();gate=threading.Event()
            spool=CaptureArchive(path,settings,capture_context);result_lock=threading.Lock()
            def failure(exc):
                context=getattr(exc,'dhdgs_metrics',{})
                if context.get('status')=='error' and 'ids' in context:return context
                return dict(status='error',ids=[],error=str(exc),request_id=context.get('request_id',uuid.uuid4().hex),
                            stats=context.get('stats',{'metrics_complete':False}))
            def capture(found,job,start,done,observed,native_capture=False):
                if native_capture:
                    record,api_offered,api_delivered=found
                    spool.append_native(record,dict(job=job,start_ns=start,service_done_ns=done,
                        observed_ns=observed,native_capture=True,api_offered_ns=api_offered,
                        api_delivered_ns=api_delivered,result_encoding_deferred=True))
                    return
                else:
                    encode_start=time.monotonic_ns();payload=msgpack.packb(found,use_bin_type=True)
                    encoding=dict(start_ns=encode_start,end_ns=time.monotonic_ns());api_offered=start;api_delivered=done
                spool.append(payload,dict(job=job,start_ns=start,service_done_ns=done,observed_ns=observed,
                    native_capture=native_capture,api_offered_ns=api_offered,api_delivered_ns=api_delivered,
                    result_encoding=encoding))
            def worker(i):
                gate.wait()
                for job in range(i,count,concurrency):
                    row=selection[job];start=time.monotonic_ns()
                    try:found=self.engine.search(vectors[row],int(ids[row]))
                    except Exception as exc:found=failure(exc)
                    done=time.monotonic_ns()
                    with result_lock:capture(found,job,start,done,time.monotonic_ns())
            pipeline=(self.c.get('native_runtime',{}).get('head_enabled',False) or
                      any(self.c['peer'].get(k,{}).get('enabled',False) for k in ('head_pipeline','head_direct','head_lanes')))
            threads=[] if pipeline else [threading.Thread(target=worker,args=(i,),name=f'head-query-{i}') for i in range(min(concurrency,count))]
            for t in threads:t.start()
            from .metrics import process_snapshot
            hub=self.engine.head_hub
            receiver_rows=hub.status() if hasattr(hub,'status') else []
            receiver_pids=[w['pid'] for w in receiver_rows]
            receiver_roles={w['pid']:w.get('role','report_receiver') for w in receiver_rows}
            report_receiver_pids=set(receiver_pids)
            if self.c['peer'].get('head_direct',{}).get('enabled',False):
                receiver_pids+=self.engine.head_flow.status()['result_processes']
            head_before=[process_snapshot(pid) for pid in [None,*receiver_pids]]
            from .gc_profile import snapshot as gc_snapshot
            head_gc_before=gc_snapshot()
            offered=time.monotonic_ns();capture_context['offered_ns']=offered;gate.set()
            if pipeline:
                completed=queue.Queue();next_job=0;inflight=0
                native_capture=hasattr(self.engine,'search_capture_async')
                search_async=self.engine.search_capture_async if native_capture else self.engine.search_async
                def submit(job):
                    row=selection[job];start=time.monotonic_ns()
                    try:future=search_async(vectors[row],int(ids[row]))
                    except Exception as exc:
                        from concurrent.futures import Future
                        future=Future();future.set_exception(exc)
                    future.add_done_callback(lambda f:completed.put((job,start,time.monotonic_ns(),f)))
                while next_job<count and inflight<concurrency:
                    submit(next_job);next_job+=1;inflight+=1
                while inflight:
                    job,start,done,future=completed.get()
                    packed=native_capture
                    try:found=future.result()
                    except Exception as exc:found=failure(exc);packed=False
                    observed=time.monotonic_ns();inflight-=1
                    # Refill search before storing a result. No recall, result
                    # decoding, JSON or full Python object graph on native path.
                    if next_job<count:submit(next_job);next_job+=1;inflight+=1
                    capture(found,job,start,done,observed,packed)
            for t in threads:t.join()
            if spool.count!=count:raise RuntimeError('Missing benchmark capture records')
            head_gc_after=gc_snapshot()
            head_after=[process_snapshot(p['pid']) for p in head_before]
            save_json(path/'head_gc.json',dict(before=head_gc_before,after=head_gc_after))
            self.quiescent();after=self.pull()
            context=dict(count=count,concurrency=concurrency,offered_ns=offered,
                config_revision=benchmark_revision,max_expansions=self.c['search']['max_expansions'],
                before=before,after=after,head_before=head_before,head_after=head_after,
                receiver_roles=receiver_roles,report_receiver_pids=list(report_receiver_pids),
                build_profile=self.engine.manifest.get('build_profile'),capture=None)
            return path,spool,settings,context
        except BaseException as exc:
            save_json(path/'failed.json',dict(error=str(exc),revision=revision(self.c)))
            if spool is not None:spool.close()
            raise

    def _finish_benchmark(self,captured):
        from .benchmark_archive import processor_command,processor_env
        import subprocess
        path,spool,settings,context=captured
        try:
            spool.flush()
            context['capture']=dict(spool.stats);spool.close();spool=None
            save_json(path/'report_context.json',context)
            with (path/'processor.log').open('ab') as errors:
                report=subprocess.run(processor_command(path),stdout=subprocess.PIPE,stderr=errors,
                    env=processor_env(),timeout=settings['processor_timeout_seconds'],check=True)
            return json.loads(report.stdout)
        except BaseException as exc:
            save_json(path/'failed.json',dict(error=str(exc),revision=context['config_revision']));raise
        finally:
            if spool is not None:spool.close()

    def close(self):
        with self.benchmark_lock:self.engine.close()
