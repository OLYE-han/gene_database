"""Offline reader for C++ result writers. No import/use on the query hot path."""
import json
import struct
from pathlib import Path
import msgpack

U64=struct.Struct('!Q')
HEADER=struct.Struct('!QQ')

def exact(f,n):
    b=f.read(n)
    if len(b)!=n:raise ValueError('Truncated native result file')
    return b

class DiskRecords:
    def __init__(self,directory):
        self.directory=Path(directory)
        self.manifest=json.loads((self.directory/'disk_manifest.json').read_text())
        self.context=json.loads((self.directory/'capture_context.json').read_text())
        if self.manifest['format']!='dhdgs-native-frames-v1' or not self.manifest['all_synced']:
            raise ValueError('Native result files are not finalized')
    def __len__(self):return self.manifest["count"]
    def __iter__(self):
        c=self.context;m=self.manifest;root=Path(m['directory'])
        for writer in m['writers']:
            stem=root/f"writer-{writer['writer']}"
            commits=[]
            with stem.with_suffix('.commits').open('rb') as f:
                while prefix:=f.read(8):
                    if len(prefix)!=8:raise ValueError('Truncated commit header')
                    size=U64.unpack(prefix)[0]
                    if size>65536:raise ValueError('Invalid commit size')
                    commit=msgpack.unpackb(exact(f,size),raw=False)
                    if commit['batch']!=len(commits):raise ValueError('Unordered disk batch')
                    commits.append(commit)
            if len(commits)!=writer['batches']:raise ValueError('Missing disk commit')
            seen=total=0
            with stem.with_suffix('.frames').open('rb') as f:
                for commit in commits:
                    begin=f.tell()
                    for _ in range(commit['records']):
                        meta_size,payload_size=HEADER.unpack(exact(f,16))
                        if meta_size>65536 or payload_size>c['max_message_bytes']:raise ValueError('Invalid disk frame length')
                        meta=msgpack.unpackb(exact(f,meta_size),raw=False)
                        q=msgpack.unpackb(exact(f,payload_size),raw=False,strict_map_key=False)
                        if meta['batch']!=commit['batch'] or meta['writer']!=writer['writer']:raise ValueError('Disk commit does not match payload')
                        yield self.normalize(q,meta,commit,writer)
                        seen+=1
                    if f.tell()-begin!=commit['bytes']:raise ValueError('Disk batch byte count mismatch')
                    total+=commit['bytes']
                if f.read(1):raise ValueError('Uncommitted trailing result bytes')
            if seen!=writer['records'] or total!=writer['bytes']:raise ValueError('Disk writer count mismatch')
    def normalize(self,q,meta,commit,writer):
        c=self.context;job=meta['job'];start=meta['start_ns']
        deferred=self.manifest.get('output_mode')=='deferred'
        output_end=commit['completion_ns']
        end=q['retrieval_done_ns'] if deferred else output_end
        if not 0<=job<len(c['selection']):raise ValueError('Invalid disk job')
        stats=q.setdefault('stats',{});q.setdefault('ids',[])
        stages=dict(result_encoding_ms=(meta['encoding_done_ns']-meta['encoding_start_ns'])/1e6,
            result_batch_wait_ms=(commit['write_start_ns']-meta['encoding_done_ns'])/1e6,
            result_write_ms=(commit['write_done_ns']-commit['write_start_ns'])/1e6,
            result_batch_sync_ms=(output_end-commit['write_done_ns'])/1e6)
        if q['status']=='ok':
            t=stats['native_head_timestamps']
            retained=meta['retained_ns'] if deferred else t['completed_dequeued_ns']
            published=retained if deferred else t['completed_enqueued_ns']
            stages.update(result_publish_ms=(published-t['native_ready_ns'])/1e6,
                result_queue_ms=0. if deferred else (retained-published)/1e6,
                result_writer_wait_ms=(meta['encoding_start_ns']-retained)/1e6,
                native_submit_ms=(t['search_enter_ns']-start)/1e6)
            if min(stages.values())<0:raise ValueError('Negative disk stage duration')
            stats.update(stages,result_delivery_ms=(output_end-t['native_ready_ns'])/1e6)
            parts=stats['latency_breakdown_ms']
            if deferred:
                # Only submission overhead belongs to retrieval latency.
                # Retention, deferred encoding, write and sync remain separate.
                parts['native_submit_ms']=stages['native_submit_ms']
                stats['postprocessing_breakdown_ms']={k:v for k,v in stages.items() if k!='native_submit_ms'}
            else:parts.update(stages)
            stats['latency_breakdown_fraction']={k:v/((end-start)/1e6) for k,v in parts.items()}
        stats['latency_ms']=(end-start)/1e6
        q.update(job=job,query_id=c['query_ids'][job],query_row=c['selection'][job],
            client_start_ns=start,client_end_ns=end,client_wall_ms=(end-start)/1e6,
            offered_ns=c['offered_ns'],client_backlog_ms=(start-c['offered_ns'])/1e6,
            offered_to_complete_ms=(end-c['offered_ns'])/1e6,
            benchmark_consumer_wait_ms=0.,benchmark_observed_wall_ms=(end-start)/1e6,
            result_sink_timestamps={**meta,**commit,'final_sync_done_ns':writer['final_sync_done_ns']},
            capture_format='native_disk_msgpack',result_encoding_phase='deferred_native_writer' if deferred else 'native_writer',
            output_complete_ns=output_end,output_after_retrieval_ms=(output_end-end)/1e6,
            truth_ids=c['truth'][job],recall=len(set(q['ids'])&set(c['truth'][job]))/len(c['truth'][job]),
            config_revision=c['config_revision'])
        return q
