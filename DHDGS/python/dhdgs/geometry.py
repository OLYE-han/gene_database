"""Epoch-bound exact routing labels and complete, navigable geometric replicas.

The cached nearest distance makes nonnegative buffered group predicates cheap.
Path closure uses original directed edges in both directions to one fixed hub.
Membership and navigation remain distinct contracts, independently rechecked.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import time
import uuid
import numpy as np
from threadpoolctl import threadpool_limits
from .index import native
from .community import metadata


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb')as f:
        for block in iter(lambda:f.read(1<<20),b''):h.update(block)
    return h.hexdigest()


def identity(shard):
    keys=('epoch','shard_id','dimension','metric','rows','record_bytes','header_bytes','max_degree')
    return dict(**{k:shard.meta[k]for k in keys},ids_sha256=hashlib.sha256(np.asarray(shard.ids,dtype='<u8').tobytes()).hexdigest())


def vectors(shard):
    m=shard.meta;mapping=np.memmap(shard.directory/'nodes.bin',dtype=np.uint8,mode='r')
    return np.ndarray((m['rows'],m['dimension']),dtype='<f4',buffer=mapping,offset=m['header_bytes'],strides=(m['record_bytes'],4))


def squared_distances(x,p,pn):
    xn=np.einsum('ij,ij->i',x,x)
    d=xn[:,None]+pn[None,:]-2*(x@p.T)
    return d,xn


def exact_minimum(x,p,pn,tolerance=1e-12,exclude=None):
    """BLAS screening, then direct FP64 differences for every plausible minimum."""
    ds,xn=squared_distances(x,p,pn)
    if exclude is not None:ds[np.arange(len(x)),exclude]=np.inf
    distances=np.empty(len(x));positions=np.empty(len(x),np.int64)
    guard=1e-10*(xn+float(pn.max())+1)
    for i in range(len(x)):
        candidates=np.flatnonzero(ds[i]<=ds[i].min()+2*guard[i])
        diff=x[i]-p[candidates];direct=np.sqrt(np.einsum('ij,ij->i',diff,diff))
        value=direct.min();ties=candidates[direct<=value+tolerance]
        positions[i]=ties.min();distances[i]=value
    return distances,positions


def small_group_distances(x,p,pn):
    """Same guarded candidate screening, vectorized across rows of small groups."""
    ds,xn=squared_distances(x,p,pn)
    guard=1e-10*(xn+float(pn.max())+1)
    upper=ds.min(axis=1)+2*guard
    result=np.full(len(x),np.inf)
    for j in range(len(p)):
        keep=ds[:,j]<=upper
        if not np.any(keep):continue
        diff=x[keep]-p[j]
        result[keep]=np.minimum(result[keep],np.sqrt(np.einsum('ij,ij->i',diff,diff)))
    return result


def build_directory(shard,portal_ids,portal_vectors,directory,settings,progress=None):
    """Explicit background/index-maintenance operation, never a search callback."""
    destination=Path(directory)
    if destination.exists():raise FileExistsError('Geometry directory already exists')
    cfg=settings['geometry'];ids=np.asarray(portal_ids,dtype='<u8');pts=np.asarray(portal_vectors,dtype=np.float64)
    if len(ids)<2 or np.any(ids[1:]<=ids[:-1])or pts.shape!=(len(ids),shard.meta['dimension'])or not np.isfinite(pts).all():
        raise ValueError('Invalid geometry routing universe')
    begun=time.monotonic();source_identity=identity(shard);before=(shard.directory/'nodes.bin').stat()
    staging=destination.with_name(destination.name+'.building.'+uuid.uuid4().hex);staging.mkdir(parents=True)
    stats={};n=len(shard.ids);check_limit=lambda:time.monotonic()-begun>cfg['cache_max_seconds']
    try:
        with threadpool_limits(limits=cfg['cache_threads']):
            pn=np.einsum('ij,ij->i',pts,pts);view=vectors(shard)
            labels=np.lib.format.open_memmap(staging/'labels.npy',mode='w+',dtype='<u8',shape=(n,))
            distance=np.lib.format.open_memmap(staging/'distance.npy',mode='w+',dtype='<f8',shape=(n,))
            local_portals=[];scales=[];positions={int(v):i for i,v in enumerate(ids)}
            for lo in range(0,n,cfg['cache_block_rows']):
                if check_limit():raise TimeoutError('Geometry directory construction deadline')
                hi=min(n,lo+cfg['cache_block_rows']);x=np.asarray(view[lo:hi],dtype=np.float64)
                values,winners=exact_minimum(x,pts,pn,cfg['tie_tolerance'])
                labels[lo:hi]=ids[winners];distance[lo:hi]=values
                entry_rows=[i for i,v in enumerate(shard.ids[lo:hi])if int(v)in positions]
                if entry_rows:
                    exclude=np.asarray([positions[int(shard.ids[lo+i])]for i in entry_rows])
                    dd,_=exact_minimum(x[entry_rows],pts,pn,cfg['tie_tolerance'],exclude)
                    local_portals.extend(map(int,shard.ids[lo+np.asarray(entry_rows)]));scales.extend(map(float,dd))
                if progress and(lo//cfg['cache_block_rows'])%20==0:progress(dict(stage='labels',completed=hi,total=n,seconds=time.monotonic()-begun))
            labels.flush();distance.flush();del labels,distance,view
        stats['label_seconds']=time.monotonic()-begun
        root=cfg['hub_row']
        if root>=n:raise ValueError('Geometry hub row out of range')
        t=time.monotonic();trees=native().geometry_trees(shard.fd,shard.ids,shard.meta,root)
        for name in ('forward','reverse','incoming','outgoing'):np.save(staging/(name+'.npy'),trees.pop(name))
        stats.update(trees,tree_seconds=time.monotonic()-t,rows=n)
        if check_limit():raise TimeoutError('Geometry directory graph construction deadline')
        np.save(staging/'portal_ids.npy',np.asarray(local_portals,dtype='<u8'))
        np.save(staging/'portal_scales.npy',np.asarray(scales,dtype='<f8'))
        after=(shard.directory/'nodes.bin').stat()
        if identity(shard)!=source_identity or(before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):raise ValueError('Source changed while constructing geometry directory')
        files={p.name:dict(bytes=p.stat().st_size,sha256=digest(p))for p in staging.iterdir()if p.is_file()}
        size=sum(v['bytes']for v in files.values())
        if size>cfg['cache_max_bytes']:raise ValueError('Geometry directory disk budget exceeded')
        stats.update(wall_seconds=time.monotonic()-begun,files_bytes=size)
        manifest=dict(kind='exact_entry_navigation_v1',identity=source_identity,root_row=root,files=files,
            routing_ids_sha256=hashlib.sha256(ids.tobytes()).hexdigest(),routing_vectors_sha256=hashlib.sha256(pts.tobytes()).hexdigest(),
            routing_entries=len(ids),tie_tolerance=cfg['tie_tolerance'],stats=stats,
            scale_definition='median singleton nearest-other routing entry distance; not group-excluded distance')
        (staging/'manifest.json').write_text(json.dumps(manifest,sort_keys=True,indent=2))
        if sum(p.stat().st_size for p in staging.iterdir())>cfg['cache_max_bytes']:raise ValueError('Geometry directory manifest exceeds disk budget')
        for path in staging.iterdir():
            with path.open('rb')as f:os.fsync(f.fileno())
        staging.rename(destination)
        fd=os.open(destination.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        return manifest
    except BaseException:
        # A failed directory is never named or accepted as the complete epoch cache.
        (staging/'FAILED.json').write_text(json.dumps(dict(seconds=time.monotonic()-begun,stats=stats)))
        raise


class Directory:
    def __init__(self,shard,settings):
        self.shard=shard;self.settings=settings;self.c=settings['geometry']
        p=Path(self.c['cache_directory']);self.path=p if p.is_absolute()else shard.directory/p
        self.manifest=json.loads((self.path/'manifest.json').read_text());m=self.manifest
        if m['kind']!='exact_entry_navigation_v1'or m['identity']!=identity(shard):raise ValueError('Geometry directory epoch/source mismatch')
        if m['tie_tolerance']!=self.c['tie_tolerance']:raise ValueError('Geometry directory numerical contract mismatch')
        for name,info in m['files'].items():
            if Path(name).name!=name or(self.path/name).stat().st_size!=info['bytes']or digest(self.path/name)!=info['sha256']:raise ValueError('Geometry directory checksum mismatch')
            setattr(self,name[:-4],np.load(self.path/name,mmap_mode='r',allow_pickle=False))
        for name in ('labels','distance','forward','reverse','incoming','outgoing'):
            if getattr(self,name).shape!=(len(shard.ids),):raise ValueError('Geometry directory array shape')
        self.root=m['root_row'];self.fingerprint=digest(self.path/'manifest.json')
        self.scales=dict(zip(map(int,self.portal_ids),map(float,self.portal_scales)))
        self.rows={int(v):i for i,v in enumerate(shard.ids)}
        self.physical_bytes=sum(p.stat().st_size for p in self.path.iterdir()if p.is_file())
        self._margin_group=None
        self._margin_values=None

    def core(self,group,factor,deadline):
        group=sorted(set(map(int,group)))
        if not group or any(e not in self.scales for e in group):raise ValueError('Geometry seed is not a source routing entry')
        scale=float(np.median([self.scales[e]for e in group]));tau=float(factor)*scale
        if factor<0 or not math.isfinite(factor):raise ValueError('Invalid geometry buffer factor')
        if factor==0:return np.flatnonzero(np.isin(self.labels,group)).astype(np.uint32),tau
        key=tuple(group)
        if self._margin_group!=key:
            view=vectors(self.shard);p=np.asarray(view[[self.rows[e]for e in group]],dtype=np.float64)
            pn=np.einsum('ij,ij->i',p,p)
            margin=np.empty(len(self.shard.ids),dtype=np.float64)
            # Direct differences for every guarded candidate retain the cache's
            # cancellation contract, without a Python loop per source row.
            # Keep only one group's margins (8 bytes/source row), reused across
            # buffer candidates. A timed-out partial array is never cached.
            with threadpool_limits(limits=self.c['blas_threads']):
                for lo in range(0,len(self.shard.ids),self.c['distance_block_rows']):
                    if time.monotonic()>deadline:raise TimeoutError('Geometry membership planning deadline')
                    hi=min(len(self.shard.ids),lo+self.c['distance_block_rows']);x=np.asarray(view[lo:hi],dtype=np.float64)
                    d=small_group_distances(x,p,pn)
                    margin[lo:hi]=d-self.distance[lo:hi]
            self._margin_group,self._margin_values=key,margin
        return np.flatnonzero(self._margin_values<=tau).astype(np.uint32),tau

    def region(self,group,factor,candidates,deadline,settings=None):
        limits=self.settings if settings is None else settings
        core,tau=self.core(group,factor,deadline);entry_rows=np.asarray([self.rows[e]for e in group],dtype=np.uint32)
        if len(core)>limits['max_region_rows']:raise ValueError('Geometry complete core exceeds row budget')
        if time.monotonic()>=deadline:raise TimeoutError('Geometry navigation planning deadline')
        closed=native().geometry_close(self.shard.fd,self.shard.ids,self.shard.meta,core,entry_rows,
            self.forward,self.reverse,self.root,limits['max_region_rows'],min(deadline-time.monotonic(),limits['geometry']['max_plan_ms']/1000))
        ids=np.asarray(closed['ids'],dtype=np.uint64)
        stats=native().geometry_assess(self.shard.fd,self.shard.ids,metadata(self.shard),ids,np.asarray(candidates,dtype=np.uint64),self.incoming,self.outgoing)
        stats.update(core_rows=len(core),guarded_rows=closed['guarded_rows'],navigation_added_rows=closed['navigation_added_rows'],tau=tau,
            directory_bytes=self.physical_bytes,navigation='forward_and_reverse_original_tree_paths')
        if stats['estimated_bytes']>limits['max_region_bytes']or stats['code_bytes']>limits['max_code_bytes']:raise ValueError('Geometry complete package exceeds byte budget')
        return dict(ids=closed['ids'],entries=stats.pop('safe_entries'),stats=stats)


def select(shard,hot_ids,weights,settings,directory=None):
    started=time.monotonic();c=settings['geometry'];deadline=started+c['max_plan_ms']/1000
    if not hot_ids or len(hot_ids)!=len(weights)or len(set(hot_ids))!=len(hot_ids)or any(not math.isfinite(w)or w<=0 for w in weights):raise ValueError('Invalid geometry hotspots')
    directory=directory or Directory(shard,settings);w=dict(zip(map(int,hot_ids),map(float,weights)))
    ordered=sorted(w,key=lambda e:(-w[e],e));view=vectors(shard);p=np.asarray(view[[directory.rows[e]for e in ordered]],dtype=np.float64)
    best=None;rejected=[];seen=set();attempts=0
    for anchor in ordered[:c['seed_starts']]:
        j=ordered.index(anchor);dist=np.sum((p-p[j])**2,axis=1);ix=sorted(range(len(ordered)),key=lambda k:(float(dist[k]),ordered[k]))[:c['group_size']]
        group=sorted(ordered[k]for k in ix)
        if tuple(group)in seen:continue
        seen.add(tuple(group))
        for factor in c['buffer_factors']:
            if time.monotonic()>deadline:break
            attempts+=1
            try:r=directory.region(group,float(factor),ordered,deadline,settings)
            except (ValueError,RuntimeError,TimeoutError)as error:rejected.append(str(error));continue
            if not r['entries']:continue
            s=r['stats'];heat=sum(w[e]for e in r['entries']);score=(heat/sum(w.values()))*(1-s['outgoing_ratio'])-c['byte_price']*s['estimated_bytes']/settings['max_region_bytes']
            if best is None or score>best['stats']['score']:
                s.update(score=score,covered_hot_weight=heat,score_kind='hot_entry_retention_minus_byte_price_proxy')
                r['region_proof']=dict(kind='complete_entry_geometry_v1',scope='source_local_records',epoch=shard.meta['epoch'],
                    directory_sha256=directory.fingerprint,group=group,buffer_factor=float(factor),tau=s['tau'],
                    candidate_entries=ordered,members_sha256=hashlib.sha256(np.asarray(r['ids'],dtype='<u8').tobytes()).hexdigest())
                best=r
    if best is None:raise ValueError('No complete geometric region fits: '+'; '.join(rejected[-3:]))
    best['stats'].update(planning_seconds=time.monotonic()-started,attempted_candidates=attempts,rejected_candidates=len(rejected),rejection_reasons=rejected[:16])
    best['selection_policy']='entry_geometry';return best


def verify(shard,plan,settings,directory=None):
    proof=plan['region_proof'];d=directory or Directory(shard,settings)
    if proof['kind']!='complete_entry_geometry_v1'or proof['scope']!='source_local_records'or proof['epoch']!=shard.meta['epoch']or proof['directory_sha256']!=d.fingerprint:
        raise ValueError('Invalid geometry region contract')
    ids=plan['ids']
    if ids!=sorted(set(ids))or hashlib.sha256(np.asarray(ids,dtype='<u8').tobytes()).hexdigest()!=proof['members_sha256']:raise ValueError('Geometry member order/digest mismatch')
    expected=d.region(proof['group'],proof['buffer_factor'],proof['candidate_entries'],time.monotonic()+settings['geometry']['max_plan_ms']/1000,settings)
    if expected['ids']!=ids or expected['stats']['tau']!=proof['tau']:raise ValueError('Geometry full membership/navigation mismatch')
    if not plan['entries']or not set(plan['entries'])<=set(expected['entries'])or len(set(plan['entries']))!=len(plan['entries']):raise ValueError('Geometry unsafe/duplicate routing entry')
    return dict(proof,verified=True,verification_stats=expected['stats'])
