"""Offline source-local boundary augmentation preserving every original edge.

A portal is useful only when reachable: add its directed connector path as
ordinary records. No query, truth, new route entry or synthetic shortcut is used.
Alpha limits the original region; ALL bytes still count against hard limits.
"""
import copy
import hashlib
import json
import time
from pathlib import Path
import numpy as np


def digest(ids):
    return hashlib.sha256(np.asarray(ids,dtype='<u8').tobytes()).hexdigest()


def validate_config(c):
    required={'enabled','mode','per_direction','projection_bits','seed','max_connector_hops',
              'max_extra_bytes','max_plan_seconds','scan_block_rows'}
    if set(c)!=required or type(c['enabled'])is not bool or c['mode']not in ('representative','all'):
        raise ValueError('Invalid boundary augmentation configuration')
    for k in ('per_direction','projection_bits','seed','max_connector_hops','max_extra_bytes','scan_block_rows'):
        if type(c[k])is not int or c[k]<0:raise ValueError('Invalid boundary integer '+k)
    if not 1<=c['projection_bits']<=12 or not 1<=c['max_connector_hops']<=16 or not c['per_direction'] or not c['scan_block_rows'] or c['max_extra_bytes']<65536 or not np.isfinite(c['max_plan_seconds'])or c['max_plan_seconds']<=0:
        raise ValueError('Invalid boundary augmentation limits')


class BoundaryGraph:
    """Build-local array scan; holds no query path locks or online worker threads."""
    def __init__(self,shard,config,*,deadline=None):
        validate_config(config);self.shard=shard;self.config=config;self.started=time.monotonic()
        self.deadline=min(self.started+config['max_plan_seconds'],float('inf') if deadline is None else deadline)
        m=shard.meta;self.ids=np.asarray(shard.ids);self.source=int(m['shard_id']);self.n=len(self.ids)
        self.lookup={int(v):i for i,v in enumerate(self.ids)}
        self.code_ids=np.asarray(shard.code_ids);self.membership=np.load(shard.directory/'code_membership.npy',mmap_mode='r')
        self.raw=np.memmap(shard.directory/'nodes.bin',dtype=np.uint8,mode='r')
        self.adj=np.ndarray((self.n,m['max_degree']),dtype='<u8',buffer=self.raw,
            offset=m['header_bytes']+4*m['dimension']+4,strides=(m['record_bytes'],8))
        self.degree=np.ndarray((self.n,),dtype='<u4',buffer=self.raw,
            offset=m['header_bytes']+4*m['dimension'],strides=(m['record_bytes'],))
        if np.any(self.degree>m['max_degree']):raise ValueError('Invalid boundary adjacency degree')
        # searchsorted supports sparse global IDs without a corpus-sized dense map.
        self.local=np.full(self.adj.shape,-1,dtype=np.int32)
        self.external=np.zeros((self.n,self.membership.shape[1]),dtype=np.uint16)
        self.scans=0;arange=np.arange(m['max_degree'])
        for begin in range(0,self.n,config['scan_block_rows']):
            self.check();end=min(self.n,begin+config['scan_block_rows']);valid=arange[None,:]<self.degree[begin:end,None]
            a=self.adj[begin:end].copy();a[~valid]=self.code_ids[0]
            cp=np.searchsorted(self.code_ids,a);cp=np.minimum(cp,len(self.code_ids)-1)
            if np.any(valid&(self.code_ids[cp]!=a)):raise ValueError('Boundary missing quantized neighbor')
            lp=np.searchsorted(self.ids,a);lp=np.minimum(lp,self.n-1);islocal=valid&(self.ids[lp]==a)
            self.local[begin:end]=np.where(islocal,lp,-1)
            owners=self.membership[cp]
            for dest in range(self.membership.shape[1]):
                if dest!=self.source:self.external[begin:end,dest]=np.sum(valid&~owners[:,:,self.source]&owners[:,:,dest],axis=1)
            self.scans+=int(valid.sum())

    def check(self):
        if time.monotonic()>self.deadline:raise TimeoutError('Boundary planning deadline')

    def estimate(self,positions,proof=None):
        """Full records + union quantization dependencies + proof/header reserve."""
        from .community import metadata
        self.check();positions=np.asarray(positions,dtype=np.int64);codes=set(map(int,self.ids[positions]));edges=0
        for begin in range(0,len(positions),8192):
            self.check();p=positions[begin:begin+8192];a=self.adj[p];valid=np.arange(a.shape[1])[None,:]<self.degree[p,None]
            codes.update(map(int,np.unique(a[valid])));edges+=int(valid.sum())
        m=metadata(self.shard);fixed=m['community_fixed_bytes']+ (len(json.dumps(proof).encode())if proof is not None else 0)+65536
        return dict(rows=len(positions),code_rows=len(codes),edges=edges,estimated_bytes=int(m['header_bytes']+len(positions)*(m['record_bytes']+8)+len(codes)*m['community_code_bytes']+fixed))

    def augment(self,base,settings):
        from .community import verify
        from .replica_budget import region_limit
        from .locality import source_index_bytes
        base=copy.deepcopy(base)
        if 'reserved_package_bytes'not in base:base['reserved_package_bytes']=int(base['stats']['estimated_bytes'])
        verify(self.shard,base,settings)
        cfg=self.config;source_bytes=source_index_bytes(self.shard);base_cap=region_limit(settings,source_bytes,base.get('replica_tier','small'))
        if base['reserved_package_bytes']>base_cap:raise ValueError('Original core exceeds alpha')
        inside=np.zeros(self.n,dtype=bool);roots=np.asarray([self.lookup[v]for v in base['ids']],dtype=np.int32);inside[roots]=True
        parent=np.full(self.n,-1,dtype=np.int32);dist=np.full(self.n,-1,dtype=np.int16);dist[roots]=0
        front=roots
        for depth in range(1,cfg['max_connector_hops']+1):
            self.check()
            if not len(front):break
            targets=self.local[front].reshape(-1);src=np.repeat(front,self.local.shape[1]);valid=targets>=0;targets=targets[valid];src=src[valid]
            valid=dist[targets]<0;targets=targets[valid];src=src[valid]
            unique,first=np.unique(targets,return_index=True);parent[unique]=src[first];dist[unique]=depth;front=unique
        eligible=(dist>=0)&~inside&(self.external.sum(axis=1)>0);candidates=np.flatnonzero(eligible)
        # Sign projection of source vector relative to source quantizer centroid
        # gives directional diversity, a geometry proxy, not a recall guarantee.
        signatures=np.zeros(self.n,dtype=np.uint16)
        if cfg['mode']=='representative' and len(candidates):
            rng=np.random.default_rng(cfg['seed']);projection=rng.standard_normal((self.shard.meta['dimension'],cfg['projection_bits'])).astype(np.float32)
            vectors=np.ndarray((self.n,self.shard.meta['dimension']),dtype='<f4',buffer=self.raw,offset=self.shard.meta['header_bytes'],strides=(self.shard.meta['record_bytes'],4))
            center=np.load(self.shard.directory/'centers.npy',mmap_mode='r')[self.source]
            for begin in range(0,len(candidates),1024):
                self.check();ix=candidates[begin:begin+1024];bits=(np.asarray(vectors[ix])-center)@projection>0
                signatures[ix]=(bits.astype(np.uint16)*(1<<np.arange(cfg['projection_bits']))).sum(axis=1)
        selected=set();direction_stats={}
        for dest in range(self.external.shape[1]):
            if dest==self.source:continue
            opts=[int(v)for v in candidates if self.external[v,dest]>0]
            opts.sort(key=lambda v:(int(dist[v]),-int(self.external[v,dest]),int(self.ids[v])))
            if cfg['mode']=='representative':
                groups={}
                for v in opts:groups.setdefault(int(signatures[v]),[]).append(v)
                # Round robin angular buckets, with shortest/strongest portal first.
                ranked=[]
                for rank in range(max(map(len,groups.values()),default=0)):
                    for b in sorted(groups):
                        if rank<len(groups[b]):ranked.append(groups[b][rank])
                    if len(ranked)>=cfg['per_direction']:break
                opts=ranked[:cfg['per_direction']]
            selected.update(opts);direction_stats[str(dest)]=dict(eligible=int(np.sum(eligible&(self.external[:,dest]>0))),selected=len(opts))
        added=set();links={}
        for v in sorted(selected):
            while not inside[v]:
                self.check();added.add(v);p=int(parent[v]);assert p>=0;links[v]=p;v=p
        positions=np.asarray(sorted(set(map(int,roots))|added),dtype=np.int64);newids=sorted(map(int,self.ids[positions]))
        core={k:copy.deepcopy(base[k])for k in ('ids','entries','certified_entries','region_proof','reserved_package_bytes','replica_tier')if k in base}
        proof=dict(kind='boundary_augmented_v1',scope='source_local_records',epoch=self.shard.meta['epoch'],base=core,
            connectors=[[int(self.ids[v]),int(self.ids[p])]for v,p in sorted(links.items(),key=lambda kv:(int(dist[kv[0]]),int(self.ids[kv[0]])))],
            portals=sorted(map(int,self.ids[list(selected)])),members_sha256=digest(newids),config=copy.deepcopy(cfg))
        out=copy.deepcopy(base);stats=self.estimate(positions,proof);baseline=self.estimate(roots,base['region_proof']);extra=max(0,stats['estimated_bytes']-baseline['estimated_bytes'])
        if baseline['estimated_bytes']>base_cap:raise ValueError('Actual original core exceeds alpha')
        if extra>cfg['max_extra_bytes']or stats['estimated_bytes']>min(settings['max_region_bytes'],base_cap+cfg['max_extra_bytes']):
            raise ValueError('Boundary full package/extra budget exceeded')
        if len(newids)>settings['max_region_rows']:raise ValueError('Boundary row hard limit exceeded')
        stats.update(base_rows=len(roots),added_rows=len(added),added_portals=len(selected),connector_only_rows=len(added-selected),extra_estimated_bytes=extra,base_estimated_bytes=baseline['estimated_bytes'],base_alpha_limit=base_cap,
            all_source_boundary_rows=int((self.external.sum(axis=1)>0).sum()),missing_boundary_unreachable=int(np.sum((self.external.sum(axis=1)>0)&~inside&(dist<0))),
            direction_stats=direction_stats,planning_seconds=time.monotonic()-self.started,max_path_hops=int(max(dist[list(selected)],default=0)),original_entries_unchanged=True)
        out.update(ids=newids,region_proof=proof,stats=stats,region_stats=stats,reserved_package_bytes=stats['estimated_bytes'])
        return out


def augment(shard,base,settings):
    c=settings.get('boundary_augmentation',{})
    if not c.get('enabled'):return base
    return BoundaryGraph(shard,c,deadline=settings.get('_planning_deadline')).augment(base,settings)


def verify(shard,plan,settings):
    """Verify base independently and every added record's original directed path."""
    from .community import verify as verify_core
    from .replica_budget import region_limit
    from .locality import source_index_bytes
    p=plan['region_proof'];cfg=settings.get('boundary_augmentation',{})
    validate_config(cfg)
    if not cfg['enabled']or p.get('kind')!='boundary_augmented_v1'or p.get('scope')!='source_local_records'or p.get('epoch')!=shard.meta['epoch']:raise ValueError('Boundary epoch/scope/config mismatch')
    validate_config(p['config'])
    if p['config']['max_extra_bytes']>cfg['max_extra_bytes']:raise ValueError('Boundary allowance exceeds configured limit')
    base=p['base'];verify_core(shard,base,settings)
    if plan.get('replica_tier','small')!=base.get('replica_tier','small'):raise ValueError('Boundary base tier changed')
    base_cap=region_limit(settings,source_index_bytes(shard),plan.get('replica_tier','small'))
    if base['reserved_package_bytes']>base_cap:raise ValueError('Boundary base exceeds alpha')
    if plan['entries']!=base['entries']or plan.get('certified_entries')!=base.get('certified_entries')or plan['ids']!=sorted(set(plan['ids']))or digest(plan['ids'])!=p['members_sha256']:raise ValueError('Boundary members/entries mismatch')
    seen=set(base['ids']);depth={v:0 for v in seen};m=shard.meta
    import os,struct
    for child,parent in p['connectors']:
        if child in seen or child not in shard.location or parent not in seen:raise ValueError('Boundary path not attached')
        raw=os.pread(shard.fd,4+8*m['max_degree'],m['header_bytes']+shard.location[parent]*m['record_bytes']+4*m['dimension'])
        d=struct.unpack_from('<I',raw)[0]
        if d>m['max_degree']or child not in np.frombuffer(raw,dtype='<u8',count=d,offset=4):raise ValueError('Boundary connector is not original directed edge')
        depth[child]=depth[parent]+1
        if depth[child]>cfg['max_connector_hops']:raise ValueError('Boundary path exceeds configured hops')
        seen.add(child)
    if seen!=set(plan['ids'])or not set(p['portals'])<=seen:raise ValueError('Boundary certificate membership mismatch')
    # Recompute complete dependency costs from immutable source; do not trust plan stats.
    graph=BoundaryGraph(shard,cfg);positions=[graph.lookup[v]for v in plan['ids']];stats=graph.estimate(positions,p);baseline=graph.estimate([graph.lookup[v]for v in base['ids']],base['region_proof'])
    for id in p['portals']:
        if not graph.external[graph.lookup[id]].any():raise ValueError('Portal has no actual source-external outgoing edge')
    extra=max(0,stats['estimated_bytes']-baseline['estimated_bytes'])
    if baseline['estimated_bytes']>base_cap:raise ValueError('Actual original core exceeds alpha')
    if extra>cfg['max_extra_bytes']or stats['estimated_bytes']>min(settings['max_region_bytes'],base_cap+cfg['max_extra_bytes']):raise ValueError('Boundary package exceeds independent capacity')
    if len(plan['ids'])>settings['max_region_rows']:raise ValueError('Boundary row hard limit')
    return dict(p,verified=True,verification_stats=stats)
