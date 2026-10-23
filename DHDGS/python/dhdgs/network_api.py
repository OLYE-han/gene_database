"""Public binary search API configuration and offline load-test record reader.

Wire: uint32 big-endian frame length followed by a MessagePack map. Search
requests carry version=1, op=search, encoding=float64_le, vector=binary data,
query_id=uint64/null. One in-flight query per persistent connection. The reply
is the final DHDGS result, not an admission acknowledgement.
"""
import copy,struct,gzip
from pathlib import Path
import msgpack

def api_options(config,settings):
    api=copy.deepcopy(settings['api'])
    n=copy.deepcopy(config['native_runtime']['network_module'])
    n.update(transport="tcp",enabled=True,receive_min_reactors=api['reactors'],receive_max_reactors=api.get('max_reactors',api['reactors']),
             handlers_per_receive_reactor=api['handlers'],receive_queue_capacity_per_reactor=api['queue_capacity'],
             max_receive_connections=api['max_connections'])
    api['native_runtime']={'network_module':n}
    return api

def records(directory):
    directory=Path(directory)
    files=list(directory.glob('lane-*.frames'))
    files.extend(f for f in directory.glob('lane-*.frames.gz') if not f.with_suffix('').exists())
    for file in sorted(files):
        opener=gzip.open if file.suffix=='.gz' else open
        with opener(file,'rb') as stream:
            while prefix:=stream.read(8):
                if len(prefix)!=8:raise ValueError('Truncated network result header')
                size,=struct.unpack('!Q',prefix)
                if size>16<<20:raise ValueError('Invalid network result length')
                raw=stream.read(size)
                if len(raw)!=size:raise ValueError('Truncated network result')
                yield msgpack.unpackb(raw,raw=False,strict_map_key=False)
