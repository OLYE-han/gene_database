"""Bounded same-host identity publication, without a per-query IPC round trip.

Only the parent writes. Receiver processes copy one identity under the process
lock, then release it before validating reports or doing any network/pipe I/O.
Open-addressed slots preserve tombstones so deletion never breaks probing.
"""
import struct
import zlib
import msgpack


class ReportRegistry:
    record_bytes=1024

    def __init__(self,context,capacity):
        self.capacity=capacity;self.slots=capacity*2+1
        self.buffer=context.RawArray('B',self.slots*self.record_bytes)
        self.lock=context.Lock();self.positions={}

    def _probe(self,qid):
        start=zlib.crc32(qid.encode())%self.slots
        for step in range(self.slots):yield (start+step)%self.slots

    def put(self,message):
        qid=message['qid'];body=msgpack.packb(message,use_bin_type=True)
        if len(body)>self.record_bytes-4:raise ValueError('Report identity exceeds shared record size')
        with self.lock:
            if qid in self.positions:raise ValueError('Duplicate report identity')
            if len(self.positions)>=self.capacity:raise RuntimeError('Report registry capacity exceeded')
            view=memoryview(self.buffer).cast('B')
            for index in self._probe(qid):
                offset=index*self.record_bytes;length=struct.unpack_from('!I',view,offset)[0]
                if length in (0,0xffffffff):
                    view[offset+4:offset+4+len(body)]=body
                    struct.pack_into('!I',view,offset,len(body));self.positions[qid]=index
                    return
        raise RuntimeError('Report registry has no free slot')

    def get(self,qid):
        if not isinstance(qid,str):return None
        with self.lock:
            view=memoryview(self.buffer).cast('B')
            for index in self._probe(qid):
                offset=index*self.record_bytes;length=struct.unpack_from('!I',view,offset)[0]
                if not length:return None
                if length==0xffffffff:continue
                message=msgpack.unpackb(view[offset+4:offset+4+length],raw=False)
                if message['qid']==qid:return message
        return None

    def remove(self,qid):
        with self.lock:
            index=self.positions.pop(qid,None)
            if index is not None:struct.pack_into('!I',self.buffer,index*self.record_bytes,0xffffffff)
