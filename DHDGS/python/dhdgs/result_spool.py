"""Bounded Python object lifetime during load generation; preserve full records."""
import struct
import tempfile

import msgpack


class ResultSpool:
    def __init__(self, directory):
        self.file = tempfile.TemporaryFile(dir=directory, prefix='query-spool-')
        self.count = 0

    def append(self, result):
        payload = msgpack.packb(result, use_bin_type=True)
        self.file.write(struct.pack('!Q', len(payload)))
        self.file.write(payload)
        self.count += 1

    def read_all(self):
        self.file.seek(0)
        results = []
        for _ in range(self.count):
            size = struct.unpack('!Q', self.file.read(8))[0]
            results.append(msgpack.unpackb(self.file.read(size), raw=False, strict_map_key=False))
        if self.file.read(1):
            raise ValueError('Unexpected trailing result spool data')
        return results

    def close(self):
        self.file.close()
