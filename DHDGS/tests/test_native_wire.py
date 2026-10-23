"""Native framed MessagePack compatibility and asynchronous transport contracts."""
import socket
import socketserver
import struct
import threading
import time
import unittest

import msgpack
try:
    from dhdgs import _native
except ImportError:
    _native = None


def receive(sock):
    def exact(n):
        chunks = bytearray()
        while len(chunks) < n:
            b = sock.recv(n-len(chunks))
            if not b:
                raise EOFError
            chunks.extend(b)
        return bytes(chunks)
    n, = struct.unpack('!I', exact(4))
    return msgpack.unpackb(exact(n), raw=False)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, action):
        self.action = action
        self.accepted = 0
        self.release = threading.Event()
        outer = self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                outer.accepted += 1
                self.request.settimeout(2)
                while True:
                    try:
                        req = receive(self.request)
                        data = outer.action(req, outer)
                        if data is None:
                            return
                        # split frame header and body, including awkward byte boundaries
                        for start, end in ((0, 1), (1, 3), (3, 8), (8, len(data))):
                            self.request.sendall(data[start:end])
                    except (OSError, EOFError):
                        return
        super().__init__(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.release.set()
        self.shutdown()
        self.server_close()
        self.thread.join()


def frame(value):
    b = msgpack.packb(value, use_bin_type=True)
    return struct.pack('!I', len(b)) + b


def echo(req, server):
    return frame({'request_id': req['request_id'], 'ok': True, 'result': req})


@unittest.skipUnless(_native is not None and hasattr(_native, 'NativeWireClient'), 'native wire unavailable')
class NativeWireTests(unittest.TestCase):
    def connect(self, action=echo, **kwargs):
        server = Server(action)
        self.addCleanup(server.close)
        client = _native.NativeWireClient(**kwargs)
        self.addCleanup(client.close)
        endpoint = _native.NativeWireEndpoint('localhost', server.server_address[1])
        return server, client, endpoint

    def collect(self, client, count):
        out = []
        deadline = time.monotonic() + 4
        while len(out) < count and time.monotonic() < deadline:
            out += client.poll()
            time.sleep(.001)
        self.assertEqual(len(out), count)
        return out

    def test_codec_python_compatibility(self):
        values = [None, True, False, -2**63, 2**64-1, -33, 128, 65536, 3.25,
                  '蛋白质', b'\x00\xff', [], {}, list(range(32)),
                  {'binary': b'abc', 'nested': [None, {'x': -123}]}]
        for value in values:
            with self.subTest(value=value):
                self.assertEqual(msgpack.unpackb(_native.native_wire_pack(value), raw=False), value)
                self.assertEqual(_native.native_wire_unpack(msgpack.packb(value, use_bin_type=True)), value)

    def test_codec_rejects_invalid_inputs(self):
        for body in (b'', b'\xc1', b'\x81\x01\x02', b'\x00\x01', b'\xdb\xff\xff\xff\xff',
                     b'\x82\xa1x\x01\xa1x\x02', b'\x91'*70+b'\xc0',
                     b'\xa1\xff', b'\xa3\xed\xa0\x80', b'\xa2\xc0\x80'):
            with self.assertRaises(ValueError):
                _native.native_wire_unpack(body)
        for value in (2**65, -2**64, {1: 'key'}, object()):
            with self.assertRaises((ValueError, OverflowError)):
                _native.native_wire_pack(value)
        with self.assertRaises((ValueError, RuntimeError)):
            _native.native_wire_pack({'too_big': 'x'*100}, 10)

    def test_persistent_connection_and_fragmented_response(self):
        server, client, ep = self.connect(connections=1)
        for i in range(10):
            ticket = client.submit(ep, {'request_id': str(i), 'query': b'abc'})
            result, = self.collect(client, 1)
            self.assertEqual(result['id'], ticket)
            self.assertTrue(result['ok'], result)
            self.assertEqual(result['response']['result']['query'], b'abc')
            self.assertEqual(result['connection_created'], i == 0)
            self.assertLessEqual(result['queued_ns'], result['send_start_ns'])
            self.assertLessEqual(result['send_start_ns'], result['send_done_ns'])
            self.assertLessEqual(result['send_done_ns'], result['done_ns'])
        self.assertEqual(server.accepted, 1)

    def test_capacity_and_close_callbacks(self):
        def blocked(req, server):
            server.release.wait(2)
            return echo(req, server)
        server, client, ep = self.connect(blocked, max_pending=2, connections=1)
        self.assertTrue(client.submit(ep, {'request_id': 'a'}))
        self.assertTrue(client.submit(ep, {'request_id': 'b'}))
        self.assertEqual(client.submit(ep, {'request_id': 'c'}), 0)
        start = time.monotonic()
        client.close()
        self.assertLess(time.monotonic()-start, .5)
        results = self.collect(client, 2)
        self.assertTrue(all(not r['ok'] for r in results))
        self.assertEqual(client.pending(), 0)
        self.assertEqual(client.submit(ep, {'request_id': 'd'}), 0)

    def test_timeout_includes_queue_and_does_not_block_other_endpoint(self):
        def blocked(req, server):
            server.release.wait(2)
            return echo(req, server)
        server, client, ep = self.connect(blocked, timeout_ms=80, connections=1)
        other = Server(echo)
        self.addCleanup(other.close)
        ep2 = _native.NativeWireEndpoint('127.0.0.1', other.server_address[1])
        client.submit(ep, {'request_id': 'a'})
        client.submit(ep, {'request_id': 'b'})
        client.submit(ep2, {'request_id': 'c'})
        results = self.collect(client, 3)
        self.assertEqual(sum(r['ok'] for r in results), 1)
        self.assertTrue(all('timeout' in r['error'] for r in results if not r['ok']))

    def test_bad_response_closes_connection_and_allows_new_request(self):
        for mode in ('zero', 'large', 'bad_id', 'not_map', 'truncated', 'invalid'):
            def action(req, server):
                if req['request_id'] == 'good':
                    return echo(req, server)
                return {'zero': b'\x00'*4,
                        'large': struct.pack('!I', 100000),
                        'bad_id': frame({'request_id': 'different'}),
                        'not_map': frame([1, 2]),
                        'truncated': struct.pack('!I', 10)+b'abc',
                        'invalid': struct.pack('!I', 1)+b'\xc1'}[mode]
            with self.subTest(mode=mode):
                server, client, ep = self.connect(action, timeout_ms=100, max_message_bytes=1024, connections=1)
                client.submit(ep, {'request_id': 'bad'})
                result, = self.collect(client, 1)
                self.assertFalse(result['ok'])
                client.submit(ep, {'request_id': 'good'})
                result, = self.collect(client, 1)
                self.assertTrue(result['ok'], result)
                self.assertTrue(result['connection_created'])

    def test_limits_and_configuration(self):
        for kwargs in ({'max_pending': 0}, {'max_message_bytes': 0},
                       {'timeout_ms': 0}, {'max_endpoints': 0}):
            with self.assertRaises(ValueError):
                _native.NativeWireClient(**kwargs)
        server, client, ep = self.connect(max_message_bytes=128, max_endpoints=1)
        with self.assertRaises((ValueError, RuntimeError)):
            client.submit(ep, {'request_id': 'a', 'x': 'x'*1024})
        with self.assertRaises(ValueError):
            client.submit(ep, [1, 2])
        self.assertEqual(client.pending(), 0)
        self.assertTrue(client.submit(ep, {'request_id': 'a'}))
        self.assertTrue(self.collect(client, 1)[0]['ok'])

    def test_endpoint_limit_evicts_idle_but_never_active(self):
        def blocked(req, server):
            server.release.wait(2)
            return echo(req, server)
        server, client, ep = self.connect(blocked, max_endpoints=1, connections=1)
        other = Server(echo)
        self.addCleanup(other.close)
        ep2 = _native.NativeWireEndpoint('127.0.0.1', other.server_address[1])
        self.assertTrue(client.submit(ep, {'request_id': 'first'}))
        self.assertEqual(client.submit(ep2, {'request_id': 'busy'}), 0)
        server.release.set()
        self.assertTrue(self.collect(client, 1)[0]['ok'])
        for i in range(6):
            target = ep2 if i % 2 == 0 else ep
            self.assertTrue(client.submit(target, {'request_id': str(i)}))
            result, = self.collect(client, 1)
            self.assertTrue(result['ok'], result)
            self.assertTrue(result['connection_created'])


if __name__ == '__main__':
    unittest.main()
