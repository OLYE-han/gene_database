from concurrent.futures import Future,ThreadPoolExecutor
from http.client import HTTPConnection
import threading
import time
import unittest

from dhdgs.head_http import DeferredHandler,PipelineHTTPServer
from dhdgs.head_pipeline import PipelineFlow
from test_head_pipeline import config


class HTTPTests(unittest.TestCase):
    def test_pending_requests_do_not_occupy_receive_workers(self):
        c=config();flow=PipelineFlow(c);futures=[];lock=threading.Lock()
        class Handler(DeferredHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                if self.path=='/admin':
                    def operation():
                        f=Future();f.set_result(17)
                        def query():value=yield f;return value
                        return flow.submit(query()).result(2)
                    self.defer_query(self.server.management.submit(operation));return
                f=Future()
                with lock:futures.append(f)
                self.defer_query(f)
            def reply(self,status,value):
                raw=b'ok';self.send_response(status);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(raw)
        server=PipelineHTTPServer(('127.0.0.1',0),Handler,flow,128)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        def query():
            con=HTTPConnection(*server.server_address,timeout=3)
            try:con.request('GET','/');r=con.getresponse();return r.status,r.read()
            finally:con.close()
        try:
            with ThreadPoolExecutor(16) as pool:
                responses=[pool.submit(query) for _ in range(16)]
                until=time.monotonic()+2
                while len(futures)<16 and time.monotonic()<until:time.sleep(.005)
                self.assertEqual(len(futures),16)
                self.assertLessEqual(flow.ingress.status()['peak_workers'],4)
                for f in futures:f.set_result({})
                self.assertEqual([r.result(2) for r in responses],[(200,b'ok')]*16)
            # A long management operation can submit queries even at receive_min=1.
            con=HTTPConnection(*server.server_address,timeout=3)
            try:con.request('GET','/admin');response=con.getresponse();self.assertEqual(response.status,200);response.read()
            finally:con.close()
        finally:
            for f in futures:
                if not f.done():f.set_result({})
            server.shutdown();thread.join();server.server_close();flow.close()
