"""Bounded HTTP ingress; pending queries retain sockets, not receive workers."""
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

from .sender import QueuedSender


class DeferredHandler(BaseHTTPRequestHandler):
    def setup(self):
        self.request.settimeout(getattr(self.server,'request_timeout',30.))
        super().setup()

    def __init__(self,*args,**kwargs):
        self.deferred=False;self.initialized=threading.Event()
        try:super().__init__(*args,**kwargs)
        finally:self.initialized.set()

    def finish(self):
        if not self.deferred:super().finish()

    def defer_query(self,future):
        self.deferred=True
        def done(f):
            try:self.server.responses.submit(self._respond,f)
            except RuntimeError:
                if self.initialized.is_set():self._release()
                else:
                    self.deferred=False
                    self.server.shutdown_request(self.request)
        future.add_done_callback(done)

    def _respond(self,future):
        self.initialized.wait()
        try:
            try:result=future.result()
            except Exception as exc:self.reply(409,dict(ok=False,error=str(exc)))
            else:self.reply(200,dict(ok=True,result=result))
        finally:self._release()

    def _release(self):
        try:super().finish()
        finally:self.server.shutdown_request(self.request)


class PipelineHTTPServer(HTTPServer):
    """The shared adaptive ingress is the only pool reading HTTP requests."""
    def __init__(self,address,handler,flow,capacity):
        self.flow=flow
        self.management=QueuedSender(1,capacity,'dhdgs-head-management')
        self.responses=QueuedSender(1,capacity,'dhdgs-head-http-response')
        super().__init__(address,handler)

    def process_request(self,request,address):
        try:self.flow.ingress.submit(self._process,request,address)
        except RuntimeError:self.shutdown_request(request)

    def _process(self,request,address):
        handler=self.RequestHandlerClass.__new__(self.RequestHandlerClass)
        try:handler.__init__(request,address,self)
        except Exception:self.handle_error(request,address)
        finally:
            if not getattr(handler,'deferred',False):self.shutdown_request(request)

    def server_close(self):
        self.management.shutdown(wait=True)
        self.responses.shutdown(wait=True)
        super().server_close()
