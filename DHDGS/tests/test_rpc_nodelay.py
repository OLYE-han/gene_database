import socket
import threading
import unittest
from unittest.mock import patch
from dhdgs.peer_rpc import FairConnectionPool,rpc_exchange
from dhdgs.transport import receive,send

class NodelayTests(unittest.TestCase):
    def test_requested_option_on_new_connection_and_reuse_preserves_wire(self):
        a,b=socket.socketpair();calls=[]
        class Socket:
            def __getattr__(self,key):return getattr(a,key)
            def setsockopt(self,*args):calls.append(args)
        wrapped=Socket();p=FairConnectionPool(1,1);p.tcp_nodelay=True
        def server():
            for _ in range(2):
                request,_=receive(b,4096);send(b,dict(ok=True,value=request['value']),4096)
        t=threading.Thread(target=server);t.start()
        try:
            with patch('dhdgs.peer_rpc.socket.create_connection',return_value=wrapped) as connect:
                for i in range(2):
                    reply,m=rpc_exchange(('host',1),dict(value=i),4096,2,p)
                    self.assertEqual(reply['value'],i)
                    self.assertEqual(m['rpc_count'],1)
                self.assertEqual(connect.call_count,1)
            self.assertEqual(calls,[(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)])
        finally:p.close();a.close();b.close();t.join(2)
