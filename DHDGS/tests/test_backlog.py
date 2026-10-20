import copy
import socket
import tempfile
from pathlib import Path
from unittest import TestCase,mock
from threadpoolctl import threadpool_limits
from test_system import fixture
from dhdgs.index import build
from dhdgs.transport import create_server
from dhdgs.peer import HeadSession

class BacklogTests(TestCase):
    def test_burst_connections_fit_configured_data_listener(self):
        with tempfile.TemporaryDirectory() as temp:
            c,_=fixture(Path(temp))
            with threadpool_limits(limits=1):build(c)
            c['network']['listen_backlog']=64
            c['cluster']['nodes'][0]['port']=0
            # Deliberately do not start the accept loop: all connections must
            # fit in the listen queue, so the old size 5 cannot pass this test.
            server=create_server(c,0);connections=[]
            try:
                for _ in range(16):connections.append(socket.create_connection(server.server_address,.25))
                self.assertEqual(len(connections),16)
            finally:
                for connection in connections:connection.close()
                server.server_close()

    def test_control_failure_remains_visible_with_operation_and_node(self):
        with tempfile.TemporaryDirectory() as temp:
            c,_=fixture(Path(temp));c['network']['listen_backlog']=64
            session=HeadSession(c,'query','token','root',0)
            with mock.patch.object(session,'exchange',side_effect=TimeoutError('deliberate timeout')):
                session.close()
            self.assertEqual(session.control['errors'],1)
            self.assertEqual(session.control_error_details,[{'kind':'cancel','node_id':0,
                'error_type':'TimeoutError','error':'deliberate timeout'}])
