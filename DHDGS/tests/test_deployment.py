import json
from pathlib import Path
import selectors
import subprocess
import sys
import tempfile
import unittest
import yaml

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from deploy_cluster import Deployment
from dhdgs.peer import execution_signature


class DeploymentTests(unittest.TestCase):
    def test_generated_host_configs_preserve_execution_signature(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            c=yaml.safe_load((ROOT/'configs/dhdgs.yaml').read_text())
            c['network']['transport']='tcp';c['peer']['head_advertise_host']='192.0.2.1'
            plan={'dhdgs':c,'deployment':{'index_source':str(root/'index'),
                'hosts':[{'id':0,'ssh':None,'root':str(root)},
                         {'id':1,'ssh':'example1','root':'/tmp/example1'},
                         {'id':2,'ssh':'example2','root':'/tmp/example2'}],
                'modes':[{'name':'blocking','execution_model':'blocking','backend':'io_uring'},
                         {'name':'async','execution_model':'resumable','backend':'io_uring'}],
                'client_concurrencies':[1,4,8],'ssh_connect_timeout_seconds':10}}
            path=root/'plan.yaml';path.write_text(yaml.safe_dump(plan))
            deployment=Deployment(path)
            for mode in deployment.modes:
                expected=execution_signature(deployment.settings(mode))
                for host in deployment.hosts:
                    for concurrency in deployment.d['client_concurrencies']:
                        c=deployment.settings(mode,host,concurrency)
                        self.assertEqual(execution_signature(c),expected)
                        self.assertEqual(c['network']['transport'],'tcp')
                        self.assertNotEqual(c['peer']['head_advertise_host'],'127.0.0.1')

    def test_supervisor_eof_stops_only_its_child(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);project=root/'project';project.mkdir()
            (project/'dhdgs.py').write_text('''import json,time
print(json.dumps({'status':'listening','endpoint':['127.0.0.1',12345]}),flush=True)
try:
    while True:time.sleep(1)
except KeyboardInterrupt:pass
''')
            args=[sys.executable,str(ROOT/'tools/deploy_node.py'),'supervise','--python',sys.executable,
                  '--project',str(project),'--config','unused.yaml','--node','0','--log',str(root/'logs'),
                  '--timeout','5','--stop-timeout','2']
            proc=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout,selectors.EVENT_READ)
                    self.assertTrue(selector.select(5))
                ready=json.loads(proc.stdout.readline());self.assertEqual(ready['status'],'ready')
                proc.stdin.write(json.dumps({'op':'probe','endpoints':[],'timeout':1,'repetitions':1})+'\n');proc.stdin.flush()
                response=json.loads(proc.stdout.readline())
                self.assertEqual(response['status'],'probe_complete')
                out,err=proc.communicate(input='',timeout=6) # SSH stdin EOF
                self.assertEqual(proc.returncode,0,err)
                stopped=json.loads(out.splitlines()[-1]);self.assertEqual(stopped['status'],'stopped')
                self.assertTrue(stopped['stopped']);self.assertEqual(stopped['pid'],ready['pid'])
                self.assertEqual(stopped['exit_code'],0)
            finally:
                if proc.poll() is None:proc.communicate(input='stop\n',timeout=6)
