from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parent
STYLE='''graph [fontname="DejaVu Sans", rankdir=LR, bgcolor="white", pad=0.2, nodesep=0.35, ranksep=0.65];
node [fontname="DejaVu Sans", shape=box, style="rounded,filled", fillcolor="#eaf2ff", color="#57759c", fontsize=11];
edge [fontname="DejaVu Sans", fontsize=9, color="#49586b"];
'''
graphs={
'live_processes':r'''
subgraph cluster_host35 {label="Intel-35 / current persistent services"; color="#b9c8db";
 subgraph cluster_head {label="Head: 2 business processes + 1 resource tracker";
  hp [label="Head parent PID 1593215\n7 observed threads\nHTTP, HNSW, query continuation, merge"];
  hr [label="Receiver child PID 1593395\n6 idle threads + connection handlers\n4 entry senders, report validation, ACK"];
  hp -> hr [label="IPC: register / entry task / unregister"];
  hr -> hp [label="control IPC: entry ACK / involved nodes"];
  hr -> hp [label="separate terminal IPC: final reports",color="#149174",penwidth=2];
 }
 d0 [label="Data 0 PID 1593211\n53 idle threads + connection handlers\n192.168.33.35:22051",fillcolor="#e7f5ed"];
}
d1 [label="Intel-32 / Data 1\n192.168.33.32:22052\nSame configured topology\nSSH snapshot unavailable",fillcolor="#e7f5ed"];
d2 [label="Intel-30 / Data 2\nPID 1296405; 53 observed threads\n192.168.33.30:22053",fillcolor="#e7f5ed"];
client [label="Client\nHTTP :22091",fillcolor="#fff3da"];
client -> hp [label="query"]; hp -> client [label="merged top-k"];
hr -> d0 [label="TCP: initial branch / heartbeat ACK"]; hr -> d1 [label="TCP: initial branch / heartbeat ACK"]; hr -> d2 [label="TCP: initial branch"];
d0 -> hr [label="TCP: heartbeat / final top-k"];
d1 -> hr [label="TCP: heartbeat / final top-k"];
d2 -> hr [label="TCP: heartbeat / final top-k"];
d0 -> d1 [dir=both,label="TCP: approved child branch + ACK",color="#c56e21"];
d1 -> d2 [dir=both,label="TCP: approved child branch + ACK",color="#c56e21"];
d0 -> d2 [dir=both,label="TCP: approved child branch + ACK",color="#c56e21"];
''',
'native_data':r'''
rankdir=TB;
remote [label="Head or another data node",fillcolor="#fff3da"];
subgraph cluster_data {label="One native data process: 44 threads with W=32, R=4";
 rx [label="Server I/O reactor x1\nNonblocking TCP read / ACK write"];
 recv [label="Receive workers x4\nDecode, validate, deduplicate, accept"];
 admin [label="Admin worker x1\nPython management callback only"];
 compute [label="Search workers x32\nOne branch per worker\nGraph + RaBitQ + exact distance"];
 transfer [label="Child RPC client reactor x1\nNonblocking send / receive ACK"];
 ctrl [label="Control scheduler x1\nHeartbeat, completion, retry, timers"];
 report [label="Control RPC client reactor x1\nNonblocking reports / heartbeat / ACK"];
 maint [label="Retention maintenance x1\nApply ACK / expiry updates"];
 aux [label="Main x1 + resource sampler x1",fillcolor="#f1f1f1"];
 rx -> recv [label="complete frame queue"];
 recv -> rx [label="reply completion queue"];
 recv -> admin [label="admin queue",style=dashed]; admin -> rx [label="reply",style=dashed];
 recv -> compute [label="bounded compute queue"];
 recv -> ctrl [label="register / lease / cancel events"];
 compute -> transfer [label="approved child submission"];
 transfer -> ctrl [label="child accepted / failed event"];
 compute -> ctrl [label="start / child / local-done events"];
 ctrl -> report [label="control RPC queue"];
 report -> ctrl [label="ACK / error event"];
 ctrl -> maint [label="terminal updates"];
}
ssd [label="Local index file\nOS page cache / SSD",shape=cylinder,fillcolor="#f1f1f1"];
remote -> rx [label="TCP: branch / lease / cancel"];
rx -> remote [label="TCP: accepted / result ACK"];
compute -> ssd [dir=both,label="sync pread: executed by search worker"];
transfer -> other [label="TCP: child branch",color="#c56e21"];
other [label="Destination data server\nParent keeps searching",fillcolor="#fff3da"];
other -> transfer [label="TCP: acceptance ACK"];
report -> head [label="TCP: heartbeat / final top-k",color="#149174"];
head [label="Head report receiver",fillcolor="#fff3da"];
head -> report [label="TCP: ACK / lease / cancel flag"];
''',
'native_head':r'''
rankdir=TB;
subgraph cluster_head {label="Native head: one process, 18 threads (direct API)";
 main [label="Main / API caller x1"];
 route [label="Route workers x4\nHNSW + RaBitQ"];
 entry [label="Entry RPC reactor x1"];
 rx [label="Report server reactor x1"];
 recv [label="Report workers x4\nValidation + registry update"];
 merge [label="Merge workers x4\nGlobal top-k"];
 timer [label="Lease / deadline timer x1"];
 lease [label="Lease RPC reactor x1"];
 final [label="Python final API consumer x1"];
 main -> route [label="route queue"];
 route -> entry [label="submit initial branch"];
 rx -> recv [dir=both,label="frames / ACK queue"];
 recv -> merge [label="all reports + entry ready"];
 merge -> final [label="completed-record queue"];
 timer -> lease [label="lease / cancel queue"];
 entry -> merge [label="entry ACK status",style=dashed];
 final -> main [label="resolve Future",style=dashed,constraint=false];
 {rank=same; entry; timer;}
}
api [label="Client / Python API",fillcolor="#fff3da"];
api -> main [dir=both,label="submit / result"];
de [label="Data node\nEntry endpoint",fillcolor="#e7f5ed"];
dr [label="Data node\nReport source",fillcolor="#e7f5ed"];
dl [label="Data node\nLease endpoint",fillcolor="#e7f5ed"];
entry -> de [dir=both,label="TCP: branch / acceptance ACK"];
dr -> rx [dir=both,label="TCP: heartbeat or top-k / ACK"];
lease -> dl [dir=both,label="TCP: lease or cancel / ACK"];
note [shape=note,fillcolor="#f1f1f1",label="The 3 green boxes are channels\nto the SAME set of data servers.\nPer-query registry is shared state,\nnot an extra thread."];
'''}
for name,body in graphs.items():
 p=ROOT/(name+'.dot');p.write_text('digraph G {\n'+STYLE+body+'\n}\n')
 for fmt in ('svg','png'):
  subprocess.run(['dot','-T'+fmt,str(p),'-o',str(ROOT/(name+'.'+fmt))],check=True)
print('Generated 3 diagrams in SVG and PNG.')
