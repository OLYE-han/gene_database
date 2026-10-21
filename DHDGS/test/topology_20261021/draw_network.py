"""Network-module topology; historical diagrams remain reproducible in draw.py."""
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parent
STYLE='''graph [fontname="DejaVu Sans",rankdir=TB,bgcolor="white",pad=0.25,nodesep=0.45,ranksep=0.65];
node [fontname="DejaVu Sans",shape=box,style="rounded,filled",fillcolor="#eaf2ff",color="#57759c",fontsize=11];
edge [fontname="DejaVu Sans",fontsize=9,color="#49586b"];
'''
graphs={
'network_module_detail':r'''
subgraph cluster_tx {label="dhnet::Client - one logical outbound channel";color="#7aa3cc";
 app [label="Application submit()\nCaller thread; no new dispatcher thread"];
 select [label="Short admission / least-pending selection\nGlobal pending limit for this channel",shape=ellipse,fillcolor="#f1f1f1"];
 lane [label="T send lanes (1..4)\nEach: queue + endpoint connection pools\n+ ONE nonblocking dhwire::Client reactor"];
 mon [label="ONE send monitor thread\npending pressure / cooldown / idle"];
 app -> select -> lane;
 mon -> lane [label="grow / retire idle lane",style=dashed];
}
subgraph cluster_rx {label="dhnet::Server - one logical listening port";color="#7aa3cc";
 listen [label="R listening sockets (1..4)\nSAME port with SO_REUSEPORT\nShared total connection budget",fillcolor="#e7f5ed"];
 react [label="R receive reactor threads\nEach owns its accepted connections"];
 workers [label="R bounded handler queues\nR x H handler threads (H=4)"];
 response [label="Per-lane completion queue\nOwning reactor writes ACK",fillcolor="#e7f5ed"];
 admin [label="Data server only:\nR admin queues + R admin threads"];
 rmon [label="ONE receive monitor thread\nHandler queue pressure / idle / drain"];
 listen -> react [label="accept new connections"];
 react -> workers [label="complete frames"];
 workers -> response [label="decoded / validated replies"];
 workers -> admin [label="admin / metrics",style=dashed];
 admin -> response [style=dashed,label="reply"];
 rmon -> listen [label="grow / stop accepting",style=dashed];
 rmon -> workers [label="sample queue depth",style=dashed];
}
appdone [label="Application callback\nRuns on chosen send reactor\nMust be short / nonblocking",fillcolor="#fff3da"];
lane -> listen [label="TCP RPC",color="#c56e21",penwidth=2];
response -> appdone [label="TCP ACK -> original send lane",color="#149174",penwidth=2];
''',
'native_data_network':r'''
subgraph cluster_data {label="ONE native data process - network_module enabled";color="#7aa3cc";
 subgraph cluster_in {label="Inbound dhnet::Server";
  rx [label="R receive lanes\nR reactors + R x H handlers\n+ R admin workers",fillcolor="#e7f5ed"];
  rm [label="Receive monitor x1",fillcolor="#e7f5ed"];
  rm -> rx [style=dashed,label="scale / drain"];
 }
 work [label="Bounded compute queue\nSearch workers x W\nGraph + RaBitQ + exact distance"];
 ctrl [label="Control scheduler x1\nstart / child / completion / retry"];
 subgraph cluster_child {label="Outbound dhnet::Client: child branches";
  child [label="T_child send reactors\nSeparate queues + connection pools",fillcolor="#fff3da"];
  cm [label="Child send monitor x1",fillcolor="#fff3da"];
  cm -> child [style=dashed,label="scale / idle retire"];
 }
 subgraph cluster_report {label="Outbound dhnet::Client: heartbeat / result";
  report [label="T_report send reactors\nSeparate queues + connection pools",fillcolor="#fff3da"];
  pm [label="Report send monitor x1",fillcolor="#fff3da"];
  pm -> report [style=dashed,label="scale / idle retire"];
 }
 aux [label="Main x1 + resource sampler x1\nRetention maintenance x1",fillcolor="#f1f1f1"];
 rx -> work [label="accepted branch -> compute queue"];
 rx -> ctrl [label="register / lease / cancel events"];
 work -> child [label="ONLY approved child submission",color="#c56e21"];
 work -> ctrl [label="local lifecycle events"];
 child -> ctrl [label="acceptance ACK / error callback"];
 ctrl -> report [label="control RPC submission"];
}
source [label="Head or another data node",fillcolor="#f1f1f1"];
source -> rx [dir=both,label="TCP task / lease / cancel and ACK"];
other [label="Destination data node",fillcolor="#f1f1f1"];
child -> other [dir=both,label="TCP child branch / acceptance ACK",color="#c56e21"];
head [label="Head report server",fillcolor="#f1f1f1"];
report -> head [dir=both,label="TCP heartbeat or top-k / ACK",color="#149174"];
ssd [label="OS page cache / local SSD",shape=cylinder,fillcolor="#f1f1f1"];
work -> ssd [dir=both,label="sync_pread on search worker"];
''',
'native_head_network':r'''
subgraph cluster_head {label="ONE native head process - network_module enabled";color="#7aa3cc";
 route [label="API -> route queue\nRoute workers x L"];
 subgraph cluster_entry {label="Outbound dhnet::Client: entry";
  entry [label="T_entry send reactors\nQueues + endpoint connection pools",fillcolor="#fff3da"];
  em [label="Entry send monitor x1",fillcolor="#fff3da"];
  em -> entry [label="scale",style=dashed];
 }
 subgraph cluster_report {label="Inbound dhnet::Server: reports";
  rx [label="R receive reactors\n+ R x H report handler threads\nSame port / SO_REUSEPORT",fillcolor="#e7f5ed"];
  rm [label="Receive monitor x1",fillcolor="#e7f5ed"];
  rm -> rx [label="scale / drain",style=dashed];
 }
 timer [label="Lease / deadline timer x1"];
 subgraph cluster_lease {label="Outbound dhnet::Client: lease / cancel";
  lease [label="T_lease send reactors\nQueues + endpoint connection pools",fillcolor="#fff3da"];
  lm [label="Lease send monitor x1",fillcolor="#fff3da"];
  lm -> lease [label="scale",style=dashed];
 }
 merge [label="Query registry -> merge queue\nMerge workers x M"];
 final [label="Completed-record queue\nPython final API thread x1\n-> Future / HTTP response"];
 main [label="Main / API caller x1\nHTTP adds 6 threads when deployed",fillcolor="#f1f1f1"];
 route -> entry [label="initial branch submission"];
 entry -> merge [label="entry ACK state",style=dashed];
 rx -> merge [label="validated reports + global completion"];
 timer -> lease [label="involved-node lease / cancel"];
 merge -> final [label="global top-k"];
}
de [label="Data nodes: entry endpoints",fillcolor="#f1f1f1"];
dr [label="Data nodes: report sources",fillcolor="#f1f1f1"];
dl [label="Data nodes: lease endpoints",fillcolor="#f1f1f1"];
entry -> de [dir=both,label="TCP initial branch / acceptance ACK",color="#c56e21"];
dr -> rx [dir=both,label="TCP heartbeat or final top-k / ACK",color="#149174"];
lease -> dl [dir=both,label="TCP lease or cancel / ACK"];
note [shape=note,fillcolor="#f1f1f1",label="The 3 external boxes denote channels\nto the SAME data-node processes.\nMonitors change network capacity only;\nthey never authorize graph expansion."];
'''}
for name,body in graphs.items():
 p=ROOT/(name+'.dot');p.write_text('digraph G {\n'+STYLE+body+'\n}\n')
 for fmt in ('svg','png'):
  subprocess.run(['dot','-T'+fmt,str(p),'-o',str(ROOT/(name+'.'+fmt))],check=True)
print('Generated network module detail, data-node and head-node diagrams.')
