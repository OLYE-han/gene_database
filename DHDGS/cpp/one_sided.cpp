#include "one_sided.hpp"
#include "value_utils.hpp"
#include <pybind11/pybind11.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <set>
#include <thread>
#include <stdexcept>
#include <limits>
#ifdef DHDGS_VERBS
#include <infiniband/verbs.h>
#endif
namespace dhonesided {
using namespace dhvalue;
static uint64_t ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
static uint64_t cpu_ns(){timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return uint64_t(t.tv_sec)*1000000000+t.tv_nsec;}
static constexpr const char* code_names[]={"code_logical","code_unique","code_local","code_overlay","code_previous","code_missing","code_fetched","code_unclassified"};
static void need(bool b,const char* s){if(!b)throw std::runtime_error(s);}
bool available(){
#ifdef DHDGS_VERBS
 return true;
#else
 return false;
#endif
}
#ifdef DHDGS_VERBS
namespace {
struct Device {
 ibv_context* ctx=nullptr;ibv_pd* pd=nullptr;ibv_cq* cq=nullptr;ibv_port_attr port{};int number=1;
 Device(const V& cfg){try{
  number=get<int>(cfg,"port");int count=0;auto list=ibv_get_device_list(&count);need(list,"ibv_get_device_list failed");
  const auto wanted=get<std::string>(cfg,"device");
  for(int i=0;i<count;++i){if(!wanted.empty()&&wanted!=ibv_get_device_name(list[i]))continue;
   auto c=ibv_open_device(list[i]);if(!c)continue;ibv_port_attr p{};
   if(!ibv_query_port(c,number,&p)&&p.state==IBV_PORT_ACTIVE&&p.link_layer==IBV_LINK_LAYER_INFINIBAND){ctx=c;port=p;break;}ibv_close_device(c);
  }
  ibv_free_device_list(list);need(ctx,"No active InfiniBand verbs port (first backend supports IB LIDs)");
  pd=ibv_alloc_pd(ctx);need(pd,"ibv_alloc_pd failed");cq=ibv_create_cq(ctx,4096,nullptr,nullptr,0);need(cq,"ibv_create_cq failed");
 }catch(...){clear();throw;}}
 void clear(){if(cq)ibv_destroy_cq(cq);if(pd)ibv_dealloc_pd(pd);if(ctx)ibv_close_device(ctx);cq=nullptr;pd=nullptr;ctx=nullptr;}
 ~Device(){clear();}
};
struct QP {
 Device& device;ibv_qp* qp=nullptr;uint32_t psn;
 explicit QP(Device& d):device(d){
  ibv_qp_init_attr init{};init.send_cq=init.recv_cq=d.cq;init.qp_type=IBV_QPT_RC;
  init.cap.max_send_wr=64;init.cap.max_recv_wr=1;init.cap.max_send_sge=1;init.cap.max_recv_sge=1;
  qp=ibv_create_qp(d.pd,&init);need(qp,"ibv_create_qp failed");psn=uint32_t(ns())&0xffffff;
  ibv_qp_attr a{};a.qp_state=IBV_QPS_INIT;a.pkey_index=0;a.port_num=d.number;a.qp_access_flags=IBV_ACCESS_REMOTE_READ;
  if(ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_PKEY_INDEX|IBV_QP_PORT|IBV_QP_ACCESS_FLAGS)){ibv_destroy_qp(qp);qp=nullptr;throw std::runtime_error("QP INIT failed");}
 }
 V info()const{return M{{"qpn",uint64_t(qp->qp_num)},{"psn",uint64_t(psn)},{"lid",uint64_t(device.port.lid)},{"mtu",uint64_t(device.port.active_mtu)}};}
 void connect(const V& remote){
  auto q=get<uint32_t>(remote,"qpn"),p=get<uint32_t>(remote,"psn"),lid=get<uint32_t>(remote,"lid"),mtu=get<uint32_t>(remote,"mtu");
  need(q&&q<=0xffffff&&p<=0xffffff&&lid&&lid<65535&&mtu>=1&&mtu<=5,"Invalid remote QP descriptor");
  ibv_qp_attr a{};a.qp_state=IBV_QPS_RTR;a.path_mtu=ibv_mtu(std::min(uint32_t(device.port.active_mtu),mtu));a.dest_qp_num=q;a.rq_psn=p;a.max_dest_rd_atomic=16;a.min_rnr_timer=12;
  a.ah_attr.dlid=lid;a.ah_attr.port_num=device.number;
  need(!ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_AV|IBV_QP_PATH_MTU|IBV_QP_DEST_QPN|IBV_QP_RQ_PSN|IBV_QP_MAX_DEST_RD_ATOMIC|IBV_QP_MIN_RNR_TIMER),"QP RTR failed");
  a={};a.qp_state=IBV_QPS_RTS;a.timeout=14;a.retry_cnt=3;a.rnr_retry=0;a.sq_psn=psn;a.max_rd_atomic=16;
  need(!ibv_modify_qp(qp,&a,IBV_QP_STATE|IBV_QP_TIMEOUT|IBV_QP_RETRY_CNT|IBV_QP_RNR_RETRY|IBV_QP_SQ_PSN|IBV_QP_MAX_QP_RD_ATOMIC),"QP RTS failed");
 }
 ~QP(){if(qp)ibv_destroy_qp(qp);}
};
struct MR {
 ibv_mr* mr=nullptr;
 MR(Device& d,const void* data,size_t bytes,int flags){need(bytes&&data,"Empty registration");mr=ibv_reg_mr(d.pd,const_cast<void*>(data),bytes,flags);if(!mr){const int e=errno;throw std::runtime_error("ibv_reg_mr failed: bytes="+std::to_string(bytes)+" errno="+std::to_string(e)+" "+std::strerror(e));}}
 ~MR(){if(mr)ibv_dereg_mr(mr);}
 V info()const{return M{{"address",uint64_t(reinterpret_cast<uintptr_t>(mr->addr))},{"length",uint64_t(mr->length)},{"rkey",uint64_t(mr->rkey)}};}
};
struct Region {uint64_t address=0,length=0;uint32_t key=0;};
Region region(const V& v){Region r{get<uint64_t>(v,"address"),get<uint64_t>(v,"length"),get<uint32_t>(v,"rkey")};need(r.length&&r.address<=UINT64_MAX-r.length,"Invalid remote memory region");return r;}
}
struct Source::Impl {
 std::shared_ptr<Image> image;Device device;std::unique_ptr<MR> records,codes,directory;
 mutable std::mutex mu;std::vector<std::unique_ptr<QP>> connections;size_t capacity;uint64_t preparation_ns;
 Impl(const V& cfg,std::shared_ptr<Image> data):image(std::move(data)),device(cfg),capacity(get<size_t>(cfg,"max_connections")){
  const auto begin=ns();need(bool(image),"Missing source registration image");
  const auto d=image->directory.size()*8,c=image->codes.size(),cap=get<size_t>(cfg,"max_registered_bytes");
  need(d<=get<size_t>(cfg,"max_directory_bytes")&&image->record_bytes<=cap&&c<=cap-image->record_bytes&&d<=cap-image->record_bytes-c,"Source registered memory capacity exceeded");
  records=std::make_unique<MR>(device,image->records,image->record_bytes,IBV_ACCESS_REMOTE_READ);
  codes=std::make_unique<MR>(device,image->codes.data(),c,IBV_ACCESS_REMOTE_READ);
  directory=std::make_unique<MR>(device,image->directory.data(),d,IBV_ACCESS_REMOTE_READ);preparation_ns=ns()-begin;
 }
 V connect(const V& peer){std::lock_guard<std::mutex> lock(mu);need(connections.size()<capacity,"One-sided source connection capacity");
  auto qp=std::make_unique<QP>(device);qp->connect(peer);V desc=M{{"qp",qp->info()},{"records",records->info()},{"codes",codes->info()},{"directory",directory->info()},
    {"base",image->base},{"header",uint64_t(image->header)},{"stride",uint64_t(image->stride)},{"dimension",uint64_t(image->dimension)},
    {"code_width",uint64_t(image->code_width)},{"epoch",image->epoch},{"endian",std::string("little")}};
  connections.push_back(std::move(qp));return desc;
 }
 ~Impl(){connections.clear();directory.reset();codes.reset();records.reset();}
 V metrics()const{std::lock_guard<std::mutex> lock(mu);return M{{"connections",uint64_t(connections.size())},{"registered_bytes",uint64_t(image->record_bytes+image->codes.size()+image->directory.size()*8)},
  {"registration_ns",preparation_ns},{"record_bytes",uint64_t(image->record_bytes)},{"code_bytes",uint64_t(image->codes.size())},{"directory_bytes",uint64_t(image->directory.size()*8)}};}
};
struct Client::Impl {
 struct Job{size_t source;V auth;std::vector<uint64_t> ids;Complete done;CodeLookup lookup;uint64_t start=ns();};
 struct Op{Region region;size_t offset,length,local;};
 struct Lane {
  size_t id,source;std::unique_ptr<QP> qp;std::unique_ptr<MR> mr,dir_mr;std::vector<uint8_t> buffer;std::vector<uint64_t> directory;
  V descriptor;Region records,codes,dir;uint64_t base=0;size_t header=0,stride=0,dim=0,width=0;
  std::unique_ptr<Job> job;std::vector<Op> ops;size_t next=0,pending=0,record_bytes=0,code_bytes=0;std::vector<uint64_t> deps;
  int phase=0;bool broken=false;uint64_t deadline=0,phase_start=0,record_ns=0,code_ns=0,read_count=0,read_bytes=0;
  uint64_t assigned=0,records_ready=0,codes_ready=0,filter_cpu=0,filter_wall=0;
  std::array<uint64_t,8> counts{};
  // phase 0: connecting; 1: directory; 2: records; 3: neighbor codes; 4: idle
  ~Lane(){qp.reset();dir_mr.reset();mr.reset();}
 };
 V cfg;Device device;size_t bytes,limit,per_source,total;uint64_t timeout_ns,poll_us;Control control;
 mutable std::mutex mu;std::condition_variable wake;bool closing=false;std::atomic<size_t> retained{0};
 std::deque<std::unique_ptr<Job>> queued;struct Event{size_t lane;dhwire::Result result;};std::deque<Event> events;
 std::vector<std::unique_ptr<Lane>> lanes;std::thread thread;
 std::atomic<uint64_t> reads{0},read_bytes{0},completed{0},errors{0},directory_bytes{0},io_cpu_ns{0},post_calls{0},completion_events{0},connections_created{0};
 std::array<std::atomic<uint64_t>,8> code_counts{};
 std::atomic<uint64_t> filter_cpu_total{0},result_cpu_total{0},poll_calls{0},poll_gap_ns{0},poll_gap_max_ns{0},callback_cpu_ns{0};
 Impl(const V& c,size_t b,size_t l,Control fn):cfg(c),device(c),bytes(b),limit(l),per_source(get<size_t>(c,"lanes_per_source")),total(get<size_t>(c,"max_connections")),
 timeout_ns(get<uint64_t>(c,"timeout_ms")*1000000),poll_us(get<uint64_t>(c,"poll_interval_us")),control(std::move(fn)){
  need(total>0&&total<=64&&per_source&&per_source<=total&&bytes>0&&bytes<=UINT32_MAX,"Invalid verbs client bounds");thread=std::thread([this]{run();});
 }
 void result(Lane& l,const std::string& error={}){
  if(!l.job)return;auto j=std::move(l.job);dhwire::Result out;out.queued_ns=j->start;out.done_ns=ns();out.response_bytes=l.read_bytes;
  if(error.empty()){
   const auto result_start=ns(),result_cpu=cpu_ns();
   V::Bytes records(l.buffer.begin(),l.buffer.begin()+l.record_bytes),codes(l.buffer.begin()+l.record_bytes,l.buffer.begin()+l.record_bytes+l.code_bytes);
   V value=M{{"accepted",true},{"epoch",at(l.descriptor,"epoch")},{"ids",vector(j->ids)},{"record_stride",uint64_t(l.stride)},{"code_stride",uint64_t(l.width)},
    {"records",std::move(records)},{"codes",std::move(codes)},{"record_mode",optional<std::string>(j->auth,"read_mode","full")},{"source_read_ms",0.},{"source_cpu_ms",0.},{"source_service_ms",0.},
    {"one_sided",true},{"one_sided_read_ops",l.read_count},{"one_sided_data_bytes",l.read_bytes},
    {"one_sided_record_ms",l.record_ns/1e6},{"one_sided_code_ms",l.code_ns/1e6},
    {"one_sided_filter_ms",l.filter_wall/1e6},{"one_sided_filter_cpu_ms",l.filter_cpu/1e6},
    {"one_sided_lane_queue_ms",(l.assigned-j->start)/1e6},
    {"one_sided_submit_ns",j->start},{"one_sided_assigned_ns",l.assigned},
    {"one_sided_records_ready_ns",l.records_ready},{"one_sided_codes_ready_ns",l.codes_ready}};
   for(size_t i=0;i<l.counts.size();++i){value.map()[std::string("one_sided_")+code_names[i]]=l.counts[i];code_counts[i]+=l.counts[i];}
   const auto copy_cpu=cpu_ns()-result_cpu;result_cpu_total+=copy_cpu;filter_cpu_total+=l.filter_cpu;
   value.map()["one_sided_result_cpu_ms"]=copy_cpu/1e6;value.map()["one_sided_result_ms"]=(ns()-result_start)/1e6;
   value.map()["one_sided_result_ready_ns"]=ns();
   out.ok=true;out.response=M{{"ok",true},{"result",std::move(value)}};++completed;l.phase=4;
  }else{out.error=error;++errors;l.broken=true;l.qp.reset();l.pending=0;}
  {std::lock_guard<std::mutex> lock(mu);--retained;}const auto done_cpu=cpu_ns();j->done(std::move(out));callback_cpu_ns+=cpu_ns()-done_cpu;
 }
 void operations(Lane& l,std::vector<Op> ops){l.ops=std::move(ops);l.next=0;l.pending=0;l.phase_start=ns();post(l);}
 void post(Lane& l){
  if(optional<bool>(cfg,"doorbell_batch",false)) {
   if(l.pending||l.next==l.ops.size())return;
   const size_t count=std::min(size_t(16),l.ops.size()-l.next);
   std::array<ibv_sge,16> sges{};std::array<ibv_send_wr,16> wrs{};
   const auto capacity=l.phase==1?l.directory.size()*8:l.buffer.size();
   auto* memory=l.phase==1?reinterpret_cast<uint8_t*>(l.directory.data()):l.buffer.data();auto* mr=l.phase==1?l.dir_mr.get():l.mr.get();
   size_t transferred=0;
   for(size_t i=0;i<count;++i){const auto& x=l.ops[l.next+i];
    need(x.length>0&&x.length<=UINT32_MAX&&x.offset<=x.region.length&&x.length<=x.region.length-x.offset,"RDMA remote range out of bounds");
    need(x.local<=capacity&&x.length<=capacity-x.local,"RDMA local range out of bounds");
    auto& s=sges[i];s.addr=reinterpret_cast<uintptr_t>(memory+x.local);s.length=x.length;s.lkey=mr->mr->lkey;
    auto& w=wrs[i];w.wr_id=l.id+1;w.sg_list=&s;w.num_sge=1;w.opcode=IBV_WR_RDMA_READ;
    w.wr.rdma.remote_addr=x.region.address+x.offset;w.wr.rdma.rkey=x.region.key;
    w.next=i+1<count?&wrs[i+1]:nullptr;w.send_flags=i+1==count?IBV_SEND_SIGNALED:0;transferred+=x.length;
   }
   // RC SQ completion of the signalled tail covers the preceding unsignalled
   // reads. A post/completion error retires the entire QP before reuse/free.
   ibv_send_wr* bad=nullptr;need(!ibv_post_send(l.qp->qp,wrs.data(),&bad),"ibv_post_send READ chain failed");
   ++post_calls;reads+=count;read_bytes+=transferred;l.next+=count;l.pending=1;
   if(l.phase!=1){l.read_count+=count;l.read_bytes+=transferred;}return;
  }
  while(l.pending<16&&l.next<l.ops.size()){
   const auto& x=l.ops[l.next];need(x.length>0&&x.length<=UINT32_MAX&&x.offset<=x.region.length&&x.length<=x.region.length-x.offset,"RDMA remote range out of bounds");
   const auto capacity=l.phase==1?l.directory.size()*8:l.buffer.size();need(x.local<=capacity&&x.length<=capacity-x.local,"RDMA local range out of bounds");
   auto* memory=l.phase==1?reinterpret_cast<uint8_t*>(l.directory.data()):l.buffer.data();auto* mr=l.phase==1?l.dir_mr.get():l.mr.get();
   ibv_sge sge{};sge.addr=reinterpret_cast<uintptr_t>(memory+x.local);sge.length=x.length;sge.lkey=mr->mr->lkey;
   ibv_send_wr wr{},*bad=nullptr;wr.wr_id=l.id+1;wr.sg_list=&sge;wr.num_sge=1;wr.opcode=IBV_WR_RDMA_READ;wr.send_flags=IBV_SEND_SIGNALED;wr.wr.rdma.remote_addr=x.region.address+x.offset;wr.wr.rdma.rkey=x.region.key;
   need(!ibv_post_send(l.qp->qp,&wr,&bad),"ibv_post_send RDMA_READ failed");++post_calls;++l.next;++l.pending;++reads;read_bytes+=x.length;
   if(l.phase!=1){++l.read_count;l.read_bytes+=x.length;}
  }
 }
 uint64_t row(const Lane& l,uint64_t id,bool code){need(id>=l.base&&(id-l.base)<l.directory.size()/2,"One-sided ID outside directory");auto n=l.directory[(id-l.base)*2+size_t(code)];need(n!=UINT64_MAX,"One-sided missing record/code");return n;}
 void records(Lane& l){
  l.phase=2;l.read_count=l.read_bytes=l.record_ns=l.code_ns=0;
  l.counts={};l.records_ready=l.codes_ready=l.filter_cpu=l.filter_wall=0;
  need(l.stride>0&&l.job->ids.size()<=bytes/l.stride,"One-sided record batch capacity");l.record_bytes=l.job->ids.size()*l.stride;l.code_bytes=0;
   const auto mode=optional<std::string>(l.job->auth,"read_mode","full");
   need(mode=="full"||mode=="adjacency"||mode=="vectors","Invalid selective read mode");
   need(l.dim*4+4<=l.stride,"Invalid selective record layout");
   const size_t skip=mode=="adjacency"?l.dim*4:0,length=mode=="vectors"?l.dim*4:l.stride-skip;
   if(mode!="full")std::fill(l.buffer.begin(),l.buffer.begin()+l.record_bytes,0);
   std::vector<Op> ops;size_t offset=0;for(auto id:l.job->ids){auto r=row(l,id,false);need(l.header<=l.records.length&&r<(l.records.length-l.header)/l.stride,"Invalid record directory row");ops.push_back({l.records,l.header+r*l.stride+skip,length,offset+skip});offset+=l.stride;}operations(l,std::move(ops));
 }
 void advance(Lane& l){
  if(l.phase==1){directory_bytes+=l.dir.length;l.dir_mr.reset();records(l);return;}
  if(l.phase==2){l.records_ready=ns();l.record_ns=l.records_ready-l.phase_start;const auto filtering=ns(),cpu=cpu_ns();std::set<uint64_t> ids;need(l.dim*4+4<=l.stride,"Invalid record dimension");
   const bool vectors=optional<std::string>(l.job->auth,"read_mode","full")=="vectors";
   for(size_t off=0;!vectors&&off<l.record_bytes;off+=l.stride){uint32_t n;std::memcpy(&n,l.buffer.data()+off+l.dim*4,4);need(n<=(l.stride-l.dim*4-4)/8,"Invalid one-sided graph degree");
    l.counts[0]+=n;for(size_t i=0;i<n;++i){uint64_t id;std::memcpy(&id,l.buffer.data()+off+l.dim*4+4+i*8,8);ids.insert(id);}}
   // Shared growth entries can outlive the query and their discovery parent.
   // A complete durable entry therefore needs its own code as well as neighbor
   // codes; legacy query-private pulls only need the neighbor estimates.
   if(!vectors&&optional<bool>(l.job->auth,"demand_shared",false))ids.insert(l.job->ids.begin(),l.job->ids.end());
   l.counts[1]=ids.size();l.deps.clear();
   const bool filter=optional<bool>(cfg,"missing_codes_only",false);
   if(!l.job->lookup){l.counts[7]=ids.size();l.deps.assign(ids.begin(),ids.end());}
   else for(auto id:ids){const auto kind=l.job->lookup(id);need(kind<=3,"Invalid code availability category");
    ++l.counts[kind?kind+1:5];if(!kind||!filter)l.deps.push_back(id);}
   l.counts[6]=l.deps.size();l.filter_cpu=cpu_ns()-cpu;l.filter_wall=ns()-filtering;
   need(l.width>=8&&l.deps.size()<=(bytes-l.record_bytes)/l.width,"One-sided dependency byte capacity exceeded");l.code_bytes=l.deps.size()*l.width;l.phase=3;
   std::vector<Op> ops;size_t offset=l.record_bytes;for(auto id:l.deps){auto r=row(l,id,true);need(r<l.codes.length/l.width,"Invalid code directory row");ops.push_back({l.codes,r*l.width,l.width,offset});offset+=l.width;}
   operations(l,std::move(ops));if(l.ops.empty())advance(l);return;
  }
  if(l.phase==3){l.codes_ready=ns();l.code_ns=l.codes_ready-l.phase_start;
   for(size_t i=0;i<l.deps.size();++i){uint64_t id;std::memcpy(&id,l.buffer.data()+l.record_bytes+i*l.width,8);need(id==l.deps[i],"One-sided code identity mismatch");}result(l);
  }
 }
 void connected(Lane& l,dhwire::Result reply){
  need(reply.ok&&get<bool>(reply.response,"ok"),"One-sided connection control failed");l.descriptor=at(reply.response,"result");
  need(get<std::string>(l.descriptor,"epoch")==get<std::string>(l.job->auth,"epoch")&&get<std::string>(l.descriptor,"endian")=="little","One-sided epoch/layout mismatch");
  l.records=region(at(l.descriptor,"records"));l.codes=region(at(l.descriptor,"codes"));l.dir=region(at(l.descriptor,"directory"));
  need(l.dir.length%16==0&&l.dir.length<=get<size_t>(cfg,"max_directory_bytes"),"One-sided directory capacity");
  l.base=get<uint64_t>(l.descriptor,"base");l.header=get<size_t>(l.descriptor,"header");l.stride=get<size_t>(l.descriptor,"stride");l.dim=get<size_t>(l.descriptor,"dimension");l.width=get<size_t>(l.descriptor,"code_width");
  need(l.dim<=l.stride/4&&l.width>=8&&l.codes.length%l.width==0,"Invalid one-sided geometry");l.qp->connect(at(l.descriptor,"qp"));
  l.directory.resize(l.dir.length/8);l.dir_mr=std::make_unique<MR>(device,l.directory.data(),l.dir.length,IBV_ACCESS_LOCAL_WRITE);l.phase=1;
  std::vector<Op> ops;for(size_t off=0;off<l.dir.length;off+=1048576)ops.push_back({l.dir,off,std::min(size_t(1048576),size_t(l.dir.length-off)),off});operations(l,std::move(ops));
 }
 void assign(){
  std::deque<std::unique_ptr<Job>> jobs;{std::lock_guard<std::mutex> lock(mu);jobs.swap(queued);}
  while(!jobs.empty()){
   auto job=std::move(jobs.front());jobs.pop_front();Lane* free=nullptr;size_t count=0;
   for(auto& lane:lanes)if(lane->source==job->source){++count;if(!lane->broken&&!lane->job&&lane->phase==4)free=lane.get();}
   if(!free&&count<per_source&&lanes.size()<total){
    auto lane=std::make_unique<Lane>();lane->id=lanes.size();lane->source=job->source;free=lane.get();lanes.push_back(std::move(lane));++connections_created;
   }
   if(!free){bool live=false;for(auto& l:lanes)live|=l->source==job->source&&!l->broken;
    if(!live){dhwire::Result r;r.error="One-sided connection capacity or failed endpoint";{std::lock_guard<std::mutex> lock(mu);--retained;}job->done(std::move(r));}
    else {std::lock_guard<std::mutex> lock(mu);queued.push_back(std::move(job));}continue;
   }
   auto& l=*free;l.job=std::move(job);l.assigned=ns();l.deadline=l.assigned+timeout_ns;
   try{if(l.phase==4){records(l);continue;}
    l.buffer.resize(bytes);l.mr=std::make_unique<MR>(device,l.buffer.data(),bytes,IBV_ACCESS_LOCAL_WRITE);l.qp=std::make_unique<QP>(device);
    V request=l.job->auth;request.map()["op"]="peer_one_sided_connect";request.map()["qp"]=l.qp->info();
    need(control(l.source,std::move(request),[this,id=l.id](dhwire::Result r){{std::lock_guard<std::mutex> lock(mu);events.push_back({id,std::move(r)});}wake.notify_one();}),"One-sided connect queue full");
   }catch(const std::exception& e){result(l,e.what());}
  }
 }
 void run(){
  const auto cpu=[](){timespec t{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&t);return uint64_t(t.tv_sec)*1000000000+t.tv_nsec;};uint64_t previous=cpu(),last_poll=ns();
  for(;;){
   std::deque<Event> incoming;{std::unique_lock<std::mutex> lock(mu);if(closing)break;incoming.swap(events);}
   for(auto& e:incoming){auto& l=*lanes.at(e.lane);if(l.broken||!l.job)continue;try{connected(l,std::move(e.result));}catch(const std::exception& x){result(l,x.what());}}
   assign();ibv_wc wc[64];const auto poll_time=ns();const auto gap=poll_time-last_poll;last_poll=poll_time;++poll_calls;poll_gap_ns+=gap;
   auto maximum=poll_gap_max_ns.load();while(maximum<gap&&!poll_gap_max_ns.compare_exchange_weak(maximum,gap)){}
   int n=ibv_poll_cq(device.cq,64,wc);
   if(n<0){for(auto& l:lanes)if(l->job)result(*l,"ibv_poll_cq failed");}
   for(int i=0;i<n;++i){++completion_events;if(!wc[i].wr_id||wc[i].wr_id>lanes.size())continue;auto& l=*lanes[wc[i].wr_id-1];if(l.broken||!l.job)continue;
    if(wc[i].status!=IBV_WC_SUCCESS){result(l,std::string("RDMA completion: ")+ibv_wc_status_str(wc[i].status));continue;}
    try{need(l.pending>0,"Unexpected RDMA completion");--l.pending;post(l);if(!l.pending&&l.next==l.ops.size())advance(l);}catch(const std::exception& x){result(l,x.what());}
   }
   auto now=ns();for(auto& l:lanes)if(l->job&&now>l->deadline)result(*l,"One-sided read timeout");
   auto current=cpu();io_cpu_ns+=current-previous;previous=current;
   if(n<=0){std::unique_lock<std::mutex> lock(mu);wake.wait_for(lock,std::chrono::microseconds(poll_us),[this]{return closing||!events.empty();});}
  }
  for(auto& l:lanes)if(l->job)result(*l,"One-sided reader closed");
  std::deque<std::unique_ptr<Job>> rest;{std::lock_guard<std::mutex> lock(mu);rest.swap(queued);retained-=rest.size();}
  for(auto& j:rest){dhwire::Result r;r.error="One-sided reader closed";j->done(std::move(r));}lanes.clear();
 }
 size_t pending_count()const{return retained.load(std::memory_order_relaxed);}
 bool submit(size_t source,V auth,std::vector<uint64_t> ids,Complete cb,CodeLookup lookup){
  if(ids.empty())return false;std::lock_guard<std::mutex> lock(mu);if(closing||retained>=limit)return false;
  ++retained;queued.push_back(std::make_unique<Job>(Job{source,std::move(auth),std::move(ids),std::move(cb),std::move(lookup)}));wake.notify_one();return true;
 }
 void close(){{std::lock_guard<std::mutex> lock(mu);closing=true;}wake.notify_all();if(thread.joinable())thread.join();}
 ~Impl(){close();}
 V metrics()const{std::lock_guard<std::mutex> lock(mu);M out{{"read_operations",reads.load()},{"read_bytes",read_bytes.load()},{"completed_batches",completed.load()},{"errors",errors.load()},
   {"post_calls",post_calls.load()},{"completion_events",completion_events.load()},{"connections_created",connections_created.load()},
   {"directory_read_bytes",directory_bytes.load()},{"io_cpu_ns",io_cpu_ns.load()},{"pending",uint64_t(retained)},{"queued",uint64_t(queued.size())},
   {"poll_calls",poll_calls.load()},{"poll_gap_ns",poll_gap_ns.load()},{"poll_gap_max_ns",poll_gap_max_ns.load()},{"callback_cpu_ns",callback_cpu_ns.load()},{"filter_cpu_ns",filter_cpu_total.load()},{"result_cpu_ns",result_cpu_total.load()}};
   for(size_t i=0;i<code_counts.size();++i)out[code_names[i]]=code_counts[i].load();
   return out;}
};
#else
struct Source::Impl{Impl(const V&,std::shared_ptr<Image>){throw std::runtime_error("Built without ibverbs");}V connect(const V&){return {};}V metrics()const{return {};}};
struct Client::Impl{Impl(const V&,size_t,size_t,Control){throw std::runtime_error("Built without ibverbs");}size_t pending_count()const{return 0;}bool submit(size_t,V,std::vector<uint64_t>,Complete,CodeLookup){return false;}V metrics()const{return {};}void close(){}};
#endif
Source::Source(const V& c,std::shared_ptr<Image> i):p_(std::make_unique<Impl>(c,std::move(i))){}
Source::~Source()=default;V Source::connect(const V& q){return p_->connect(q);}V Source::metrics()const{return p_->metrics();}
Client::Client(const V& c,size_t b,size_t n,Control f){
  const auto threads=dhvalue::optional<size_t>(c,"io_threads",1);
  const auto per=dhvalue::get<size_t>(c,"lanes_per_source"),total=dhvalue::get<size_t>(c,"max_connections");
  if(!threads||threads>16||threads>per||total%threads||per%threads||n<threads)
    throw std::invalid_argument("Invalid one-sided I/O shards");
  for(size_t k=0;k<threads;++k){V cfg=c;cfg.map()["lanes_per_source"]=uint64_t(per/threads);cfg.map()["max_connections"]=uint64_t(total/threads);
    engines_.push_back(std::make_unique<Impl>(cfg,b,n/threads+(k<n%threads),f));}
}
Client::~Client()=default;
bool Client::submit(size_t s,V a,std::vector<uint64_t> i,Complete f,CodeLookup lookup){
  // Independent CQ/QP ownership; total connections and pending memory stay
  // bounded across shards. One accepted job invokes its continuation once.
  if(engines_.size()==1)return engines_.front()->submit(s,std::move(a),std::move(i),std::move(f),std::move(lookup));
  const auto cursor=next_by_source_[s%next_by_source_.size()].next.fetch_add(1,std::memory_order_relaxed)%engines_.size();
  // Short bursts need not wait behind a slow shard while another CQ is idle.
  // This is advisory: submit still checks capacity under its own queue lock.
  auto first=cursor;auto pending=engines_[first]->pending_count();
  for(size_t k=1;k<engines_.size();++k){const auto i=(cursor+k)%engines_.size();
    const auto load=engines_[i]->pending_count();if(load<pending){first=i;pending=load;}}
  for(size_t k=0;k<engines_.size();++k){
    auto& engine=engines_[(first+k)%engines_.size()];
    if(engine->submit(s,a,i,f,lookup))return true;
  }
  return false;
}
V Client::metrics()const{
  V::Map sum;V::Array parts;
  for(const auto& engine:engines_){auto m=engine->metrics();
    for(const auto& [key,value]:m.map()){auto it=sum.find(key);const auto old=it==sum.end()?0:dhvalue::cast<uint64_t>(it->second);const auto v=dhvalue::cast<uint64_t>(value);sum[key]=key=="poll_gap_max_ns"?std::max(old,v):old+v;}
    parts.push_back(std::move(m));}
  sum["io_threads"]=uint64_t(engines_.size());sum["engines"]=std::move(parts);return sum;
}
void Client::close(){for(auto& engine:engines_)engine->close();}
void bind(pybind11::module_& m){m.def("one_sided_available",&available);}
}
