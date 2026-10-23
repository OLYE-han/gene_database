#include "native_search_api.hpp"
#include "value_utils.hpp"
#include "retained_size.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <cmath>
#include <deque>
#include <filesystem>
#include <fcntl.h>
#include <mutex>
#include <thread>
#include <unistd.h>
namespace dhruntime {
using namespace dhvalue;using M=V::Map;using A=V::Array;
static uint64_t stamp(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
static void check(bool x,const char* s){if(!x)throw std::invalid_argument(s);}
SearchApi::SearchApi(HeadRuntime& head,size_t dim,const V& options){
 dhwire::ServerConfig sc;sc.host=get<std::string>(options,"host");sc.port=get<uint16_t>(options,"port");sc.workers=get<size_t>(options,"handlers");sc.queue_capacity=get<size_t>(options,"queue_capacity");sc.max_connections=get<size_t>(options,"max_connections");sc.max_message_bytes=get<size_t>(options,"max_message_bytes");sc.listen_backlog=4096;sc.idle_timeout_ms=180000;
 // This adapter owns only the public endpoint. Graph routing remains HeadRuntime's job.
 server_=std::make_unique<dhnet::Server>(sc,[&head,dim](V request,uint64_t received,size_t){
  auto handled=stamp();check(get<uint64_t>(request,"version")==1&&get<std::string>(request,"op")=="search","Unsupported search API operation");
  check(get<std::string>(request,"encoding")=="float64_le","Unsupported vector encoding");
  const auto& bytes=std::get<V::Bytes>(at(request,"vector").data);check(bytes.size()==dim*sizeof(double),"Invalid query dimensions");
  uint16_t endian=1;check(*reinterpret_cast<uint8_t*>(&endian)==1,"float64_le requires little-endian host");
  std::vector<double> vector(dim);std::memcpy(vector.data(),bytes.data(),bytes.size());for(double x:vector)check(std::isfinite(x),"Nonfinite query");
  V qid=at(request,"query_id");if(!null(qid))cast<uint64_t>(qid);
  dhwire::Reply reply;
  reply.deferred=[&head,vector=std::move(vector),qid=std::move(qid),received,handled](auto deliver)mutable{
   auto submitted=stamp();
   head.submit(std::move(vector),std::move(qid),{},[deliver,received,handled,submitted](V result)mutable{
    result.map()["api_timestamps"]=M{{"message_received_ns",received},{"handler_started_ns",handled},{"submit_ns",submitted},{"completion_callback_ns",stamp()}};
    dhwire::Reply r;r.message=std::move(result);deliver(std::move(r));
   });
  };return reply;
 },dhwire::Server::Handler{},options);
}
static void write_all(int fd,const std::vector<uint8_t>& b){size_t n=0;while(n<b.size()){auto r=::write(fd,b.data()+n,b.size()-n);if(r<0&&errno==EINTR)continue;if(r<=0)throw std::runtime_error("Load client output write failed");n+=r;}}
V run_network_load(const double* vectors,const uint64_t* ids,size_t count,size_t dim,size_t concurrency,const V& opts,const std::string& directory){
 size_t lanes=get<size_t>(opts,"lanes"),capacity=get<size_t>(opts,"consumer_queue_records"),memory=get<size_t>(opts,"consumer_queue_bytes"),message=get<size_t>(opts,"max_message_bytes"),connections=get<size_t>(opts,"connections");
 check(lanes&&lanes<=64&&count&&concurrency>=lanes&&capacity&&memory&&connections>=concurrency,"Invalid load settings");
 check(std::filesystem::create_directory(directory),"Load output directory exists");
 struct Item{V value;uint64_t bytes;};
 struct Lane{std::mutex mu;std::condition_variable cv;std::deque<Item> queue;size_t active=0,received=0,consumed=0,peak=0,used=0,peak_bytes=0;bool done=false;std::string error;uint64_t last=0,encode_ns=0,write_ns=0,sync_started=0,sync_barrier_ns=0;};
 std::vector<std::unique_ptr<Lane>> state;for(size_t i=0;i<lanes;++i)state.emplace_back(new Lane);
 dhwire::Endpoint endpoint(get<std::string>(opts,"host"),get<uint16_t>(opts,"port"));
 uint64_t offered=stamp();std::vector<std::thread> producers,consumers;
 std::mutex sync_mu;std::condition_variable sync_cv;bool all_network_done=false;
 // Each network lane has a producer, an asynchronous reactor, and an independent
 // consuming writer. No service object or shared result registry crosses this API.
 for(size_t lane=0;lane<lanes;++lane){
  auto& l=*state[lane];
  consumers.emplace_back([&,lane]{auto& x=*state[lane];int fd=-1;
   try{fd=::open((directory+"/lane-"+std::to_string(lane)+".frames").c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);check(fd>=0,"Cannot open client output");std::vector<uint8_t> batch;
    for(;;){Item item;
     {std::unique_lock<std::mutex> lock(x.mu);x.cv.wait(lock,[&]{return x.done||!x.queue.empty();});if(x.queue.empty())break;item=std::move(x.queue.front());x.queue.pop_front();x.used-=item.bytes;}
     auto begin=stamp();item.value.map()["consume_started_ns"]=begin;auto packed=dhwire::pack(item.value,message+65536);auto encoded=stamp();
     auto n=packed.size();for(int i=7;i>=0;--i)batch.push_back(uint8_t(n>>(i*8)));batch.insert(batch.end(),packed.begin(),packed.end());
     if(batch.size()>=1048576){auto t=stamp();write_all(fd,batch);x.write_ns+=stamp()-t;batch.clear();}
     x.encode_ns+=encoded-begin;++x.consumed;
    }
    auto t=stamp();write_all(fd,batch);x.write_ns+=stamp()-t;
    // A fast lane's fsync must not stall slower lanes still consuming responses
    // on the same filesystem. Keep continuous writes, but defer final sync.
    t=stamp();{std::unique_lock<std::mutex> lock(sync_mu);sync_cv.wait(lock,[&]{return all_network_done;});}x.sync_barrier_ns=stamp()-t;
    x.sync_started=stamp();if(::fdatasync(fd))throw std::runtime_error("Load output sync failed");x.write_ns+=stamp()-x.sync_started;::close(fd);fd=-1;
   }catch(const std::exception& e){if(fd>=0)::close(fd);std::lock_guard<std::mutex> lock(x.mu);x.error=e.what();x.cv.notify_all();}
  });
  producers.emplace_back([&,lane]{auto& x=*state[lane];
   try{
    dhwire::Config config;config.max_pending=concurrency/lanes+2;config.max_connections_per_endpoint=connections/lanes;config.max_message_bytes=message;config.timeout_ms=get<uint64_t>(opts,"timeout_ms");dhwire::Client client(config);
    size_t window=concurrency/lanes+(lane<concurrency%lanes);
    for(size_t job=lane;job<count;job+=lanes){
     {std::unique_lock<std::mutex> lock(x.mu);x.cv.wait(lock,[&]{return !x.error.empty()||x.active<window;});if(!x.error.empty())break;++x.active;}
     V::Bytes vector(dim*sizeof(double));std::memcpy(vector.data(),vectors+job*dim,vector.size());
     V request=M{{"version",uint64_t(1)},{"op","search"},{"encoding","float64_le"},{"query_id",ids[job]},{"vector",std::move(vector)}};auto start=stamp();
     bool accepted=client.submit(endpoint,request,[&,job,start](dhwire::Result r){
      V value=M{{"job",uint64_t(job)},{"client_start_ns",start},{"client_done_ns",r.done_ns},{"queued_ns",r.queued_ns},{"send_start_ns",r.send_start_ns},{"send_done_ns",r.send_done_ns},{"request_bytes",r.request_bytes},{"response_bytes",r.response_bytes},{"transport_ok",r.ok},{"transport_error",r.error},{"result",std::move(r.response)}};
      auto bytes=retained_extra_bytes(value)+sizeof(Item);auto received=stamp();value.map()["consumer_enqueued_ns"]=received;
      std::lock_guard<std::mutex> lock(x.mu);--x.active;++x.received;x.last=std::max(x.last,r.done_ns);
      if(x.queue.size()>=capacity||bytes>memory-x.used){x.error="Client result consumer capacity exceeded; run invalid";}
      else{x.used+=bytes;x.peak_bytes=std::max(x.peak_bytes,x.used);x.queue.push_back({std::move(value),bytes});x.peak=std::max(x.peak,x.queue.size());}x.cv.notify_all();
     });
     if(!accepted){std::lock_guard<std::mutex> lock(x.mu);--x.active;x.error="Client network admission rejected";break;}
    }
    {std::unique_lock<std::mutex> lock(x.mu);x.cv.wait(lock,[&]{return x.active==0;});}
    client.close();
   }catch(const std::exception& e){std::lock_guard<std::mutex> lock(x.mu);x.error=e.what();}
   {std::lock_guard<std::mutex> lock(x.mu);x.done=true;x.cv.notify_all();}
  });
 }
 for(auto& t:producers)t.join();
 {std::lock_guard<std::mutex> lock(sync_mu);all_network_done=true;}sync_cv.notify_all();
 for(auto& t:consumers)t.join();
 A summaries;uint64_t last=0,total=0;std::string error;
 for(auto& p:state){last=std::max(last,p->last);total+=p->received;if(!p->error.empty())error=p->error;summaries.emplace_back(M{{"received",uint64_t(p->received)},{"consumed",uint64_t(p->consumed)},{"peak_consumer_queue",uint64_t(p->peak)},{"peak_consumer_bytes",uint64_t(p->peak_bytes)},{"encode_ms",p->encode_ns/1e6},{"write_sync_ms",p->write_ns/1e6},{"output_sync_started_ns",p->sync_started},{"output_sync_barrier_ms",p->sync_barrier_ns/1e6},{"error",p->error}});}
 return M{{"count",uint64_t(count)},{"received",total},{"concurrency",uint64_t(concurrency)},{"offered_ns",offered},{"last_received_ns",last},{"all_consumed_synced_ns",stamp()},{"error",error},{"lanes",std::move(summaries)}};
}
}
