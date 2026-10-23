#include "native_result_sink.hpp"
#include "retained_size.hpp"
#include "value_utils.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

namespace dhruntime {
using M=V::Map;using A=V::Array;
using namespace dhvalue;
static void check(bool ok,const char* message){if(!ok)throw std::invalid_argument(message);}
static uint64_t now_ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
struct File {
  int fd=-1;
  explicit File(const std::string& p){fd=::open(p.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);if(fd<0)throw std::system_error(errno,std::generic_category(),p);}
  ~File(){if(fd>=0)::close(fd);}
  void write(const std::vector<uint8_t>& b){size_t n=0;while(n<b.size()){auto r=::write(fd,b.data()+n,b.size()-n);if(r<0&&errno==EINTR)continue;if(r<=0)throw std::system_error(r<0?errno:EIO,std::generic_category(),"result write");n+=size_t(r);}}
  void sync(){int r;do{r=::fdatasync(fd);}while(r<0&&errno==EINTR);if(r<0)throw std::system_error(errno,std::generic_category(),"result fdatasync");}
};
static void u64(std::vector<uint8_t>& b,uint64_t v){for(int i=7;i>=0;--i)b.push_back(uint8_t(v>>(i*8)));}
static void frame(std::vector<uint8_t>& b,const V& meta,const std::vector<uint8_t>& payload){auto m=dhwire::pack(meta);u64(b,m.size());u64(b,payload.size());b.insert(b.end(),m.begin(),m.end());b.insert(b.end(),payload.begin(),payload.end());}
V run_disk_queries(HeadRuntime& runtime,const double* vectors,const uint64_t* ids,
 size_t count,size_t dim,size_t concurrency,const std::string& directory,const V& settings,size_t max_message) {
 const size_t writers=get<size_t>(settings,"writers"),batch=get<size_t>(settings,"batch_records"),limit=get<size_t>(settings,"batch_bytes");
 const uint64_t budget=optional<uint64_t>(settings,"max_retained_bytes",uint64_t(8)<<30);
 check(count&&concurrency&&writers&&writers<=64&&batch&&batch<=4096&&limit&&budget,"Invalid result sink limits");
 const bool batch_sync=get<std::string>(settings,"completion")=="fdatasync";
 check(batch_sync||get<std::string>(settings,"completion")=="write","Invalid result output completion mode");
 // Preflight metadata before allocating and before admitting a single query.
 constexpr uint64_t slot_bytes=sizeof(V)+2*sizeof(uint64_t);
 check(count<=budget/slot_bytes,"Native result retention budget exceeded before retrieval");
 check(std::filesystem::create_directory(directory),"Result directory already exists");
 std::vector<V> retained(count);
 std::vector<uint64_t> starts(count),retained_at(count);
 uint64_t retained_bytes=retained.capacity()*sizeof(V)+(starts.capacity()+retained_at.capacity())*sizeof(uint64_t);
 check(retained_bytes<=budget,"Native result retention budget exceeded before retrieval");
 std::mutex mu;std::condition_variable cv;
 size_t submitted=0,completed=0;std::string error;
 uint64_t last=0;
 const std::string prefix="disk-"+std::to_string(::getpid())+"-"+std::to_string(now_ns())+"-";
 const uint64_t offered=now_ns();
 // No writer, serializer, Python consumer or output queue exists in this phase.
 // Head merge has already released its active slot when this callback runs.
 for(size_t job=0;job<count;++job){
  {
   std::unique_lock<std::mutex> l(mu);
   cv.wait(l,[&]{return !error.empty()||submitted-completed<concurrency;});
   if(!error.empty())break;
   starts[job]=now_ns();++submitted;
  }
  try {
   runtime.submit(std::vector<double>(vectors+job*dim,vectors+(job+1)*dim),V(ids[job]),prefix+std::to_string(job),
    [&,job](V result){
     // Count retained allocations without encoding; this bookkeeping can use
     // CPU but cannot wait for file I/O. Callback must not throw into the pool.
     uint64_t bytes=retained_extra_bytes(result);
     uint64_t done=get<uint64_t>(result,"retrieval_done_ns");
     std::lock_guard<std::mutex> l(mu);
     last=std::max(last,done);
     if(error.empty()) {
      if(bytes>budget-retained_bytes)error="Native result retention budget exceeded; run invalid (no output backpressure)";
      else {retained[job]=std::move(result);retained_bytes+=bytes;retained_at[job]=now_ns();}
     }
     ++completed;
     // Notify while holding the mutex. No reference to driver state may be
     // touched after unlocking: the final notification can end this phase.
     cv.notify_all();
    });
  }catch(const std::exception& e){
   std::lock_guard<std::mutex> l(mu);--submitted;if(error.empty())error=e.what();break;
  }
 }
 {
  // On failure drain all already accepted native callbacks before destroying
  // their captured storage. Do not publish a partial run as successful.
  std::unique_lock<std::mutex> l(mu);cv.wait(l,[&]{return completed==submitted;});
  if(!error.empty())throw std::runtime_error(error);
 }
 check(completed==count,"Incomplete native retrieval run");
 const uint64_t retained_done=now_ns();

 // Postprocessing starts only AFTER every query has its retrieval completion.
 // Disjoint file ownership; writer count cannot control retrieval admission.
 std::vector<std::thread> threads;std::vector<V> stats(writers);
 std::atomic<bool> abort{false};
 auto fail=[&](const std::string& e){std::lock_guard<std::mutex> l(mu);if(error.empty())error=e;abort=true;};
 const uint64_t output_start=now_ns();
 try {
  for(size_t w=0;w<writers;++w)threads.emplace_back([&,w]{
   try {
    std::string base=directory+"/writer-"+std::to_string(w);
    File data(base+".frames"),journal(base+".commits");
    uint64_t records=0,bytes=0,batches=0,write_ns=0,encode_ns=0,last_done=0,sync_ns=0;
    std::vector<uint8_t> buffer;size_t buffered=0;
    auto flush=[&]{
     if(!buffered)return;
     uint64_t begin=now_ns();data.write(buffer);uint64_t written=now_ns();
     if(batch_sync)data.sync();
     uint64_t done=now_ns(),completion=batch_sync?done:written;
     auto commit=dhwire::pack(M{{"batch",batches},{"records",uint64_t(buffered)},{"bytes",uint64_t(buffer.size())},
        {"write_start_ns",begin},{"write_done_ns",written},{"sync_done_ns",done},{"completion_ns",completion}});
     std::vector<uint8_t> entry;u64(entry,commit.size());entry.insert(entry.end(),commit.begin(),commit.end());journal.write(entry);
     records+=buffered;bytes+=buffer.size();++batches;write_ns+=written-begin;sync_ns+=done-written;last_done=completion;
     buffer.clear();buffered=0;
    };
    for(size_t job=w;job<count&&!abort;job+=writers){
     auto begin=now_ns();auto payload=dhwire::pack(retained[job],max_message);auto end=now_ns();encode_ns+=end-begin;
     if(buffered&&buffer.size()+payload.size()+1024>limit)flush();
     frame(buffer,M{{"job",uint64_t(job)},{"start_ns",starts[job]},{"retained_ns",retained_at[job]},
        {"batch",batches},{"writer",uint64_t(w)},{"encoding_start_ns",begin},{"encoding_done_ns",end}},payload);
     retained[job]=V();
     if(++buffered>=batch)flush();
    }
    if(abort)return;
    flush();
    uint64_t sync_begin=now_ns();data.sync();journal.sync();auto sync_end=now_ns();
    stats[w]=M{{"writer",uint64_t(w)},{"records",records},{"bytes",bytes},{"batches",batches},{"last_completion_ns",last_done},
      {"write_ms",write_ns/1e6},{"encode_ms",encode_ns/1e6},{"batch_sync_ms",sync_ns/1e6},
      {"final_sync_start_ns",sync_begin},{"final_sync_done_ns",sync_end},{"final_sync_ms",(sync_end-sync_begin)/1e6}};
   }catch(const std::exception& e){fail(e.what());}
  });
 }catch(const std::exception& e){fail(e.what());}
 for(auto& t:threads)if(t.joinable())t.join();
 if(!error.empty())throw std::runtime_error("Native deferred result output failed: "+error);
 int d=::open(directory.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(d<0)throw std::system_error(errno,std::generic_category(),"open result directory");
 int r=::fsync(d);int err=errno;::close(d);if(r<0)throw std::system_error(err,std::generic_category(),"sync result directory");
 uint64_t output_last=output_start,total_bytes=0,total_records=0;A output;
 for(auto& s:stats){output_last=std::max(output_last,get<uint64_t>(s,"last_completion_ns"));total_bytes+=get<uint64_t>(s,"bytes");total_records+=get<uint64_t>(s,"records");output.push_back(std::move(s));}
 check(total_records==count,"Incomplete deferred output run");
 return M{{"format","dhdgs-native-frames-v1"},{"count",uint64_t(count)},{"concurrency",uint64_t(concurrency)},
    {"offered_ns",offered},{"last_completion_ns",last},{"all_retained_ns",retained_done},
    {"output_started_ns",output_start},{"last_output_completion_ns",output_last},{"all_synced_ns",now_ns()},{"bytes",total_bytes},
    {"completion","native_ready"},{"output_completion",batch_sync?"fdatasync":"write"},{"output_mode","deferred"},
    {"max_retained_bytes",budget},{"peak_retained_bytes_estimate",retained_bytes},
    {"retention_accounting","native_value_capacity_estimate_not_process_rss"},{"output_backpressure",false},
    {"writers",std::move(output)},{"python_per_query_consumer",false},{"all_synced",true},{"directory",directory}};
}
}
