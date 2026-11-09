#include "demand_growth.hpp"
#include "value_utils.hpp"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <future>
#include <map>
#include <set>
#include <shared_mutex>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <openssl/sha.h>

namespace dhgrowth {
using namespace dhvalue;
static uint64_t now(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
static void need(bool b,const char* e){if(!b)throw std::runtime_error(e);}
static void io(int fd,void* data,size_t size,uint64_t offset,bool write){
  auto* p=static_cast<uint8_t*>(data);while(size){auto n=write ? ::pwrite(fd,p,size,offset) : ::pread(fd,p,size,offset);
    if(n<0&&errno==EINTR)continue;need(n>0,"Demand growth short file I/O");p+=n;size-=n;offset+=n;}
}
File::~File(){if(fd>=0)::close(fd);}

struct Cache::Impl {
  std::weak_ptr<dhsearch::ReplicaOverlay> core;
  V cfg;size_t source_id,source_bytes,dim,nodes,stride,width,capacity,top,limit,memory_limit,path_limit;
  std::string epoch,version,trigger;bool persist;
  mutable std::shared_mutex mu;
  struct LockStats {std::atomic<uint64_t> count{0},wait{0},hold{0};};
  mutable LockStats reads,writes;
  struct WriteLock {
    Impl& p;uint64_t began,held;std::unique_lock<std::shared_mutex> lock;
    explicit WriteLock(Impl& x):p(x),began(now()),lock(x.mu){held=now();}
    ~WriteLock(){const auto done=now();lock.unlock();++p.writes.count;p.writes.wait+=held-began;p.writes.hold+=done-held;}
  };
  // Append-only publication: readers traverse immutable nodes; reclamation is
  // at Cache destruction, after all query pins/owners and writer threads retire.
  struct Published {std::shared_ptr<Entry> entry;Published* next;};
  static constexpr size_t buckets=65536,filter_words=1024;
  std::unique_ptr<std::atomic<Published*>[]> directory;
  std::array<std::atomic<uint64_t>,filter_words> admitted{};
  bool indexed=false,pipeline=false;size_t queue_limit=2,queue_byte_limit=0,scan_ms=0,sample_every=0;
  static uint64_t hash(Id id){id^=id>>30;id*=0xbf58476d1ce4e5b9ULL;id^=id>>27;id*=0x94d049bb133111ebULL;return id^(id>>31);}
  void remember(Id id){auto h=hash(id);admitted[(h>>6)%filter_words].fetch_or(uint64_t(1)<<(h%64),std::memory_order_release);}
  bool maybe_cached(Id id)const{auto h=hash(id);return admitted[(h>>6)%filter_words].load(std::memory_order_acquire)&(uint64_t(1)<<(h%64));}
  std::shared_ptr<Entry> indexed_get(Id id)const{for(auto p=directory[hash(id)%buckets].load(std::memory_order_acquire);p;p=p->next)if(p->entry->id==id)return p->entry;return {};}
  void publish_index(const std::shared_ptr<Entry>& e){if(!indexed)return;auto& head=directory[hash(e->id)%buckets];auto p=new Published{e,head.load(std::memory_order_relaxed)};head.store(p,std::memory_order_release);}

  std::unordered_map<Id,std::shared_ptr<Entry>> cache,durable;
  struct Wait {Entries entries;Done done;};
  std::deque<Wait> waits;
  Read remote;dhonesided::CodeLookup stable;
  std::shared_ptr<File> file=std::make_shared<File>();uint64_t end=64;
  struct Batch {Entries seeds,records;V::Bytes payload;std::shared_ptr<dhsearch::ReplicaOverlay> bank;uint64_t offset=0,bytes=0,queued=0;};
  std::unordered_map<Id,std::shared_ptr<Entry>> pending; // private to maintenance, not search-visible
  std::deque<std::shared_ptr<Batch>> write_queue;
  std::thread worker,writer;std::mutex wake_mu,queue_mu;std::condition_variable cv,queue_cv;
  bool producer_done=false;std::atomic<bool> writer_failed{false};
  size_t queued_batches=0,queued_bytes=0;uint64_t reserved_end=64;
  std::atomic<uint64_t> queue_peak{0},queue_bytes_peak{0},queued_count{0},queue_wait_ns{0},prepare_ns{0},data_flush_ns{0},commit_flush_ns{0},publish_ns{0},scans{0};
  // Fixture-only injected barrier. Production never supplies a hook.
  std::function<void()> before_write;

  std::atomic<bool> dirty{false},closing{false},active{true};
  std::atomic<uint64_t> start{0},hits{0},misses{0},uses{0},durable_uses{0},admission_rejects{0},read_batches{0},read_bytes{0},read_errors{0},evictions{0},unused_evictions{0},coalesced{0};
  std::atomic<uint64_t> committed{64},batches{0},hot_vertices{0},connectors{0},write_ns{0},promote_ns{0},maintenance_reads{0},wait_peak{0},occupancy_peak{0};
  size_t inflight=0,promoting=0;std::string reason,error;A events;
  Impl(std::shared_ptr<dhsearch::ReplicaOverlay> c,V settings,size_t sb,size_t src,std::string ep,size_t d,size_t n,std::string path,std::function<void()> hook={})
      :core(c),cfg(std::move(settings)),source_id(src),source_bytes(sb),dim(d),nodes(n),stride(c->stride),
       width(8+c->code_bytes+c->extra_bytes+16+n),capacity(get<size_t>(cfg,"cache_records")),top(get<size_t>(cfg,"promotion_hot_vertices")),
       limit(size_t(get<double>(cfg,"growth_fraction")*sb)),memory_limit(get<size_t>(cfg,"cache_bytes")),path_limit(get<size_t>(cfg,"max_path_records")),
       epoch(std::move(ep)),version(c->version),trigger(get<std::string>(cfg,"promotion_trigger")),persist(get<bool>(cfg,"persist")) {
    need(capacity&&top&&top<=capacity&&limit>=64&&memory_limit>=capacity*stride,"Invalid demand growth capacity");
    need(trigger=="full_no_singleton"||trigger=="full_hot_batch","Invalid demand promotion trigger");
    before_write=std::move(hook);
    indexed=has(cfg,"indexed_directory")&&get<bool>(cfg,"indexed_directory");
    pipeline=has(cfg,"persistence_pipeline")&&get<bool>(cfg,"persistence_pipeline");
    queue_limit=has(cfg,"persistence_queue_batches")?get<size_t>(cfg,"persistence_queue_batches"):2;
    queue_byte_limit=has(cfg,"persistence_queue_bytes")?get<size_t>(cfg,"persistence_queue_bytes"):get<size_t>(cfg,"maintenance_bytes")*2;
    scan_ms=has(cfg,"maintenance_interval_ms")?get<size_t>(cfg,"maintenance_interval_ms"):0;
    sample_every=has(cfg,"lock_sample_every")?get<size_t>(cfg,"lock_sample_every"):0;
    need(queue_limit&&queue_byte_limit>=get<size_t>(cfg,"maintenance_bytes"),"Invalid persistence queue bounds");
    if(indexed){directory=std::make_unique<std::atomic<Published*>[]>(buckets);for(size_t i=0;i<buckets;++i)directory[i].store(nullptr);}
    for(auto& v:admitted)v.store(0);
    cache.reserve(capacity*2);durable.reserve(std::min(limit/stride,size_t(200000)));
    file->fd=::open(path.c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600);need(file->fd>=0,"Cannot open demand growth file");
    struct stat st{};need(!fstat(file->fd,&st)&&S_ISREG(st.st_mode),"Invalid demand growth file");
    if(st.st_size==0){
      uint64_t h[8]={0x314854574f524744ULL,stride,dim,nodes,source_bytes,limit,0,0};
      unsigned char hash[32];const auto identity=epoch+":"+version+":"+std::to_string(source_id)+":"+(has(cfg,"target_identity")?std::to_string(get<size_t>(cfg,"target_identity")):"fixture");SHA256(reinterpret_cast<const unsigned char*>(identity.data()),identity.size(),hash);std::memcpy(h+6,hash,16);
      io(file->fd,h,sizeof(h),0,true);need(!::fsync(file->fd),"Demand growth header fsync failed");
    }else recover();
    // Real storage reservation, not just an unchecked byte counter.
    need(::posix_fallocate(file->fd,0,limit)==0,"Cannot reserve demand growth SSD capacity");
    reserved_end=end;
    if(pipeline)writer=std::thread([this]{write_run();});
    worker=std::thread([this]{run();});
  }
  ~Impl(){close();if(indexed)for(size_t i=0;i<buckets;++i){auto p=directory[i].load();while(p){auto next=p->next;delete p;p=next;}}}
  std::shared_ptr<dhsearch::ReplicaOverlay> decode(const V::Bytes& bytes)const{
    auto c=core.lock();need(bool(c),"Replica retired");auto b=std::make_shared<dhsearch::ReplicaOverlay>();b->code_bytes=c->code_bytes;b->extra_bytes=c->extra_bytes;
    need(bytes.size()%width==0,"Demand code width mismatch");
    for(size_t off=0;off<bytes.size();off+=width){const auto* p=bytes.data()+off;Id id;std::memcpy(&id,p,8);p+=8;size_t row=b->locations.size();need(b->locations.emplace(id,row).second,"Duplicate demand code");
      b->codes.insert(b->codes.end(),p,p+b->code_bytes);p+=b->code_bytes;b->extra.insert(b->extra.end(),p,p+b->extra_bytes);p+=b->extra_bytes;
      for(size_t i=0;i<3;++i){float f;std::memcpy(&f,p,4);p+=4;need(std::isfinite(f),"Nonfinite demand factor");b->factors.push_back(f);}
      uint32_t center;std::memcpy(&center,p,4);p+=4;b->centers.push_back(center);std::vector<int> owners;
      for(size_t j=0;j<nodes;++j){need(p[j]<=1,"Invalid demand owners");if(p[j])owners.push_back(int(j));}need(!owners.empty(),"Demand record lacks owner");b->owners.push_back(std::move(owners));
    }return b;
  }
  std::vector<Id> neighbors(const uint8_t* raw)const{
    uint32_t n;std::memcpy(&n,raw+dim*4,4);need(dim*4+4+size_t(n)*8<=stride,"Invalid demand adjacency");
    std::vector<Id> ids(n);if(n)std::memcpy(ids.data(),raw+dim*4+4,n*8);return ids;
  }
  Entries unpack(const std::vector<Id>& ids,const dhwire::Result& r)const{
    need(r.ok,"Demand RDMA transport failed");need(get<bool>(r.response,"ok"),"Demand READ failed");const auto& v=at(r.response,"result");
    need(get<bool>(v,"accepted")&&get<std::string>(v,"epoch")==epoch&&get<std::vector<Id>>(v,"ids")==ids&&get<size_t>(v,"record_stride")==stride,"Demand READ identity mismatch");
    const auto& raw=std::get<V::Bytes>(at(v,"records").data);const auto& codes=std::get<V::Bytes>(at(v,"codes").data);
    need(raw.size()==ids.size()*stride,"Demand record length mismatch");auto b=decode(codes);Entries out;
    for(size_t i=0;i<ids.size();++i){auto e=std::make_shared<Entry>();e->id=ids[i];e->bytes.assign(raw.begin()+i*stride,raw.begin()+(i+1)*stride);e->codes=b;e->ready=true;
      need(b->locations.count(e->id)||(stable&&stable(e->id)),"Missing demand self code");for(auto id:neighbors(e->bytes.data()))need(b->locations.count(id)||(stable&&stable(id)),"Missing demand neighbor code");out.push_back(std::move(e));}
    return out;
  }
  size_t memory_locked()const{
    size_t bytes=0;std::set<const void*> banks;
    for(const auto& [id,e]:cache){bytes+=stride+e->path.size()*8+256;if(e->codes&&banks.insert(e->codes.get()).second)bytes+=e->codes->locations.size()*(width+96);}
    return bytes;
  }
  void reap_locked(Entries& retired){for(auto it=cache.begin();it!=cache.end();)if(it->second->promoted&&it->second.use_count()==1){retired.push_back(std::move(it->second));it=cache.erase(it);}else ++it;}
  void notify(){if(!dirty.exchange(true))cv.notify_one();}
  void event(const char* what,size_t count=0,size_t bytes=0){if(events.size()<get<size_t>(cfg,"max_events"))events.emplace_back(M{{"ns",now()},{"kind",what},{"records",uint64_t(count)},{"bytes",uint64_t(bytes)}});}
  void deliver(){
    std::deque<Wait> ready;{
      WriteLock lock(*this);
      for(auto it=waits.begin();it!=waits.end();){bool done=true;for(auto& e:it->entries)if(!e->ready&&cache.count(e->id))done=false;
        if(done){ready.push_back(std::move(*it));it=waits.erase(it);}else ++it;}
    }
    for(auto& w:ready){bool ok=true;for(auto& e:w.entries)ok&=e->ready;w.done(ok?std::move(w.entries):Entries{},ok?"":"shared_read_failed");}
  }
  void finish(const std::vector<Id>& ids,dhwire::Result r){
    Entries got,retired;std::string failure;try{got=unpack(ids,r);}catch(const std::exception& e){failure=e.what();++read_errors;}
    {
      WriteLock lock(*this);--inflight;
      if(failure.empty()){
        for(size_t i=0;i<ids.size();++i){auto e=cache.at(ids[i]);e->bytes=std::move(got[i]->bytes);e->codes=got[i]->codes;e->ready=true;read_bytes+=stride;}
        if(!got.empty())read_bytes+=got[0]->codes->locations.size()*width;
        // Admission reserves a worst-case dependency response, checked before READ.
        if(memory_locked()>memory_limit){failure="cache_memory_bound";++read_errors;}
      }
      if(!failure.empty()){error=failure;for(auto id:ids){auto it=cache.find(id);if(it!=cache.end()){it->second->ready=false;retired.push_back(std::move(it->second));cache.erase(it);}}}
    }
    deliver();notify();
  }
  void request(const std::vector<Id>& ids,const std::vector<std::vector<Id>>& paths,Read rd,dhonesided::CodeLookup lookup,Done done,std::shared_ptr<Cache> self){
    std::vector<Id> missing;Entries selected,retired;bool reject=false;
    {
      WriteLock lock(*this);reap_locked(retired);if(!remote){remote=std::move(rd);stable=std::move(lookup);}
      if(!start.load())start=now();
      if(!active||closing||waits.size()>=get<size_t>(cfg,"max_waiters")||ids.size()!=paths.size())reject=true;
      if(!reject){
        std::set<Id> unique(ids.begin(),ids.end());if(unique.size()!=ids.size())reject=true;
        for(auto id:ids)if(!cache.count(id)&&!durable.count(id))missing.push_back(id);
        size_t need_slots=cache.size()+missing.size()>capacity?cache.size()+missing.size()-capacity:0;
        struct Victim {Id id;uint64_t count,last;};std::vector<Victim> victims;
        for(const auto& [id,e]:cache){auto count=e->count.load(std::memory_order_relaxed);if(!unique.count(id)&&e->ready&&!e->promoting&&e.use_count()==1&&count<=1)victims.push_back({id,count,e->last.load(std::memory_order_relaxed)});}
        std::sort(victims.begin(),victims.end(),[](const auto& x,const auto& y){if(x.count!=y.count)return x.count<y.count;if(x.last!=y.last)return x.last<y.last;return x.id<y.id;});
        // Conservative bytes reserve: one full bounded response for each in-flight batch.
        const size_t response=get<size_t>(cfg,"read_batch_bytes");
        if(need_slots>victims.size()||(!missing.empty()&&(inflight>=get<size_t>(cfg,"max_inflight_batches")||memory_locked()+(inflight+1)*response>memory_limit)))reject=true;
        if(!reject){
          for(size_t i=0;i<need_slots;++i){auto e=cache.at(victims[i].id);unused_evictions+=e->count==0;++evictions;cache.erase(victims[i].id);retired.push_back(std::move(e));}
          for(size_t i=0;i<ids.size();++i){Id id=ids[i];auto p=durable.find(id);if(p!=durable.end()){selected.push_back(p->second);continue;}
            auto it=cache.find(id);if(it==cache.end()){auto e=std::make_shared<Entry>();e->id=id;e->path=paths[i];if(e->path.size()>path_limit)e->path.clear();e->promoting=pending.count(id)!=0;remember(id);cache.emplace(id,e);selected.push_back(e);++misses;}
            else {selected.push_back(it->second);if(!it->second->ready)++coalesced;else ++hits;}}
          if(!missing.empty()){++inflight;++read_batches;}
          waits.push_back({std::move(selected),std::move(done)});wait_peak=std::max(wait_peak.load(),uint64_t(waits.size()));occupancy_peak=std::max(occupancy_peak.load(),uint64_t(cache.size()));
        }
      }
    }
    if(reject){++admission_rejects;done({},"cache_admission_full_or_frozen");notify();return;}
    if(!missing.empty()){
      bool submitted=false;try{submitted=remote(missing,[self,ids=missing](dhwire::Result r){self->p_->finish(ids,std::move(r));},stable);}catch(const std::exception&){}
      if(!submitted){dhwire::Result r;r.error="cache_read_queue_full";finish(missing,std::move(r));}
    }deliver();
  }
  V::Bytes pack_codes(const Entries& records)const{
    auto c=core.lock();need(bool(c),"Replica retired");std::map<Id,std::pair<std::shared_ptr<dhsearch::ReplicaOverlay>,size_t>> rows;
    for(const auto& e:records){std::vector<uint8_t> raw(stride);read(e,raw.data(),stride);auto ids=neighbors(raw.data());ids.push_back(e->id);
      for(auto id:ids){if(stable&&stable(id))continue;const auto& b=e->codes;auto at=b->locations.find(id);need(at!=b->locations.end(),"Missing promotion code");rows.emplace(id,std::make_pair(b,at->second));}}
    V::Bytes out;out.reserve(rows.size()*width);
    auto append=[&](const void* p,size_t n){auto b=static_cast<const uint8_t*>(p);out.insert(out.end(),b,b+n);};
    for(const auto& [id,location]:rows){auto b=location.first;size_t r=location.second;append(&id,8);append(b->codes.data()+r*c->code_bytes,c->code_bytes);if(c->extra_bytes)append(b->extra.data()+r*c->extra_bytes,c->extra_bytes);append(b->factors.data()+r*3,12);append(&b->centers[r],4);
      std::vector<uint8_t> owners(nodes);for(int n:b->owners[r])owners.at(n)=1;append(owners.data(),owners.size());}return out;
  }
  void read(const std::shared_ptr<Entry>& e,void* out,size_t n)const{need(n==stride&&e->ready,"Invalid demand record read");if(e->file)io(e->file->fd,out,n,e->offset,false);else{need(e->bytes.size()==n,"Missing cache record bytes");std::memcpy(out,e->bytes.data(),n);}}
  Entries load_background(const std::vector<Id>& ids){
    Entries out;size_t batch=get<size_t>(cfg,"read_batch_records");for(size_t i=0;i<ids.size();i+=batch){
      need(!closing,"Demand maintenance closed");std::vector<Id> part(ids.begin()+i,ids.begin()+std::min(ids.size(),i+batch));
      auto promise=std::make_shared<std::promise<dhwire::Result>>();auto future=promise->get_future();
      need(remote(part,[promise](dhwire::Result r){promise->set_value(std::move(r));},stable),"Connector READ queue full");++maintenance_reads;
      need(future.wait_for(std::chrono::milliseconds(get<size_t>(cfg,"maintenance_timeout_ms")))==std::future_status::ready,"Connector READ timeout");
      auto got=unpack(part,future.get());out.insert(out.end(),got.begin(),got.end());
    }return out;
  }
  // A pending closure can be referenced only by the single preparation worker.
  // FIFO commits ensure a predecessor is durable before its dependent frame.
  bool planned(Id id)const{std::shared_lock<std::shared_mutex> lock(mu);return durable.count(id)||pending.count(id);}
  std::shared_ptr<Entry> background_get(Id id){std::shared_lock<std::shared_mutex> lock(mu);for(auto* map:{&durable,&pending,&cache}){auto it=map->find(id);if(it!=map->end()&&it->second->ready)return it->second;}return {};}
  std::shared_ptr<Batch> prepare(Entries seeds){
    auto c=core.lock();need(bool(c),"Replica retired");std::map<Id,std::shared_ptr<Entry>> picked;std::set<Id> missing;
    for(const auto& seed:seeds){need(!seed->path.empty()&&seed->path.back()==seed->id,"Missing durable discovery path");
      need(c->contains(seed->path.front())||planned(seed->path.front()),"Promotion path lacks durable root");
      for(auto id:seed->path)if(!c->contains(id)&&!planned(id)){auto e=background_get(id);if(e)picked.emplace(id,e);else missing.insert(id);}}
    need((picked.size()+missing.size())*stride<=get<size_t>(cfg,"maintenance_bytes"),"Promotion closure memory bound");
    auto fetched=load_background(std::vector<Id>(missing.begin(),missing.end()));for(auto& e:fetched)picked.emplace(e->id,e);
    std::vector<uint8_t> raw(stride);for(auto& seed:seeds)for(size_t i=1;i<seed->path.size();++i){auto parent=seed->path[i-1];
      if(c->contains(parent))io(c->fd,raw.data(),stride,c->header+c->locate(parent)*stride,false);
      else if(auto e=background_get(parent))read(e,raw.data(),stride);else read(picked.at(parent),raw.data(),stride);
      auto ns=neighbors(raw.data());need(std::find(ns.begin(),ns.end(),seed->path[i])!=ns.end(),"Invalid original promotion edge");}
    auto batch=std::make_shared<Batch>();batch->seeds=seeds;for(auto& [id,e]:picked)batch->records.push_back(e);auto codes=pack_codes(batch->records);
    auto& payload=batch->payload;auto append=[&](const void* p,size_t n){auto b=static_cast<const uint8_t*>(p);payload.insert(payload.end(),b,b+n);};
    uint64_t h[3]={batch->records.size(),codes.size(),seeds.size()};append(h,24);
    for(auto& e:batch->records){append(&e->id,8);read(e,raw.data(),stride);append(raw.data(),stride);}append(codes.data(),codes.size());
    for(auto& e:seeds){uint64_t n=e->path.size();append(&n,8);append(e->path.data(),n*8);}
    need(payload.size()<=get<size_t>(cfg,"maintenance_bytes"),"Promotion payload memory bound");
    batch->bytes=payload.size()+48;batch->offset=reserved_end;
    if(batch->bytes>limit-reserved_end){if(seeds.size()>1){seeds.pop_back();return prepare(std::move(seeds));}seal("beta_remaining_insufficient");return {};}
    batch->bank=decode(codes);return batch;
  }
  void write_batch(const std::shared_ptr<Batch>& b){
    if(before_write)before_write(); // deterministic stalled-writer test, never a runtime knob
    const auto written=now();uint64_t n=b->payload.size();io(file->fd,&n,8,b->offset,true);io(file->fd,b->payload.data(),n,b->offset+8,true);
    unsigned char sha[32];SHA256(b->payload.data(),n,sha);io(file->fd,sha,32,b->offset+8+n,true);
    auto t=now();need(!::fdatasync(file->fd),"Demand data flush failed");data_flush_ns+=now()-t;
    uint64_t marker=0x54494d4d4f434744ULL;io(file->fd,&marker,8,b->offset+8+n+32,true);
    t=now();need(!::fdatasync(file->fd),"Demand commit flush failed");commit_flush_ns+=now()-t;write_ns+=now()-written;
    Entries published,retired;for(size_t i=0;i<b->records.size();++i){auto e=std::make_shared<Entry>();e->id=b->records[i]->id;e->file=file;e->offset=b->offset+8+24+i*(8+stride)+8;e->codes=b->bank;e->ready=true;published.push_back(e);}
    t=now();{WriteLock lock(*this);need(end==b->offset,"Out of order demand commit");
      for(auto& e:published){need(durable.emplace(e->id,e).second,"Duplicate demand publication");publish_index(e);pending.erase(e->id);auto it=cache.find(e->id);if(it!=cache.end()){it->second->promoted=true;it->second->promoting=false;}}
      end+=b->bytes;committed=end;++batches;hot_vertices+=b->seeds.size();connectors+=b->records.size()-b->seeds.size();event("commit",b->records.size(),b->bytes);}
    publish_ns+=now()-t;
  }
  void failed(const std::string& why){WriteLock lock(*this);error=why;event("promotion_error");active=false;reason="promotion_error";writer_failed=true;}
  void complete_batch(const std::shared_ptr<Batch>& b){
    Entries retired;{WriteLock lock(*this);for(auto& e:b->records){auto it=cache.find(e->id);if(it!=cache.end())it->second->promoting=false;pending.erase(e->id);}--promoting;}
    {std::lock_guard<std::mutex> lock(queue_mu);--queued_batches;queued_bytes-=b->bytes;}queue_cv.notify_all();notify();
  }
  void write_run(){
    for(;;){std::shared_ptr<Batch> b;{std::unique_lock<std::mutex> lock(queue_mu);queue_cv.wait(lock,[this]{return producer_done||!write_queue.empty();});if(write_queue.empty())break;b=std::move(write_queue.front());write_queue.pop_front();}
      queue_wait_ns+=now()-b->queued;
      if(!writer_failed)try{write_batch(b);}catch(const std::exception& e){failed(e.what());}
      complete_batch(b);
    }
  }
  bool contains_durable(Id id)const{if(indexed)return bool(indexed_get(id));std::shared_lock<std::shared_mutex> lock(mu);return durable.count(id);}
  std::shared_ptr<Entry> acquire(Id id,bool count=true){
    if(indexed){if(auto e=indexed_get(id))return e;if((count&&!active)||!maybe_cached(id))return {};}
    static thread_local uint64_t samples=0;bool sample=sample_every&&++samples%sample_every==0;auto began=sample?now():0;
    std::shared_lock<std::shared_mutex> lock(mu);auto held=sample?now():0;std::shared_ptr<Entry> result;
    auto p=durable.find(id);if(p!=durable.end())result=p->second;
    else if(!count||active){auto it=cache.find(id);if(it!=cache.end()&&it->second->ready){result=it->second;if(count)++hits;}}
    if(sample){auto done=now();lock.unlock();++reads.count;reads.wait+=held-began;reads.hold+=done-held;}return result;
  }
  void seal(std::string why){active=false;{WriteLock lock(*this);if(reason.empty()){reason=std::move(why);event("seal");}}notify();}
  void run(){
    for(;;){
      {std::unique_lock<std::mutex> lock(wake_mu);
        if(scan_ms)cv.wait_for(lock,std::chrono::milliseconds(scan_ms),[this]{return closing.load();});
        else cv.wait_for(lock,std::chrono::milliseconds(20),[this]{return dirty.load()||closing.load();});dirty=false;}
      if(closing)break;
      if(active&&start&&get<double>(cfg,"training_seconds")>0&&(now()-start)/1e9>=get<double>(cfg,"training_seconds"))seal("training_deadline");
      if(pipeline){std::lock_guard<std::mutex> lock(queue_mu);if(queued_batches>=queue_limit||queued_bytes>queue_byte_limit-get<size_t>(cfg,"maintenance_bytes"))continue;}
      Entries seeds,retired;{
        WriteLock lock(*this);++scans;reap_locked(retired);
        if(!active){for(auto it=cache.begin();it!=cache.end();)if(it->second->ready&&!it->second->promoting&&it->second.use_count()==1){retired.push_back(std::move(it->second));it=cache.erase(it);}else ++it;continue;}
        if(!persist||(!pipeline&&promoting)||cache.size()<capacity)continue;
        struct Ranked {std::shared_ptr<Entry> entry;uint64_t count,last;};std::vector<Ranked> ranked;ranked.reserve(cache.size());bool strict=true;
        for(auto& [id,e]:cache){const auto count=e->count.load(std::memory_order_relaxed);if(!e->ready||count<2||e->promoted)strict=false;
          if(e->ready&&count>=2&&!e->promoted&&!e->promoting&&!e->path.empty())ranked.push_back({e,count,e->last.load(std::memory_order_relaxed)});}
        if(ranked.size()<top||(trigger=="full_no_singleton"&&!strict))continue;
        std::sort(ranked.begin(),ranked.end(),[](const Ranked& a,const Ranked& b){if(a.count!=b.count)return a.count>b.count;if(a.last!=b.last)return a.last>b.last;return a.entry->id<b.entry->id;});
        seeds.reserve(top);for(size_t i=0;i<top;++i)seeds.push_back(std::move(ranked[i].entry));++promoting;for(auto& e:seeds)e->promoting=true;
      }
      const auto begun=now();std::shared_ptr<Batch> batch;
      try{batch=prepare(seeds);}catch(const std::exception& e){failed(e.what());}prepare_ns+=now()-begun;
      {WriteLock lock(*this);for(auto& e:seeds)e->promoting=false;
        if(batch){for(auto& e:batch->records){pending.emplace(e->id,e);auto it=cache.find(e->id);if(it!=cache.end())it->second->promoting=true;}reserved_end+=batch->bytes;}else --promoting;}
      if(batch){
        batch->queued=now();{std::lock_guard<std::mutex> lock(queue_mu);++queued_batches;queued_bytes+=batch->bytes;queue_peak=std::max(queue_peak.load(),uint64_t(queued_batches));queue_bytes_peak=std::max(queue_bytes_peak.load(),uint64_t(queued_bytes));++queued_count;if(pipeline)write_queue.push_back(batch);}
        if(pipeline)queue_cv.notify_one();else {try{write_batch(batch);}catch(const std::exception& e){failed(e.what());}complete_batch(batch);}
        if(limit-reserved_end<stride+64)seal("beta_exhausted");
      }
      promote_ns+=now()-begun;notify();
    }
    {std::lock_guard<std::mutex> lock(queue_mu);producer_done=true;}queue_cv.notify_all();
  }
  void recover(){
    uint64_t h[8];io(file->fd,h,64,0,false);unsigned char identity_sha[32];auto identity=epoch+":"+version+":"+std::to_string(source_id)+":"+(has(cfg,"target_identity")?std::to_string(get<size_t>(cfg,"target_identity")):"fixture");SHA256(reinterpret_cast<const unsigned char*>(identity.data()),identity.size(),identity_sha);
    need(h[0]==0x314854574f524744ULL&&h[1]==stride&&h[2]==dim&&h[3]==nodes&&h[4]==source_bytes&&h[5]==limit&&!std::memcmp(h+6,identity_sha,16),"Demand file identity mismatch");
    struct stat st{};need(!fstat(file->fd,&st),"Demand recovery stat failed");
    while(end+8<=uint64_t(st.st_size)){
      uint64_t n;io(file->fd,&n,8,end,false);if(!n||n>get<size_t>(cfg,"maintenance_bytes")||n+48>uint64_t(st.st_size)-end)break;
      uint64_t marker;io(file->fd,&marker,8,end+8+n+32,false);if(marker!=0x54494d4d4f434744ULL)break;
      V::Bytes payload(n);io(file->fd,payload.data(),n,end+8,false);unsigned char expected[32],actual[32];io(file->fd,expected,32,end+8+n,false);SHA256(payload.data(),n,actual);need(!std::memcmp(expected,actual,32),"Corrupt committed demand batch");
      need(n>=24,"Invalid demand commit");uint64_t q[3];std::memcpy(q,payload.data(),24);need(q[0]<=(n-24)/(8+stride),"Invalid demand record count");size_t off=24+q[0]*(8+stride);need(q[1]<=n-off,"Invalid demand code count");
      auto bank=decode(V::Bytes(payload.begin()+off,payload.begin()+off+q[1]));for(size_t i=0;i<q[0];++i){auto e=std::make_shared<Entry>();std::memcpy(&e->id,payload.data()+24+i*(8+stride),8);e->offset=end+8+24+i*(8+stride)+8;e->file=file;e->codes=bank;e->ready=true;need(durable.emplace(e->id,e).second,"Duplicate durable demand ID");publish_index(e);}
      end+=n+48;++batches;hot_vertices+=q[2];connectors+=q[0]-q[2];
    }
    committed=end;need(!ftruncate(file->fd,end),"Cannot truncate uncommitted demand tail");
  }
  void close(){if(closing.exchange(true))return;active=false;cv.notify_all();if(worker.joinable())worker.join();if(writer.joinable())writer.join();}
  V metrics()const{
    std::shared_lock<std::shared_mutex> lock(mu);uint64_t hot=0,one=0,reading=0;for(const auto& [id,e]:cache){hot+=e->count>=2;one+=e->count==1;reading+=!e->ready;}
    return M{{"enabled",true},{"version",version},{"source",uint64_t(source_id)},{"learning",active.load()},{"reason",reason},{"error",error},{"trigger",trigger},{"persist",persist},
      {"cache_records",uint64_t(cache.size())},{"hot_records",hot},{"singleton_records",one},{"reading_records",reading},{"cache_bytes",uint64_t(memory_locked())},{"durable_records",uint64_t(durable.size())},
      {"cache_hits",hits.load()},{"cache_misses",misses.load()},{"expanded",uses.load()},{"durable_expanded",durable_uses.load()},{"rejected",admission_rejects.load()},{"read_batches",read_batches.load()},{"read_bytes",read_bytes.load()},{"read_errors",read_errors.load()},
      {"evictions",evictions.load()},{"unused_evictions",unused_evictions.load()},{"coalesced",coalesced.load()},{"committed_bytes",committed.load()},{"reserved_bytes",uint64_t(limit)},{"promotion_batches",batches.load()},{"hot_vertices",hot_vertices.load()},{"connector_vertices",connectors.load()},
      {"indexed_directory",indexed},{"persistence_pipeline",pipeline},{"maintenance_scans",scans.load()},
      {"read_lock_samples",reads.count.load()},{"read_lock_wait_ms",reads.wait.load()/1e6},{"read_lock_hold_ms",reads.hold.load()/1e6},
      {"write_lock_samples",writes.count.load()},{"write_lock_wait_ms",writes.wait.load()/1e6},{"write_lock_hold_ms",writes.hold.load()/1e6},
      {"prepare_ms",prepare_ns.load()/1e6},{"data_flush_ms",data_flush_ns.load()/1e6},{"commit_flush_ms",commit_flush_ns.load()/1e6},{"publish_ms",publish_ns.load()/1e6},
      {"persistence_queue_wait_ms",queue_wait_ns.load()/1e6},{"persistence_queue_peak",queue_peak.load()},{"persistence_queue_bytes_peak",queue_bytes_peak.load()},{"prepared_batches",queued_count.load()},
      {"write_ms",write_ns.load()/1e6},{"promotion_ms",promote_ns.load()/1e6},{"maintenance_read_batches",maintenance_reads.load()},{"waiters",uint64_t(waits.size())},{"waiter_peak",wait_peak.load()},{"occupancy_peak",occupancy_peak.load()},{"inflight",uint64_t(inflight)},{"promoting",promoting!=0},{"start_ns",start.load()},{"events",events}};
  }
};
Cache::Cache(std::shared_ptr<dhsearch::ReplicaOverlay> c,V cfg,size_t bytes,size_t source,std::string epoch,size_t dim,size_t nodes,std::string path,std::function<void()> hook)
  :p_(std::make_unique<Impl>(std::move(c),std::move(cfg),bytes,source,std::move(epoch),dim,nodes,std::move(path),std::move(hook))){}
Cache::~Cache()=default;
std::shared_ptr<Entry> Cache::acquire(Id id){return p_->acquire(id);}
bool Cache::contains_durable(Id id)const{return p_->contains_durable(id);}
bool Cache::can_request(const std::vector<Id>& ids)const{
  std::shared_lock<std::shared_mutex> lock(p_->mu);if(!p_->active||p_->closing||p_->waits.size()>=get<size_t>(p_->cfg,"max_waiters"))return false;
  size_t missing=0,victims=0;for(auto id:ids)missing+=!p_->cache.count(id)&&!p_->durable.count(id);
  for(auto& [id,e]:p_->cache)if(e->ready&&!e->promoting&&e.use_count()==1&&e->count<=1&&std::find(ids.begin(),ids.end(),id)==ids.end())++victims;
  return p_->cache.size()+missing<=p_->capacity+victims&&(!missing||(p_->inflight<get<size_t>(p_->cfg,"max_inflight_batches")&&p_->memory_locked()+(p_->inflight+1)*get<size_t>(p_->cfg,"read_batch_bytes")<=p_->memory_limit));
}
void Cache::request(const std::vector<Id>& ids,const std::vector<std::vector<Id>>& paths,Read r,dhonesided::CodeLookup lookup,Done done){p_->request(ids,paths,std::move(r),std::move(lookup),std::move(done),shared_from_this());}
void Cache::expanded(const std::shared_ptr<Entry>& e){auto count=++e->count;e->last=now();++p_->uses;if(e->file)++p_->durable_uses;else if(!p_->scan_ms||count==2)p_->notify();}
void Cache::read(const std::shared_ptr<Entry>& e,void* data,size_t n)const{p_->read(e,data,n);}
V Cache::metrics()const{return p_->metrics();}
void Cache::seal(const std::string& why){p_->seal(why);}
void Cache::close(){p_->close();}
bool Cache::learning()const{return p_->active.load();}
size_t Cache::source()const{return p_->source_id;}
std::string Cache::version()const{return p_->version;}

// Deterministic native fixture: exercises the production cache and journal with
// an in-memory transport. This is never used to report RDMA/performance results.
class Probe {
  std::shared_ptr<dhsearch::ReplicaOverlay> core=std::make_shared<dhsearch::ReplicaOverlay>();
  std::shared_ptr<Cache> cache;
  Read transport;Entries held;
  struct Barrier {std::mutex mutex;std::condition_variable cv;bool paused=false,entered=false;};
  std::shared_ptr<Barrier> barrier=std::make_shared<Barrier>();
public:
  Probe(pybind11::object config,const std::string& file,pybind11::object graph,const std::vector<Id>& roots,size_t bytes){
    auto value=dhwire::from_python(graph);auto edges=std::make_shared<std::map<Id,std::vector<Id>>>();
    for(const auto& [id,ns]:value.map())edges->emplace(std::stoull(id),cast<std::vector<Id>>(ns));
    core->version="unit";core->stride=4096;core->code_bytes=1;core->degree=500;
    core->fd=::open((file+".core").c_str(),O_RDWR|O_CREAT|O_TRUNC|O_CLOEXEC,0600);need(core->fd>=0,"Cannot create fixture core");
    auto raw=[edges](Id id){V::Bytes out(4096);float f=float(id);auto& ns=edges->at(id);uint32_t n=ns.size();need(n<=500,"Fixture degree");std::memcpy(out.data(),&f,4);std::memcpy(out.data()+4,&n,4);if(n)std::memcpy(out.data()+8,ns.data(),n*8);return out;};
    for(auto id:roots){size_t row=core->records.size();core->records.emplace(id,row);auto data=raw(id);io(core->fd,data.data(),data.size(),row*4096,true);}
    cache=std::make_shared<Cache>(core,dhwire::from_python(config),bytes,0,"unit_epoch",1,1,file,[b=barrier]{std::unique_lock<std::mutex> lock(b->mutex);b->entered=true;b->cv.notify_all();b->cv.wait(lock,[&]{return !b->paused;});});
    transport=[raw,edges](const std::vector<Id>& ids,dhonesided::Complete done,dhonesided::CodeLookup lookup){
      dhwire::Result r;try{V::Bytes data,codes;std::set<Id> all;
        for(auto id:ids){auto bytes=raw(id);data.insert(data.end(),bytes.begin(),bytes.end());all.insert(id);all.insert(edges->at(id).begin(),edges->at(id).end());}
        for(auto id:all){if(lookup&&lookup(id))continue;size_t off=codes.size();codes.resize(off+26);auto p=codes.data()+off;std::memcpy(p,&id,8);p[8]=0;float factors[3]={1,1,1};std::memcpy(p+9,factors,12);uint32_t center=0;std::memcpy(p+21,&center,4);p[25]=1;}
        r.ok=true;r.response=M{{"ok",true},{"result",M{{"accepted",true},{"epoch","unit_epoch"},{"ids",vector(ids)},{"record_stride",uint64_t(4096)},{"records",std::move(data)},{"codes",std::move(codes)}}}};
      }catch(const std::exception& e){r.error=e.what();}done(std::move(r));return true;
    };
  }
  ~Probe(){pause_writer(false);cache->close();}
  void pause_writer(bool pause){std::lock_guard<std::mutex> lock(barrier->mutex);barrier->paused=pause;barrier->entered=false;barrier->cv.notify_all();}
  bool writer_entered(){std::lock_guard<std::mutex> lock(barrier->mutex);return barrier->entered;}
  void close(){pybind11::gil_scoped_release release;cache->close();}
  bool request(const std::vector<Id>& ids,const std::vector<std::vector<Id>>& paths){
    pybind11::gil_scoped_release release;std::promise<bool> p;auto f=p.get_future();cache->request(ids,paths,transport,[](Id){return 0;},[&p](Entries e,std::string error){p.set_value(error.empty()&&!e.empty());});return f.get();
  }
  void touch(const std::vector<Id>& ids,size_t times){pybind11::gil_scoped_release release;for(auto id:ids){auto e=cache->acquire(id);need(bool(e),"Fixture missing record");for(size_t i=0;i<times;++i)cache->expanded(e);}}
  bool contains(Id id){return bool(cache->acquire(id));}
  pybind11::bytes record(Id id){auto e=cache->acquire(id);need(bool(e),"Fixture missing record");std::vector<char> data(4096);cache->read(e,data.data(),data.size());return pybind11::bytes(data.data(),data.size());}
  void hold(Id id){auto e=cache->acquire(id);need(bool(e),"Fixture missing pin");held.push_back(e);}
  void release(){held.clear();}
  pybind11::object metrics(){return to_python(cache->metrics());}
  void seal(){cache->seal();}
};
void bind(pybind11::module_& m){
  pybind11::class_<Probe>(m,"DemandGrowthTestStore")
    .def(pybind11::init<pybind11::object,const std::string&,pybind11::object,const std::vector<Id>&,size_t>())
    .def("request",&Probe::request).def("touch",&Probe::touch).def("contains",&Probe::contains).def("record",&Probe::record)
    .def("pause_writer",&Probe::pause_writer).def("writer_entered",&Probe::writer_entered).def("close",&Probe::close).def("hold",&Probe::hold).def("release",&Probe::release).def("metrics",&Probe::metrics).def("seal",&Probe::seal);
}
}
