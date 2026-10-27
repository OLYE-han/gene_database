#pragma once
#include "value_utils.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace dhsearch {
// Producers own their maps; only immutable batches cross an SPSC queue.
// The single consumer maintains weighted Space-Saving in a common decay epoch.
// Scores are exact until eviction; score-error is a conservative lower bound
// for the delivered stream. Queue drops are counted, never hidden.
class DecayedHotness {
public:
  using V=dhwire::Value;
  struct Item {uint64_t id,last;double weight;};
  struct Batch {std::vector<Item> items;uint64_t events=0;};
  static uint64_t clock(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
  struct Lane {
    const size_t capacity,queue_size;const uint64_t interval;const double lambda;
    std::unordered_map<uint64_t,Item> local;
    std::vector<std::unique_ptr<Batch>> queue;
    alignas(64) std::atomic<uint64_t> write{0};
    alignas(64) std::atomic<uint64_t> read{0};
    std::atomic<uint64_t> recorded{0},submitted{0},dropped{0},flushes{0};
    uint64_t pending=0,last_flush=clock();
    Lane(size_t c,size_t q,uint64_t i,double l):capacity(c),queue_size(q),interval(i),lambda(l),queue(q){local.reserve(c);}
    void flush(){
      if(local.empty())return;
      auto w=write.load(std::memory_order_relaxed);
      if(w-read.load(std::memory_order_acquire)==queue_size){dropped.fetch_add(pending,std::memory_order_relaxed);}
      else {
        auto b=std::make_unique<Batch>();b->events=pending;b->items.reserve(local.size());
        for(auto& [id,item]:local)b->items.push_back(item);
        queue[w%queue_size]=std::move(b);submitted.fetch_add(pending,std::memory_order_relaxed);
        write.store(w+1,std::memory_order_release);
      }
      local.clear();pending=0;last_flush=clock();flushes.fetch_add(1,std::memory_order_relaxed);
    }
    void record(const std::vector<uint64_t>& ids,uint64_t now){
      for(auto id:ids){
        auto it=local.find(id);
        if(it==local.end()){
          if(local.size()==capacity)flush();
          local.emplace(id,Item{id,now,1.});
        }else {auto& x=it->second;x.weight=x.weight*std::exp(-lambda*double(now-x.last))+1.;x.last=now;}
        ++pending;
      }
      recorded.fetch_add(ids.size(),std::memory_order_relaxed);
      if(now>=last_flush&&now-last_flush>=interval)flush();
    }
    std::unique_ptr<Batch> pop(){
      auto r=read.load(std::memory_order_relaxed);
      if(r==write.load(std::memory_order_acquire))return {};
      auto out=std::move(queue[r%queue_size]);read.store(r+1,std::memory_order_release);return out;
    }
  };
private:
  struct Entry {uint64_t id;double score,error;};
  size_t local_capacity_,queue_size_,capacity_,topk_,max_lanes_;
  uint64_t interval_,epoch_,merged_=0,updates_=0,evictions_=0,cycles_=0;
  double lambda_,min_score_,merge_cpu_ms_=0;
  std::vector<Entry> heap_;std::unordered_map<uint64_t,size_t> positions_;
  std::mutex lanes_mu_,wait_mu_;std::condition_variable wake_;
  std::vector<std::shared_ptr<Lane>> lanes_;
  std::atomic<uint64_t> lane_rejections_{0};
  std::atomic<bool> stop_{false};std::thread thread_;
  std::shared_ptr<const V> snapshot_=std::make_shared<const V>(dhvalue::M{});
  const uint64_t identity_;inline static std::atomic<uint64_t> next_identity_{1};
  struct Cache{uint64_t id=0;std::weak_ptr<Lane> lane;};
  static Cache& cache(){static thread_local Cache value;return value;}
  bool less(size_t a,size_t b)const{return heap_[a].score<heap_[b].score||(heap_[a].score==heap_[b].score&&heap_[a].id<heap_[b].id);}
  void exchange(size_t a,size_t b){std::swap(heap_[a],heap_[b]);positions_[heap_[a].id]=a;positions_[heap_[b].id]=b;}
  void down(size_t i){for(;;){size_t j=i,l=2*i+1,r=l+1;if(l<heap_.size()&&less(l,j))j=l;if(r<heap_.size()&&less(r,j))j=r;if(j==i)return;exchange(i,j);i=j;}}
  void up(size_t i){while(i){auto p=(i-1)/2;if(!less(i,p))break;exchange(i,p);i=p;}}
  void add(const Item& item){
    double weight=item.weight*std::exp(lambda_*(double(item.last)-double(epoch_)));
    auto found=positions_.find(item.id);
    if(found!=positions_.end()){auto i=found->second;heap_[i].score+=weight;down(i);}
    else if(heap_.size()<capacity_){auto i=heap_.size();heap_.push_back({item.id,weight,0});positions_[item.id]=i;up(i);}
    else{double error=heap_[0].score;positions_.erase(heap_[0].id);heap_[0]={item.id,error+weight,error};positions_[item.id]=0;down(0);++evictions_;}
    ++updates_;
  }
  void merge(){
    timespec cpu{};clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu);double begin=cpu.tv_sec*1000.+cpu.tv_nsec/1e6;
    uint64_t now=clock();
    if(lambda_*double(now-epoch_)>32.){double f=std::exp(-lambda_*double(now-epoch_));for(auto& e:heap_){e.score*=f;e.error*=f;}epoch_=now;}
    std::vector<std::shared_ptr<Lane>> lanes;{std::lock_guard<std::mutex> l(lanes_mu_);lanes=lanes_;}
    uint64_t recorded=0,submitted=0,dropped=0,queued=0,flushes=0;
    for(auto& lane:lanes){
      // Bounded per cycle: a busy producer cannot starve other lanes.
      for(size_t i=0;i<queue_size_;++i){auto b=lane->pop();if(!b)break;for(auto& item:b->items)add(item);merged_+=b->events;}
      recorded+=lane->recorded.load();submitted+=lane->submitted.load();dropped+=lane->dropped.load();
      auto w=lane->write.load(),r=lane->read.load();queued+=w>=r?w-r:0;flushes+=lane->flushes.load();
    }
    now=clock();double decay=std::exp(-lambda_*double(now-epoch_));
    auto top=heap_;auto order=[](const Entry&a,const Entry&b){return a.score>b.score||(a.score==b.score&&a.id<b.id);};
    if(top.size()>topk_){std::nth_element(top.begin(),top.begin()+topk_,top.end(),order);top.resize(topk_);}std::sort(top.begin(),top.end(),order);
    dhvalue::A rows;for(auto& e:top)if(e.score*decay>=min_score_)rows.emplace_back(dhvalue::M{{"id",e.id},{"score",e.score*decay},{"error",e.error*decay},{"lower_bound",std::max(0.,(e.score-e.error)*decay)}});
    clock_gettime(CLOCK_THREAD_CPUTIME_ID,&cpu);merge_cpu_ms_+=cpu.tv_sec*1000.+cpu.tv_nsec/1e6-begin;
    V value=dhvalue::M{{"time_ns",now},{"lanes",uint64_t(lanes.size())},{"candidates",uint64_t(heap_.size())},
      {"capacity",uint64_t(capacity_)},{"top_k_limit",uint64_t(topk_)},{"events_recorded",recorded},{"events_submitted",submitted},
      {"events_merged",merged_},{"events_dropped",dropped},{"queued_batches",queued},{"flushes",flushes},
      {"merge_updates",updates_},{"evictions",evictions_},{"merge_cycles",++cycles_},{"merge_cpu_ms",merge_cpu_ms_},
      {"lane_rejections",lane_rejections_.load()},{"top",std::move(rows)},{"counting","successful_expansion_per_query_physical_node"},
      {"approximation","exponentially_decayed_weighted_space_saving"}};
    std::atomic_store(&snapshot_,std::make_shared<const V>(std::move(value)));
  }
  void run(){
    for(;;){merge();std::unique_lock<std::mutex> l(wait_mu_);if(wake_.wait_for(l,std::chrono::nanoseconds(interval_),[&]{return stop_.load();}))break;}
    merge();
  }
public:
  explicit DecayedHotness(const V& c):local_capacity_(dhvalue::get<size_t>(c,"local_capacity")),queue_size_(dhvalue::get<size_t>(c,"queue_batches")),
      capacity_(dhvalue::get<size_t>(c,"max_candidates")),topk_(dhvalue::get<size_t>(c,"top_k")),max_lanes_(dhvalue::get<size_t>(c,"max_lanes")),
      interval_(uint64_t(dhvalue::get<double>(c,"merge_interval_ms")*1e6)),epoch_(clock()),
      lambda_(std::log(2.)/(dhvalue::get<double>(c,"half_life_ms")*1e6)),min_score_(dhvalue::get<double>(c,"min_score")),identity_(next_identity_.fetch_add(1)){
    if(!local_capacity_||!queue_size_||!capacity_||!topk_||topk_>capacity_||!max_lanes_||!interval_||!std::isfinite(lambda_)||lambda_<=0||!std::isfinite(min_score_)||min_score_<0)
      throw std::invalid_argument("Invalid decayed hotness configuration");
    heap_.reserve(capacity_);positions_.reserve(capacity_);thread_=std::thread([this]{run();});
  }
  ~DecayedHotness(){close();}
  void close(){if(!stop_.exchange(true)){wake_.notify_all();if(thread_.joinable())thread_.join();}}
  std::shared_ptr<Lane> lane(){
    auto& c=cache();if(c.id==identity_)if(auto p=c.lane.lock())return p;
    std::lock_guard<std::mutex> l(lanes_mu_);if(lanes_.size()>=max_lanes_){++lane_rejections_;return {};}
    auto out=std::make_shared<Lane>(local_capacity_,queue_size_,interval_,lambda_);lanes_.push_back(out);c={identity_,out};return out;
  }
  static void flush_current_thread(){if(auto l=cache().lane.lock())l->flush();}
  V snapshot()const{return *std::atomic_load(&snapshot_);}
  V metrics()const{dhvalue::M out;auto s=std::atomic_load(&snapshot_);for(const auto& [k,v]:s->map())if(k!="top")out[k]=v;return out;}
};
} // namespace dhsearch
