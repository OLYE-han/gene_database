#include "entry_routing.hpp"
#include <cassert>
#include <iostream>
#include <thread>
using namespace dhentry;
V options(const std::string& mode){return M{{"mode",mode},{"telemetry",true},{"poll_ms",uint64_t(20)},{"ttl_ms",uint64_t(100)}};}
V load(uint64_t q,uint64_t tick=1000000000){return M{{"sampled_ns",tick},{"cpu_ns",uint64_t(0)},{"wall_ns",uint64_t(0)},
 {"completed",uint64_t(0)},{"workers",uint64_t(4)},{"active",uint64_t(0)},{"queued",q}};}
int main(){
 Policy p;p.init(3,options("least_queue"));uint64_t now=1000000000;
 p.update(0,load(10),now,1);p.update(1,load(0),now,1);p.update(2,load(0),now,1);
 std::vector<Candidate> c{{0,p.sources[0]},{1,p.sources[1]}};
 assert(p.choose(c,now,.9).index==1); // No unrelated idle node2 in candidates.
 p.update(0,load(0),now,1);
 assert(p.choose(c,now,.1).index==0&&p.choose(c,now,.9).index==1);
 assert(p.choose(c,now+101000000,.1).stale);
 p.update(0,load(4),now+102000000,1);
 assert(p.choose(c,now+102000000,.9).index==0); // Expired replica excluded.
 p.update(1,load(0),now+102000000,1);now+=102000000;
 p.configure(options("cost"));std::vector<std::shared_ptr<Ticket>> tickets;
 for(int k=0;k<100;++k)tickets.push_back(std::make_shared<Ticket>(p.nodes[1]));
 assert(p.choose(c,now,.9).index==0);tickets.clear();assert(p.nodes[1]->outstanding==0);
 // A replica that creates expensive downstream work can be inferior despite no queue.
 p.sources[1]->samples=10;p.sources[1]->nodes[0]->visits=10;p.sources[1]->nodes[0]->cpu_ns=1000000000;
 assert(p.choose(c,now,.9).index==0);
 bool bad=false;try{p.configure(options("invalid"));}catch(const std::invalid_argument&){bad=true;}assert(bad);
 // Concurrent reservation lifetime is independent of immutable stats publication.
 std::vector<std::thread> threads;for(int k=0;k<16;++k)threads.emplace_back([&]{for(int j=0;j<10000;++j){Ticket t(p.nodes[0]);}});
 for(auto& t:threads)t.join();assert(p.nodes[0]->outstanding==0);
 std::cout<<"entry routing selector, freshness, downstream cost, lifetime tests passed\n";
}
