#pragma once
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstring>
#include <limits>

// Immutable source-local path directory. No original graph edges are changed.
namespace dhgeometry {
using U32=uint32_t;using Id=uint64_t;
constexpr U32 absent=std::numeric_limits<U32>::max();
struct View {
 const uint8_t* data;size_t bytes,dim,stride,header,n,maxdeg;
 const Id* ids;std::unordered_map<Id,U32> row;
 View(int fd,const Array<Id>& source,const py::dict& meta):ids(source.data()){
  n=source.size();require(n&&n<absent&&source.ndim()==1,"Geometry source dimensions");
  dim=meta["dimension"].cast<size_t>();stride=meta["record_bytes"].cast<size_t>();header=meta["header_bytes"].cast<size_t>();maxdeg=meta["max_degree"].cast<size_t>();
  require(dim*4+4+maxdeg*8<=stride,"Geometry record layout");
  struct stat s;if(fstat(fd,&s))throw std::runtime_error("Geometry fstat");bytes=s.st_size;
  require(bytes==header+n*stride,"Geometry source size");
  auto ptr=mmap(nullptr,bytes,PROT_READ,MAP_SHARED,fd,0);if(ptr==MAP_FAILED)throw std::runtime_error("Geometry mmap");data=static_cast<const uint8_t*>(ptr);
 }
 ~View(){munmap(const_cast<uint8_t*>(data),bytes);}
 void index(){row.reserve(n);for(size_t i=0;i<n;++i)if(!row.emplace(ids[i],U32(i)).second)throw std::invalid_argument("Duplicate geometry source ID");}
 template<class F>void edges(U32 r,F f)const{
  if(r>=n)throw std::invalid_argument("Geometry invalid row");
  const auto p=data+header+size_t(r)*stride+4*dim;U32 d;std::memcpy(&d,p,4);
  if(d>maxdeg)throw std::invalid_argument("Geometry invalid degree");
  for(U32 j=0;j<d;++j){Id id;std::memcpy(&id,p+4+8*j,8);f(id);}
 }
 bool edge(U32 a,U32 b)const{bool found=false;edges(a,[&](Id v){found|=v==ids[b];});return found;}
};
inline py::dict trees(int fd,Array<Id> source,py::dict meta,U32 root){
 View v(fd,source,meta);require(root<v.n,"Geometry root row");
 Array<U32> forward(v.n),reverse(v.n),incoming(v.n),outgoing(v.n);
 auto f=forward.mutable_data(),r=reverse.mutable_data(),in=incoming.mutable_data(),out=outgoing.mutable_data();size_t nf=0,nr=0,edges=0;
 {
 py::gil_scoped_release release;v.index();std::fill(f,f+v.n,absent);std::fill(r,r+v.n,absent);std::fill(in,in+v.n,0);std::fill(out,out+v.n,0);
 for(U32 a=0;a<v.n;++a)v.edges(a,[&](Id id){auto b=v.row.find(id);if(b!=v.row.end()){++in[b->second];++out[a];++edges;}});
 std::vector<size_t> offsets(v.n+1);for(size_t i=0;i<v.n;++i)offsets[i+1]=offsets[i]+in[i];
 auto next=offsets;std::vector<U32> from(edges);
 for(U32 a=0;a<v.n;++a)v.edges(a,[&](Id id){auto b=v.row.find(id);if(b!=v.row.end())from[next[b->second]++]=a;});
 std::vector<U32> q;q.reserve(v.n);q.push_back(root);f[root]=root;
 for(size_t i=0;i<q.size();++i){auto a=q[i];v.edges(a,[&](Id id){auto b=v.row.find(id);if(b!=v.row.end()&&f[b->second]==absent){f[b->second]=a;q.push_back(b->second);}});}nf=q.size();
 q.clear();q.push_back(root);r[root]=root;
 for(size_t i=0;i<q.size();++i){auto a=q[i];for(size_t j=offsets[a];j<offsets[a+1];++j)if(r[from[j]]==absent){r[from[j]]=a;q.push_back(from[j]);}}nr=q.size();
 }
 py::dict result;result["forward"]=forward;result["reverse"]=reverse;result["incoming"]=incoming;result["outgoing"]=outgoing;
 result["forward_reached"]=nf;result["reverse_reached"]=nr;result["source_local_edges"]=edges;return result;
}
inline py::dict close(int fd,Array<Id> source,py::dict meta,Array<U32> core,Array<U32> entries,
 Array<U32> forward,Array<U32> reverse,U32 root,size_t maxrows,double seconds){
 View v(fd,source,meta);require(forward.ndim()==1&&reverse.ndim()==1&&size_t(forward.size())==v.n&&size_t(reverse.size())==v.n&&root<v.n,"Geometry tree shape");
 require(core.ndim()==1&&entries.ndim()==1&&maxrows>0&&std::isfinite(seconds)&&seconds>0,"Geometry closure limits");
 std::vector<Id> output;size_t guarded=0;auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(size_t(seconds*1000));
 {
 py::gil_scoped_release release;v.index();std::vector<uint8_t> mask(v.n);std::vector<U32> base;
 auto add=[&](U32 row){if(row>=v.n)throw std::invalid_argument("Geometry core nonlocal");if(!mask[row]){mask[row]=1;base.push_back(row);if(base.size()>maxrows)throw std::invalid_argument("Geometry complete core exceeds row budget");}};
 for(ssize_t i=0;i<core.size();++i)add(core.data()[i]);for(ssize_t i=0;i<entries.size();++i){auto row=entries.data()[i];add(row);v.edges(row,[&](Id id){auto it=v.row.find(id);if(it!=v.row.end())add(it->second);});}guarded=base.size();
 size_t count=base.size();
 for(int direction=0;direction<2;++direction){
  auto parent=direction?reverse.data():forward.data();std::vector<uint8_t> state(v.n);state[root]=2;
  if(parent[root]!=root)throw std::invalid_argument("Geometry tree root mismatch");
  for(U32 start:base){
   if(std::chrono::steady_clock::now()>deadline)throw std::runtime_error("Geometry navigation deadline");
   std::vector<U32> path;U32 a=start;
   while(state[a]!=2){
    if(state[a]==1)throw std::invalid_argument("Geometry tree cycle");state[a]=1;path.push_back(a);
    U32 b=parent[a];if(b==absent||b>=v.n)throw std::invalid_argument("Geometry core outside hub strongly connected component");
    if(!v.edge(direction?a:b,direction?b:a))throw std::invalid_argument("Geometry parent is not an original directed edge");
    a=b;
   }
   for(auto b:path){state[b]=2;if(!mask[b]){mask[b]=1;if(++count>maxrows)throw std::invalid_argument("Geometry complete navigation exceeds row budget");}}
  }
 }
 if(!mask[root]){mask[root]=1;++count;}if(count>maxrows)throw std::invalid_argument("Geometry hub exceeds row budget");
 for(U32 i=0;i<v.n;++i)if(mask[i])output.push_back(v.ids[i]);std::sort(output.begin(),output.end());
 }
 py::dict result;result["ids"]=output;result["guarded_rows"]=guarded;result["navigation_added_rows"]=output.size()-guarded;return result;
}
inline py::dict assess(int fd,Array<Id> source,py::dict meta,Array<Id> members,Array<Id> candidates,
 Array<U32> incoming,Array<U32> outgoing){
 View v(fd,source,meta);require(size_t(incoming.size())==v.n&&size_t(outgoing.size())==v.n,"Geometry degree shape");
 size_t edgecount=0,exits=0,internal=0,volume=0,total=0,codes=0;std::vector<Id> safe;
 {
 py::gil_scoped_release release;v.index();std::vector<uint8_t> mask(v.n);std::unordered_set<Id> deps;
 for(ssize_t i=0;i<members.size();++i){Id id=members.data()[i];auto it=v.row.find(id);if(it==v.row.end()||mask[it->second])throw std::invalid_argument("Geometry invalid membership");mask[it->second]=1;deps.insert(id);}
 for(size_t i=0;i<v.n;++i)total+=incoming.data()[i]+outgoing.data()[i];
 for(ssize_t i=0;i<members.size();++i){Id id=members.data()[i];auto a=v.row.at(id);volume+=incoming.data()[a]+outgoing.data()[a];v.edges(a,[&](Id b){++edgecount;deps.insert(b);auto it=v.row.find(b);bool inside=it!=v.row.end()&&mask[it->second];internal+=inside;exits+=!inside;});}
 for(ssize_t i=0;i<candidates.size();++i){Id id=candidates.data()[i];auto it=v.row.find(id);if(it==v.row.end()||!mask[it->second])continue;bool ok=true;v.edges(it->second,[&](Id b){auto jt=v.row.find(b);if(jt!=v.row.end()&&!mask[jt->second])ok=false;});if(ok)safe.push_back(id);}
 codes=deps.size();std::sort(safe.begin(),safe.end());safe.erase(std::unique(safe.begin(),safe.end()),safe.end());
 }
 size_t per=meta["community_code_bytes"].cast<size_t>(),fixed=meta["community_fixed_bytes"].cast<size_t>();
 py::dict out;out["rows"]=members.size();out["code_rows"]=codes;out["estimated_bytes"]=v.header+members.size()*(v.stride+8)+codes*per+fixed;
 out["code_bytes"]=codes*per+fixed;out["edges"]=edgecount;out["exit_edges"]=exits;out["outgoing_ratio"]=edgecount?double(exits)/edgecount:0.;
 out["conductance"]=std::min(volume,total-volume)?double(volume-2*internal)/std::min(volume,total-volume):0.;out["safe_entries"]=safe;return out;
}
inline void bind(py::module_& m){
 m.def("geometry_trees",&trees);m.def("geometry_close",&close);m.def("geometry_assess",&assess);
}
}
