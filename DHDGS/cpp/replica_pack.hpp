#pragma once
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <vector>
#include <stdexcept>

namespace dhpack {
inline void read(int fd,void* p,size_t n,uint64_t off){
  size_t done=0;while(done<n){ssize_t k=::pread(fd,(char*)p+done,n-done,off+done);if(k<0&&errno==EINTR)continue;
    if(k<=0)throw std::runtime_error("Replica short read");done+=size_t(k);}
}
inline void write(int fd,const void* p,size_t n){
  size_t done=0;while(done<n){ssize_t k=::write(fd,(char*)p+done,n-done);if(k<0&&errno==EINTR)continue;
    if(k<=0)throw std::runtime_error("Replica short write");done+=size_t(k);}
}
inline uint32_t degree(const char* p,size_t dim,size_t stride,size_t maxdeg){uint32_t d;std::memcpy(&d,p+4*dim,4);
 if(d>maxdeg||4*dim+4+8*uint64_t(d)>stride)throw std::invalid_argument("Invalid replica degree");return d;}
inline uint64_t neighbor(const char* p,size_t dim,size_t i){uint64_t v;std::memcpy(&v,p+4*dim+4+8*i,8);return v;}
template<class T> py::array_t<T> array(const std::vector<T>& v){py::array_t<T> a(v.size());if(!v.empty())std::memcpy(a.mutable_data(),v.data(),v.size()*sizeof(T));return a;}
inline py::dict extract(int fd,Array<uint64_t> source,Array<uint64_t> core,size_t dim,size_t stride,size_t header,size_t maxdeg,size_t hops,size_t limit,const std::string& path,py::bytes prefix){
 require(source.ndim()==1&&core.ndim()==1&&core.size()>0&&size_t(core.size())<=limit,"Invalid replica core");
 require(stride>=4*dim+4&&header>=32&&header<=16*1024*1024,"Invalid replica record layout");std::string pre=prefix;require(pre.size()==header,"Invalid replica prefix");
 std::vector<uint64_t> ids(core.data(),core.data()+core.size()),ghost,src,dst;std::vector<double> degrees;
 {
 py::gil_scoped_release release;
 std::unordered_map<uint64_t,size_t> rows;rows.reserve(source.size());for(ssize_t i=0;i<source.size();++i)if(!rows.emplace(source.data()[i],i).second)throw std::invalid_argument("Duplicate source ID");
 std::sort(ids.begin(),ids.end());ids.erase(std::unique(ids.begin(),ids.end()),ids.end());
 std::unordered_set<uint64_t> selected(ids.begin(),ids.end());auto frontier=ids;std::vector<char> buffer(stride);
 for(auto id:ids)if(!rows.count(id))throw std::invalid_argument("Nonlocal replica record");
 for(size_t hop=0;hop<hops&&selected.size()<limit;++hop){
  std::unordered_map<uint64_t,size_t> counts;
  for(auto id:frontier){read(fd,buffer.data(),stride,header+rows.at(id)*stride);auto d=degree(buffer.data(),dim,stride,maxdeg);
   for(size_t j=0;j<d;++j){auto n=neighbor(buffer.data(),dim,j);if(!selected.count(n)&&rows.count(n))++counts[n];}}
  std::vector<std::pair<uint64_t,size_t>> ranked(counts.begin(),counts.end());std::sort(ranked.begin(),ranked.end(),[](auto a,auto b){return a.second!=b.second?a.second>b.second:a.first<b.first;});
  frontier.clear();size_t room=limit-selected.size();for(size_t i=0;i<std::min(room,ranked.size());++i){frontier.push_back(ranked[i].first);selected.insert(ranked[i].first);}if(frontier.empty())break;
 }
 ids.assign(selected.begin(),selected.end());std::sort(ids.begin(),ids.end());std::unordered_map<uint64_t,size_t> local;local.reserve(ids.size());for(size_t i=0;i<ids.size();++i)local[ids[i]]=i;
 std::unordered_set<uint64_t> codes(selected);degrees.resize(ids.size());int out=::open(path.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);if(out<0)throw std::runtime_error("Cannot create replica records");
 try{
  // Persist final row count in the standard little-endian header, not core count.
  uint64_t count=ids.size();std::memcpy(pre.data()+16,&count,8);write(out,pre.data(),pre.size());
  size_t i=0;while(i<ids.size()){
   size_t row=rows.at(ids[i]),j=i+1;while(j<ids.size()&&(j-i+1)*stride<=1048576&&rows.at(ids[j])==row+j-i)++j;
   buffer.resize((j-i)*stride);read(fd,buffer.data(),buffer.size(),header+row*stride);
   for(size_t k=i;k<j;++k){auto raw=buffer.data()+(k-i)*stride;auto d=degree(raw,dim,stride,maxdeg);degrees[k]=d;
    for(size_t e=0;e<d;++e){auto n=neighbor(raw,dim,e);codes.insert(n);auto f=local.find(n);if(f!=local.end()){src.push_back(k);dst.push_back(f->second);}}}
   write(out,buffer.data(),buffer.size());i=j;
  }
  ::close(out);
 }catch(...){::close(out);::unlink(path.c_str());throw;}
 ghost.assign(codes.begin(),codes.end());std::sort(ghost.begin(),ghost.end());
 }
 py::dict result;result["ids"]=array(ids);result["code_ids"]=array(ghost);result["src"]=array(src);result["dst"]=array(dst);result["degrees"]=array(degrees);return result;
}
inline size_t validate_records(int fd,Array<uint64_t> ids,Array<uint64_t> codeids,size_t dim,size_t stride,size_t header,size_t maxdeg){
 require(ids.ndim()==1&&codeids.ndim()==1&&stride>=4*dim+4,"Invalid replica validation layout");py::gil_scoped_release release;
 std::unordered_set<uint64_t> codes(codeids.data(),codeids.data()+codeids.size());std::vector<char> buffer;
 for(size_t i=0;i<size_t(ids.size());){size_t count=std::min(std::max(size_t(1),size_t(1048576/stride)),size_t(ids.size())-i);buffer.resize(count*stride);read(fd,buffer.data(),buffer.size(),header+i*stride);
  for(size_t j=0;j<count;++j){auto p=buffer.data()+j*stride;auto d=degree(p,dim,stride,maxdeg);for(size_t k=0;k<d;++k)if(!codes.count(neighbor(p,dim,k)))throw std::invalid_argument("Missing replica neighbor code");}i+=count;}
 return size_t(ids.size())*stride;
}
inline void bind(py::module_& m){m.def("replica_extract",&extract);m.def("replica_validate_records",&validate_records);}
}
