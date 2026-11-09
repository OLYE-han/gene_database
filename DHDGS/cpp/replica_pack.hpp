#pragma once
#include <fcntl.h>
#include <sys/stat.h>
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
inline py::dict extract(int fd,Array<uint64_t> source,Array<uint64_t> core,size_t dim,size_t stride,size_t header,size_t maxdeg,size_t hops,size_t limit,const std::string& path,py::bytes prefix,py::object progress=py::none()){
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
  const uint64_t total=header+ids.size()*stride;uint64_t reported=0;
  auto notify=[&](uint64_t ready){if(!progress.is_none()){py::gil_scoped_acquire acquire;progress(total,ready);}};
  notify(0); // Halo and final row order are frozen before opening the stream.
  // Persist final row count in the standard little-endian header, not core count.
  uint64_t count=ids.size();std::memcpy(pre.data()+16,&count,8);write(out,pre.data(),pre.size());
  size_t i=0;while(i<ids.size()){
   size_t row=rows.at(ids[i]),j=i+1;while(j<ids.size()&&(j-i+1)*stride<=1048576&&rows.at(ids[j])==row+j-i)++j;
   buffer.resize((j-i)*stride);read(fd,buffer.data(),buffer.size(),header+row*stride);
   for(size_t k=i;k<j;++k){auto raw=buffer.data()+(k-i)*stride;auto d=degree(raw,dim,stride,maxdeg);degrees[k]=d;
    for(size_t e=0;e<d;++e){auto n=neighbor(raw,dim,e);codes.insert(n);auto f=local.find(n);if(f!=local.end()){src.push_back(k);dst.push_back(f->second);}}}
   write(out,buffer.data(),buffer.size());i=j;
   const uint64_t ready=header+i*stride;
   if(ready-reported>=1048576||i==ids.size()){notify(ready);reported=ready;}
  }
  ::close(out);
 }catch(...){::close(out);::unlink(path.c_str());throw;}
 ghost.assign(codes.begin(),codes.end());std::sort(ghost.begin(),ghost.end());
 }
 py::dict result;result["ids"]=array(ids);result["code_ids"]=array(ghost);result["src"]=array(src);result["dst"]=array(dst);result["degrees"]=array(degrees);return result;
}
// One maintenance producer owns this writer. Physical slots are assigned once,
// at admission time; a later, smaller ID never relocates an existing record.
class GrowingWriter {
  int source_=-1,out_=-1;
  size_t dim_,stride_,header_,degree_,limit_,bytes_;
  bool quality_,sealed_=false,failed_=false;
  struct stat identity_{};
  std::string prefix_;
  std::unordered_map<uint64_t,size_t> rows_,local_;
  std::unordered_set<uint64_t> codes_;
  std::vector<uint64_t> ids_,edge_src_,edge_dst_;
  std::vector<double> degrees_;
  void check() {
    if(sealed_||failed_||out_<0)throw std::runtime_error("Growing replica writer closed/failed");
    struct stat st{};
    if(::fstat(source_,&st)||st.st_dev!=identity_.st_dev||st.st_ino!=identity_.st_ino||
       st.st_size!=identity_.st_size||st.st_mtim.tv_sec!=identity_.st_mtim.tv_sec||
       st.st_mtim.tv_nsec!=identity_.st_mtim.tv_nsec||st.st_ctim.tv_sec!=identity_.st_ctim.tv_sec||
       st.st_ctim.tv_nsec!=identity_.st_ctim.tv_nsec)throw std::runtime_error("Growing replica source identity changed");
  }
public:
  GrowingWriter(int fd,Array<uint64_t> source,size_t dim,size_t stride,size_t header,size_t maxdeg,
                size_t limit,size_t maxbytes,const std::string& path,py::bytes prefix,bool quality)
      :dim_(dim),stride_(stride),header_(header),degree_(maxdeg),limit_(limit),bytes_(maxbytes),quality_(quality),prefix_(prefix) {
    require(source.ndim()==1&&source.size()>0&&limit>0&&dim<=(SIZE_MAX-4)/4&&stride>=4*dim+4&&
        header>=32&&header<=16777216&&prefix_.size()==header&&maxbytes>=header,"Invalid growing replica layout");
    py::gil_scoped_release release;
    for(ssize_t i=0;i<source.size();++i)require(rows_.emplace(source.data()[i],i).second,"Duplicate growing source ID");
    source_=::fcntl(fd,F_DUPFD_CLOEXEC,0);
    if(source_<0||::fstat(source_,&identity_)){close();throw std::runtime_error("Cannot pin growing source");}
    out_=::open(path.c_str(),O_RDWR|O_CREAT|O_EXCL|O_CLOEXEC,0600);
    if(out_<0){close();throw std::runtime_error("Cannot create growing replica");}
    try {uint64_t count=0;std::memcpy(prefix_.data()+16,&count,8);write(out_,prefix_.data(),header_);}
    catch(...){close();::unlink(path.c_str());throw;}
  }
  ~GrowingWriter(){close();}
  void close(){if(out_>=0){::close(out_);out_=-1;}if(source_>=0){::close(source_);source_=-1;}}
  size_t append(std::vector<uint64_t> batch) {
    py::gil_scoped_release release;check();
    require(!batch.empty()&&batch.size()<=65536,"Invalid growing admission batch");
    require(ids_.size()<=limit_&&batch.size()<=limit_-ids_.size()&&
      batch.size()<=(bytes_-header_)/stride_-ids_.size(),"Growing replica capacity exceeded");
    std::sort(batch.begin(),batch.end(),[&](auto a,auto b){return rows_.at(a)<rows_.at(b);});
    require(std::adjacent_find(batch.begin(),batch.end())==batch.end(),"Duplicate admission ID");
    for(auto id:batch)require(rows_.count(id)&&!local_.count(id),"Unknown/duplicate admission ID");
    try {
      std::vector<char> buffer;
      for(size_t i=0;i<batch.size();) {
        size_t row=rows_.at(batch[i]),j=i+1;
        while(j<batch.size()&&(j-i+1)*stride_<=1048576&&rows_.at(batch[j])==row+j-i)++j;
        buffer.resize((j-i)*stride_);read(source_,buffer.data(),buffer.size(),header_+row*stride_);
        for(size_t k=i;k<j;++k) {
          auto raw=buffer.data()+(k-i)*stride_;auto d=degree(raw,dim_,stride_,degree_);
          size_t slot=ids_.size();local_.emplace(batch[k],slot);ids_.push_back(batch[k]);codes_.insert(batch[k]);
          if(quality_)degrees_.push_back(d);
          for(size_t e=0;e<d;++e) {auto n=neighbor(raw,dim_,e);codes_.insert(n);
            if(quality_){edge_src_.push_back(slot);edge_dst_.push_back(n);}}
        }
        write(out_,buffer.data(),buffer.size());i=j;
      }
      check();return header_+ids_.size()*stride_;
    }catch(...){failed_=true;throw;}
  }
  py::dict finish(std::vector<uint64_t> expected) {
    std::vector<uint64_t> codeids,src,dst;
    {
      py::gil_scoped_release release;check();auto sorted=ids_;std::sort(sorted.begin(),sorted.end());
      require(!ids_.empty()&&expected==sorted,"Growing replica final members differ");
      uint64_t count=ids_.size();std::memcpy(prefix_.data()+16,&count,8);
      size_t done=0;while(done<header_){auto n=::pwrite(out_,prefix_.data()+done,header_-done,done);
        if(n<0&&errno==EINTR)continue;if(n<=0){failed_=true;throw std::runtime_error("Growing header short write");}done+=size_t(n);}
      codeids.assign(codes_.begin(),codes_.end());std::sort(codeids.begin(),codeids.end());
      if(quality_)for(size_t i=0;i<edge_src_.size();++i) {auto it=local_.find(edge_dst_[i]);
        if(it!=local_.end()){src.push_back(edge_src_[i]);dst.push_back(it->second);}}
      sealed_=true;close();
    }
    py::dict r;r["ids"]=array(ids_);r["code_ids"]=array(codeids);r["src"]=array(src);r["dst"]=array(dst);r["degrees"]=array(degrees_);
    return r;
  }
};

inline size_t validate_records(int fd,Array<uint64_t> ids,Array<uint64_t> codeids,size_t dim,size_t stride,size_t header,size_t maxdeg){
 require(ids.ndim()==1&&codeids.ndim()==1&&stride>=4*dim+4,"Invalid replica validation layout");py::gil_scoped_release release;
 std::unordered_set<uint64_t> codes(codeids.data(),codeids.data()+codeids.size());std::vector<char> buffer;
 for(size_t i=0;i<size_t(ids.size());){size_t count=std::min(std::max(size_t(1),size_t(1048576/stride)),size_t(ids.size())-i);buffer.resize(count*stride);read(fd,buffer.data(),buffer.size(),header+i*stride);
  for(size_t j=0;j<count;++j){auto p=buffer.data()+j*stride;auto d=degree(p,dim,stride,maxdeg);for(size_t k=0;k<d;++k)if(!codes.count(neighbor(p,dim,k)))throw std::invalid_argument("Missing replica neighbor code");}i+=count;}
 return size_t(ids.size())*stride;
}
inline void bind(py::module_& m){
py::class_<GrowingWriter>(m,"ReplicaGrowingWriter")
  .def(py::init<int,Array<uint64_t>,size_t,size_t,size_t,size_t,size_t,size_t,const std::string&,py::bytes,bool>())
  .def("append",&GrowingWriter::append).def("finish",&GrowingWriter::finish).def("close",&GrowingWriter::close);
m.def("replica_extract",&extract,py::arg("fd"),py::arg("source"),py::arg("core"),py::arg("dim"),py::arg("stride"),py::arg("header"),py::arg("maxdeg"),py::arg("hops"),py::arg("limit"),py::arg("path"),py::arg("prefix"),py::arg("progress")=py::none());m.def("replica_validate_records",&validate_records);}
}
