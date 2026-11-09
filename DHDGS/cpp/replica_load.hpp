#pragma once
#include "replica_overlay.hpp"
#include "value_utils.hpp"
#include <openssl/evp.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <regex>
#include <chrono>
#include <atomic>
#include <map>
#include <limits>

namespace dhreplica {
using Clock=std::chrono::steady_clock;
struct Cancel {
  std::atomic<bool> stopped{false};Clock::time_point deadline;
  explicit Cancel(double seconds){
    if(!std::isfinite(seconds)||seconds<=0||seconds>86400)throw std::invalid_argument("Invalid replica deadline");
    deadline=Clock::now()+std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double>(seconds));}
  void cancel(){stopped=true;}
  void check()const{if(stopped||Clock::now()>deadline)throw std::runtime_error("Replica cancelled/deadline exceeded");}
};
inline size_t product(size_t a,size_t b){if(b&&a>std::numeric_limits<size_t>::max()/b)throw std::invalid_argument("Replica size overflow");return a*b;}
struct File {
  int fd=-1;size_t size=0;
  explicit File(const std::string& p){fd=::open(p.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);if(fd<0)throw std::runtime_error("Cannot open replica file");
    struct stat st{};if(::fstat(fd,&st)||!S_ISREG(st.st_mode)||st.st_size<0){::close(fd);fd=-1;throw std::invalid_argument("Invalid replica file");}size=size_t(st.st_size);}
  ~File(){if(fd>=0)::close(fd);}File(const File&)=delete;
  void read(void* out,size_t n,size_t off)const{if(off>size||n>size-off)throw std::invalid_argument("Replica file truncated");dhpack::read(fd,out,n,off);}
};
inline std::string digest(const std::string& path,const Cancel& stop){
  File f(path);std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),EVP_MD_CTX_free);
  if(!ctx||EVP_DigestInit_ex(ctx.get(),EVP_sha256(),nullptr)!=1)throw std::runtime_error("SHA256 init failed");
  std::vector<char> buf(1048576);for(size_t o=0;o<f.size;){stop.check();size_t n=std::min(buf.size(),f.size-o);f.read(buf.data(),n,o);
    if(EVP_DigestUpdate(ctx.get(),buf.data(),n)!=1)throw std::runtime_error("SHA256 update failed");o+=n;}
  unsigned char hash[32];unsigned int n=0;if(EVP_DigestFinal_ex(ctx.get(),hash,&n)!=1||n!=32)throw std::runtime_error("SHA256 finish failed");
  std::string result;const char* hex="0123456789abcdef";for(auto b:hash){result+=hex[b>>4];result+=hex[b&15];}return result;
}
struct Npy {
  File file;std::vector<size_t> shape;size_t offset=0,count=1;
  Npy(const std::string& path,const std::string& dtype,size_t item):file(path){
    unsigned char prefix[12]{};file.read(prefix,8,0);
    if(std::memcmp(prefix,"\x93NUMPY",6)||prefix[7]!=0||(prefix[6]!=1&&prefix[6]!=2&&prefix[6]!=3))throw std::invalid_argument("Unsupported NPY version");
    size_t start=prefix[6]==1?10:12;file.read(prefix+8,start-8,8);size_t len=prefix[8]+(size_t(prefix[9])<<8);
    if(start==12)len+=(size_t(prefix[10])<<16)+(size_t(prefix[11])<<24);
    if(!len||len>1048576||start>file.size||len>file.size-start)throw std::invalid_argument("Invalid NPY header size");
    offset=start+len;std::string h(len,'\0');file.read(h.data(),len,start);
    std::smatch m;
    if(!std::regex_search(h,m,std::regex("['\"]descr['\"]\\s*:\\s*['\"]([^'\"]+)['\"]"))||
        (m[1].str()!=dtype&&!(dtype=="|b1"&&m[1].str()=="|u1")))
      throw std::invalid_argument("Invalid replica NPY dtype");
    if(!std::regex_search(h,std::regex("['\"]fortran_order['\"]\\s*:\\s*False\\s*[,}]")))throw std::invalid_argument("NPY Fortran layout unsupported");
    if(!std::regex_search(h,m,std::regex("['\"]shape['\"]\\s*:\\s*\\(([^)]*)\\)")))throw std::invalid_argument("Invalid NPY shape");
    auto s=m[1].str();size_t i=0;
    while(i<s.size()){while(i<s.size()&&std::isspace(static_cast<unsigned char>(s[i])))++i;if(i==s.size())break;
      if(!std::isdigit(static_cast<unsigned char>(s[i])))throw std::invalid_argument("Invalid NPY dimension");
      size_t v=0;while(i<s.size()&&std::isdigit(static_cast<unsigned char>(s[i]))){unsigned x=s[i++]-'0';if(v>(SIZE_MAX-x)/10)throw std::invalid_argument("NPY shape overflow");v=v*10+x;}
      shape.push_back(v);count=product(count,v);if(shape.size()>3)throw std::invalid_argument("NPY rank too large");
      while(i<s.size()&&std::isspace(static_cast<unsigned char>(s[i])))++i;if(i<s.size()){if(s[i++]!=',')throw std::invalid_argument("Invalid NPY shape separator");}}
    if(shape.empty()||product(count,item)!=file.size-offset)throw std::invalid_argument("NPY payload length mismatch");
  }
  template<class T>std::vector<T> load(const Cancel& stop){std::vector<T> v(count);size_t bytes=product(count,sizeof(T));
    for(size_t o=0;o<bytes;){stop.check();size_t n=std::min(size_t(1048576),bytes-o);file.read(reinterpret_cast<char*>(v.data())+o,n,offset+o);o+=n;}return v;}
};
struct Prepared {
  std::shared_ptr<dhsearch::ReplicaOverlay> overlay;
  std::shared_ptr<Cancel> stop;
  std::string epoch;
  std::map<std::string,double> timings;
};
inline double elapsed(Clock::time_point a){return std::chrono::duration<double>(Clock::now()-a).count();}
inline std::shared_ptr<Prepared> load(const std::string& version,const std::string& path,const dhwire::Value& meta,
    const std::string& centers_sha,size_t dim,size_t nodes,size_t code_width,size_t extra_width,
    const std::vector<uint64_t>& entries,size_t lookup_bytes,size_t warm_bytes,std::shared_ptr<Cancel> stop){
  using namespace dhvalue;stop->check();auto begin=Clock::now();auto p=std::make_shared<Prepared>();p->stop=stop;
  p->epoch=get<std::string>(meta,"epoch");auto r=p->overlay=std::make_shared<dhsearch::ReplicaOverlay>();r->version=version;
  r->stride=get<size_t>(meta,"record_bytes");r->header=get<size_t>(meta,"header_bytes");r->degree=get<size_t>(meta,"max_degree");
  require(dim<=(SIZE_MAX-4)/4&&r->stride>=dim*4+4&&r->header>=32,"Invalid replica layout");
  Npy ids_file(path+"/ids.npy","<u8",8),bank_file(path+"/code_ids.npy","<u8",8);
  auto ids=ids_file.load<uint64_t>(*stop),bank=bank_file.load<uint64_t>(*stop);
  require(ids_file.shape.size()==1&&bank_file.shape.size()==1&&!ids.empty()&&!bank.empty(),"Invalid replica ID rank/empty");
  require(ids.size()==get<size_t>(meta,"rows")&&bank.size()==get<size_t>(meta,"code_rows"),"Replica row count mismatch");
  auto sorted=[](const auto& x){return std::adjacent_find(x.begin(),x.end(),std::greater_equal<uint64_t>())==x.end();};
  const auto order=optional<std::string>(meta,"replica_record_order",std::string("sorted_v1"));
  require(order=="sorted_v1"||order=="admission_v1","Unsupported replica record order");
  require(sorted(bank)&&(order=="admission_v1"||sorted(ids)),"Replica IDs not strictly sorted");
  auto read_array=[&](const char* name,const char* dtype,size_t item,const std::vector<size_t>& shape){
    auto f=std::make_unique<Npy>(path+"/"+name,dtype,item);require(f->shape==shape,"Replica array shape mismatch");return f;};
  r->codes=read_array("codes.npy","|u1",1,{bank.size(),code_width})->load<uint8_t>(*stop);
  r->extra=read_array("extra.npy","|u1",1,{bank.size(),extra_width})->load<uint8_t>(*stop);
  r->factors=read_array("factors.npy","<f4",4,{bank.size(),3})->load<float>(*stop);
  r->centers=read_array("center_ids.npy","<u4",4,{bank.size()})->load<uint32_t>(*stop);
  auto owners=read_array("code_membership.npy","|b1",1,{bank.size(),nodes})->load<uint8_t>(*stop);
  Npy centers(path+"/centers.npy","<f4",4);require(centers.shape.size()==2&&centers.shape[1]==dim&&centers.shape[0]>0,"Invalid replica centers");
  require(digest(path+"/centers.npy",*stop)==centers_sha,"Replica centers mismatch");
  require(digest(path+"/rotation.bin",*stop)==get<std::string>(meta,"rotation_sha256"),"Replica rotation mismatch");
  for(float f:r->factors)require(std::isfinite(f),"Nonfinite replica factors");
  r->code_bytes=code_width;r->extra_bytes=extra_width;r->records.reserve(ids.size());r->locations.reserve(bank.size());r->owners.resize(bank.size());
  for(size_t i=0;i<ids.size();++i)require(r->records.emplace(ids[i],i).second,"Duplicate replica record ID");
  r->sorted_record_ids=ids;std::sort(r->sorted_record_ids.begin(),r->sorted_record_ids.end());
  for(size_t i=0;i<bank.size();++i){if((i&4095)==0)stop->check();r->locations.emplace(bank[i],i);require(r->centers[i]<centers.shape[0],"Invalid replica center ID");
    for(size_t n=0;n<nodes;++n){auto v=owners[i*nodes+n];require(v<=1,"Invalid replica membership");if(v)r->owners[i].push_back(n);}require(!r->owners[i].empty(),"Replica code lacks owners");}
  for(auto id:ids)require(r->locations.count(id),"Replica record lacks code");for(auto id:entries)require(r->records.count(id),"Replica entry missing");
  if(lookup_bytes&&bank.back()-bank.front()<lookup_bytes/(2*sizeof(size_t))){r->lookup_base=bank.front();size_t n=size_t(bank.back()-bank.front())+1;
    r->dense_records.assign(n,dhsearch::ReplicaOverlay::missing);r->dense_codes.assign(n,dhsearch::ReplicaOverlay::missing);
    for(auto [id,row]:r->records)r->dense_records[id-r->lookup_base]=row;for(auto [id,row]:r->locations)r->dense_codes[id-r->lookup_base]=row;}
  File records(path+"/nodes.bin");const size_t bytes=product(ids.size(),r->stride);require(bytes<=SIZE_MAX-r->header&&records.size==r->header+bytes,"Replica node file length mismatch");
  char h[32];records.read(h,32,0);uint32_t ver,d;uint64_t count,stride;std::memcpy(&ver,h+8,4);std::memcpy(&d,h+12,4);std::memcpy(&count,h+16,8);std::memcpy(&stride,h+24,8);
  require(!std::memcmp(h,"DHDGS001",8)&&ver==1&&d==dim&&count==ids.size()&&stride==r->stride,"Replica node header mismatch");
  p->timings["load_seconds"]=elapsed(begin);auto validation=Clock::now();std::vector<char> buffer;
  for(size_t i=0;i<ids.size();){stop->check();size_t n=std::min(std::max(size_t(1),size_t(1048576/r->stride)),ids.size()-i);buffer.resize(n*r->stride);records.read(buffer.data(),buffer.size(),r->header+i*r->stride);
    for(size_t j=0;j<n;++j){auto raw=buffer.data()+j*r->stride;auto deg=dhpack::degree(raw,dim,r->stride,r->degree);
      for(size_t k=0;k<deg;++k)require(r->locations.count(dhpack::neighbor(raw,dim,k)),"Missing replica neighbor code");}i+=n;}
  p->timings["record_validation_seconds"]=elapsed(validation);p->timings["validation_read_bytes"]=bytes;
  auto warm=Clock::now();size_t warmed=0;buffer.resize(r->stride);std::unordered_set<uint64_t> selected(entries.begin(),entries.end());
  auto touch=[&](uint64_t id){if(r->stride>warm_bytes-warmed)return false;stop->check();records.read(buffer.data(),r->stride,r->header+r->records.at(id)*r->stride);warmed+=r->stride;return true;};
  if(warm_bytes){for(auto id:entries)if(!touch(id))break;for(auto id:ids)if(!selected.count(id)&&!touch(id))break;}
  p->timings["extra_warm_seconds"]=elapsed(warm);p->timings["warm_read_bytes"]=warmed;
  r->fd=records.fd;records.fd=-1;stop->check();p->timings["warm_seconds"]=elapsed(begin);return p;
}
}
