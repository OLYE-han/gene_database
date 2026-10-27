#pragma once
#include "value_utils.hpp"
#include <algorithm>
#include <stdexcept>

namespace dhregion {
using namespace dhvalue;
inline std::string base(size_t node){return "base:"+std::to_string(node);}
inline std::string current(const V& s){return has(s,"replica_version")?"replica:"+get<std::string>(s,"replica_version"):base(get<size_t>(s,"target_node"));}
inline std::vector<std::string> path(const V& s){
  if(has(s,"visited_regions"))return get<std::vector<std::string>>(s,"visited_regions");
  std::vector<std::string> p;for(auto n:get<std::vector<size_t>>(s,"visited_nodes"))p.push_back(base(n));
  if(!p.empty())p.back()=current(s);return p;
}
inline void normalize(V& s){s.map()["logical_region"]=current(s);s.map()["visited_regions"]=vector(path(s));}
inline V encode(const std::vector<uint64_t>& ids){V::Bytes out(ids.size()*8);for(size_t i=0;i<ids.size();++i)for(size_t j=0;j<8;++j)out[8*i+j]=uint8_t(ids[i]>>(j*8));return V(std::move(out));}
inline std::vector<uint64_t> decode(const V& s,size_t limit){
  if(!has(s,"excluded_region_ids"))return {};
  const auto& b=std::get<V::Bytes>(at(s,"excluded_region_ids").data);
  if(b.size()%8||b.size()/8>limit)throw std::invalid_argument("Logical region exclusion limit/encoding");
  std::vector<uint64_t> out(b.size()/8);
  for(size_t i=0;i<out.size();++i){uint64_t n=0;for(size_t j=0;j<8;++j)n|=uint64_t(b[8*i+j])<<(j*8);out[i]=n;
    if(i&&out[i]<=out[i-1])throw std::invalid_argument("Logical region exclusions must be sorted and unique");}
  return out;
}
} // namespace dhregion
