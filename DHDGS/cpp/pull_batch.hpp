#pragma once
#include "value_utils.hpp"
#include <algorithm>
#include <cstring>
#include <functional>
#include <unordered_map>
#include <unordered_set>

// Wire data is immutable and shared across continuations. Only the small row
// directory is query-specific. Search retains its existing bounded byte API.
namespace dhpull {
using namespace dhvalue;
inline V gather(const V& common, const V& descriptor) {
  if (!optional<bool>(descriptor,"shared",false)) return descriptor;
  M out=descriptor.map();out.erase("shared");
  for (const auto* key : {"records","codes"}) {
    const auto& bytes=std::get<V::Bytes>(at(common,key).data);
    const std::string index=std::string(key)+"_rows";
    const auto rows=get<std::vector<size_t>>(descriptor,index);
    const size_t width=get<size_t>(common,std::string(key)+"_width");
    if (!width || bytes.size()%width) throw std::invalid_argument("Invalid common pull layout");
    V::Bytes data;data.reserve(rows.size()*width);
    for(auto row:rows) {
      if(row>=bytes.size()/width)throw std::invalid_argument("Invalid common pull row");
      data.insert(data.end(),bytes.begin()+row*width,bytes.begin()+(row+1)*width);
    }
    out.erase(index);out[key]=std::move(data);
  }
  return out;
}

// Each item already passed independent query admission. A failed union read
// falls back to isolated bounded reads so one invalid record cannot fail peers.
inline V read(const A& items,size_t dimension,size_t single_limit,size_t total_limit,
              const std::function<V(const std::vector<uint64_t>&,size_t)>& reader) {
  std::vector<uint64_t> ids;std::unordered_map<uint64_t,size_t> positions;
  for(const auto& item:items)for(auto id:get<std::vector<uint64_t>>(item,"ids"))
    if(positions.emplace(id,ids.size()).second)ids.push_back(id);
  V common;A replies;
  try { common=reader(ids,total_limit); }
  catch(const std::exception&) {
    for(const auto& item:items)try {replies.emplace_back(M{{"ok",true},{"result",reader(get<std::vector<uint64_t>>(item,"ids"),single_limit)}});}
      catch(const std::exception& e){replies.emplace_back(M{{"ok",false},{"error",e.what()}});}
    return M{{"items",std::move(replies)},{"union_failed",true},{"unique_records",uint64_t(0)}};
  }
  const auto stride=get<size_t>(common,"record_stride");
  const auto& records=std::get<V::Bytes>(at(common,"records").data);
  const auto& codes=std::get<V::Bytes>(at(common,"codes").data);
  const auto code_width=get<size_t>(common,"code_stride");
  if(!stride||!code_width||records.size()!=ids.size()*stride||codes.size()%code_width)
    throw std::invalid_argument("Invalid union pull data");
  std::unordered_map<uint64_t,size_t> code_rows;
  for(size_t row=0;row<codes.size()/code_width;++row){uint64_t id;std::memcpy(&id,codes.data()+row*code_width,8);code_rows.emplace(id,row);}
  uint64_t logical_bytes=0,logical_records=0;
  for(const auto& item:items) {
    try {
      auto wanted=get<std::vector<uint64_t>>(item,"ids");std::vector<size_t> rr,cr;
      std::unordered_set<uint64_t> deps;
      for(auto id:wanted){size_t row=positions.at(id);rr.push_back(row);const auto* data=records.data()+row*stride;
        if(dimension*4+4>stride)throw std::invalid_argument("Invalid pulled dimension");
        uint32_t degree;std::memcpy(&degree,data+dimension*4,4);
        if(degree>(stride-dimension*4-4)/8)throw std::invalid_argument("Invalid pulled degree");
        for(size_t j=0;j<degree;++j){uint64_t n;std::memcpy(&n,data+dimension*4+4+j*8,8);deps.insert(n);}}
      for(auto id:deps)cr.push_back(code_rows.at(id));std::sort(cr.begin(),cr.end());
      const auto bytes=rr.size()*stride+cr.size()*code_width;
      if(bytes>single_limit)throw std::invalid_argument("Pull dependency byte capacity exceeded");
      logical_bytes+=bytes;logical_records+=wanted.size();
      M result{{"accepted",true},{"shared",true},{"ids",vector(wanted)},
        {"epoch",at(common,"epoch")},{"record_stride",uint64_t(stride)},
        {"records_rows",vector(rr)},{"codes_rows",vector(cr)}};
      // Allocated cost shares sum to measured union costs; not per-query CPU measurements.
      for(const auto* key:{"source_read_ms","source_cpu_ms","source_service_ms"})result[key]=number(at(common,key))/items.size();
      replies.emplace_back(M{{"ok",true},{"result",std::move(result)}});
    }catch(const std::exception& e){replies.emplace_back(M{{"ok",false},{"error",e.what()}});}
  }
  const auto unique_bytes=records.size()+codes.size();
  return M{{"items",std::move(replies)},{"records",std::move(common.map().at("records"))},
    {"codes",std::move(common.map().at("codes"))},{"records_width",uint64_t(stride)},
    {"codes_width",uint64_t(code_width)},{"unique_records",uint64_t(ids.size())},
    {"logical_records",logical_records},{"unique_bytes",uint64_t(unique_bytes)},
    {"logical_bytes",logical_bytes},{"union_failed",false}};
}
}
