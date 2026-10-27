#pragma once
#include <memory>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <vector>
#include <unistd.h>

namespace dhsearch {
// Owns native buffers only. A queued/running branch pins this object; removal
// from the admission directory cannot close its fd or release its code bank.
struct ReplicaOverlay {
  int fd=-1;
  size_t stride=0,header=0,degree=0,code_bytes=0,extra_bytes=0;
  std::string version;
  std::unordered_map<uint64_t,size_t> records,locations;
  std::vector<uint64_t> sorted_record_ids;
  uint64_t lookup_base=0;
  std::vector<size_t> dense_records,dense_codes;
  static constexpr size_t missing=size_t(-1);
  size_t locate(uint64_t id,bool code=false)const{
    const auto& dense=code?dense_codes:dense_records;
    if(!dense.empty())return id>=lookup_base&&id-lookup_base<dense.size()?dense[size_t(id-lookup_base)]:missing;
    const auto& map=code?locations:records;auto it=map.find(id);return it==map.end()?missing:it->second;
  }
  bool contains(uint64_t id)const{return locate(id)!=missing;}
  std::vector<uint8_t> codes,extra;
  std::vector<float> factors;
  std::vector<uint32_t> centers;
  std::vector<std::vector<int>> owners;
  ~ReplicaOverlay(){if(fd>=0)::close(fd);}
};
}
