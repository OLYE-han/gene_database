#pragma once
#include "logical_region.hpp"

// The pair identity follows only the direct replica/source branch. Ordinary
// children lose it, even when they later visit the same physical server.
namespace dhpair {
using namespace dhvalue;
inline bool scoped(const V& peer) {
  return has(peer,"candidate_sync") && optional<std::string>(at(peer,"candidate_sync"),"scope","all")=="replica_source";
}
inline bool member(const V& state) {return has(state,"replica_pair_version");}
inline bool active(const V& peer,const V& state) {
  return has(peer,"candidate_sync") && get<bool>(at(peer,"candidate_sync"),"enabled") && (!scoped(peer)||member(state));
}
inline size_t partner(const V& state,size_t node) {
  auto source=get<size_t>(state,"replica_pair_source"),host=get<size_t>(state,"replica_pair_host");
  if(node!=source&&node!=host)throw std::invalid_argument("Replica pair endpoint mismatch");
  return node==source?host:source;
}
inline bool target(const V& peer,const V& state,size_t from,size_t to) {
  return active(peer,state)&&(!scoped(peer)||partner(state,from)==to);
}
inline std::string region(const V& peer,const V& state,size_t node) {
  return scoped(peer)&&member(state)?"pair:"+get<std::string>(state,"replica_pair_version")+":"+std::to_string(node):dhregion::base(node);
}
inline void copy(const V& from,V& to) {
  for(auto k:{"replica_pair_source","replica_pair_host","replica_pair_version"})if(has(from,k))to.map()[k]=at(from,k);
}
inline void clear(V& state) {
  for(auto k:{"replica_pair_source","replica_pair_host","replica_pair_version"})state.map().erase(k);
}
}
