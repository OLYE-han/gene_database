#pragma once
#include <cstddef>
#include <set>

namespace dhsearch {
// Query-local state, touched by exactly one compute continuation at a time.
// Counts dependent *successful batches*, never CQ polls or neighbor edges.
struct RemotePullPolicy {
  int source=-1;
  size_t consecutive=0;
  std::set<int> disabled;
  bool switch_to_push(int target,size_t limit)const {
    return disabled.count(target)||(target==source&&consecutive>=limit);
  }
  void success(int target) {
    consecutive=target==source?consecutive+1:1;source=target;
  }
  void local(){source=-1;consecutive=0;}
  void failure(int target){disabled.insert(target);}
};
}
