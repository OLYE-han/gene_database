#pragma once
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <vector>
#include <stdexcept>
#include <cstring>
namespace dhruntime {
// Per-process/thread policy only; never changes system CPU or IRQ placement.
inline void bind_thread(std::thread& thread,const std::vector<int>& cpus) {
  if(cpus.empty())return;
  cpu_set_t set;CPU_ZERO(&set);
  for(int cpu:cpus){if(cpu<0||cpu>=CPU_SETSIZE)throw std::invalid_argument("Invalid thread CPU id");CPU_SET(cpu,&set);}
  int error=pthread_setaffinity_np(thread.native_handle(),sizeof(set),&set);
  if(error)throw std::runtime_error(std::string("Thread CPU affinity: ")+std::strerror(error));
}
}
