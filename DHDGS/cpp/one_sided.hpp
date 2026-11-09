#pragma once
#include "native_wire.hpp"
#include <functional>
#include <atomic>
#include <array>
#include <memory>
#include <vector>

namespace dhonesided {
using V=dhwire::Value;
// Immutable, version-pinned source memory. Destruction follows QP destruction.
struct Image {
  virtual ~Image()=default;
  const void* records=nullptr;
  size_t record_bytes=0,header=0,stride=0,dimension=0,code_width=0;
  uint64_t base=0;
  std::string epoch;
  std::vector<uint64_t> directory; // per ID: record row, code row; UINT64_MAX absent
  std::vector<uint8_t> codes;
};
using Export=std::function<std::shared_ptr<Image>(const V&)>;
bool available();
class Source {
  struct Impl; std::unique_ptr<Impl> p_;
public:
  Source(const V&,std::shared_ptr<Image>);
  ~Source();
  V connect(const V&);
  V metrics()const;
};
// Control uses the existing asynchronous network module. Data uses verbs READ.
using Complete=std::function<void(dhwire::Result)>;
using Control=std::function<bool(size_t,V,Complete)>;
// Exact availability in a pinned, suspended query: 0 missing, 1 local,
// 2 replica overlay, 3 previously installed query code. Never a Bloom hint.
using CodeLookup=std::function<unsigned(uint64_t)>;
class Client {
  struct Impl;std::vector<std::unique_ptr<Impl>> engines_;
  // A global round robin correlates with alternating source IDs: a source can
  // then use only half the engines and strand its other QP quotas. Rotate each
  // source independently. Hash collisions affect balance, never ownership.
  struct alignas(64) Cursor {std::atomic<size_t> next{0};};
  std::array<Cursor,64> next_by_source_;
public:
  Client(const V&,size_t max_bytes,size_t max_pending,Control);
  ~Client();
  bool submit(size_t source,V authorization,std::vector<uint64_t>,Complete,CodeLookup = {});
  V metrics()const;
  void close();
};
void bind(pybind11::module_&);
}
