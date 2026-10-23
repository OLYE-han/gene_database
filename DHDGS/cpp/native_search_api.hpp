#pragma once
#include "native_head.hpp"
#include "native_network.hpp"
namespace dhruntime {
class SearchApi {
 std::unique_ptr<dhnet::Server> server_;
public:
 SearchApi(HeadRuntime&,size_t,const V&);
 void close(){server_->close();}
 V metrics()const{return server_->metrics();}
 uint16_t port()const{return server_->port();}
};
V run_network_load(const double*,const uint64_t*,size_t,size_t,size_t,const V&,const std::string&);
}
