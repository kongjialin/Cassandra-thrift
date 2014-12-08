#ifndef RING_CACHE_H_
#define RING_CACHE_H_

#include "Cassandra.h"

#include <stddef.h>
#include <atomic>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cass_client_pool.h"
#include "range.h"
#include "thirdparty/boost/thread.hpp"

using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;
using namespace ::org::apache::cassandra;

class RingCache {
 public:
  static RingCache& GetInstance() {
    static RingCache instance;
    return instance;
  }
  ~RingCache();
  void RefreshEndpointMap();
  void RefreshClientPools();
  CassClientPool::Node* GetClientNode(std::string row_key);
  void ReturnClientNode(CassClientPool::Node* node);

 private:
  RingCache();
  void InitRefreshClient();
  size_t GetRoundPos(int round_index, size_t bound);
  boost::shared_ptr<CassandraClient> refresh_client_;
  boost::shared_ptr<TTransport> refresh_transport_;
  std::multimap<boost::shared_ptr<Range>, std::string> range_map_;
  std::vector<boost::shared_ptr<std::atomic<size_t>>> round_pos_;
  std::unordered_set<std::string> cass_servers_;
  std::unordered_map<std::string,
                     boost::shared_ptr<CassClientPool>> client_pools_;
  boost::shared_mutex shared_mutex_;
};

#endif // RING_CACHE_H_
              
