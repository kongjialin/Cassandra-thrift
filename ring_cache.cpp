#include "ring_cache.h"

#include <stdlib.h>

#include "lock_guard.h"
#include "murmurhash3.h"
#include "thirdparty/boost/thread/shared_lock_guard.hpp"
#include "thirdparty/glog/logging.h"
#include "thirdparty/thrift/protocol/TBinaryProtocol.h"
#include "thirdparty/thrift/transport/TSocket.h"
#include "thirdparty/thrift/transport/TTransportUtils.h"

DEFINE_string(seed_node_ip, "127.0.0.1",
             "Server node for client to fetch the ring information");

using namespace ::apache::thrift::protocol;

RingCache::RingCache() {
  InitRefreshClient();
  RefreshEndpointMap();
  RefreshClientPools();
}

void RingCache::InitRefreshClient() {
  try {
    boost::shared_ptr<TTransport> socket =
        boost::shared_ptr<TSocket>(new TSocket(FLAGS_seed_node_ip, 9160));
    refresh_transport_ = boost::shared_ptr<TFramedTransport>(
        new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol = boost::shared_ptr<TBinaryProtocol>(
        new TBinaryProtocol(refresh_transport_));
    refresh_client_.reset(new CassandraClient(protocol));
    refresh_transport_->open();
  } catch (TTransportException& te) {
    printf("Exception: %s [%d]\n", te.what(), te.getType());
  }
}

RingCache::~RingCache() {
  refresh_transport_->close();
}

void RingCache::RefreshEndpointMap() {
  try {
    std::string keyspace = "offline_keyspace";
    std::vector<TokenRange> ring;
    //describe_ring return both normal and down nodes!!
    refresh_client_->describe_ring(ring, keyspace);
    LockGuard<boost::shared_mutex> lock(shared_mutex_);
    range_map_.clear();
    round_pos_.clear();
    cass_servers_.clear();
    for (auto& range : ring) {
      int64_t left = strtoll(range.start_token.c_str(), NULL, 10);
      int64_t right = strtoll(range.end_token.c_str(), NULL, 10);
      boost::shared_ptr<Range> r = 
          boost::shared_ptr<Range>(new Range(left, right));
      for (auto& host : range.endpoints) {
        range_map_.insert(
            std::pair<boost::shared_ptr<Range>,std::string>(r, host));
        cass_servers_.insert(host);
      }
      round_pos_.push_back(boost::shared_ptr<std::atomic<size_t>>(
                               new std::atomic<size_t>(0)));
    }
  } catch (InvalidRequestException& ire) {
    printf("Exception: %s [%s]\n", ire.what(), ire.why.c_str());
  }
}

void RingCache::RefreshClientPools() {
  LockGuard<boost::shared_mutex> lock(shared_mutex_);
  for (auto it = cass_servers_.begin(); it != cass_servers_.end(); ++it) {
    boost::shared_ptr<CassClientPool> pool =
        boost::shared_ptr<CassClientPool>(new CassClientPool(*it));
    client_pools_.insert(
        std::pair<std::string, boost::shared_ptr<CassClientPool>>(*it, pool));
  }
}

CassClientPool::Node* RingCache::GetClientNode(std::string row_key) {
  const char* byte = row_key.c_str();
  int64_t hash[2];
  MurmurHash3_x64_128(byte, row_key.size(), 0, hash);
  int64_t token = hash[0];
  int round_index = 0;
  boost::shared_lock_guard<boost::shared_mutex> shared_lock(shared_mutex_);
  for (auto it = range_map_.begin(); it != range_map_.end();
       it = range_map_.upper_bound(it->first)) {
    if (it->first->Contain(token)) {
      size_t bound = range_map_.count(it->first);
      int pos = GetRoundPos(round_index, bound);
      for (int i = 0; i < pos; ++i)
        ++it;
      return client_pools_[it->second]->AcquireNode();
    }
    ++round_index;
  }
}

void RingCache::ReturnClientNode(CassClientPool::Node* node) {
  boost::shared_lock_guard<boost::shared_mutex> shared_lock(shared_mutex_);
  client_pools_[node->cass_server]->ReturnNode(node);
}

size_t RingCache::GetRoundPos(int round_index, size_t bound) {
  for (;;) {
    size_t old = round_pos_[round_index]->load();
    size_t newval = (old + 1) % bound;
    if (round_pos_[round_index]->compare_exchange_weak(old, newval))
      return old;
  }
}

