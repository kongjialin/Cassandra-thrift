#ifndef CASS_CLIENT_POOL_H_
#define CASS_CLIENT_POOL_H_

#include "Cassandra.h"

#include <stddef.h>

#include <atomic>
#include <vector>

#include "thirdparty/thrift/transport/TTransportUtils.h"

using namespace ::apache::thrift::transport;
using namespace ::org::apache::cassandra;

class CassClientPool {
 public:
  struct Node {
    boost::shared_ptr<CassandraClient> client;
    std::atomic<Node*> next;
    boost::shared_ptr<TTransport> transport;
    std::string cass_server;

    Node(CassClientPool* pool);
  };

  CassClientPool(std::string cass_server);
  ~CassClientPool();
  
  Node* AcquireNode();
  void ReturnNode(Node* node);
  std::string cass_server_;

 private:
  void ConstructPool(CassClientPool* pool);
  void IncreaseNumClients();

  std::atomic<Node*> head_;
  std::atomic<size_t> num_clients_;
};

#endif // CASS_CLIENT_POOL_H_
