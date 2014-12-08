#include "cass_client_pool.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/thrift/protocol/TBinaryProtocol.h"
#include "thirdparty/thrift/transport/TSocket.h"
#include "thirdparty/thrift/transport/TTransportUtils.h"

DEFINE_int32(num_cass_clients, 10,
             "number of Cassandra clients initiated in the client object pool");

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::org::apache::cassandra;

CassClientPool::CassClientPool(std::string cass_server) {
  num_clients_ = FLAGS_num_cass_clients;
  cass_server_ = cass_server;
  ConstructPool(this);
} 

void CassClientPool::ConstructPool(CassClientPool* pool) {
  head_ = new Node(pool);
  Node* p = NULL;
  for (int i = 1; i < num_clients_; ++i) {
    p = new Node(pool);
    p->next = head_.load();
    head_ = p;
  }
}

CassClientPool::Node::Node(CassClientPool* pool) {
  cass_server = pool->cass_server_; 
  next = NULL;
  boost::shared_ptr<TTransport> socket;
  boost::shared_ptr<TProtocol> protocol;

  try {
    socket = boost::shared_ptr<TSocket>(
        new TSocket(cass_server, 9160));
    transport = boost::shared_ptr<TFramedTransport>(
        new TFramedTransport(socket));
    protocol = boost::shared_ptr<TBinaryProtocol>(
        new TBinaryProtocol(transport));
    client = boost::shared_ptr<CassandraClient>(new CassandraClient(protocol));
   
    transport->open();
    std::string query = "USE offline_keyspace;";
    CqlResult result;
    client->execute_cql3_query(result, query, Compression::NONE,
                               ConsistencyLevel::ONE); 
  } catch (TTransportException& te) {
    LOG(INFO) << "TTransportException: " << te.what()
              << " [" << te.getType() << "]";
  } catch (InvalidRequestException& ire) {
    LOG(INFO) << "InvalidRequestException: " << ire.what()
              << " [" << ire.why.c_str() << "]";
  }
}

CassClientPool::Node* CassClientPool::AcquireNode() {
  for (;;) {
    Node* h = head_.load();
    if (h == NULL) {
      std::atomic_fetch_add(&num_clients_, static_cast<size_t>(1));
      return new Node(this); 
    }
    Node* next = h->next.load();
    if (head_.compare_exchange_weak(h, next))
      return h;
  }
}

void CassClientPool::ReturnNode(Node* node) {
  for (;;) {
    Node* h = head_.load();
    node->next = h;
    if (head_.compare_exchange_weak(h, node))
      return;
  }
}

CassClientPool::~CassClientPool() {
  Node* h;
  for (size_t i = 0; i < num_clients_; ++i) {
    h = head_.load();
    h->transport->close();
    head_ = h->next.load();
    delete h;
  }
}

