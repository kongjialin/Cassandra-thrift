#include "Cassandra.h"

#include <algorithm>
#include <atomic>
#include <string>
#include <vector>

#include "common/base/timestamp.h"
#include "common/idl/message_types.h"
#include "random_message.h"
#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "thirdparty/thrift/protocol/TBinaryProtocol.h"
#include "thirdparty/thrift/transport/TSocket.h"
#include "thirdparty/thrift/transport/TTransportUtils.h"

DEFINE_int32(thread_count, 1, "Number of client threads");
DEFINE_int32(operation_count, 10000, "Count of operations");
DEFINE_string(operation_type, "INSERT",
             "Type of operation--INSERT, SELECT or MIX");
DEFINE_string(cass_servers_ip, "127.0.0.1",
             "Comma delimited Cassandra sever ip addresses");

using namespace std;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::org::apache::cassandra;

class CassandraStressClient {
 public:
  CassandraStressClient(std::string const& server_ip);
  virtual ~CassandraStressClient();
  void Setup(std::string const& server_ip);
  void StoreMessage(Message const& message, size_t index);
  void RetrieveMessage(std::string const& receiver, size_t index);
  void Operate(std::string operation_type, size_t loop_count);
  uint64_t GetSuccessCount() { return success_count_; }
  boost::scoped_array<double> latencies_;

 private:
  std::atomic<uint64_t> success_count_;
  boost::shared_ptr<CassandraClient> client_;
  boost::shared_ptr<TTransport> transport_;
};

CassandraStressClient::CassandraStressClient(std::string const& server_ip) {
  Setup(server_ip);
}

CassandraStressClient::~CassandraStressClient() {
  transport_->close();
}

void CassandraStressClient::Setup(std::string const& server_ip) {
  success_count_ = 0;
  latencies_.reset(new double[FLAGS_operation_count/FLAGS_thread_count]);
  try {
    boost::shared_ptr<TTransport> socket =
        boost::shared_ptr<TSocket>(new TSocket(server_ip, 9160));
    transport_ = boost::shared_ptr<TFramedTransport>(
        new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol =
        boost::shared_ptr<TBinaryProtocol>(new TBinaryProtocol(transport_));
    client_.reset(new CassandraClient(protocol));
    transport_->open();
    std::string query = "USE offline_keyspace";
    CqlResult result;
    client_->execute_cql3_query(result, query, Compression::NONE,
                                ConsistencyLevel::ONE);
  } catch (TTransportException& te) {
    printf("Exception: %s [%d]\n", te.what(), te.getType());
  } catch (InvalidRequestException& ire) {
    printf("Exception: %s [%s]\n", ire.what(), ire.why.c_str());
  }
}

void CassandraStressClient::StoreMessage(Message const& message,
                                         size_t index) {
  try {
    CqlResult result;
    std::string query = "INSERT INTO receiver_table(receiver_id, ts, msg_id,"
        "group_id, msg, sender_id) VALUES('" + message.receiver_id + "','" +
        message.timestamp + "','" + message.msg_id + "','" + message.group_id
        + "','" + message.msg + "','" + message.sender_id + "');";
    int64_t start_time = GetTimeStampInUs();
    client_->execute_cql3_query(result, query, Compression::NONE,
                                ConsistencyLevel::ONE);
    int64_t end_time = GetTimeStampInUs();
    double latency = (end_time - start_time) / 1000.0;
    latencies_[index] = latency;
  } catch (InvalidRequestException& ire) {
    printf("Exception in StoreMessage: %s [%s]\n", ire.what(), ire.why.c_str());
  } catch (TimedOutException& te) {
    printf("TimedOutException in StoreMessage: %s\n", te.what());
  } catch (SchemaDisagreementException& sde) {
    printf("Exception in StoreMessage: %s\n", sde.what());
  }
}

void CassandraStressClient::RetrieveMessage(std::string const& receiver,
                                            size_t index) {
  try {
    CqlResult result;
    std::string query = "SELECT * FROM receiver_table WHERE receiver_id = '"
                        + receiver + "';";
    int64_t start_time = GetTimeStampInUs();
    client_->execute_cql3_query(result, query, Compression::NONE,
                                ConsistencyLevel::ONE);
    int64_t end_time = GetTimeStampInUs();
    double latency = (end_time - start_time) / 1000.0;
    latencies_[index] = latency;
    if (result.rows.size())
      std::atomic_fetch_add(&success_count_, static_cast<uint64_t>(1));
  } catch (InvalidRequestException& ire) {
    printf("InvalidException in RetrieveMessage: %s [%s]\n", ire.what(),
           ire.why.c_str());
  } catch (UnavailableException& ue) {
    printf("UnavailableException in RetrieveMessage: %s\n", ue.what());
  } catch (TimedOutException& te) {
    printf("TimedOutException in RetrieveMessage: %s\n", te.what());
  } catch (SchemaDisagreementException& sde) {
    printf("SchemaDisagreeException in RetrieveMessage: %s\n", sde.what());
  }
}

void CassandraStressClient::Operate(std::string operation_type,
                                    size_t loop_count) {
  if (operation_type == "INSERT") {
    Message message;
    for (size_t i = 0; i < loop_count; ++i) {
      RandomMessage::GenerateMessage(&message);
      StoreMessage(message, i);//synchronous
    }
  } else if (operation_type == "SELECT") {
    for (size_t i = 0; i < loop_count; ++i) {
      std::string receiver;
      RandomMessage::GenerateString(&receiver);
      RetrieveMessage(receiver, i);//synchronous
    }
  }
}


double RankLatency(double rank, double* arr, size_t size) {
  size_t index = rank * size;
  if (index > 0)
    index--;
  return arr[index];
}

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, false);

  std::vector<std::string> cass_servers;
  std::string addresses = FLAGS_cass_servers_ip;
  int pos = 0;
  char comma = ',';
  cass_servers.push_back("");
  for (int i = 0; i < addresses.size(); ++i) {
    if (addresses[i] != comma) {
      cass_servers[pos] += addresses[i];
    } else {
      cass_servers.push_back("");
      ++pos;
    }
  }
  int server_count = cass_servers.size();
  printf("server count: %d\n", server_count);

  std::vector<CassandraStressClient*> client_objects;
  for (int i = 0; i < FLAGS_thread_count / server_count; ++i) {
    for (int j = 0; j < server_count; ++j) {
      CassandraStressClient* pclient = new CassandraStressClient(cass_servers[j]);
      client_objects.push_back(pclient);
    }
  }
  size_t loop_count = FLAGS_operation_count / FLAGS_thread_count;

  std::vector<boost::thread*> client_threads;
  int64_t stress_start_time = GetTimeStampInMs();
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    boost::thread* client = new boost::thread(&CassandraStressClient::Operate,
        client_objects[i], FLAGS_operation_type, loop_count);
    client_threads.push_back(client);
  }

  // Now join all
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    client_threads[i]->join();
    delete client_threads[i];
  }

  int64_t stress_end_time = GetTimeStampInMs();
  double* latency_array = new double[FLAGS_operation_count];
  uint64_t success_count = 0;
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    for (size_t j = 0; j < loop_count; ++j) {
      latency_array[i * loop_count + j] = client_objects[i]->latencies_[j];
    }      
    success_count += client_objects[i]->GetSuccessCount();
  }
  std::sort(latency_array, latency_array + FLAGS_operation_count);
  double total_latency = 0;
  for (size_t i = 1; i < FLAGS_operation_count - 1; ++i) {
    total_latency += latency_array[i];
  }
  LOG(INFO) << "Operation type: " << FLAGS_operation_type;
  LOG(INFO) << "Operation count: " << FLAGS_operation_count;
  LOG(INFO) << "Thread count:" << FLAGS_thread_count;
  LOG(INFO) << "QPS: "
      << FLAGS_operation_count * 1000 / (stress_end_time - stress_start_time);
  LOG(INFO) << "Success count: " << success_count;
  LOG(INFO) << "Average latency: " 
            << total_latency / (FLAGS_operation_count - 2) << " ms";
  LOG(INFO) << "Min latency: " << latency_array[0] << " ms";
  LOG(INFO) << "Max latency: " << latency_array[FLAGS_operation_count - 1]
            << " ms";
  LOG(INFO) << ".95 latency: "
            << RankLatency(0.95, latency_array, FLAGS_operation_count)
            << " ms";
  LOG(INFO) << ".99 latency: "
            << RankLatency(0.99, latency_array, FLAGS_operation_count)
            << " ms";
  LOG(INFO) << ".999 latency: "
            << RankLatency(0.999, latency_array, FLAGS_operation_count)
            << " ms";
  delete [] latency_array;

  for (int i = 0; i < FLAGS_thread_count; ++i) {
    delete client_objects[i];
  }
  return 0;
  
}
