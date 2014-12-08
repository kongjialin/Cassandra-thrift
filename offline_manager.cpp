#include "offline_manager.h"

#include "cass_client_pool.h"
#include "thirdparty/cass/Cassandra.h"
#include "thirdparty/glog/logging.h"

using namespace ::apache::thrift;
using namespace ::org::apache::cassandra;

OfflineManager::OfflineManager() {
  ring_cache_ = &RingCache::GetInstance();
  // Todo: create a thread specially for refreshing endpointmap(and maybe clientpools)
}

OfflineManager::~OfflineManager() {
}

void OfflineManager::Store(std::tr1::function<void(bool success)>cob,
                           const Message& message) {
  std::string row_key = message.receiver_id;
  CassClientPool::Node* pnode = ring_cache_->GetClientNode(row_key);
  try {
    CqlResult result;
    std::string query = "INSERT INTO receiver_table(receiver_id, ts, msg_id, "
        "group_id, msg, sender_id) VALUES('" + message.receiver_id + "','" +
        message.timestamp + "','" + message.msg_id + "','" + message.group_id
        + "','" + message.msg + "','" + message.sender_id + "');";
    pnode->client->execute_cql3_query(result, query, Compression::NONE,
                                      ConsistencyLevel::ONE);
    ring_cache_->ReturnClientNode(pnode);
    cob(true);
  } catch (InvalidRequestException& ire) {
    printf("Exception in OfflineManager::Store: %s, [%s]\n", ire.what(),
           ire.why.c_str());
  } catch (TimedOutException& te) {
    printf("Exception in OfflineManager::Store: %s\n", te.what());
  } catch (SchemaDisagreementException& sde) {
    printf("Exception in OfflineManager::Store: %s\n", sde.what());
  }
}

void OfflineManager::Retrieve(
    std::tr1::function<void(std::vector<Message> const& msgs)>cob,
    std::string const& receiver) {
  CassClientPool::Node* pnode = ring_cache_->GetClientNode(receiver);
  try {
    CqlResult result;
    std::string query = "SELECT * FROM receiver_table WHERE receiver_id = '"
                        + receiver + "';";
    pnode->client->execute_cql3_query(result, query, Compression::NONE,
                                      ConsistencyLevel::ONE);
    Message message;
    std::vector<Message> msgs;
    for (int i = 0; i < result.rows.size(); ++i) {
      message.__set_receiver_id(result.rows[i].columns[0].value);
      message.__set_timestamp(result.rows[i].columns[1].value);
      message.__set_msg_id(result.rows[i].columns[2].value);
      message.__set_group_id(result.rows[i].columns[3].value);
      message.__set_msg(result.rows[i].columns[4].value);
      message.__set_sender_id(result.rows[i].columns[5].value);
      msgs.push_back(message);
    }
    ring_cache_->ReturnClientNode(pnode);
    cob(msgs);
  } catch (InvalidRequestException& ire) {
    printf("Exception in OfflineManager::Retrieve: %s, [%s]\n", ire.what(),
           ire.why.c_str());
  } catch (UnavailableException& ue) {
    printf("Exception in OfflineManager::Retrieve: %s\n", ue.what());
  } catch (TimedOutException& te) {
    printf("Exception in OfflineManager::Retrieve: %s\n", te.what());
  } catch (SchemaDisagreementException& sde) {
    printf("Exception in OfflineManager::Retrieve: %s\n", sde.what());
  }
}

