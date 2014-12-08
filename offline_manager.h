#ifndef OFFLINE_MANAGER_H_
#define OFFLINE_MANAGER_H_

#include <vector>

#include "common/idl/message_types.h"
#include "ring_cache.h"
#include "thirdparty/boost/thread/thread.hpp"
#include "thirdparty/gflags/gflags.h"

class OfflineManager {
 public:
  static OfflineManager& GetInstance() {
    static OfflineManager instance;
    return instance;
  }
  ~OfflineManager();

  // public API
  void Store(std::tr1::function<void(bool success)>cob,
             const Message& message);
  void Retrieve(std::tr1::function<void(std::vector<Message> const& msgs)>cob,
                std::string const& receiver);

 private:
  OfflineManager();
  RingCache* ring_cache_;
};

#endif // OFFLINE_MANAGER_H_
