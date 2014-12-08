#ifndef LOCK_GUARD_H_
#define LOCK_GUARD_H_

#include "thirdparty/boost/thread.hpp"

namespace pushing {

template<typename Mutex>
class LockGuard {
 public:
  explicit LockGuard(Mutex& m) : mutex_(m) {
    mutex_.lock();
  }
  ~LockGuard() {
    mutex_.unlock();
  }

 private:
  Mutex& mutex_;
};


