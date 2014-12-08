#ifndef RANDOM_MESSAGE_H_
#define RANDOM_MESSAGE_H_

#include "common/idl/message_types.h"

namespace pushing {

class RandomMessage {
 public:
  static void GenerateMessage(Message* message);
  static void GenerateString(std::string* str);

 private:
  static std::string charset_;
  static int size_;
};


#endif // RANDOM_MESSAGE_H_
