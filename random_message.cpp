#include "random_message.h"

#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>

using std::string;

string RandomMessage::charset_ = "0123456789"
                                 "abcdefghijklmnopqrstuvwxyz"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "!@#$%^&*(),./;:";
int RandomMessage::size_ = charset_.length();

// generate message for write(insert)
void RandomMessage::GenerateMessage(Message* message) {
  srand(time(0));

  int length = (rand() % 9) + 8;
  for (int i = 0; i < length; ++i) {
    receiver_id += charset_[rand() % size_];
  }

  length = (rand() % 9) + 8;
  string sender_id;
  for (int i = 0; i < length; ++i) {
    sender_id += charset_[rand() % size_];
  }

  struct timeval tv;
  gettimeofday(&tv, NULL);
  int64_t result = tv.tv_sec;
  result *= 1000000;
  result += tv.tv_usec;
  string timestamp = std::to_string(result);

  string msg_id = std::to_string(rand() % 100000000);

  string group_id = std::to_string(rand() % 100000000);

  length = 50;//(rand() % 11) + 10;
  string msg;
  for (int i = 0; i < length; ++i) {
    msg += charset_[rand() % size_];
  }

  message->__set_receiver_id(receiver_id);
  message->__set_timestamp(timestamp);
  message->__set_msg_id(msg_id);
  message->__set_group_id(group_id);
  message->__set_msg(msg);
  message->__set_sender_id(sender_id);
}

// generate row key for read(select)
void RandomMessage::GenerateString(string* str) {
  srand(time(0));

  int length = (rand() % 9) + 8;
  for (int i = 0; i < length; ++i) {
    (*str) += charset_[rand() % size_];
  }
}


