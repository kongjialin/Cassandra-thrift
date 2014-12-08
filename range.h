#ifndef RANGE_H_
#define RANGE_H_

#include <stdint.h>

class Range {
 public:
  Range(int64_t left, int64_t right);
  bool Contain(int64_t token);

 private:
  int64_t left_;
  int64_t right_;
};

#endif // RANGE_H_
