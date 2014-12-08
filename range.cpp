#include "range.h"

Range::Range(int64_t left, int64_t right) : left_(left), right_(right){
}

bool Range::Contain(int64_t token) {
  if (left_ >= right_) {
    if (token > left_)
      return true;
    else
      return right_ >= token;
  } else {
    return token > left_ && right_ >= token;
  }
}

