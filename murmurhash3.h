// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.
// Modified by Kong, Jialin

#ifndef MURMURHASH3_H_
#define MURMURHASH3_H_

#include <stdint.h>

void MurmurHash3_x64_128 ( const void * key, int len, int32_t seed, void * out );

#endif // MURMURHASH3_H_

