// Define the document API for mvcc and ACID transactions
// implemented for the database project.

#include "mvcc_dbapi.h"
#include "mvcc_dbdoc.h"

uint32_t hashVal(uint8_t *src, uint32_t len) {
  uint32_t val = 0;
  uint32_t b = 378551;
  uint32_t a = 63689;

  while (len) {
    val *= a;
    a *= b;
    if (len < sizeof(uint32_t))
      val += src[--len];
    else {
      len -= 4;
      val += *(uint32_t *)(src + len);
    }
  }

  return val;
}
