// Define the document API for mvcc and ACID transactions
// implemented for the database project.

#include "mvcc.h"

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

typedef struct {
  DbHandle dbHndl[1];
  DbHandle docHndl[1];
} MVCC_Interface;

/*
MVCCResult mvcc_OpenDocumentInterface(DbHandle hndl[1], char *name, uint32_t len, Params *params) {
  Handle *dbHndl = bindHandle(hndl, Hndl_database);
  MVCCResult result = {.value = 0, .count = 0, .objType = objHndl, .status = DB_OK};


  }
  */