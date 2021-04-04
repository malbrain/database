#pragma once

// #include "Hi-Performance-Timestamps/timestamps.h"

#include "base64.h"
#include "db.h"


typedef enum {      
  objNone,
  objDoc,
  objVer,
  objHndl,
  objTxn,
  objString,
  objRec,
  objErr,
} MVCCType;

uint32_t hashVal(uint8_t* src, uint32_t len);

typedef struct {
  union {
    void *object;
    uint8_t *buff;
    uint64_t bits;
  };
  uint64_t value;
  uint32_t count;
  uint32_t size;
  MVCCType objType;
  DbStatus status;
  DbAddr dbobject;
} MVCCResult;

#include "Hi-Performance-Timestamps/timestamps.h"
 
#include "mvcc_dbdoc.h"
#include "mvcc_dbtxn.h"
#include "mvcc_dbapi.h"
//#include "mvcc_dbidx.h"
//#include "mvcc_dbssn.h"

