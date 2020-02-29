//  mvcc api
#pragma once

#include "db.h"
#include "db_api.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbidx.h"

typedef enum {      
  objNone,
  objVer,
  objHndl,
  objTxn,
  objString,
  objErr,
} MVCCType;

uint32_t hashVal(uint8_t* src, uint32_t len);

typedef struct {
  union {
    void *object;
    uint64_t bits;
    uint64_t value;
  };    
  uint32_t count;
  MVCCType objType : 8;
  DbStatus status : 8;
} MVCCResult;

MVCCResult mvcc_BeginTxn(Params* params, ObjId nestedTxn);
MVCCResult mvcc_RollbackTxn(Params* params, uint64_t txnBits);
MVCCResult mvcc_CommitTxn(Params* params, uint64_t txnBits);

MVCCResult mvcc_UpdateDoc(DbHandle hndl[1], uint8_t* val, uint32_t valSize,
                          uint64_t docBits, ObjId txnId, uint32_t keyCount);

MVCCResult mvcc_InsertDoc(DbHandle hndl[1], uint8_t* val, uint32_t valSize,
                          ObjId txnId, uint32_t keyCount);

MVCCResult mvcc_ProcessKey(DbHandle hndl[1], DbHandle hndlIdx[1], Ver* prevVer, Ver* ver, ObjId docId, KeyValue *srcKey);

MVCCResult mvcc_OpenDocumentInterface (DbHandle dbHndl[1], char *name, uint32_t len, Params *params);
