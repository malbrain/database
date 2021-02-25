//  mvcc api

#pragma once

#include "db.h"
#include "db_api.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_cursor.h"
#include "db_handle.h"
#include "db_frame.h"

typedef struct DbMvcc DbMvcc;
typedef struct Version Ver;
typedef struct MVCCDoc Doc;
typedef struct Transaction Txn;

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
    uint64_t value;
  };
  union {
    uint32_t count;
    uint32_t size;
  };
  MVCCType objType : 8;
  DbStatus status : 16;
} MVCCResult;

#include "Hi-Performance-Timestamps/timestamps.h"
#include "mvcc_dbtxn.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbidx.h"
 
MVCCResult mvcc_BeginTxn(Params* params, ObjId nestedTxn);
MVCCResult mvcc_RollbackTxn(Params* params, uint64_t txnBits);
MVCCResult mvcc_CommitTxn(Txn *txn, Params* params);

MVCCResult mvcc_installNewDocVer(Handle *docHndl, uint32_t valSize,
                                 ObjId* docId);
MVCCResult mvcc_WriteDoc(Txn *txn, DbHandle dbHndl[1], ObjId *docId, uint32_t valSize,
  uint8_t *valBytes, uint16_t keyCount);

// MVCCResult mvcc_ProcessKey(DbHandle hndl[1], DbHandle hndlIdx[1], Ver* prevVer, Ver* ver, ObjId docId, KeyValue *srcKey);

// MVCCResult mvcc_OpenDocumentInterface (DbHandle dbHndl[1], char *name, uint32_t len, Params *params);
