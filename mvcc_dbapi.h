//  mvcc api

#include "db.h"
#include "db_api.h"
#include "mvcc_dbdoc.h"

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
    uint64_t value;
  };    
  uint32_t count;
  MVCCType objType : 8;
  DbStatus status : 8;
} MVCCResult;

MVCCResult mvcc_BeginTxn(Params* params, uint64_t* nestedTxnBits);
MVCCResult mvcc_RollbackTxn(Params* params, uint64_t* txnBits);
MVCCResult mvcc_CommitTxn(Params* params, uint64_t* txnBits);

MVCCResult mvcc_UpdateDoc(Handle* docHndl, uint8_t* val, uint32_t valSize,
                          uint64_t docBits, ObjId txnId, uint16_t keyCount,
                          uint8_t* keyList);
MVCCResult mvcc_InsertDoc(Handle* docHndl, uint8_t* val, uint32_t valSize,
                          ObjId txnId, uint16_t keyCount, uint8_t *keyList);

