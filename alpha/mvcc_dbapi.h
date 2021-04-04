//  mvcc api

#pragma once

typedef struct DbMvcc DbMvcc;


//MVCCResult mvcc_BeginTxn(Params* params, ObjId nestedTxn);
//MVCCResult mvcc_RollbackTxn(Params* params, uint64_t txnBits);
//MVCCResult mvcc_CommitTxn(Txn *txn, Params* params);

MVCCResult mvcc_installNewVersion(Handle *docHndl, uint32_t valSize, ObjId* docId, uint16_t keyCnt);

MVCCResult mvcc_WriteDoc(Txn *txn, DbHandle dbHndl[1], ObjId *docId, uint32_t valSize, uint8_t *valBytes, uint16_t keyCount);

MVCCResult mvcc_ProcessKey(DbHandle hndl[1], DbHandle hndlIdx[1], Ver* prevVer, Ver* ver, DocId docId, KeyValue *srcKey);

// MVCCResult mvcc_OpenDocumentInterface (DbHandle dbHndl[1], char *name, uint32_t len, Params *params);
