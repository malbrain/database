//  mvcc api

#pragma once

//  Pending Doc action

typedef enum {
    OpRaw = 0,		// not in a doc
    OpWrt,			// insert new doc
    OpDel,			// delete the doc
    OpRdr,			// update the doc
    OpMask = 7,
    OpCommit = 8	// version committed bit
} DocAction;

// document mvcc version header

typedef struct Version {
  union {
    struct Stop {
      uint32_t verSize;  // total version size
      uint32_t offset;   // offset from beginning of doc header
    } stop[1];  
    uint8_t verBase[sizeof(struct Stop)];
  };
  uint64_t verNo;       // version number
  Timestamp commit[1];	// commit timestamp
  Timestamp pstamp[1];	// highest access timestamp
  Timestamp sstamp[1];	// successor's commit timestamp, or infinity
  ObjId txnId;
  DocId docId;
  uint8_t deferred;     // some keys have deferred constraints
  DbVector keys[1];     // vector of keys for this version
} Ver;

//	Document base for mvcc version set reached by docId

typedef struct {
  DbDoc dbDoc[1];     // std document header
  DbAddr prevAddr;		// previous document set
  DbAddr nextAddr;		// next newer document version set
  uint32_t commitVer;	// offset of most recent committed version
  uint32_t pendingVer;	// offset of pending uncommitted version
  ObjId txnId;      // txn slot sequence number
  uint64_t verNo;       // next version number, increment on assignment
  DocAction op;         // pending document action/committing bit
  DocId docId;          // pending uncommitted txn ID
} Doc;


MVCCResult mvcc_beginTxn(Params* params, ObjId nestedTxn);
MVCCResult mvcc_rollbackTxn(Params* params, uint64_t txnBits);
MVCCResult mvcc_commitTxn(Txn *txn, Params* params);

MVCCResult mvcc_installNewVersion(Handle *docHndl, uint32_t valSize, DocId* docId, uint16_t keyCnt);

MVCCResult mvcc_writeDoc(Txn *txn, DbHandle dbHndl, DocId *docId, uint32_t valSize, uint8_t *valBytes, uint16_t keyCnt);

MVCCResult mvcc_processKey(DbHandle hndl, DbHandle hndlIdx, Ver* prevVer, Ver* ver, DbKeyValue *srcKey);

// MVCCResult mvcc_OpenDocumentInterface (DbHandle dbHndl[1], char *name, uint32_t len, Params *params);                          