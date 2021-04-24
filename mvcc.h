#pragma once

#include "base64.h"
#include "db.h"
#include "db_api.h"
#include "db_index.h"
#include "Hi-Performance-Timestamps/timestamps.h"

// MVCC and TXN definitions for DATABASE project

typedef enum {
	TxnDone,			// fully committed
	TxnGrowing,			// reading and upserting
	TxnCommitting,		// committing
	TxnCommitted,		// committed
	TxnRollback			// roll back
} TxnState;

typedef enum {
	TxnRaw = 0,		// txn step raw read/write
	TxnMap,			// txn step is a Catalog handle idx
	TxnRdr,			// txn step is a docId & version
	TxnWrt,			// txn step is write of version
	TxnVer			// txn step is verNo
} TxnStep;

typedef enum {
	TxnNotSpecified,
	TxnSnapShot,
	TxnReadCommitted,
	TxnSerializable
} TxnCC;

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
 

uint32_t hashVal(uint8_t* src, uint32_t len);


// catalog concurrency parameters

typedef struct {
	TxnCC isolation;
} CcMethod;

//	Database transactions: DbAddr housed in database ObjId frames

typedef struct {
	Timestamp reader[1];	// txn begin timestamp
	Timestamp commit[1];	// txn commit timestamp
	Timestamp pstamp[1];	// predecessor high water
	Timestamp sstamp[1];	// successor low water
  DbAddr rdrFrame[1];   // head read set DocIds
  DbAddr rdrFirst[1];   // first read set DocIds
  DbAddr wrtFrame[1];   // head write set DocIds
  DbAddr wrtFirst[1];   // first write set DocIds
  ObjId nextTxn, txnId;	// nested txn next, this txn
	ObjId hndlId;				  // current docStore handle
	uint32_t wrtCount;		// size of write set
  uint32_t txnVer;		// txn slot sequence number
  union {
		struct {
			uint8_t latch[1];
			uint8_t isolation:3;
			uint8_t state : 5;
      uint16_t tsClnt;  // timestamp generator slot
    };
		TxnCC disp : 8;		  // display isolation mode in debugger
	};
} Txn;

typedef struct {
	ObjId txnId;
	DbAddr deDup[1];	// de-duplication set membership
	DbHandle hndl[1];	// docStore DbHandle
	Timestamp reader[1];// read timestamp
	TxnCC isolation;	// txn isolation mode
} DbMvcc;

Txn* mvcc_fetchTxn(ObjId txnId);
void mvcc_releaseTxn(Txn* txn);
 
#include "mvcc_dbapi.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbtxn.h"
#include "mvcc_dbidx.h"
#include "mvcc_dbssn.h"

