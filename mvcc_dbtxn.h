// mvcc transactions

#pragma once

// MVCC and TXN definitions for DATABASE project

DbMap* txnMap, memMap[1];

typedef enum {
	TxnDone,			// fully committed
	TxnGrowing,			// reading and upserting
	TxnCommitting,		// committing
	TxnCommitted,		// committed
	TxnRollback			// roll back
} TxnState;

typedef enum {
	TxnRaw = 0,	// step raw read/write
	TxnMap,		// step is a Catalog idx map
	TxnVer,		// step is a version number
	TxnRdr,		// step is a docId & next is version
	TxnWrt		// step is write
} TxnStep;

typedef enum {
	TxnNotSpecified,
	TxnSnapShot,
	TxnReadCommitted,
	TxnSerializable
} TxnCC;

//  cursor/iterator handle extension

struct DbMvcc {
	ObjId txnId;
	DbAddr deDup[1];	// de-duplication set membership
	DbHandle hndl[1];	// docStore DbHandle
	Timestamp reader[1];// read timestamp
	TxnCC isolation;	// txn isolation mode
};

// catalog concurrency parameters

typedef struct {
	TxnCC isolation;
} CcMethod;

//	Database transactions: DbAddr housed in database ObjId frames

typedef struct Transaction {
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
			volatile uint8_t latch[1];
			uint8_t isolation:3;
			uint8_t state : 5;
      uint16_t tsClnt;  // timestamp generator slot
    };
		TxnCC disp : 8;		  // display isolation mode in debugger
	};
} Txn;

DbStatus mvcc_scan1(Txn* txn);
DbStatus mvcc_scan2(Txn* txn);
DbStatus mvcc_scan3(Txn* txn);
DbStatus mvcc_scan4(Txn* txn);
DbStatus mvcc_scan5(Txn* txn);

Txn* mvcc_fetchTxn(ObjId txnId);
void mvcc_releaseTxn(Txn* txn);

//MVCCResult mvcc_findCursorVer(DbCursor* dbCursor, DbMap* map, DbMvcc* dbMvcc, Ver* ver);
MVCCResult mvcc_addDocRdToTxn(Txn* txn, Handle *docHndl, Ver* ver);
MVCCResult mvcc_addDocWrToTxn(Txn* txn, Handle *docHndl, Doc* doc);

Ver * mvcc_getVersion(DbMap *map, Doc *doc, uint64_t verNo);
