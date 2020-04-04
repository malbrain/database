// mvcc transactions

#pragma once

//	Database transactions: DbAddr housed in database ObjId slots

// MVCC and TXN definitions for DATABASE project

enum TxnState {
	TxnDone,			// fully committed
	TxnGrowing,			// reading and upserting
	TxnCommitting,		// committing
	TxnCommitted,		// committed
	TxnRollback			// roll back
};

enum TxnStep {
	TxnRaw = 0,		// txn step raw read/write
	TxnIdx,			// txn step is a Catalog handle idx
	TxnRdr,			// txn step is a docId & version
	TxnWrt			// txn step is write
};

enum TxnCC {
	TxnNotSpecified,
	TxnSnapShot,
	TxnReadCommitted,
	TxnSerializable
};

struct Transaction {
	Timestamp reader[1];	// txn begin timestamp
	Timestamp commit[1];	// txn commit timestamp
	Timestamp pstamp[1];	// predecessor high water
	Timestamp sstamp[1];	// successor low water
    DbAddr rdrFrame[1];     // head read set DocIds
    DbAddr rdrFirst[1];     // first read set DocIds
    DbAddr wrtFrame[1];     // head write set DocIds
    DbAddr wrtFirst[1];     // first write set DocIds
    ObjId nextTxn, txnId;	// nested txn next, this txn
	uint32_t wrtCount;		// size of write set
    uint32_t txnVer;		// txn slot sequence number
	uint32_t hndlIdx;		// current DocStore handle idx
    union {
		struct {
			volatile uint8_t latch[1];
			uint8_t isolation:3;
			uint8_t state : 5;
            uint16_t tsClnt;  // timestamp generator slot
        };
		enum TxnCC disp : 8;		  // display isolation mode in debugger
	};
};

Txn* mvcc_fetchTxn(ObjId txnId);
void mvcc_releaseTxn(Txn* txn);

MVCCResult mvcc_findCursorVer(DbCursor* dbCursor, DbMap* map, DbMvcc* dbMvcc,
                       Ver* ver);
MVCCResult mvcc_addDocRdToTxn(Txn* txn, Ver* ver);
MVCCResult mvcc_addDocWrToTxn(Txn* txn, Doc* doc);

MVCCResult mvcc_findDocVer(Txn *txn, DbMap* docStore, Doc* doc, DbMvcc* dbMvcc);
