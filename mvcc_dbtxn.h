// mvcc transactions

//	Database transactions: DbAddr housed in database ObjId slots

// MVCC and TXN definitions for DATABASE project

//  Pending TXN action

#include "Hi-Performance-Timestamps/timestamps.h"

typedef enum {
	TxnNone = 0,		// not in a txn
	TxnInsert,			// insert new doc
	TxnDelete,			// delete the doc
	TxnUpdate,			// update the doc
	TxnCommit = 128		// version being committed
} TxnAction;

typedef enum {
	TxnDone,			// fully committed
	TxnGrow,			// reading and upserting
	TxnShrink,			// committing
	TxnRollback			// roll back
} TxnState;

typedef enum {
	TxnKill = 0,		// txn step removed
	TxnHndl,			// txn step is a docStore handle
	TxnRdr,				// txn step is a docId & version
	TxnWrt				// txn step is write
} TxnType;

typedef enum {
	TxnNotSpecified,
	TxnSnapShot,
	TxnReadCommitted,
	TxnSerializable
} TxnCC;

typedef struct {
	Timestamp reader[1];	// txn begin timestamp
	Timestamp commit[1];	// txn commit timestamp
	Timestamp pstamp[1];	// predecessor high water
	Timestamp sstamp[1];	// successor low water
    ObjId hndlId[1];        // current DocStore handle
    DbAddr rdrFrame[1];     // head read set DocIds
    DbAddr rdrFirst[1];     // first read set DocIds
    DbAddr wrtFrame[1];     // head write set DocIds
    DbAddr wrtFirst[1];     // first write set DocIds
    uint64_t nextTxn;       // nested txn next
	uint32_t wrtCount;		// size of write set
	union {
		struct {
			uint8_t isolation;
			volatile uint8_t state[1];
            uint16_t tsClnt;  // timestamp generator slot
        };
		TxnCC disp : 8;		  // display isolation mode in debugger
	};
} Txn;

Txn* mvcc_fetchTxn(ObjId txnId);

MVCCResult mvcc_findCursorVer(DbCursor* dbCursor, DbMap* map, DbMvcc* dbMvcc,
                       Ver* ver);
MVCCResult mvcc_addDocRdToTxn(ObjId txnId, DbMap*map, ObjId docId, Ver* ver, DbHandle hndl[1]);

MVCCResult mvcc_addDocWrToTxn(ObjId txnId, DbMap *docMap, ObjId* docId, int tot,
                              DbHandle hndl[1]);

MVCCResult mvcc_findDocVer(DbMap* docStore, Doc* doc, DbMvcc* dbMvcc);
