#pragma once
#define _GNU_SOURCE 1

#include "db.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_object.h"
#include "db_cursor.h"
#include "db_handle.h"
#include "db_frame.h"
#include "Hi-Performance-Timestamps/timestamps.h"

#include <stdint.h>
#include <stddef.h>

// MVCC and TXN definitions for DATABASE project

//  Pending TXN action

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
	TxnDoc				// txn step is a docId
} TxnType;

typedef enum {
	TxnNotSpecified,
	TxnSnapShot,
	TxnReadCommitted,
	TxnSerializable
} TxnCC;

typedef enum {
	Concurrency = UserParams + 1,
} UserParmSlots;

// document version header

typedef struct {
  struct Stopper_ {
    uint32_t verSize;  // total version size
    uint32_t offset;   // offset from beginning of doc header
  };
  uint64_t verNo;		// version number
  Timestamp commit[1];	// commit timestamp
  Timestamp pstamp[1];	// highest access timestamp
  Timestamp sstamp[1];	// successor's commit timestamp, or infinity
  uint8_t deferred;		// some keys have deferred constraints
  DbVector keys[1];     // vector of keys for this version
} Ver;


//	Document header for set pointed to by docId

	typedef struct {
		ObjId docId;			// document ID
		ObjId txnId;			// pending uncommitted txn ID
		DbAddr ourAddr;			// address of this version set
		DbAddr prevAddr;		// previous doc-version set
		DbAddr nextAddr;		// next doc-version set
		TxnAction op;			// pending document action/committing bit
		uint32_t lastVer;		// offset of most recent version
		uint32_t setSize;		// offset of end of last version
} Doc;

//  cursor/iterator handle extension

typedef struct {
	ObjId txnId;
	
	DbAddr deDup[1];		// de-duplication set membership
	DbHandle docHndl[1];	// docStore DbHandle
	Timestamp reader[1];	// read timestamp
	TxnCC isolation;		// txn isolation mode
} DbMvcc;

//	catalog concurrency parameters

typedef struct {
	TxnCC isolation;
} CcMethod;

// database docStore indexing extension

typedef struct {
	uint32_t idxMax;
	DbHandle idxHndls[];		// array of index handles
} DocIdx;

//	Database transactions: DbAddr housed in database ObjId slots

typedef struct {
	Timestamp reader[1];	// txn begin timestamp
	Timestamp commit[1];	// txn commit timestamp
	Timestamp pstamp[1];	// predecessor high water
	Timestamp sstamp[1];	// successor low water
    ObjId hndlId[1];        // current DocStore handle
    DbAddr rdrFrame[1];     // head read set DocIds
    DbAddr rdrFirst[1];     // first read set DocIds
    DbAddr docFrame[1];     // head write set DocIds
    DbAddr docFirst[1];     // first write set DocIds
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

DbStatus installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr, DbAddr* slot);

DbStatus findCursorVer(DbCursor* dbCursor, DbMap* map, DbMvcc* dbMvcc, Ver* ver[1]);
uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus addDocRdToTxn(ObjId txnId, ObjId docId, Ver* ver, uint64_t hndlBits);
DbStatus addDocWrToTxn(ObjId txnId, ObjId docId, Ver* ver, Ver* prevVer, uint64_t hndlBits);
Txn* fetchTxn(ObjId txnId);


DbStatus findDocVer(DbMap* docStore, Doc* doc, DbMvcc* dbMvcc, Ver *ver[1]);
