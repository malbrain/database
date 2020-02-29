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

// document mvcc version header

typedef struct {
  union {
    uint8_t verBase[8];
    struct Stopper_ {
      uint32_t verSize;  // total version size
      uint32_t offset;   // offset from beginning of doc header
    };
  };
  Timestamp commit[1];	// commit timestamp
  Timestamp pstamp[1];	// highest access timestamp
  Timestamp sstamp[1];	// successor's commit timestamp, or infinity
  DbVector keys[1];     // vector of keys for this version
  uint8_t deferred;     // some keys have deferred constraints
  uint8_t overWritten;  // newer version committed
} Ver;

//	Document header for mvcc set reached by docId

typedef struct {
  struct Document doc[1];
  ObjId txnId;			// pending uncommitted txn ID
  DbAddr prevAddr;		// previous doc-version set
  DbAddr nextAddr;		// next doc-version set
  enum TxnAction op;	// pending document action/committing bit
  uint32_t lastVer;		// offset of most recent version
  uint32_t setSize;		// offset of end of last/stopper version
  uint64_t verNo;       // next version number, increment on commit
} Doc;

//  cursor/iterator handle extension

typedef struct {
	ObjId txnId;
	DbAddr deDup[1];		// de-duplication set membership
	DbHandle docHndl[1];	// docStore DbHandle
	Timestamp reader[1];	// read timestamp
	enum TxnCC isolation;		// txn isolation mode
} DbMvcc;

//	catalog concurrency parameters

typedef struct {
	enum TxnCC isolation;
} CcMethod;

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr, DbAddr* slot);

