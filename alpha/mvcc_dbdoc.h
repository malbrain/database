#pragma once
#define _GNU_SOURCE 1

typedef uint32_t (*FillFcn)(uint8_t* rec, uint32_t size, void *fillOpt);
typedef int (*Intfcnp)();

//  Pending Doc action

typedef enum {
    OpRaw = 0,		// not in a txn
    OpWrt,			// insert new doc
    OpDel,			// delete the doc
    OpRdr,			// update the doc
    OpMask = 7,
    OpCommit = 8	// version committed bit
} DocAction;

// document mvcc version header

struct Version {
  union {
    uint8_t verBase[8];
    struct Stopper_ {
      uint32_t verSize;  // total version size
      uint32_t offset;   // offset from beginning of doc header
    } stop[1];
  };
  uint64_t verNo;       // version number
  Timestamp commit[1];	// commit timestamp
  Timestamp pstamp[1];	// highest access timestamp
  Timestamp sstamp[1];	// successor's commit timestamp, or infinity
  ObjId txnId;
  uint8_t deferred;     // some keys have deferred constraints
  DbVector keys[1];     // vector of keys for this version
};

//	Document header for mvcc set reached by docId

struct MVCCDoc {
  struct Document doc[1];
  DbAddr prevAddr;		// previous doc-version set
  DbAddr nextAddr;		// next doc-version set
  uint32_t newestVer;	// offset of most recent committed version
  uint32_t pendingVer;	// offset of pending uncommitted version
  uint32_t verNo;       // next version number, increment on assignment
  uint32_t txnVer;      // txn slot sequence number
  DocAction op;         // pending document action/committing bit
  ObjId txnId;          // pending uncommitted txn ID
};

//  cursor/iterator handle extension

struct DbMvcc {
	ObjId txnId;
	DbAddr deDup[1];		// de-duplication set membership
	DbHandle hndl[1];	    // docStore DbHandle
	Timestamp reader[1];	// read timestamp
	enum TxnCC isolation;	// txn isolation mode
};

// catalog concurrency parameters

typedef struct {
	enum TxnCC isolation;
} CcMethod;

uint64_t mvcc_allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);
DbStatus mvcc_installKeys(Handle* idxHndls[1], Ver* ver);
DbStatus mvcc_removeKeys(Handle* idxHndls[1], Ver* ver, DbMmbr* mmbr,
                         DbAddr* slot);
