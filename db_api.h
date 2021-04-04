//	database API interface

#pragma once
#include "base64.h"
#include "db.h"


// database docStore Arena extension

typedef struct {
  uint64_t docCount[1]; // count of active documents
  uint32_t blkSize;     // standard new mvccDoc size
  uint16_t keyCnt;      // number of cached keys per version
  DocType docType:16;   //  docStore raw, or under mvcc
} DocStore;

typedef enum { IterNone, IterLeftEof, IterRightEof, IterPosAt } IterState;

//	Iterator operations

typedef enum {
  IterNext = 'n',
  IterPrev = 'p',
  IterBegin = 'b',
  IterEnd = 'e',
  IterSeek = 's',
} IteratorOp;

//	Unique Key evaluation fcn

typedef struct {
  uint32_t refCnt[1];
  uint16_t keyLen; 	    // len of base key
  uint16_t vecIdx;		// index in document key vector
  uint16_t suffix;      // number of suffix ending bytes
  uint64_t keyHash;     // used by MVCC if key changed
  ObjId payLoad;        // docId key comes from
  uint8_t unique : 1;   // index is unique
  uint8_t deferred : 1;	// uniqueness deferred
  uint8_t binaryKeys : 1;	// uniqueness deferred
  uint8_t bytes[];		// bytes of the key with suffix
} KeyValue;

typedef bool(UniqCbFcn)(DbMap *map, DbCursor *dbCursor);

void initialize(void);

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t len,
                      Params *params);
DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name,
                      uint32_t len, Params *params);
DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name,
                     uint32_t len, Params *params);
DbStatus cloneHandle(DbHandle hndl[1], DbHandle fromHndl[1]);
DbStatus dropArena(DbHandle hndl[1], bool dropDefinitions);
DbStatus closeHandle(DbHandle dbHndl[1]);

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], Params *params);
DbStatus closeCursor(DbHandle dbHndl[1]);
DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key,
                       uint32_t keyLen);
DbStatus keyAtCursor(DbHandle hndl[1], uint8_t **key, uint32_t *keyLen);
DbStatus moveCursor(DbHandle hndl[1], CursorOp op);

DbStatus insertKey(DbHandle hndl[1], KeyValue *kv);
DbStatus deleteKey(DbHandle hndl[1], uint8_t *key, uint32_t len);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit,
  bool dbArena);

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, DocId *docId);
DbStatus deleteDoc(DbHandle hndl[1], DocId docId);
DbStatus fetchDoc(DbHandle hndl[1], void **doc, DocId docId);

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], Params *params);
DbStatus moveIterator(DbHandle hndl[1], IteratorOp op, void **doc,
                      DocId *docId);

void *docStoreObj(DbAddr addr);
