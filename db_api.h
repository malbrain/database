//	database API interface

#pragma once
#include "base64.h"
#include "db.h"

extern DbMap *hndlMap;

// document header in docStore
// next hdrs in set follow, up to docMin

typedef enum {
    VerRaw,
    VerMvcc
} DocType;

typedef struct {
  union {
    uint8_t base[4];
    uint32_t refCnt[1];
  };
  uint32_t docSize;
  DocType docType:8;
  DbAddr keyValues;
  DocId docId[1];
} DbDoc;

//  fields in basic key
// database docStore Arena extension

typedef struct {
  uint64_t docCount[1]; // count of active document ID
  uint32_t blkSize;     // standard new mvccDoc size
  uint16_t keyCnt;      // number of cached keys per version
  DocType docType:16;   //  docStore raw, or under mvcc
} DocStore;



//	Unique Key evaluation fcn

typedef bool(UniqCbFcn)(DbMap *map, DbCursor *dbCursor);

void initialize(void);

DbStatus openDatabase(DbHandle *hndl, char *name, uint32_t len,
                      Params *params);
DbStatus openDocStore(DbHandle *hndl, DbHandle dbHndl, char *name, uint32_t len, Params *params);
DbStatus createIndex(DbHandle *hndl, DbHandle docHndl, char *name,uint32_t len, Params *params);
DbStatus cloneHandle(DbHandle *hndl, DbHandle fromHndl);
DbStatus dropArena(DbHandle hndl, bool dropDefinitions);
DbStatus closeHandle(DbHandle dbHndl);

DbStatus createCursor(DbHandle *hndl, DbHandle idxHndl, Params *params);
DbStatus closeCursor(DbHandle dbHndl);
DbStatus positionCursor(DbHandle hndl, CursorOp op, void *key, uint32_t keyLen);
DbStatus keyAtCursor(DbHandle hndl, uint8_t **key, uint32_t *keyLen);
DbStatus moveCursor(DbHandle hndl, CursorOp op);

DbStatus insertKey(DbHandle hndl, uint8_t *keyBuff, uint32_t keylen, DocId docId, uint32_t maxLen);
DbStatus deleteKey(DbHandle hndl, uint8_t *key, uint32_t len, uint64_t suffix);

uint64_t arenaAlloc(DbHandle arenaHndl, uint32_t size, bool zeroit,
  bool dbArena);

DbStatus storeDoc(DbHandle hndl, void *obj, uint32_t objSize, DocId *docId);
DbStatus deleteDoc(DbHandle hndl, DocId docId);
DbDoc *fetchDoc(DbHandle hndl, DocId docId);

void *docStoreObj(DbAddr addr);
