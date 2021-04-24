//	database API interface

#pragma once
#include "base64.h"
#include "db.h"

DbMap *hndlMap;

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

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t len,
                      Params *params);
DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t len, Params *params);
DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name,uint32_t len, Params *params);
DbStatus cloneHandle(DbHandle hndl[1], DbHandle fromHndl[1]);
DbStatus dropArena(DbHandle hndl[1], bool dropDefinitions);
DbStatus closeHandle(DbHandle dbHndl[1]);

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], Params *params);
DbStatus closeCursor(DbHandle dbHndl[1]);
DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key, uint32_t keyLen);
DbStatus keyAtCursor(DbHandle hndl[1], uint8_t **key, uint32_t *keyLen);
DbStatus moveCursor(DbHandle hndl[1], CursorOp op);

DbStatus insertKey(DbHandle hndl[1], uint8_t *keyBuff, uint32_t keylen, DocId docId, uint32_t maxLen);
DbStatus deleteKey(DbHandle hndl[1], uint8_t *key, uint32_t len, uint64_t suffix);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit,
  bool dbArena);

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, DocId *docId);
DbStatus deleteDoc(DbHandle hndl[1], DocId docId);
DbDoc *fetchDoc(DbHandle hndl[1], DocId docId);

void *docStoreObj(DbAddr addr);
