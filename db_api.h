//	database API interface

#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "db.h"
#include "db_cursor.h"
#include "db_error.h"
#include "db_redblack.h"

DbMap *hndlMap;

// document header in docStore
// next hdrs in set follow, up to docMin

typedef enum {
    DocRaw,
    DocMvcc
} DocType;

struct Document {
  union {
    uint8_t base[4];
    uint32_t refCnt[1];
  };
  uint32_t docMin;      // offset of begining of client area
  uint32_t hndlIdx;     // linear segmented handle Catalog index
  DocType docType;
  DbAddr ourAddr;
  ObjId docId;
};

#define HandleAddr(dbHndl) fetchIdSlot(hndlMap, dbHndl->hndlId)
#define MapAddr(handle) (DbMap *)(db_memObj(handle->mapAddr))
#define ClntAddr(handle) getObj(MapAddr(handle), handle->clientAddr)

// database docStore Arena extension

typedef struct {
  uint64_t docCount[1]; // count of active documents
  uint32_t blkSize;     // standard new mvccDoc size
  uint16_t keyCnt;      // number of cached keys per version
  DocType docType:16;   //  docStore raw, or under mvcc
} DocStore;

//	Global Index data structure after DbArena object

typedef struct {
  uint64_t numKeys[1];  // number of keys in index
  DbAddr keySpec;
  char binaryFlds;  // keys made with field values
  bool uniqueKeys;  // keys made with field values
} DbIndex;

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

DbStatus insertKey(DbHandle hndl[1], uint8_t *key, uint32_t len,
                   uint32_t sfxLen);
DbStatus deleteKey(DbHandle hndl[1], uint8_t *key, uint32_t len);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit,
                    bool dbArena);

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId);
DbStatus deleteDoc(DbHandle hndl[1], ObjId docId);
DbStatus fetchDoc(DbHandle hndl[1], void **doc, ObjId docId);

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], Params *params);
DbStatus moveIterator(DbHandle hndl[1], IteratorOp op, void **doc,
                      ObjId *docId);

void *docStoreObj(DbAddr addr);
