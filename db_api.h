//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#include "db_error.h"

void initialize();

DbStatus openDatabase(DbHandle hndl[1], char *filePath, uint32_t pathLen, Params *params);
DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params);
DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], HandleType type, char *idxName, uint32_t nameLen, Params *params);
DbStatus cloneHandle(DbHandle hndl[1], DbHandle fromHndl[1]);
DbStatus addIndexes(DbHandle docHndl[1]);

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], ObjId txnId, Params *params);
DbStatus positionCursor(DbHandle hndl[1], CursorOp op, uint8_t *key, uint32_t keyLen);
DbStatus moveCursor(DbHandle hndl[1], CursorOp op);
DbStatus setCursorMax(DbHandle hndl[1], uint8_t *max, uint32_t maxLen);
DbStatus setCursorMin(DbHandle hndl[1], uint8_t *max, uint32_t minLen);

DbStatus keyAtCursor(DbHandle cursor[1], uint8_t **key, uint32_t *keyLen);
DbStatus docAtCursor(DbHandle cursor[1], Doc **doc);
DbStatus nextDoc(DbHandle cursor[1], Doc **doc);
DbStatus prevDoc(DbHandle cursor[1], Doc **doc);

uint64_t beginTxn(DbHandle dbHndl[1]);
DbStatus rollbackTxn(DbHandle dbHndl[1], uint64_t txnBits);
DbStatus commitTxn(DbHandle dbHnd[1], uint64_t txnBits);

DbStatus insertKey(DbHandle index[1], uint8_t *key, uint32_t len);

DbStatus addDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *objId, ObjId txnId);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit, bool dbArena);
Object *arenaObj(DbHandle arenaHndl[1], uint64_t addr, bool dbArena);

uint16_t keyGenerator(uint8_t *key, Doc *doc, Object *spec);
bool evalPartial(Doc *doc, Object *spec);

DbStatus createIterator(DbHandle hndl[1], DbHandle docHnd[1]);
Doc *iteratorNext(DbHandle hndl[1]);
Doc *iteratorPrev(DbHandle hndl[1]);
