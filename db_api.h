//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#include "db_error.h"

void initialize(void);

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t len, Params *params);
DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t len, Params *params);
DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name, uint32_t len, Params *params);
DbStatus cloneHandle(DbHandle hndl[1], DbHandle fromHndl[1]);
DbStatus deleteHandle(DbHandle hndl[1]);
DbStatus dropArena(DbHandle hndl[1], bool dropDefinitions);
DbStatus addIndexes(DbHandle docHndl[1]);

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], Params *params);
DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key, uint32_t keyLen);
DbStatus moveCursor(DbHandle hndl[1], CursorOp op);
DbStatus setCursorMax(DbHandle hndl[1], void *max, uint32_t maxLen);
DbStatus setCursorMin(DbHandle hndl[1], void *min, uint32_t minLen);

DbStatus keyAtCursor(DbHandle cursor[1], void **key, uint32_t *keyLen);
DbStatus docAtCursor(DbHandle cursor[1], Doc **doc);
DbStatus nextDoc(DbHandle cursor[1], Doc **doc);
DbStatus prevDoc(DbHandle cursor[1], Doc **doc);

uint64_t beginTxn(DbHandle dbHndl[1], Params *param);
DbStatus rollbackTxn(DbHandle dbHndl[1], uint64_t txnBits);
DbStatus commitTxn(DbHandle dbHnd[1], uint64_t txnBits);

DbStatus insertKey(DbHandle index[1], void *key, uint32_t len);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit, bool dbArena);

DbStatus allocDoc(DbHandle hndl[1], Doc **doc, uint32_t objSize);
DbStatus assignDoc(DbHandle hndl[1], Doc *doc, uint64_t txnBits);
DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId, uint64_t txnBits);
DbStatus deleteDoc(DbHandle hndl[1], uint64_t docBits, uint64_t txnBits);

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], Params *params);
Ver *iteratorSeek(DbHandle hndl[1], uint64_t objBits,  uint32_t *offset);
Ver *iteratorNext(DbHandle hndl[1], uint32_t *offset);
Ver *iteratorPrev(DbHandle hndl[1], uint32_t *offset);

//	callback functions

uint16_t keyGenerator(char *key, Ver *ver, ParamVal *spec);
DbAddr compileKeys(DbMap *map, Params *params);
bool evalPartial(Ver *ver, ParamVal *spec);
