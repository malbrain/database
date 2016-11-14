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
DbStatus deleteHandle(DbHandle hndl[1]);
DbStatus dropArena(DbHandle hndl[1], bool dropDefinitions);
DbStatus addIndexes(DbHandle docHndl[1]);

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], ObjId txnId, Params *params);
DbStatus positionCursor(DbHandle hndl[1], CursorOp op, char *key, uint32_t keyLen);
DbStatus moveCursor(DbHandle hndl[1], CursorOp op);
DbStatus setCursorMax(DbHandle hndl[1], char *max, uint32_t maxLen);
DbStatus setCursorMin(DbHandle hndl[1], char *min, uint32_t minLen);

DbStatus keyAtCursor(DbHandle cursor[1], char **key, uint32_t *keyLen);
DbStatus docAtCursor(DbHandle cursor[1], Doc **doc);
DbStatus nextDoc(DbHandle cursor[1], Doc **doc);
DbStatus prevDoc(DbHandle cursor[1], Doc **doc);

uint64_t beginTxn(DbHandle dbHndl[1]);
DbStatus rollbackTxn(DbHandle dbHndl[1], uint64_t txnBits);
DbStatus commitTxn(DbHandle dbHnd[1], uint64_t txnBits);

DbStatus insertKey(DbHandle index[1], char *key, uint32_t len);

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit, bool dbArena);

DbStatus allocDoc(DbHandle hndl[1], Doc **doc, uint32_t objSize);
DbStatus assignDoc(DbHandle hndl[1], Doc *doc, uint64_t txnBits);
DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId, uint64_t txnBits);
DbStatus deleteDoc(DbHandle hndl[1], uint64_t docBits, uint64_t txnBits);

uint16_t keyGenerator(char *key, Doc *doc, char *spec, uint32_t specLen);
bool evalPartial(Doc *doc, char *spec, uint32_t specLen);

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], uint64_t txnBits);
Doc *iteratorNext(DbHandle hndl[1]);
Doc *iteratorPrev(DbHandle hndl[1]);
