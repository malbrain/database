//	database API interface

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

#include "db_error.h"

void initialize();

Status openDatabase(DbHandle *hndl, char *name, uint32_t nameLen, Params *params);
Status openDocStore(DbHandle *hndl, DbHandle *dbHndl, char *name, uint32_t nameLen, Params *params);
Status createIndex(DbHandle *hndl, DbHandle *docHndl, ArenaType type, char *idxName, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params);
Status createCursor(DbHandle *hndl, DbHandle *idxHndl, ObjId txnId, char type);
Status cloneHandle(DbHandle *hndl, DbHandle *fromhndl);

uint64_t beginTxn(DbHandle *dbHndl);
Status rollbackTxn(DbHandle *dbHndl, ObjId txnId);
Status commitTxn(DbHandle *dbHndl, ObjId txnId);

Status addDocument(DbHandle *hndl, void *obj, uint32_t objSize, ObjId *objId, ObjId txnId);
Status insertKey(DbHandle *index, uint8_t *key, uint32_t len);
Status nextDoc(DbHandle *hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen);
Status prevDoc(DbHandle *hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen);
Status addIndexKeys(DbHandle *dochndl);

Status nextKey(DbHandle *hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen);
Status prevKey(DbHandle *hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen);

Status positionCursor(DbHandle *hndl, uint8_t *key, uint32_t keyLen);
