#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

int cursorSize[8] = {0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), 0};
int maxType[8] = {0, 0, 0, MaxARTType, MAXBtree1Type, 0};

extern DbMap memMap[1];

void initialize() {
	memInit();
}

Status openDatabase(DbHandle hndl[1], char *name, uint32_t nameLen, Params *params) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (hndl, 0, sizeof(DbHandle));

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->arenaType = DatabaseType;
	arenaDef->objSize = sizeof(Txn);

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return ERROR_createdatabase;

	*map->arena->type = DatabaseType;
	hndl->handle.bits = makeHandle(map, 0, 0, DatabaseType);
	return OK;
}

Status openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
DocArena *docArena;
uint64_t *inUse;
DataBase *db;
DbAddr *addr;
int idx, jdx;
Status stat;

	memset (hndl, 0, sizeof(DbHandle));

	if (dbHndl)
	  if ((stat = bindHandle(dbHndl, &database)))
		return stat;

	parent = database->map, db = database(parent);

	//  create the docArena and assign database txn idx

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	if ((map = createMap(parent, DocStoreType, name, nameLen, 0, sizeof(DocArena), sizeof(ObjId), params)))
		docArena = docarena(map);
	else
		return ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!docArena->init) {
	  if (parent)
		docArena->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	  docArena->init = 1;
	}

	if (database)
		releaseHandle(database);

	map->arena->type[0] = DocStoreType;
	hndl->handle.bits = makeHandle(map, sizeof(DocStore), MAX_blk, DocStoreType);

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	return OK;
}

Status createIndex(DbHandle hndl[1], DbHandle docStore[1], HandleType type, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params) {
Handle *parentHndl = NULL;
uint32_t baseSize = 0;
DbMap *map, *parent;
DbIndex *dbIndex;
Handle *index;
Object *obj;
Status stat;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(docStore, &parentHndl)))
		return stat;

	parent = parentHndl->map;
	lockLatch(parent->arenaDef->nameTree->latch);

	switch (type) {
	case ARTreeIndexType:
		baseSize = sizeof(ArtIndex);
		break;
	case Btree1IndexType:
		baseSize = sizeof(Btree1Index);
		break;
	}

	map = createMap(parent, type, name, nameLen, 0, baseSize, sizeof(ObjId), params);

	if (!map) {
	  unlockLatch(parent->arenaDef->nameTree->latch);
	  return ERROR_createindex;
	}

	hndl->handle.bits = makeHandle(map, 0, maxType[type], type);
	index = getObj(memMap, hndl->handle);
	dbIndex = dbindex(map);

	if (!dbIndex->keySpec.addr) {
		dbIndex->keySpec.bits = allocBlk(map, specSize + sizeof(Object), false);
		obj = getObj(map, dbIndex->keySpec);

		memcpy(obj + 1, keySpec, specSize);
		obj->size = specSize;

		switch (type) {
		case ARTreeIndexType:
			artInit(index, params);
			break;

		case Btree1IndexType:
			btree1Init(index, params);
			break;
		}
	}

	*map->arena->type = type;
	unlockLatch(parent->arenaDef->nameTree->latch);

	if (parentHndl)
		releaseHandle(parentHndl);

	return OK;
}

//	create new cursor

Status createCursor(DbHandle hndl[1], DbHandle idxHndl[1], ObjId txnId, char type) {
uint64_t timestamp;
DbCursor *cursor;
Handle *index;
Status stat;
Txn *txn;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(idxHndl, &index)))
		return stat;

	if (txnId.bits) {
		txn = fetchIdSlot(index->map->db, txnId);
		timestamp = txn->timestamp;
	} else
		timestamp = allocateTimestamp(index->map->db, en_reader);

	hndl->handle.bits = makeHandle(index->map, cursorSize[*index->map->arena->type], 0, CursorType);
	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	switch (*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artNewCursor(index, (ArtCursor *)cursor, timestamp, txnId, type);
		break;

	case Btree1IndexType:
		stat = btree1NewCursor(index, (Btree1Cursor *)cursor, timestamp, txnId, type);
		break;
	}

	releaseHandle(index);
	return stat;
}

Status returnCursor(DbHandle hndl[1]) {
DbCursor *cursor;
Status stat = OK;
Handle *index;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	lockLatch (hndl->handle.latch);
	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	switch (*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artReturnCursor(index, cursor);
		break;

	case Btree1IndexType:
		stat = btree1ReturnCursor(index, cursor);
		break;
	}

	releaseHandle(index);
	returnHandle(index);

	hndl->handle.bits = 0;
	return stat;
}

//	position cursor on a key

Status positionCursor(DbHandle hndl[1], uint8_t *key, uint32_t keyLen) {
DbCursor *cursor;
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	if ((stat = dbPositionCursor(index->map, cursor, key, keyLen)))
		cursor->foundKey = false;

	releaseHandle(index);
	return cursor->foundKey ? OK : CURSOR_notfound;
}

//	iterate cursor to next key
//	return zero on Eof

Status nextKey(DbHandle hndl[1], uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen) {
DbCursor *cursor;
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	stat = dbNextKey(index->map, cursor, maxKey, maxLen);

	if (key)
		*key = cursor->key;
	if (keyLen)
		*keyLen = cursor->keyLen;

	releaseHandle(index);
	return stat;
}

//	iterate cursor to prev key
//	return zero on Bof

Status prevKey(DbHandle hndl[1], uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen) {
DbCursor *cursor;
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	stat = dbPrevKey(index->map, cursor, maxKey, maxLen);

	if (key)
		*key = cursor->key;
	if (keyLen)
		*keyLen = cursor->keyLen;

	releaseHandle(index);
	return stat;
}

//	iterate cursor to next document

Status nextDoc(DbHandle hndl[1], Document **doc, uint8_t *maxKey, uint32_t maxLen) {
DbCursor *cursor;
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	stat = dbNextDoc(index->map, cursor, maxKey, maxLen);

	if (!stat)
		*doc = cursor->doc;

	releaseHandle(index);
	return stat;
}

//	iterate cursor to previous document

Status prevDoc(DbHandle hndl[1], Document **doc, uint8_t *maxKey, uint32_t maxLen) {
DbCursor *cursor;
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)((Handle *)getObj(memMap, hndl->handle) + 1);

	stat = dbPrevDoc(index->map, cursor, maxKey, maxLen);

	if (!stat)
		*doc = cursor->doc;

	releaseHandle(index);
	return stat;
}

Status cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
Handle *hndl;
Status stat;

	if ((stat = bindHandle(oldHndl, &hndl)))
		return stat;

	newHndl->handle.bits = makeHandle(hndl->map, hndl->xtraSize, hndl->maxType, hndl->hndlType);
	return OK;
}

Status rollbackTxn(DbHandle hndl[1], ObjId txnId);

Status commitTxn(DbHandle hndl[1], ObjId txnId);

Status addIndexKeys(DbHandle hndl[1]) {
Handle *docHndl;
Status stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = installIndexes(docHndl);
	releaseHandle(docHndl);
	return stat;
}

Status addDocument(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
Handle *docHndl;
Status stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = storeDoc(docHndl, obj, objSize, result, txnId);
	releaseHandle(docHndl);
	return stat;
}

Status insertKey(DbHandle hndl[1], uint8_t *key, uint32_t len) {
Handle *index;
Status stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	switch (*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artInsertKey(index, key, len);
		break;

	case Btree1IndexType:
		stat = btree1InsertKey(index, key, len, 0, Btree1_indexed);
		break;
	}

	releaseHandle(index);
	return stat;
}
