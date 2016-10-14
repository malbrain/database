#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

int cursorSize[8] = {0, 0, 0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), 0};
int maxType[8] = {0, 0, 0, 0, 0, MaxARTType, MAXBtree1Type, 0};

extern DbMap memMap[1];

void initialize() {
	memInit();
}

Status openDatabase(DbHandle *hndl, char *name, uint32_t nameLen, Params *params) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (hndl, 0, sizeof(DbHandle));

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->objSize = sizeof(Txn);

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return ERROR_createdatabase;

	*map->arena->type = DatabaseType;
	hndl->handle.bits = makeHandle(map, 0, 0);
	return OK;
}

Status openDocStore(DbHandle *hndl, DbHandle *dbHndl, char *path, uint32_t pathLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
DocStore *docStore;
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

	//  create the docStore and assign database txn idx

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	if ((map = createMap(parent, path, pathLen, 0, sizeof(DocStore), sizeof(ObjId), params)))
		docStore = docstore(map);
	else
		return ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!docStore->init) {
	  if (parent)
		docStore->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	  docStore->init = 1;
	}

	if (database)
		releaseHandle(database);

	map->arena->type[0] = DocStoreType;
	hndl->handle.bits = makeHandle(map, sizeof(DocHndl), MAX_blk);

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	return OK;
}

Status createIndex(DbHandle *hndl, DbHandle *docStore, ArenaType type, char *name, uint32_t nameLen, void *keySpec, uint16_t specSize, Params *params) {
Handle *docHndl = NULL;
uint32_t baseSize = 0;
DbMap *map, *parent;
Handle *idxHndl;
DbIndex *index;
Object *obj;
Status stat;

	memset (hndl, 0, sizeof(DbHandle));

	if (docStore->handle.bits)
	  if ((stat = bindHandle(docStore, &docHndl)))
		return stat;
	  else
		parent = docHndl->map;
	else
		parent = NULL;

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	switch (type) {
	case ARTreeIndexType:
		baseSize = sizeof(ArtIndex);
		break;
	case Btree1IndexType:
		baseSize = sizeof(Btree1Index);
		break;
	}

	map = createMap(parent, name, nameLen, 0, baseSize, sizeof(ObjId), params);

	if (!map) {
	  if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	  return ERROR_createindex;
	}

	hndl->handle.bits = makeHandle(map, 0, maxType[type]);
	idxHndl = getObj(memMap, hndl->handle);
	index = dbindex(map);

	if (!index->keySpec.addr) {
		index->keySpec.bits = allocBlk(map, specSize + sizeof(Object), false);
		obj = getObj(map, index->keySpec);

		memcpy(obj + 1, keySpec, specSize);
		obj->size = specSize;

		switch (type) {
		case ARTreeIndexType:
			artInit(idxHndl, params);
			break;

		case Btree1IndexType:
			btree1Init(idxHndl, params);
			break;
		}
	}

	*map->arena->type = type;

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	if (docHndl)
		releaseHandle(docHndl);

	return OK;
}

//	create new cursor

Status createCursor(DbHandle *hndl, DbHandle *idxHndl, ObjId txnId, char type) {
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

	hndl->handle.bits = makeHandle(index->map, cursorSize[*index->map->arena->type], 0);
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

Status returnCursor(DbHandle *hndl) {
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

Status positionCursor(DbHandle *hndl, uint8_t *key, uint32_t keyLen) {
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

Status nextKey(DbHandle *hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen) {
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

Status prevKey(DbHandle *hndl, uint8_t **key, uint32_t *keyLen, uint8_t *maxKey, uint32_t maxLen) {
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

Status nextDoc(DbHandle *hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen) {
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

Status prevDoc(DbHandle *hndl, Document **doc, uint8_t *maxKey, uint32_t maxLen) {
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

Status cloneHandle(DbHandle *newHndl, DbHandle *oldHndl) {
Handle *hndl;
Status stat;

	if ((stat = bindHandle(oldHndl, &hndl)))
		return stat;

	newHndl->handle.bits = makeHandle(hndl->map, hndl->xtraSize, hndl->maxType);
	return OK;
}

Status rollbackTxn(DbHandle *hndl, ObjId txnId);

Status commitTxn(DbHandle *hndl, ObjId txnId);

Status addIndexKeys(DbHandle *hndl) {
Handle *docHndl;
Status stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = installIndexes(docHndl);
	releaseHandle(docHndl);
	return stat;
}

Status addDocument(DbHandle *hndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
Handle *docHndl;
Status stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = storeDoc(docHndl, obj, objSize, result, txnId);
	releaseHandle(docHndl);
	return stat;
}

Status insertKey(DbHandle *hndl, uint8_t *key, uint32_t len) {
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
