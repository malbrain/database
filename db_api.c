#include "db.h"
#include "db_txn.h"
#include "db_malloc.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

int cursorSize[8] = {0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), 0};
int maxType[8] = {0, 0, 0, MaxARTType, MAXBtree1Type, 0};

void initialize() {
	memInit();
}

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit, bool dbArena) {
Handle *arena;
uint64_t bits;
DbStatus stat;
DbMap *map;

	if ((stat = bindHandle(arenaHndl, &arena)))
		return 0;

	map = arena->map;

	if (dbArena)
		map = map->db;

	bits = allocBlk(map, size, zeroit);
	releaseHandle(arena);
	return bits;
}

Object *arenaObj(DbHandle arenaHndl[1], uint64_t bits, bool dbArena) {
Handle *arena;
DbStatus stat;
Object *obj;
DbAddr addr;
DbMap *map;

	if ((stat = bindHandle(arenaHndl, &arena)))
		return NULL;

	map = arena->map;

	if (dbArena)
		map = map->db;

	addr.bits = bits;
	obj = getObj(map, addr);
	releaseHandle(arena);
	return obj;
}

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t nameLen, Params *params) {
ArenaDef arenaDef[1];
DbMap *map;

	memset (hndl, 0, sizeof(DbHandle));

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->arenaType = Hndl_database;
	arenaDef->objSize = sizeof(Txn);

	map = openMap(NULL, name, nameLen, arenaDef);

	if (!map)
		return DB_ERROR_createdatabase;

	*map->arena->type = Hndl_database;
	*hndl->handle = makeHandle(map, 0, 0, Hndl_database);
	return DB_OK;
}

DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
DocStore *docStore;
DocArena *docArena;
uint64_t *inUse;
DataBase *db;
DbAddr *addr;
int idx, jdx;
DbStatus stat;
Handle *ds;

	memset (hndl, 0, sizeof(DbHandle));

	if (dbHndl)
	  if ((stat = bindHandle(dbHndl, &database)))
		return stat;

	parent = database->map, db = database(parent);

	//  create the docArena and assign database txn idx

	if (parent)
		lockLatch(parent->arenaDef->nameTree->latch);

	if ((map = createMap(parent, Hndl_docStore, name, nameLen, 0, sizeof(DocArena), sizeof(ObjId), params)))
		docArena = docarena(map);
	else
		return DB_ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!*map->arena->type)
	  if (parent)
		docArena->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	if (database)
		releaseHandle(database);

	map->arena->type[0] = Hndl_docStore;
	*hndl->handle = makeHandle(map, sizeof(DocStore), MAX_blk, Hndl_docStore);

	ds = db_memObj(*hndl->handle);
	docStore = (DocStore *)(ds + 1);
	initLock(docStore->indexes->lock);

	if (parent)
		unlockLatch(parent->arenaDef->nameTree->latch);

	return DB_OK;
}

DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], HandleType type, char *name, uint32_t nameLen, Params *params) {
Handle *parentHndl = NULL;
uint32_t baseSize = 0;
DbMap *map, *parent;
DbIndex *dbIndex;
Handle *index;
DbStatus stat;
Object *obj;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(docHndl, &parentHndl)))
		return stat;

	parent = parentHndl->map;
	lockLatch(parent->arenaDef->nameTree->latch);

	switch (type) {
	case Hndl_artIndex:
		baseSize = sizeof(ArtIndex);
		break;
	case Hndl_btree1Index:
		baseSize = sizeof(Btree1Index);
		break;
	}

	map = createMap(parent, type, name, nameLen, 0, baseSize, sizeof(ObjId), params);

	if (!map) {
	  unlockLatch(parent->arenaDef->nameTree->latch);
	  return DB_ERROR_createindex;
	}

	*hndl->handle = makeHandle(map, 0, maxType[type], type);

	if (*map->arena->type)
		goto createXit;

	dbIndex = dbindex(map);
	dbIndex->noDocs = params[NoDocs].boolVal;

	map->arenaDef->partialAddr = params[IdxKeyPartial].int64Val;
	map->arenaDef->specAddr = params[IdxKeySpec].int64Val;

	index = db_memObj(*hndl->handle);

	switch (type) {
	  case Hndl_artIndex:
		artInit(index, params);
		break;

	  case Hndl_btree1Index:
		btree1Init(index, params);
		break;
	}

	*map->arena->type = type;

createXit:
	unlockLatch(parent->arenaDef->nameTree->latch);

	if (parentHndl)
		releaseHandle(parentHndl);

	return DB_OK;
}

//	create new cursor

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], ObjId txnId, Params *params) {
uint64_t timestamp;
DbCursor *cursor;
Handle *index;
DbStatus stat;
Txn *txn;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(idxHndl, &index)))
		return stat;

	if (txnId.bits) {
		txn = fetchIdSlot(index->map->db, txnId);
		timestamp = txn->timestamp;
	} else
		timestamp = allocateTimestamp(index->map->db, en_reader);

	*hndl->handle = makeHandle(index->map, cursorSize[*index->map->arena->type], 0, Hndl_cursor);
	cursor = (DbCursor *)(index + 1);
	cursor->noDocs = params[NoDocs].boolVal;
	cursor->txnId.bits = txnId.bits;
	cursor->ts = timestamp;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artNewCursor(index, (ArtCursor *)cursor);
		break;

	case Hndl_btree1Index:
		stat = btree1NewCursor(index, (Btree1Cursor *)cursor);
		break;
	}

	releaseHandle(index);
	return stat;
}

DbStatus destoryCursor(DbHandle hndl[1]) {
DbCursor *cursor;
DbStatus stat;
Handle *index;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	lockAddr (hndl->handle);
	cursor = (DbCursor *)(index + 1);

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artReturnCursor(index, cursor);
		break;

	case Hndl_btree1Index:
		stat = btree1ReturnCursor(index, cursor);
		break;
	}

	releaseHandle(index);
	returnHandle(index);

	*hndl->handle = 0;
	return DB_OK;
}

//	position cursor on a key

DbStatus positionCursor(DbHandle hndl[1], CursorOp op, uint8_t *key, uint32_t keyLen) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)(index + 1);

	switch (op) {
	  case OpFind:
		stat = dbFindKey(cursor, index->map, key, keyLen, false);
		break;
	  case OpOne:
		stat = dbFindKey(cursor, index->map, key, keyLen, true);
		break;
	}

	releaseHandle(index);
	return stat;
}

//	move cursor

DbStatus moveCursor(DbHandle hndl[1], CursorOp op) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)(index + 1);

	switch (op) {
	  case OpLeft:
		if (cursor->minKeyLen)
			stat = dbFindKey(cursor, index->map, cursor->minKey, cursor->minKeyLen, false);
		else
			stat = dbLeftKey(cursor, index->map);
		break;
	  case OpRight:
		if (cursor->maxKeyLen)
			stat = dbFindKey(cursor, index->map, cursor->maxKey, cursor->maxKeyLen, false);
		else
			stat = dbRightKey(cursor, index->map);
		break;
	  case OpNext:
		stat = dbNextKey(cursor, index->map);
		break;
	  case OpPrev:
		stat = dbPrevKey(cursor, index->map);
		break;
	}

	releaseHandle(index);
	return stat;
}

//	return cursor key

DbStatus keyAtCursor(DbHandle *hndl, uint8_t **key, uint32_t *keyLen) {
DbCursor *cursor;
DbStatus stat;

	cursor = (DbCursor *)((Handle *)db_memObj(*hndl->handle) + 1);

	switch (cursor->state) {
	case CursorPosAt:
		if (key)
			*key = cursor->key;

		if (keyLen)
			*keyLen = cursor->userLen;

		return DB_OK;
	}

	return DB_CURSOR_notpositioned;
}

DbStatus docAtCursor(DbHandle *hndl, Doc **doc) {
DbCursor *cursor;
uint32_t keyLen;
DbStatus stat;

	cursor = (DbCursor *)((Handle *)db_memObj(*hndl->handle) + 1);
	keyLen = cursor->keyLen;

	switch (cursor->state) {
	case CursorPosAt:
		if (doc)
			*doc = cursor->doc;

		return DB_OK;
	}

	return DB_CURSOR_notpositioned;
}

//	iterate cursor to next document

DbStatus nextDoc(DbHandle hndl[1], Doc **doc) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)(index + 1);

	stat = dbNextDoc(cursor, index->map);

	if (!stat && doc)
		*doc = cursor->doc;

	releaseHandle(index);
	return stat;
}

//	iterate cursor to previous document

DbStatus prevDoc(DbHandle hndl[1], Doc **doc) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)(index + 1);

	stat = dbPrevDoc(cursor, index->map);

	if (!stat && doc)
		*doc = cursor->doc;

	releaseHandle(index);
	return stat;
}

DbStatus cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
Handle *hndl;
DbStatus stat;

	if ((stat = bindHandle(oldHndl, &hndl)))
		return stat;

	*newHndl->handle = makeHandle(hndl->map, hndl->xtraSize, hndl->maxType, hndl->hndlType);
	return DB_OK;
}

uint64_t beginTxn(DbHandle hndl[1]) {
Handle *database;
DbStatus stat;
ObjId txnId;
Txn *txn;

	if ((stat = bindHandle(hndl, &database)))
		return stat;

	txnId.bits = allocObjId(database->map, database->list, 0);
	txn = fetchIdSlot(database->map, txnId);
	txn->timestamp = allocateTimestamp(database->map, en_reader);
	return txnId.bits;
}

DbStatus rollbackTxn(DbHandle hndl[1], uint64_t txnBits) {
	return DB_OK;
}

DbStatus commitTxn(DbHandle hndl[1], uint64_t txnBits) {
	return DB_OK;
}

DbStatus addIndexes(DbHandle hndl[1]) {
Handle *docHndl;
DbStatus stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = installIndexes(docHndl);
	releaseHandle(docHndl);
	return stat;
}

DbStatus addDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
Handle *docHndl;
DbStatus stat;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	stat = storeDoc(docHndl, obj, objSize, result, txnId);
	releaseHandle(docHndl);
	return stat;
}

DbStatus insertKey(DbHandle hndl[1], uint8_t *key, uint32_t len) {
Handle *index;
DbStatus stat;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artInsertKey(index, key, len);
		break;

	case Hndl_btree1Index:
		stat = btree1InsertKey(index, key, len, 0, Btree1_indexed);
		break;
	}

	releaseHandle(index);
	return stat;
}

DbStatus setCursorMax(DbHandle hndl[1], uint8_t *max, uint32_t maxLen) {
DbCursor *cursor;

	cursor = (DbCursor *)((Handle *)db_memObj(*hndl->handle) + 1);
	cursor->maxKey = max;
	cursor->maxKeyLen = maxLen;
	return DB_OK;
}

DbStatus setCursorMin(DbHandle hndl[1], uint8_t *min, uint32_t minLen) {
DbCursor *cursor;

	cursor = (DbCursor *)((Handle *)db_memObj(*hndl->handle) + 1);
	cursor->minKey = min;
	cursor->minKeyLen = minLen;
	return DB_OK;
}
