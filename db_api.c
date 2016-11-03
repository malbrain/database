#include "db.h"
#include "db_txn.h"
#include "db_malloc.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

int cursorSize[8] = {0, 0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), 0};
int maxType[8] = {0, 0, 0, 0, MaxARTType, MAXBtree1Type, 0};

void initialize() {
	memInit();
}

extern char hndlInit[1];
extern DbMap *hndlMap;
extern char *hndlPath;

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

DbObject *arenaObj(DbHandle arenaHndl[1], uint64_t bits, bool dbArena) {
Handle *arena;
DbStatus stat;
DbObject *obj;
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

DbStatus dropArena(DbHandle hndl[1], bool dropDefinitions) {
Handle *arena;
DbStatus stat;

  if ((stat = bindHandle(hndl, &arena)))
	return stat;

  dropMap(arena->map);
  releaseHandle(arena);
  return stat;
}

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t nameLen, Params *params) {
ArenaDef *arenaDef;
RedBlack *rbEntry;
DbMap *map;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(*hndlInit & ALIVE_BIT))
		initHndlMap(NULL, 0, NULL, 0, true);

	rbEntry = createArenaDef(hndlMap, name, nameLen, params);
	arenaDef = getObj(hndlMap, rbEntry->payLoad);

	memset (arenaDef, 0, sizeof(ArenaDef));
	arenaDef->initSize = params[InitSize].int64Val;
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->useTxn = params[UseTxn].boolVal;
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->arenaType = Hndl_database;
	arenaDef->objSize = sizeof(Txn);

	//  create the database

	if ((map = arenaRbMap(hndlMap, rbEntry, arenaDef)))
		*map->arena->type = Hndl_database;
	else
		return DB_ERROR_createdatabase;

	hndl->hndlBits = makeHandle(map, sizeof(Txn), ObjIdType, Hndl_database);
	return DB_OK;
}

DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
DocStore *docStore;
DocArena *docArena;
ArenaDef *arenaDef;
RedBlack *rbEntry;
uint64_t *inUse;
DataBase *db;
DbAddr *addr;
int idx, jdx;
DbStatus stat;
Handle *ds;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(dbHndl, &database)))
		return stat;

	parent = database->map, db = database(parent);

	//  create the docArena and assign database txn idx

	rbEntry = createArenaDef(parent, name, nameLen, params);

	arenaDef = getObj(parent->db, rbEntry->payLoad);
	arenaDef->initSize = params[InitSize].int64Val;
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->useTxn = params[UseTxn].boolVal;
	arenaDef->baseSize = sizeof(DocArena);
	arenaDef->arenaType = Hndl_docStore;
	arenaDef->objSize = sizeof(ObjId);

	if ((map = arenaRbMap(parent, rbEntry, arenaDef)))
		docArena = docarena(map);
	else
		return DB_ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!*map->arena->type)
	  if (parent)
		docArena->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	releaseHandle(database);

	map->arena->type[0] = Hndl_docStore;
	hndl->hndlBits = makeHandle(map, sizeof(DocStore), MAX_blk, Hndl_docStore);

	ds = getHandle(hndl);
	docStore = (DocStore *)(ds + 1);
	initLock(docStore->indexes->lock);

	return DB_OK;
}

DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], HandleType type, char *name, uint32_t nameLen, Params *params) {
uint32_t baseSize = 0;
DbMap *map, *parent;
Handle *parentHndl;
ArenaDef *arenaDef;
RedBlack *rbEntry;
DbIndex *dbIndex;
Handle *index;
DbStatus stat;
DbObject *obj;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(docHndl, &parentHndl)))
		return stat;

	parent = parentHndl->map;

	switch (type) {
	case Hndl_artIndex:
		baseSize = sizeof(ArtIndex);
		break;
	case Hndl_btree1Index:
		baseSize = sizeof(Btree1Index);
		break;
	}

	//  create the index

	rbEntry = createArenaDef(parent, name, nameLen, params);

	arenaDef = getObj(parent->db, rbEntry->payLoad);
	arenaDef->initSize = params[InitSize].int64Val;
	arenaDef->onDisk = params[OnDisk].boolVal;
	arenaDef->useTxn = params[UseTxn].boolVal;
	arenaDef->objSize = sizeof(ObjId);
	arenaDef->baseSize = baseSize;
	arenaDef->arenaType = type;

	if (!(map = arenaRbMap(parent, rbEntry, arenaDef)))
	  return DB_ERROR_createindex;

	hndl->hndlBits = makeHandle(map, 0, maxType[type], type);

	if (*map->arena->type)
		goto createXit;

	dbIndex = dbindex(map);
	dbIndex->noDocs = params[NoDocs].boolVal;

	map->arenaDef->partialAddr = params[IdxKeyPartial].int64Val;
	map->arenaDef->specAddr = params[IdxKeySpec].int64Val;

	index = getHandle(hndl);

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
	releaseHandle(parentHndl);
	return DB_OK;
}

//	create new cursor

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], ObjId txnId, Params *params) {
Handle *index, *cursorHndl;
uint64_t timestamp;
DbCursor *cursor;
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

	hndl->hndlBits = makeHandle(index->map, cursorSize[*index->map->arena->type], 0, Hndl_cursor);

	cursorHndl = getHandle(hndl);
	cursor = (DbCursor *)(cursorHndl + 1);
	cursor->noDocs = params[NoDocs].boolVal;
	cursor->txnId.bits = txnId.bits;
	cursor->ts = timestamp;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artNewCursor((ArtCursor *)cursor, index->map);
		break;

	case Hndl_btree1Index:
		stat = btree1NewCursor((Btree1Cursor *)cursor, index->map);
		break;
	}

	releaseHandle(index);
	return stat;
}

DbStatus closeCursor(DbHandle hndl[1]) {
DbCursor *cursor;
DbStatus stat;
Handle *index;

	if ((stat = bindHandle(hndl, &index)))
		return stat;

	cursor = (DbCursor *)(index + 1);
	stat = dbCloseCursor (cursor, index->map);

	releaseHandle(index);
	closeHandle(hndl);
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

	cursor = (DbCursor *)(getHandle(hndl) + 1);

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

	cursor = (DbCursor *)(getHandle(hndl) + 1);
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

	newHndl->hndlBits = makeHandle(hndl->map, hndl->xtraSize, hndl->maxType, hndl->hndlType);
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

// install document in docId slot

DbStatus installDoc(Handle *docHndl, Doc *doc, Txn *txn) {
DbStatus stat;
DbAddr *slot;

	slot = fetchIdSlot(docHndl->map, doc->docId);
	slot->bits = doc->addr.bits;

	if ((stat = installIndexes(docHndl)))
		return stat;

	//	add keys for the document
	//	enumerate children (e.g. indexes)

	installIndexKeys(docHndl, doc);

	if (txn)
		addDocToTxn(docHndl->map->db, txn, doc->docId, TxnAddDoc); 

	return DB_OK;
}

DbStatus assignDoc(DbHandle hndl[1], Doc *doc, uint64_t txnBits) {
DocArena *docArena;
Handle *docHndl;
Txn *txn = NULL;
DbStatus stat;
ObjId docId;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	docArena = docarena(docHndl->map);

	if ((doc->txnId.bits = txnBits))
		txn = fetchIdSlot(docHndl->map->db, doc->txnId);

	doc->docId.bits = allocObjId(docHndl->map, docHndl->list, docArena->docIdx);

	stat = installDoc(docHndl, doc, txn);
	releaseHandle(docHndl);

	return stat;
}

DbStatus allocDoc(DbHandle hndl[1], Doc **doc, uint32_t objSize) {
Handle *docHndl;
DbStatus stat;
DbAddr addr;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	if ((addr.bits = allocObj(docHndl->map, docHndl->list->free, docHndl->list->tail, -1, objSize + sizeof(Doc), false)))
		*doc = getObj(docHndl->map, addr);
	else
		return DB_ERROR_outofmemory;

	memset (*doc, 0, sizeof(Doc));
	(*doc)->addr.bits = addr.bits;
	(*doc)->size = objSize;

	releaseHandle(docHndl);
	return DB_OK;
}

DbStatus deleteDoc(DbHandle hndl[1], uint64_t docBits, uint64_t txnBits) {
Handle *docHndl;
Txn *txn = NULL;
DbStatus stat;
DbAddr *slot;
ObjId docId;
Doc *doc;

	if ((stat = bindHandle(hndl, &docHndl)))
		return stat;

	docId.bits = docBits;

	slot = fetchIdSlot(docHndl->map, docId);
	doc = getObj(docHndl->map, *slot);

	if ((doc->delId.bits = txnBits))
		txn = fetchIdSlot(docHndl->map->db, doc->delId);

	doc->state = DocDeleted;
	return DB_OK;
}

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId, uint64_t txnBits) {
DbStatus stat;
Doc *doc;

	if ((stat = allocDoc(hndl, &doc, objSize)))
		return stat;

	memcpy(doc + 1, obj, objSize);

	if ((stat = assignDoc (hndl, doc, txnBits)))
		return stat;

	if (docId)
		docId->bits = doc->docId.bits;

	return DB_OK;
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

	cursor = (DbCursor *)(getHandle(hndl) + 1);
	cursor->maxKey = max;
	cursor->maxKeyLen = maxLen;
	return DB_OK;
}

DbStatus setCursorMin(DbHandle hndl[1], uint8_t *min, uint32_t minLen) {
DbCursor *cursor;

	cursor = (DbCursor *)(getHandle(hndl) + 1);
	cursor->minKey = min;
	cursor->minKeyLen = minLen;
	return DB_OK;
}
