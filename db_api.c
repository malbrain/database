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

char *hndlNames[] = {
	"newarena",
	"catalog",
	"database",
	"docStore",
	"artIndex",
	"btree1Index",
	"btree2Index",
	"colIndex",
	"iterator",
	"cursor",
	"docVersion"
};

int cursorSize[8] = {0, 0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), 0};

extern void memInit(void);
extern char hndlInit[1];
extern DbMap *hndlMap;
extern char *hndlPath;

void initialize(void) {
	memInit();
}

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit, bool dbArena) {
Handle *arena;
uint64_t bits;
DbMap *map;

	if (!(arena = bindHandle(arenaHndl)))
		return 0;

	map = arena->map;

	if (dbArena)
		map = map->db;

	bits = allocBlk(map, size, zeroit);
	releaseHandle(arena);
	return bits;
}

DbStatus dropArena(DbHandle hndl[1], bool dropDefs) {
Handle *arena;
DbMap *map;

	if (!(arena = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	map = arena->map;

	releaseHandle(arena);

	//	wait until arena is created

	waitNonZero(map->arena->type);
	dropMap(map, dropDefs);

	return DB_OK;
}

DbStatus openDatabase(DbHandle hndl[1], char *name, uint32_t nameLen, Params *params) {
ArenaDef arenaDef[1];
uint64_t dbVer = 0;
PathStk pathStk[1];
RedBlack *rbEntry;
Catalog *catalog;
DbMap *map;

	memset (arenaDef, 0, sizeof(ArenaDef));
	memset (hndl, 0, sizeof(DbHandle));

	if (!(*hndlInit & TYPE_BITS))
		initHndlMap(NULL, 0, NULL, 0, true);

	//	find/create our database in the catalog

	catalog = (Catalog *)(hndlMap + 1);
	lockLatch(catalog->dbList->latch);

	if ((rbEntry = rbFind(hndlMap, catalog->dbList, name, nameLen, pathStk)))
		dbVer = *(uint64_t *)(rbEntry + 1);
	else {
		rbEntry = rbNew(hndlMap, name, nameLen, sizeof(uint64_t));
		rbAdd(hndlMap, catalog->dbList, rbEntry, pathStk);
	}

	unlockLatch(catalog->dbList->latch);
	memcpy (arenaDef->params, params, sizeof(Params) * MaxParam);

	arenaDef->mapIdx = arrayAlloc(hndlMap, catalog->openMap, sizeof(void *));
	arenaDef->baseSize = sizeof(DataBase);
	arenaDef->arenaType = Hndl_database;
	arenaDef->numTypes = ObjIdType + 1;
	arenaDef->objSize = sizeof(Txn);
	arenaDef->ver = dbVer;

	//  create the database

	if ((map = openMap(NULL, name, nameLen, arenaDef, NULL)))
		*map->arena->type = Hndl_database;
	else
		return DB_ERROR_createdatabase;

	hndl->hndlBits = makeHandle(map, sizeof(Txn), Hndl_database);
	return DB_OK;
}

DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
DocStore *docStore;
DocArena *docArena;
ArenaDef *arenaDef;
RedBlack *rbEntry;
DataBase *db;
Handle *ds;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(database = bindHandle(dbHndl)))
		return DB_ERROR_handleclosed;

	parent = database->map, db = database(parent);

	//  create the docArena and assign database txn idx

	rbEntry = procParam(parent, name, nameLen, params);

	arenaDef = (ArenaDef *)(rbEntry + 1);
	arenaDef->baseSize = sizeof(DocArena);
	arenaDef->arenaType = Hndl_docStore;
	arenaDef->objSize = sizeof(ObjId);
	arenaDef->numTypes = MAX_blk + 1;

	if ((map = arenaRbMap(parent, rbEntry)))
		docArena = docarena(map);
	else
		return DB_ERROR_arenadropped;

	//	allocate a map index for use in TXN document steps

	if (!*map->arena->type)
	  if (parent)
		docArena->docIdx = arrayAlloc(parent, db->txnIdx, sizeof(uint64_t));

	releaseHandle(database);

	map->arena->type[0] = Hndl_docStore;
	hndl->hndlBits = makeHandle(map, sizeof(DocStore), Hndl_docStore);

	ds = getHandle(hndl);
	docStore = (DocStore *)(ds + 1);
	initLock(docStore->indexes->lock);

	return DB_OK;
}

DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name, uint32_t nameLen, Params *params) {
int type = params[IdxType].intVal + Hndl_artIndex;
DbMap *map, *parent;
Handle *parentHndl;
ArenaDef *arenaDef;
RedBlack *rbEntry;
DbIndex *dbIndex;
Handle *index;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(parentHndl = bindHandle(docHndl)))
		return DB_ERROR_handleclosed;

	parent = parentHndl->map;

	//  create the index

	rbEntry = procParam(parent, name, nameLen, params);

	arenaDef = (ArenaDef *)(rbEntry + 1);
	arenaDef->objSize = sizeof(ObjId);
	arenaDef->arenaType = type;

	switch (type) {
	case Hndl_artIndex:
		arenaDef->numTypes = MaxARTType;
		arenaDef->baseSize = sizeof(ArtIndex);
		break;
	case Hndl_btree1Index:
		arenaDef->numTypes = MAXBtree1Type;
		arenaDef->baseSize = sizeof(Btree1Index);
		break;
	default:
		releaseHandle(parentHndl);
		return DB_ERROR_indextype;
	}

	if (!(map = arenaRbMap(parent, rbEntry)))
	  return DB_ERROR_createindex;

	hndl->hndlBits = makeHandle(map, 0, type);

	if (*map->arena->type)
		goto createXit;

	dbIndex = dbindex(map);
	dbIndex->idxKeys = compileKeys(map, params);

	index = getHandle(hndl);

	switch (type) {
	  case Hndl_artIndex:
		artInit(index, params);
		break;

	  case Hndl_btree1Index:
		btree1Init(index, params);
		break;

	  default:
		break;
	}

	*map->arena->type = type;

createXit:
	releaseHandle(parentHndl);
	return DB_OK;
}

//	create new cursor

DbStatus createCursor(DbHandle hndl[1], DbHandle idxHndl[1], Params *params) {
Handle *index, *cursorHndl;
uint64_t timestamp;
DbCursor *cursor;
DbStatus stat;
ObjId txnId;
Txn *txn;

	txnId.bits = params[CursorTxn].intVal;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(index = bindHandle(idxHndl)))
		return DB_ERROR_handleclosed;

	if (txnId.bits) {
		txn = fetchIdSlot(index->map->db, txnId);
		timestamp = txn->timestamp;
	} else
		timestamp = allocateTimestamp(index->map->db, en_reader);

	hndl->hndlBits = makeHandle(index->map, cursorSize[(uint8_t)*index->map->arena->type], Hndl_cursor);

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

DbStatus closeHandle(DbHandle dbHndl[1]) {
HandleId *slot;
Handle *hndl;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	lockLatch(slot->addr->latch);

	if (*slot->addr->latch & KILL_BIT) {
		*slot->addr->latch = KILL_BIT;
		return DB_ERROR_handleclosed;
	}

	hndl = getObj(hndlMap, *slot->addr);

	//  specific handle cleanup

	switch (hndl->hndlType) {
	case Hndl_cursor:
		dbCloseCursor((void *)(hndl + 1), hndl->map);
	}

	destroyHandle (hndl->map, slot->addr);
	return DB_OK;
}

//	delete unreferenced handle

DbStatus deleteHandle(DbHandle dbHndl[1]) {
DbStatus stat = DB_OK;
HandleId *slot;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (*slot->addr->latch & KILL_BIT)
	  stat = closeHandle(dbHndl);

	freeId(hndlMap, hndlId);
	return stat;
}

//	close handle

//	position cursor on a key

DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key, uint32_t keyLen) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	cursor = (DbCursor *)(index + 1);

	switch (op) {
	  case OpFind:
		stat = dbFindKey(cursor, index->map, key, keyLen, false);
		break;
	  case OpOne:
		stat = dbFindKey(cursor, index->map, key, keyLen, true);
		break;
	  default:
		stat = DB_ERROR_cursorop;
	}

	releaseHandle(index);
	return stat;
}

//	move cursor

DbStatus moveCursor(DbHandle hndl[1], CursorOp op) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

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
	  default:
		stat = DB_ERROR_cursorop;
		break;
	}

	releaseHandle(index);
	return stat;
}

//	return cursor key

DbStatus keyAtCursor(DbHandle *hndl, void **key, uint32_t *keyLen) {
DbCursor *cursor;

	cursor = (DbCursor *)(getHandle(hndl) + 1);

	switch (cursor->state) {
	case CursorPosAt:
		if (key)
			*key = cursor->key;

		if (keyLen)
			*keyLen = cursor->userLen;

		return DB_OK;

	default:
		break;
	}

	return DB_CURSOR_notpositioned;
}

DbStatus docAtCursor(DbHandle *hndl, Doc **doc) {
DbCursor *cursor;

	cursor = (DbCursor *)(getHandle(hndl) + 1);

	switch (cursor->state) {
	case CursorPosAt:
		if (doc)
			*doc = cursor->doc;

		return DB_OK;

	default:
		break;
	}

	return DB_CURSOR_notpositioned;
}

//	iterate cursor to next document

DbStatus nextDoc(DbHandle hndl[1], Doc **doc) {
DbCursor *cursor;
Handle *index;
DbStatus stat;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

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

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	cursor = (DbCursor *)(index + 1);

	stat = dbPrevDoc(cursor, index->map);

	if (!stat && doc)
		*doc = cursor->doc;

	releaseHandle(index);
	return stat;
}

DbStatus cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
Handle *arena;

	if (!(arena = bindHandle(oldHndl)))
		return DB_ERROR_handleclosed;

	newHndl->hndlBits = makeHandle(arena->map, arena->xtraSize, arena->hndlType);

	releaseHandle(arena);
	return DB_OK;
}

ObjId beginTxn(DbHandle hndl[1], Params *params) {
Handle *database;
ObjId txnId;
Txn *txn;

	txnId.bits = 0;

	if (!(database = bindHandle(hndl)))
		return txnId;

	txnId.bits = allocObjId(database->map, listFree(database,0), listTail(database,0), 0);
	txn = fetchIdSlot(database->map, txnId);
	txn->timestamp = allocateTimestamp(database->map, en_reader);

	releaseHandle(database);
	return txnId;
}

DbStatus rollbackTxn(DbHandle hndl[1], ObjId txnId) {
	return DB_OK;
}

DbStatus commitTxn(DbHandle hndl[1], ObjId txnId) {
	return DB_OK;
}

DbStatus addIndexes(DbHandle hndl[1]) {
Handle *docHndl;
DbStatus stat;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	stat = installIndexes(docHndl);
	releaseHandle(docHndl);
	return stat;
}

// install new document in docId slot

DbStatus installDoc(Handle *docHndl, Doc *doc, Txn *txn) {
DbStatus stat;
DbAddr *slot;

	slot = fetchIdSlot(docHndl->map, doc->ver->docId);
	slot->bits = doc->addr.bits;

	if ((stat = installIndexes(docHndl)))
		return stat;

	//	add keys for the document
	//	enumerate children (e.g. indexes)

	installIndexKeys(docHndl, doc->ver);

	if (txn)
		addVerToTxn(docHndl->map->db, txn, doc->ver, TxnAddDoc); 

	return DB_OK;
}

DbStatus assignDoc(DbHandle hndl[1], Doc *doc, ObjId txnId) {
DocArena *docArena;
Handle *docHndl;
Txn *txn = NULL;
DbStatus stat;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	docArena = docarena(docHndl->map);

	if ((doc->ver->txnId.bits = txnId.bits))
		txn = fetchIdSlot(docHndl->map->db, doc->ver->txnId);

	doc->ver->docId.bits = allocObjId(docHndl->map, listFree(docHndl,0), listTail(docHndl,0), docArena->docIdx);

	stat = installDoc(docHndl, doc, txn);
	releaseHandle(docHndl);

	return stat;
}

DbStatus allocDoc(DbHandle hndl[1], Doc **doc, uint32_t objSize) {
Handle *docHndl;
DbAddr addr;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	if ((addr.bits = allocObj(docHndl->map, listFree(docHndl,0), listTail(docHndl,0), -1, objSize + sizeof(Doc), false)))
		*doc = getObj(docHndl->map, addr);
	else
		return DB_ERROR_outofmemory;

	memset (*doc, 0, sizeof(Doc));
	(*doc)->size = sizeof(Doc) + objSize;
	(*doc)->addr.bits = addr.bits;

	(*doc)->ver->offset = sizeof(Doc);
	(*doc)->ver->size = objSize;
	(*doc)->ver->version = 1;
	(*doc)->size = objSize;

	releaseHandle(docHndl);
	return DB_OK;
}

DbStatus deleteDoc(DbHandle hndl[1], ObjId docId, ObjId txnId) {
Handle *docHndl;
Txn *txn = NULL;
DbAddr *slot;
Doc *doc;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	slot = fetchIdSlot(docHndl->map, docId);
	doc = getObj(docHndl->map, *slot);

	if ((doc->delId.bits = txnId.bits))
		txn = fetchIdSlot(docHndl->map->db, txnId);

	doc->state = DocDeleted;
	releaseHandle(docHndl);

	return DB_OK;
}

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId, ObjId txnId) {
DbStatus stat;
Doc *doc;

	if ((stat = allocDoc(hndl, &doc, objSize)))
		return stat;

	memcpy(doc + 1, obj, objSize);

	if ((stat = assignDoc (hndl, doc, txnId)))
		return stat;

	//  return the docId

	if (docId)
		docId->bits = doc->ver->docId.bits;

	return DB_OK;
}

DbStatus deleteKey(DbHandle hndl[1], void *key, uint32_t len) {
Handle *index;
DbStatus stat;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artDeleteKey(index, key, len);
		break;

	case Hndl_btree1Index:
		stat = btree1DeleteKey(index, key, len);
		break;
	}

	releaseHandle(index);
	return stat;
}

DbStatus insertKey(DbHandle hndl[1], void *key, uint32_t len) {
Handle *index;
DbStatus stat;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

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

DbStatus setCursorMax(DbHandle hndl[1], void *max, uint32_t maxLen) {
DbCursor *cursor;

	cursor = (DbCursor *)(getHandle(hndl) + 1);
	cursor->maxKey = max;
	cursor->maxKeyLen = maxLen;
	return DB_OK;
}

DbStatus setCursorMin(DbHandle hndl[1], void *min, uint32_t minLen) {
DbCursor *cursor;

	cursor = (DbCursor *)(getHandle(hndl) + 1);
	cursor->minKey = min;
	cursor->minKeyLen = minLen;
	return DB_OK;
}
