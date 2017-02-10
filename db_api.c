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
	releaseHandle(arena, arenaHndl);
	return bits;
}

DbStatus dropArena(DbHandle hndl[1], bool dropDefs) {
Handle *arena;
DbMap *map;

	if (!(arena = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	map = arena->map;

	releaseHandle(arena, hndl);

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

	catalog = (Catalog *)(hndlMap->arena + 1);
	lockLatch(catalog->dbList->latch);

	if ((rbEntry = rbFind(hndlMap, catalog->dbList, name, nameLen, pathStk)))
		dbVer = *(uint64_t *)(rbEntry + 1);
	else {
		rbEntry = rbNew(hndlMap, name, nameLen, sizeof(uint64_t));
		rbAdd(hndlMap, catalog->dbList, rbEntry, pathStk);
	}

	unlockLatch(catalog->dbList->latch);
	memcpy (arenaDef->params, params, sizeof(Params) * (MaxParam + 1));

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

	arrayActivate(hndlMap, catalog->openMap, arenaDef->mapIdx);
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
Catalog *catalog;
Handle *ds;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(database = bindHandle(dbHndl)))
		return DB_ERROR_handleclosed;

	catalog = (Catalog *)(hndlMap->arena + 1);
	parent = database->map;

	//  process the docStore parameters

	rbEntry = procParam(parent, name, nameLen, params);

	arenaDef = (ArenaDef *)(rbEntry + 1);
	arenaDef->baseSize = sizeof(DocArena);
	arenaDef->arenaType = Hndl_docStore;
	arenaDef->objSize = sizeof(ObjId);
	arenaDef->numTypes = MAX_blk + 1;

	//  open/create the docStore arena

	if ((map = arenaRbMap(parent, rbEntry)))
		docArena = docarena(map);
	else
		return DB_ERROR_arenadropped;

	//	allocate a catalog storeId for use in TXN steps and Doc references

	if (!*map->arena->type) {
		docArena->storeId = arrayAlloc(hndlMap, catalog->storeId, 0);
		arrayActivate(hndlMap, catalog->storeId, docArena->storeId);
	}
	
	params[DocStoreId].intVal = docArena->storeId;
	releaseHandle(database, dbHndl);

	map->arena->type[0] = Hndl_docStore;
	hndl->hndlBits = makeHandle(map, sizeof(DocStore), Hndl_docStore);

	if ((ds = bindHandle(hndl))) {
		docStore = (DocStore *)(ds + 1);
		initLock(docStore->indexes->lock);
		releaseHandle(ds, hndl);
		return DB_OK;
	}

	return DB_ERROR_handleclosed;
}

DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name, uint32_t nameLen, Params *params) {
int type = params[IdxType].intVal + Hndl_artIndex;
DbMap *map, *parent;
Handle *parentHndl;
ArenaDef *arenaDef;
RedBlack *rbEntry;
Handle *idxHndl;

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
		releaseHandle(parentHndl, docHndl);
		return DB_ERROR_indextype;
	}

	if (!(map = arenaRbMap(parent, rbEntry)))
	  return DB_ERROR_createindex;

	hndl->hndlBits = makeHandle(map, 0, type);

	if (*map->arena->type)
		goto createXit;

	if ((idxHndl = bindHandle(hndl)))
	  switch (type) {
	  case Hndl_artIndex:
		artInit(idxHndl, params);
		break;

	  case Hndl_btree1Index:
		btree1Init(idxHndl, params);
		break;

	  default:
		break;
	  }
	else
		return DB_ERROR_handleclosed;

	releaseHandle(idxHndl, hndl);

createXit:
	releaseHandle(parentHndl, docHndl);
	return DB_OK;
}

//	create new cursor

DbStatus createCursor(DbHandle hndl[1], DbHandle dbIdxHndl[1], Params *params, ObjId txnId) {
Handle *idxHndl, *cursorHndl;
DbStatus stat = DB_OK;
uint64_t timestamp;
DbCursor *cursor;
Txn *txn;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(idxHndl = bindHandle(dbIdxHndl)))
		return DB_ERROR_handleclosed;

	if (txnId.bits) {
		txn = fetchIdSlot(idxHndl->map->db, txnId);
		timestamp = txn->timestamp;
	} else
		timestamp = allocateTimestamp(idxHndl->map->db, en_reader);

	hndl->hndlBits = makeHandle(idxHndl->map, cursorSize[(uint8_t)*idxHndl->map->arena->type], Hndl_cursor);

	if ((cursorHndl = bindHandle(hndl)))
		cursor = (DbCursor *)(cursorHndl + 1);
	else
		return DB_ERROR_handleclosed;

	cursor->binaryFlds = idxHndl->map->arenaDef->params[IdxBinary].boolVal;
	cursor->txnId.bits = txnId.bits;
	cursor->ts = timestamp;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artNewCursor((ArtCursor *)cursor, idxHndl->map);
		break;

	case Hndl_btree1Index:
		stat = btree1NewCursor((Btree1Cursor *)cursor, idxHndl->map);
		break;
	}

	releaseHandle(idxHndl, dbIdxHndl);
	releaseHandle(cursorHndl, hndl);
	return stat;
}

//	position cursor on a key

DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key, uint32_t keyLen) {
DbCursor *cursor;
Handle *idxHndl;
DbStatus stat;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	cursor = (DbCursor *)(idxHndl + 1);
	stat = dbFindKey(cursor, idxHndl->map, key, keyLen, op);
	releaseHandle(idxHndl, hndl);
	return stat;
}

//	move cursor

DbStatus moveCursor(DbHandle hndl[1], CursorOp op) {
DbCursor *cursor;
Handle *idxHndl;
DbStatus stat;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	cursor = (DbCursor *)(idxHndl + 1);

	switch (op) {
	  case OpLeft:
		stat = dbLeftKey(cursor, idxHndl->map);
		break;
	  case OpRight:
		stat = dbRightKey(cursor, idxHndl->map);
		break;
	  case OpNext:
		stat = dbNextKey(cursor, idxHndl->map);
		break;
	  case OpPrev:
		stat = dbPrevKey(cursor, idxHndl->map);
		break;
	  default:
		stat = DB_ERROR_cursorop;
		break;
	}

	releaseHandle(idxHndl, hndl);
	return stat;
}

//	return cursor key

DbStatus keyAtCursor(DbHandle *hndl, void **key, uint32_t *keyLen) {
DbCursor *cursor;
Handle *idxHndl;

	if ((idxHndl = bindHandle(hndl)))
		cursor = (DbCursor *)(idxHndl + 1);
	else
		return DB_ERROR_handleclosed;

	switch (cursor->state) {
	case CursorPosAt:
		if (key)
			*key = cursor->key;

		if (keyLen)
			*keyLen = cursor->keyLen;

		releaseHandle(idxHndl, hndl);
		return DB_OK;

	default:
		break;
	}

	return DB_CURSOR_notpositioned;
}

DbStatus closeHandle(DbHandle dbHndl[1]) {
	Handle *hndl;
	ObjId hndlId;
	DbAddr *slot;

	if ((hndlId.bits = dbHndl->hndlBits))
		slot = slotHandle (hndlId);
	else
		return DB_ERROR_handleclosed;

	hndl = getObj(hndlMap, *slot);
	dbHndl->hndlBits = 0;

	atomicOr8((volatile char *)hndl->status, KILL_BIT);

	if (!hndl->bindCnt[0]);
  		destroyHandle(hndl, slot);

	return DB_OK;
}

DbStatus cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
Handle *hndl;

	if (!(hndl = bindHandle(oldHndl)))
		return DB_ERROR_handleclosed;

	newHndl->hndlBits = makeHandle(hndl->map, hndl->xtraSize, hndl->hndlType);
	releaseHandle(hndl, oldHndl);
	return DB_OK;
}

ObjId beginTxn(DbHandle hndl[1], Params *params) {
Handle *database;
ObjId txnId;
Txn *txn;

	txnId.bits = 0;

	if (!(database = bindHandle(hndl)))
		return txnId;

	txnId.bits = allocObjId(database->map, listFree(database,0), listWait(database,0), 0);
	txn = fetchIdSlot(database->map, txnId);
	txn->timestamp = allocateTimestamp(database->map, en_reader);

	releaseHandle(database, hndl);
	return txnId;
}

DbStatus rollbackTxn(DbHandle hndl[1], ObjId txnId) {
	return DB_OK;
}

DbStatus commitTxn(DbHandle hndl[1], ObjId txnId) {
	return DB_OK;
}

//	fetch document from a docStore by docId

DbStatus fetchDoc(DbHandle hndl[1], Doc **doc, ObjId docId) {
Handle *docHndl;
DbAddr *slot;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	slot = fetchIdSlot(docHndl->map, docId);
	*doc = getObj(docHndl->map, *slot);

	atomicAdd32 (docHndl->lockedDocs, 1);
	releaseHandle(docHndl, hndl);
	return DB_OK;
}

//	helper function to allocate a new document

DbStatus allocDoc(Handle *docHndl, Doc **doc, uint32_t objSize) {
DocArena *docArena = docarena(docHndl->map);
DbAddr addr;

	if ((addr.bits = allocObj(docHndl->map, listFree(docHndl,0), listWait(docHndl,0), -1, objSize + sizeof(Doc), false)))
		*doc = getObj(docHndl->map, addr);
	else
		return DB_ERROR_outofmemory;

	memset (*doc, 0, sizeof(Doc));
	(*doc)->addr.bits = addr.bits;
	(*doc)->lastVer = sizeof(Doc) - sizeof(Ver);
	(*doc)->verCnt = 1;

	(*doc)->ver->docId.bits = allocObjId(docHndl->map, listFree(docHndl,0), listWait(docHndl,0), docArena->storeId);
	(*doc)->ver->offset = sizeof(Doc) - sizeof(Ver);
	(*doc)->ver->size = objSize;
	(*doc)->ver->version = 1;
	return DB_OK;
}

DbStatus deleteDoc(DbHandle hndl[1], ObjId docId, ObjId txnId) {
Handle *docHndl;
//Txn *txn = NULL;
DbAddr *slot;
Doc *doc;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	slot = fetchIdSlot(docHndl->map, docId);
	doc = getObj(docHndl->map, *slot);

//	if ((doc->delId.bits = txnId.bits))
//		txn = fetchIdSlot(docHndl->map->db, txnId);

	doc->state = DocDeleted;
	releaseHandle(docHndl, hndl);

	return DB_OK;
}

//	Entry point to store a new document

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId, ObjId txnId) {
DocArena *docArena;
Handle *docHndl;
DbStatus stat;
DbAddr *slot;
Doc *doc;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	if ((stat = allocDoc(docHndl, &doc, objSize)))
		return stat;

	memcpy(doc + 1, obj, objSize);

	docArena = docarena(docHndl->map);

	doc->ver->docId.bits = allocObjId(docHndl->map, listFree(docHndl,0), listWait(docHndl,0), docArena->storeId);

	//	add the new document to the txn

	if ((doc->ver->txnId.bits = txnId.bits)) {
		Txn *txn = fetchIdSlot(docHndl->map->db, doc->ver->txnId);
		addVerToTxn(docHndl->map->db, txn, doc->ver, TxnAddDoc); 
	}

	//  return the docId to the caller

	if (docId)
		docId->bits = doc->ver->docId.bits;

	slot = fetchIdSlot(docHndl->map, doc->ver->docId);
	slot->bits = doc->addr.bits;

	releaseHandle(docHndl, hndl);
	return DB_OK;
}

DbStatus deleteKey(DbHandle hndl[1], void *key, uint32_t len) {
DbStatus stat = DB_OK;
Handle *idxHndl;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artDeleteKey(idxHndl, key, len);
		break;

	case Hndl_btree1Index:
		stat = btree1DeleteKey(idxHndl, key, len);
		break;
	}

	releaseHandle(idxHndl, hndl);
	return stat;
}

DbStatus insertKey(DbHandle hndl[1], void *key, uint32_t len) {
DbStatus stat = DB_OK;
Handle *idxHndl;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artInsertKey(idxHndl, key, len);
		break;

	case Hndl_btree1Index:
		stat = btree1InsertKey(idxHndl, key, len, 0, Btree1_indexed);
		break;
	}

	releaseHandle(idxHndl, hndl);
	return stat;
}
