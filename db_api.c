#include "btree1/btree1.h"
#include "artree/artree.h"
#include "db_iterator.h"
#include "db_malloc.h"

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
	arenaDef->objSize = params[ObjIdSize].intVal;
	arenaDef->baseSize = params[MapXtra].intVal;
	arenaDef->arenaType = Hndl_database;
	arenaDef->numTypes = ObjIdType + 1;
	arenaDef->ver = dbVer;

	//  create the database

	if ((map = openMap(NULL, name, nameLen, arenaDef, NULL)))
		*map->arena->type = Hndl_database;
	else
		return DB_ERROR_createdatabase;

	arrayActivate(hndlMap, catalog->openMap, arenaDef->mapIdx);
	hndl->hndlBits = makeHandle(map, 0, Hndl_database)->hndlId.bits;
	return DB_OK;
}

DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name, uint32_t nameLen, Params *params) {
DbMap *map, *parent = NULL;
Handle *database = NULL;
ArenaDef *arenaDef;
RedBlack *rbEntry;
Catalog *catalog;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(database = bindHandle(dbHndl)))
		return DB_ERROR_handleclosed;

	catalog = (Catalog *)(hndlMap->arena + 1);
	parent = database->map;

	//  process the docStore parameters

	rbEntry = procParam(parent, name, nameLen, params);

	arenaDef = (ArenaDef *)(rbEntry + 1);
	arenaDef->localSize = params[MapXtra].intVal;
	arenaDef->arenaType = Hndl_docStore;
	arenaDef->objSize = sizeof(ObjId);
	arenaDef->numTypes = MAX_blk + 1;

	//  open/create the docStore arena

	if (!(map = arenaRbMap(parent, rbEntry)))
		return DB_ERROR_arenadropped;

	//	allocate a global storeId for use in TXN steps and Doc references

	if (!*map->arena->type) {
		map->arenaDef->storeId = arrayAlloc(hndlMap, catalog->storeId, 0);
		arrayActivate(hndlMap, catalog->storeId, map->arenaDef->storeId);
		map->arena->type[0] = Hndl_docStore;
	}
	
	releaseHandle(database, dbHndl);

	hndl->hndlBits = makeHandle(map, params[HndlXtra].intVal, Hndl_docStore)->hndlId.bits;
	return DB_OK;
}

DbStatus createIndex(DbHandle hndl[1], DbHandle docHndl[1], char *name, uint32_t nameLen, Params *params) {
int type = params[IdxType].intVal + Hndl_artIndex;
uint32_t xtra = params[ArenaXtra].intVal;
DbMap *map, *parent;
Handle *parentHndl;
ArenaDef *arenaDef;
RedBlack *rbEntry;
Handle *idxHndl;
DbIndex *index;

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
		arenaDef->baseSize = sizeof(DbIndex) + sizeof(ArtIndex) + xtra;
		break;
	case Hndl_btree1Index:
		arenaDef->numTypes = MAXBtree1Type;
		arenaDef->baseSize = sizeof(DbIndex) + sizeof(Btree1Index) + xtra;
		break;
	default:
		releaseHandle(parentHndl, docHndl);
		return DB_ERROR_indextype;
	}

	if (!(map = arenaRbMap(parent, rbEntry)))
	  return DB_ERROR_createindex;

	idxHndl = makeHandle(map, 0, type);
	hndl->hndlBits = idxHndl->hndlId.bits;

	if (*map->arena->type)
		goto createXit;

	index = (DbIndex *)(map->arena + 1);
	index->xtra = xtra;

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

createXit:
	releaseHandle(parentHndl, docHndl);
	releaseHandle(idxHndl, NULL);
	return DB_OK;
}

//
// create and position the start of an iterator
//

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], Params *params) {
uint32_t xtra = params[HndlXtra].intVal;
Handle *parentHndl, *iterator;
Iterator *it;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(parentHndl = bindHandle(docHndl)))
		return DB_ERROR_handleclosed;

	iterator = makeHandle(parentHndl->map, sizeof(Iterator) + xtra, Hndl_iterator);
	it = (Iterator *)(iterator + 1);
	it->xtra = xtra;

	if (params[IteratorEnd].boolVal) {
		it->docId.bits = parentHndl->map->arena->segs[parentHndl->map->arena->currSeg].nextId.bits;
		it->state = IterRightEof;
	} else {
		it->docId.bits = 0;
		it->state = IterLeftEof;
	}

	//	return handle for iterator

	hndl->hndlBits = iterator->hndlId.bits;

	releaseHandle(parentHndl, docHndl);
	releaseHandle(iterator, hndl);
	return DB_OK;
}

//	advance/reverse iterator

DbStatus moveIterator (DbHandle hndl[1], IteratorOp op, void **doc, ObjId *docId) {
Handle *docHndl;
DbStatus stat;
Iterator *it;
DbAddr *slot;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	it = (Iterator *)(docHndl + 1);

	switch (op) {
	  case IterNext:
		if ((slot = iteratorNext(docHndl))) {
			*doc = getObj(docHndl->map, *slot);
			stat = DB_OK;
		} else
			stat = DB_ITERATOR_eof;

		break;
	  case IterPrev:
		if ((slot = iteratorPrev(docHndl))) {
			*doc = getObj(docHndl->map, *slot);
			stat = DB_OK;
		} else
			stat = DB_ITERATOR_eof;

		break;
	  case IterSeek:
	  case IterBegin:
	  case IterEnd:
		if ((slot = iteratorSeek(docHndl, op, *docId))) {
			*doc = getObj(docHndl->map, *slot);
			stat = DB_OK;
		} else
			stat = DB_ITERATOR_notfound;

		break;
	}

	docId->bits = it->docId.bits;
	releaseHandle(docHndl, hndl);
	return stat;
}

//	create new cursor

DbStatus createCursor(DbHandle hndl[1], DbHandle dbIdxHndl[1], Params *params) {
uint32_t xtra = params[HndlXtra].intVal;
Handle *idxHndl, *cursorHndl;
DbStatus stat = DB_OK;
DbCursor *dbCursor;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(idxHndl = bindHandle(dbIdxHndl)))
		return DB_ERROR_handleclosed;

	cursorHndl = makeHandle(idxHndl->map, xtra + sizeof(DbCursor) + cursorSize[*(uint8_t *)idxHndl->map->arena->type], Hndl_cursor);

	dbCursor = (DbCursor *)(cursorHndl + 1);
	dbCursor->binaryFlds = idxHndl->map->arenaDef->params[IdxKeyFlds].boolVal;
	dbCursor->xtra = xtra + cursorSize[*(uint8_t *)idxHndl->map->arena->type];
	dbCursor->deDup = params[CursorDeDup].boolVal;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artNewCursor(dbCursor, idxHndl->map);
		break;

	case Hndl_btree1Index:
		stat = btree1NewCursor(dbCursor, idxHndl->map);
		break;
	}

	hndl->hndlBits = cursorHndl->hndlId.bits;
	releaseHandle(idxHndl, dbIdxHndl);
	releaseHandle(cursorHndl, hndl);
	return stat;
}

//	release cursor resources

DbStatus closeCursor(DbHandle hndl[1]) {
DbStatus stat = DB_ERROR_indextype;
DbCursor *dbCursor;
Handle *idxHndl;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	dbCursor = (DbCursor *)(idxHndl + 1);

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artReturnCursor(dbCursor, idxHndl->map);
		break;

	case Hndl_btree1Index:
		stat = btree1ReturnCursor(dbCursor, idxHndl->map);
		break;
	}

	return stat;
}


//	position cursor on a key

DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key, uint32_t keyLen) {
DbCursor *dbCursor;
Handle *idxHndl;
DbStatus stat;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	dbCursor = (DbCursor *)(idxHndl + 1);
	stat = dbFindKey(dbCursor, idxHndl->map, key, keyLen, op);
	releaseHandle(idxHndl, hndl);
	return stat;
}

//	move cursor

DbStatus moveCursor(DbHandle hndl[1], CursorOp op) {
DbCursor *dbCursor;
Handle *idxHndl;
DbStatus stat;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	dbCursor = (DbCursor *)(idxHndl + 1);

	switch (op) {
	  case OpLeft:
		stat = dbLeftKey(dbCursor, idxHndl->map);
		break;
	  case OpRight:
		stat = dbRightKey(dbCursor, idxHndl->map);
		break;
	  case OpNext:
		stat = dbNextKey(dbCursor, idxHndl->map);
		break;
	  case OpPrev:
		stat = dbPrevKey(dbCursor, idxHndl->map);
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
DbCursor *dbCursor;
Handle *idxHndl;

	if ((idxHndl = bindHandle(hndl)))
		dbCursor = (DbCursor *)(idxHndl + 1);
	else
		return DB_ERROR_handleclosed;

	switch (dbCursor->state) {
	case CursorPosAt:
		if (key)
			*key = dbCursor->key;

		if (keyLen)
			*keyLen = dbCursor->keyLen;

		releaseHandle(idxHndl, hndl);
		return DB_OK;

	default:
		break;
	}

	return DB_CURSOR_notpositioned;
}

DbStatus closeHandle(DbHandle hndl[1]) {
	Handle *handle;
	ObjId hndlId;
	DbAddr *slot;

	if ((hndlId.bits = hndl->hndlBits))
		slot = slotHandle (hndlId);
	else
		return DB_ERROR_handleclosed;

	handle = getObj(hndlMap, *slot);
	hndl->hndlBits = 0;

	atomicOr8((volatile char *)handle->status, KILL_BIT);

	if (!handle->bindCnt[0]);
  		destroyHandle(handle, slot);

	return DB_OK;
}

DbStatus cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
Handle *hndl;

	if (!(hndl = bindHandle(oldHndl)))
		return DB_ERROR_handleclosed;

	newHndl->hndlBits = makeHandle(hndl->map, hndl->xtraSize, hndl->hndlType)->hndlId.bits;
	releaseHandle(hndl, oldHndl);
	return DB_OK;
}

//	fetch document from a docStore by docId

DbStatus fetchDoc(DbHandle hndl[1], void **doc, ObjId docId) {
Handle *docHndl;
DbAddr *slot;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	slot = fetchIdSlot(docHndl->map, docId);
	*doc = getObj(docHndl->map, *slot);

	releaseHandle(docHndl, hndl);
	return DB_OK;
}

DbStatus deleteDoc(DbHandle hndl[1], ObjId docId) {
Handle *docHndl;
DbAddr *slot;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	slot = fetchIdSlot(docHndl->map, docId);
	freeBlk(docHndl->map, *slot);
	slot->bits = 0;

	releaseHandle(docHndl, hndl);

	return DB_OK;
}

//	Entry point to store a new document

DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId) {
DbAddr *slot, addr;
Handle *docHndl;
void *doc;

	if (!(docHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	if ((addr.bits = allocObj(docHndl->map, listFree(docHndl,0), listWait(docHndl,0), -1, objSize, false)))
		doc = getObj(docHndl->map, addr);
	else
		return DB_ERROR_outofmemory;

	memcpy(doc, obj, objSize);

	docId->bits = allocObjId(docHndl->map, listFree(docHndl,ObjIdType), listWait(docHndl,ObjIdType));

	slot = fetchIdSlot(docHndl->map, *docId);
	slot->bits = addr.bits;

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
		stat = artDeleteKey(idxHndl, key, len, 0);
		break;

	case Hndl_btree1Index:
		stat = btree1DeleteKey(idxHndl, key, len);
		break;
	}

	releaseHandle(idxHndl, hndl);
	return stat;
}

//	call back fcn to determine if key is unique
//	since we are not MVCC, all calls are from duplicates

//	return true if key is already in the index

bool uniqueKey(DbMap *map, DbCursor *dbCursor) {
	return true;
}

DbStatus insertKey(DbHandle hndl[1], void *key, uint32_t len, uint32_t suffixLen) {
DbStatus stat = DB_OK;
Handle *idxHndl;

	if (!(idxHndl = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex: {
		uint8_t defer = idxHndl->map->arenaDef->params[IdxKeyDeferred].boolVal;

		if (idxHndl->map->arenaDef->params[IdxKeyUnique].boolVal)
			stat = artInsertUniq(idxHndl, key, len, suffixLen, uniqueKey, &defer);
		else
			stat = artInsertKey(idxHndl, key, len + suffixLen);
		break;
	}

	case Hndl_btree1Index:
		stat = btree1InsertKey(idxHndl, key, len, 0, Btree1_indexed);
		break;
	}

	releaseHandle(idxHndl, hndl);
	return stat;
}
