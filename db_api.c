#include "db_api.h"

#include "artree/artree.h"
#include "btree1/btree1.h"
#include "btree2/btree2.h"
#include "db_iterator.h"
#include "db_malloc.h"
#include "db_object.h"

char *hndlNames[] = {"newarena", "catalog",     "database",    "docStore",
                     "artIndex", "btree1Index", "btree2Index", "colIndex",
                     "iterator", "cursor",      "docVersion"};

uint32_t cursorSize[Hndl_max] = {
    0, 0, 0, 0, sizeof(ArtCursor), sizeof(Btree1Cursor), sizeof(Btree2Cursor)};
uint32_t indexSize[Hndl_max] = {sizeof(ArtIndex), sizeof(Btree1Index), sizeof(Btree2Index)};
uint32_t clntXtra[Hndl_max];

extern void memInit(void);
DbAddr hndlInit[1];
extern DbMap *hndlMap;
char *hndlPath;

//	open the catalog
//	return pointer to the Handle map

Catalog *initHndlMap(char *path, int pathLen, char *name, bool onDisk) {
  int nameLen = (int)strlen(name);
  ArenaDef arenaDef[1];

  lockLatch(hndlInit->latch);

  if (hndlInit->type) {
    unlockLatch(hndlInit->latch);
    return (Catalog *)(hndlMap->arena + 1);
  }

  if (pathLen) {
    hndlPath = db_malloc(pathLen + 1, false);
    memcpy(hndlPath, path, pathLen);
    hndlPath[pathLen] = 0;
  }

  //    configure local process Catalog
  //	which contains all the Handles
  //	and has databases for children

  memset(arenaDef, 0, sizeof(arenaDef));
  arenaDef->arenaXtra = sizeof(Catalog);
  arenaDef->params[OnDisk].boolVal = onDisk;
  arenaDef->arenaType = Hndl_catalog;
  arenaDef->objSize = sizeof(Handle);

  hndlMap = openMap(NULL, name, nameLen, arenaDef, NULL);
  hndlMap->db = hndlMap;

  *hndlMap->arena->type = Hndl_catalog;
  hndlInit->type = Hndl_catalog;

  return (Catalog *)(hndlMap->arena + 1);
}

void initialize(void) {
  memInit(); 
  
  //    "Catalog" contans all open maps in the current process
  //    indexed in an array, and all Handles indexed by ObjId

  if (!hndlInit->type) 
      catalog = initHndlMap(NULL, 0, "Catalog", true);
}

uint64_t arenaAlloc(DbHandle arenaHndl[1], uint32_t size, bool zeroit,
                    bool dbArena) {
  Handle *arena = HandleAddr(arenaHndl);
  DbMap *map = MapAddr(arena);
  uint64_t bits;

  if (dbArena) map = map->db;

  bits = allocBlk(map, size, zeroit);
  releaseHandle(arena, arenaHndl);
  return bits;
}

DbStatus dropArena(DbHandle hndl[1], bool dropDefs) {
  Handle *arena = bindHandle(hndl, Hndl_any);
  DbMap *map = MapAddr(arena);

  releaseHandle(arena, hndl);


  //	wait until arena is created

  waitNonZero(map->arena->type);
  dropMap(map, dropDefs);
  return DB_OK;
}

DbStatus openDatabase(DbHandle *hndl, char *name, uint32_t nameLen,
                      Params *params) {
  ArenaDef *arenaDef;
  RedBlack *rbEntry;
  Handle *dbHndl;
  DbMap *map;

  hndl->hndlId.bits = 0;

  //	create second copy of rbEntry in Catalog

  rbEntry = procParam(hndlMap, name, nameLen, params);
  arenaDef = (ArenaDef *)(rbEntry + 1);

  arenaDef->clntSize = (uint32_t)params[ClntSize].intVal;
  arenaDef->clntXtra = (uint32_t)params[ClntXtra].intVal;
  arenaDef->objSize = (uint32_t)params[ObjIdSize].intVal;
  arenaDef->arenaType = Hndl_database;
  arenaDef->numTypes = ObjIdType + 1;

  //  create the database

  if ((map = arenaRbMap(hndlMap, rbEntry)))
    *map->arena->type = Hndl_database;
  else
    return DB_ERROR_createdatabase;

  dbHndl = makeHandle(map, 0, 0, Hndl_database);
  hndl->hndlId.bits = dbHndl->hndlId.bits;
  return DB_OK;
}

DbStatus openDocStore(DbHandle hndl[1], DbHandle dbHndl[1], char *name,
                      uint32_t nameLen, Params *params) {
  DbMap *map, *parent = NULL;
  Handle *database, *docHndl;
  DbStatus stat = DB_OK;
  ArenaDef *arenaDef;
  RedBlack *rbEntry;

  hndl->hndlId.bits = 0;

  if (!(database = bindHandle(dbHndl, Hndl_database))) return DB_ERROR_handleclosed;

  parent = MapAddr(database);

  //  process the docStore parameters

  rbEntry = procParam(parent, name, nameLen, params);

  arenaDef = (ArenaDef *)(rbEntry + 1);
  arenaDef->clntSize = sizeof(Iterator);
  arenaDef->clntXtra = (uint32_t)params[ClntXtra].intVal;
  arenaDef->arenaType = Hndl_docStore;
  arenaDef->objSize = sizeof(ObjId);
  arenaDef->numTypes = MAX_blk + 1;
  arenaDef->arenaXtra = sizeof(DocStore);

  if ((map = arenaRbMap(parent, rbEntry))) {
    if (!*map->arena->type) {
      map->arena->type[0] = Hndl_docStore;
    }
  } else
    stat = DB_ERROR_arenadropped;

  releaseHandle(database, dbHndl);

  if ((docHndl = makeHandle(map, arenaDef->clntSize, arenaDef->clntXtra, Hndl_docStore)))
    hndl->hndlId.bits = docHndl->hndlId.bits;
  else
    return DB_ERROR_outofmemory;

  releaseHandle(docHndl, hndl);
  return stat;
}

DbStatus createIndex(DbHandle dbIdxHndl[1], DbHandle dbParentHndl[1], char *name,
                     uint32_t nameLen, Params *params) {
  uint32_t type = (uint32_t)(params[IdxType].intVal + Hndl_artIndex);
  Handle *parentHndl, *idxHndl;
  DbMap *idxMap, *parentMap;
  ArenaDef *arenaDef;
  RedBlack *rbEntry;

  dbIdxHndl->hndlId.bits = 0;

  if (!(parentHndl = bindHandle(dbParentHndl, Hndl_any))) return DB_ERROR_handleclosed;

  parentMap = MapAddr(parentHndl);

  //  create the index

  rbEntry = procParam(parentMap, name, nameLen, params);

  arenaDef = (ArenaDef *)(rbEntry + 1);
  arenaDef->objSize = sizeof(ObjId);
  arenaDef->clntSize = cursorSize[type];
  arenaDef->clntXtra = clntXtra[type];
  arenaDef->arenaType = type;

  switch (type) {
    case Hndl_artIndex:
      arenaDef->numTypes = MaxARTType;
      arenaDef->arenaXtra = sizeof(ArtIndex);
      break;
    case Hndl_btree1Index:
      arenaDef->numTypes = MAXBtree1Type;
      arenaDef->arenaXtra = sizeof(Btree1Index);
      break;
    case Hndl_btree2Index:
      arenaDef->numTypes = MAXBtree2Type;
      arenaDef->arenaXtra = sizeof(Btree2Index);
      break;
    default:
      releaseHandle(parentHndl, dbParentHndl);
      return DB_ERROR_indextype;
  }

  if (!(idxMap = arenaRbMap(parentMap, rbEntry))) {
    releaseHandle(parentHndl, dbParentHndl);
    return DB_ERROR_createindex;
  }

  if ((idxHndl = makeHandle(idxMap, arenaDef->clntSize, arenaDef->clntXtra, type)))
    dbIdxHndl->hndlId.bits = idxHndl->hndlId.bits;
  else {
    releaseHandle(parentHndl, dbParentHndl);
    return DB_ERROR_outofmemory;
  }

  if (*idxMap->arena->type) goto createXit;

  // each arena map is followed by a DbIndex base instance

  switch (type) {
    case Hndl_artIndex:
      artInit(idxHndl, params);
      break;

    case Hndl_btree1Index:
      btree1Init(idxHndl, params);
      break;

    case Hndl_btree2Index:
      btree2Init(idxHndl, params);
      break;

    default:
      break;
  }

createXit:
  releaseHandle(parentHndl, dbParentHndl);
  releaseHandle(idxHndl, dbIdxHndl);
  return DB_OK;
}


//
// create and position the start of an iterator
//

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], Params *params) {
  Handle *parentHndl, *iterHndl;
  Iterator *it;
  DbMap *docMap;

  hndl->hndlId.bits = 0;

  if (!(parentHndl = bindHandle(docHndl, Hndl_docStore))) return DB_ERROR_handleclosed;

  docMap = MapAddr(parentHndl);

  if ((iterHndl = makeHandle(docMap, sizeof(Iterator), 0, Hndl_iterator)))
    it = getObj(docMap, iterHndl->clientAddr);
  else {
    releaseHandle(parentHndl, docHndl);
    return DB_ERROR_outofmemory;
  }

  it->state = IterLeftEof;
  it->docId.bits = 0;

  //	return handle for iterator

  hndl->hndlId.bits = iterHndl->hndlId.bits;

  releaseHandle(parentHndl, docHndl);
  releaseHandle(iterHndl, hndl);
  return DB_OK;
}

//	advance/reverse iterator

DbStatus moveIterator(DbHandle hndl[1], IteratorOp op, void **doc,
                      ObjId *docId) {
  DbStatus stat = DB_ERROR_indextype;
  Handle *docHndl;
  Iterator *it;
  DbAddr *slot;
  DbMap *docMap;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
    return DB_ERROR_handleclosed;

  it = getObj(docMap, docHndl->clientAddr);

  switch (op) {
    case IterNext:
      if ((slot = iteratorNext(docHndl))) {
        *doc = getObj(docMap, *slot);
        stat = DB_OK;
      } else
        stat = DB_ITERATOR_eof;

      break;
    case IterPrev:
      if ((slot = iteratorPrev(docHndl))) {
        *doc = getObj(docMap, *slot);
        stat = DB_OK;
      } else
        stat = DB_ITERATOR_eof;

      break;
    case IterSeek:
    case IterBegin:
    case IterEnd:
      if ((slot = iteratorSeek(docHndl, op, *docId))) {
        *doc = getObj(docMap, *slot);
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
  Handle *idxHndl, *cursorHndl;
  DbStatus stat = DB_OK;
  DbCursor *dbCursor;
  DbMap *idxMap;

  if ((idxHndl = bindHandle(dbIdxHndl, Hndl_anyIdx)))
      idxMap = MapAddr(idxHndl);
  else
      return DB_ERROR_handleclosed;

  if ((cursorHndl = makeHandle(idxMap, cursorSize[*idxMap->arena->type],
                               clntXtra[*idxMap->arena->type], Hndl_cursor)))
    dbCursor = getObj(idxMap, cursorHndl->clientAddr);
  else {
    releaseHandle(idxHndl, dbIdxHndl);
    return DB_ERROR_outofmemory;
  }

  dbCursor->binaryFlds = params[IdxKeyFlds].charVal;
  dbCursor->deDup = params[CursorDeDup].boolVal;

  switch (*idxMap->arena->type) {
    case Hndl_artIndex:
      stat = artNewCursor(dbCursor, idxMap);
      break;

    case Hndl_btree1Index:
      stat = btree1NewCursor(dbCursor, idxMap);
      break;

    case Hndl_btree2Index:
      stat = btree2NewCursor(dbCursor, idxMap);
      break;
  }

  hndl->hndlId.bits = cursorHndl->hndlId.bits;
  releaseHandle(idxHndl, dbIdxHndl);
  releaseHandle(cursorHndl, hndl);
  return stat;
}

//	release cursor resources

DbStatus closeCursor(DbHandle hndl[1]) {
  DbStatus stat = DB_ERROR_indextype;
  DbCursor *dbCursor;
  Handle *idxHndl;
  DbMap *idxMap;

  if ((idxHndl = bindHandle(hndl, Hndl_cursor)))
    idxMap = MapAddr(idxHndl);
  else
    return DB_ERROR_handleclosed;

  dbCursor = getObj(idxMap, idxHndl->clientAddr);

  switch (*idxMap->arena->type) {
    case Hndl_artIndex:
      stat = artReturnCursor(dbCursor, idxMap);
      break;

    case Hndl_btree1Index:
      stat = btree1ReturnCursor(dbCursor, idxMap);
      break;

    case Hndl_btree2Index:
      stat = btree2ReturnCursor(dbCursor, idxMap);
      break;
  }

  releaseHandle(idxHndl, hndl);
  return stat;
}

//	position cursor on a key

DbStatus positionCursor(DbHandle hndl[1], CursorOp op, void *key,
                        uint32_t keyLen) {
  DbCursor *dbCursor;
  Handle *idxHndl;
  DbStatus stat;
  DbMap *idxMap;

  if ((idxHndl = bindHandle(hndl, Hndl_cursor)))
      idxMap = MapAddr(idxHndl);
  else
      return DB_ERROR_handleclosed;

  dbCursor = getObj(idxMap, idxHndl->clientAddr);

  stat = dbFindKey(dbCursor, idxMap, key, keyLen, op);
  releaseHandle(idxHndl, hndl);
  return stat;
}

//	move cursor

DbStatus moveCursor(DbHandle hndl[1], CursorOp op) {
  DbCursor *dbCursor;
  Handle *cursorHndl;
  DbStatus stat;
  DbMap *idxMap;

  if ((cursorHndl = bindHandle(hndl, Hndl_cursor)))
      idxMap = MapAddr(cursorHndl);
  else
      return DB_ERROR_handleclosed;

  dbCursor = getObj(idxMap, cursorHndl->clientAddr);

  switch (op) {
    case OpLeft:
      stat = dbLeftKey(dbCursor, idxMap);
      break;
    case OpRight:
      stat = dbRightKey(dbCursor, idxMap);
      break;
    case OpNext:
      stat = dbNextKey(dbCursor, idxMap);
      break;
    case OpPrev:
      stat = dbPrevKey(dbCursor, idxMap);
      break;
    default:
      stat = DB_ERROR_cursorop;
      break;
  }

  releaseHandle(cursorHndl, hndl);
  return stat;
}

//	return cursor key

DbStatus keyAtCursor(DbHandle *hndl, uint8_t **key, uint32_t *keyLen) {
  DbCursor *dbCursor;
  Handle *cursorHndl;
  DbMap *idxMap;

  if ((cursorHndl = bindHandle(hndl, Hndl_cursor)))
    idxMap = MapAddr(cursorHndl);
  else
    return DB_ERROR_handleclosed;

  dbCursor = getObj(idxMap, cursorHndl->clientAddr);

  switch (dbCursor->state) {
    case CursorPosAt:
      if (key) *key = dbCursor->key;

      if (keyLen) *keyLen = dbCursor->keyLen;

      releaseHandle(cursorHndl, hndl);
      return DB_OK;

    default:
      break;
  }

  releaseHandle(cursorHndl, hndl);
  return DB_CURSOR_notpositioned;
}

DbStatus closeHandle(DbHandle hndl[1]) {
  Handle *handle;
  ObjId hndlId;

  if ((hndlId.bits = hndl->hndlId.bits))
    handle = fetchIdSlot(hndlMap, hndlId);
  else
    return DB_ERROR_handleclosed;

  hndl->hndlId.bits = 0;

  atomicOr8((volatile uint8_t *)handle->status, KILL_BIT);

  if (!handle->bindCnt[0]) destroyHandle(handle);

  return DB_OK;
}

DbStatus cloneHandle(DbHandle newHndl[1], DbHandle oldHndl[1]) {
  DbStatus stat = DB_OK;
  Handle *hndl, *hndl2;
  DbMap *map;

  if (oldHndl->hndlId.bits == 0) return newHndl->hndlId.bits = 0, DB_OK;

  if ((hndl = bindHandle(oldHndl, Hndl_any)))
    map = MapAddr(hndl);
  else
    return DB_ERROR_handleclosed;

  if ((hndl2 = makeHandle(map, hndl->clntSize, hndl->clntXtra,
                          hndl->hndlType)))
    newHndl->hndlId.bits = hndl2->hndlId.bits;
  else
    stat = DB_ERROR_outofmemory;

  if( !stat ) switch (hndl2->hndlType) {
      case Hndl_artIndex:
        stat = artNewCursor(getObj(map, hndl2->clientAddr), map);
        break;

      case Hndl_btree1Index:
        stat = btree1NewCursor(getObj(map, hndl2->clientAddr), map);
        break;

      case Hndl_btree2Index:
        stat = btree2NewCursor(getObj(map, hndl2->clientAddr), map);
        break;

      case Hndl_iterator: {
        Iterator *it = getObj(map, hndl2->clientAddr);
        it->state = IterLeftEof;
        it->docId.bits = 0;
        break;
      }
    }
  releaseHandle(hndl2, newHndl);
  releaseHandle(hndl, oldHndl);
  return stat;
}

//	fetch document from a docStore by docId

DbStatus fetchDoc(DbHandle hndl[1], void **doc, ObjId docId) {
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
      docMap = MapAddr(docHndl);
  else
      return DB_ERROR_handleclosed;

  slot = fetchIdSlot(docMap, docId);
  *doc = getObj(docMap, *slot);

  releaseHandle(docHndl, hndl);
  return DB_OK;
}

DbStatus deleteDoc(DbHandle hndl[1], ObjId docId) {
  DocStore *docStore;
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;

  if ((docHndl = bindHandle(hndl, Hndl_docStore))) 
      docMap = MapAddr(docHndl);
  else
      return DB_ERROR_handleclosed;

  docStore = (DocStore *)(docMap->arena + 1);
  atomicAdd64(docStore->docCount, -1LL);

  slot = fetchIdSlot(docMap, docId);
  freeBlk(docMap, *slot);
  slot->bits = 0;

  releaseHandle(docHndl, hndl);
  return DB_OK;
}


DbStatus storeDoc(DbHandle hndl[1], void *obj, uint32_t objSize, ObjId *docId) {
  DocStore *docStore;
  DbAddr *slot, addr;
  Handle *docHndl;
  DbMap *docMap;
  void *doc;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
      docMap = MapAddr(docHndl);
  else
      return DB_ERROR_handleclosed;

  if ((addr.bits = allocObj(docMap, listFree(docHndl, 0),
                            listWait(docHndl, 0), -1, objSize, false)))
    doc = getObj(docMap, addr);
  else {
    releaseHandle(docHndl, hndl);
    return DB_ERROR_outofmemory;
  }

  memcpy(doc, obj, objSize);

  docId->bits = allocObjId(docMap, listFree(docHndl, ObjIdType),
                           listWait(docHndl, ObjIdType));

  slot = fetchIdSlot(docMap, *docId);
  slot->bits = addr.bits;

  docStore = (DocStore *)(docMap->arena + 1);
  atomicAdd64(docStore->docCount, 1ULL);

  releaseHandle(docHndl, hndl);
  return DB_OK;
}

DbStatus deleteKey(DbHandle hndl[1], uint8_t *key, uint32_t len) {
  DbStatus stat = DB_OK;
  Handle *idxHndl;
  DbIndex *index;
  DbMap *idxMap;

  if ((idxHndl = bindHandle(hndl, Hndl_anyIdx))) 
      idxMap = MapAddr(idxHndl);
  else
      return DB_ERROR_handleclosed;

  switch (*idxMap->arena->type) {
    case Hndl_artIndex:
      stat = artDeleteKey(idxHndl, key, len, 0);
      break;

    case Hndl_btree1Index:
      stat = btree1DeleteKey(idxHndl, key, len);
      break;

    case Hndl_btree2Index:
      stat = btree2DeleteKey(idxHndl, key, len);
      break;
  }

  index = (DbIndex *)(idxMap->arena + 1);

  if (stat == DB_OK) 
      atomicAdd64(index->numKeys, -1LL);

  releaseHandle(idxHndl, hndl);
  return stat;
}

//	call back fcn to determine if key is unique
//	since we are not MVCC, all calls are from duplicates

//	return true if key is already in the index

bool uniqueKey(DbMap *map, DbCursor *dbCursor) { return true; }

DbStatus insertKey(DbHandle hndl[1], uint8_t *key, uint32_t len,
                   uint32_t sfxLen) {
  DbStatus stat = DB_OK;
  Handle *idxHndl;
  DbIndex *index;
  DbMap *idxMap;

  if ((idxHndl = bindHandle(hndl, Hndl_anyIdx))) 
      idxMap = MapAddr(idxHndl);
  else
      return DB_ERROR_handleclosed;

  switch (*idxMap->arena->type) {
    case Hndl_artIndex: {
      //	bool defer =
      //idxHndl->map->arenaDef->params[IdxKeyDeferred].boolVal;

      //	if (idxHndl->map->arenaDef->params[IdxKeyUnique].boolVal)
      //		stat = artInsertUniq(idxHndl, key, len, uniqueKey,
      //&defer); 	else
      stat = artInsertKey(idxHndl, key, len, sfxLen);
      break;
    }

    case Hndl_btree1Index:
      stat = btree1InsertKey(idxHndl, key, len, sfxLen, 0, Btree1_indexed);
      break;

    case Hndl_btree2Index:
      stat = btree2InsertKey(idxHndl, key, len, sfxLen, 0, Btree2_slotactive);
      break;
  }

  index = (DbIndex *)(idxMap->arena + 1);

  if (stat == DB_OK) atomicAdd64(index->numKeys, 1LL);

  releaseHandle(idxHndl, hndl);
  return stat;
}
