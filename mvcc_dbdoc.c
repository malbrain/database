// mvcc document implementation for database project

#include "mvcc_dbapi.h"

//	allocate docStore power-of-two memory

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit) {
  DbAddr* free = listFree(docHndl, 0);
  DbAddr* wait = listWait(docHndl, 0);

  return allocObj(MapAddr(docHndl), free, wait, -1, size, zeroit);
}

//	prepare space for an uncommitted new version in a docStore
//  if it doesn't fit, install new head of doc chain

MVCCResult mvcc_installNewDocVer(DbHandle hndl[1], uint32_t valSize,
                         ObjId *docId, ObjId txnId) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objErr, .status = DB_OK};
  Handle* docHndl = bindHandle(hndl, Hndl_docStore);
  DbMap* docMap = MapAddr(docHndl);
  DbAddr* docSlot;
  DocStore* docStore;
  Doc *doc, *prevDoc;
  uint32_t blkSize;
  uint32_t verSize;
  uint32_t stopSize;
  DbAddr docAddr;
  Ver* ver;

  if (!docHndl) 
      return result.status = DB_ERROR_badhandle, result;

  docStore = (DocStore*)(docMap->arena + 1);
  blkSize = docStore->blkSize;

  verSize = sizeof(Ver) + docStore->keyCnt * sizeof(DbAddr) + valSize;
  verSize += 15;
  verSize &= -16;
 
  //	set up the document version

  if (!docId->bits)
    if (!(docId->bits =
              allocObjId(docMap, listFree(docHndl, 0), listWait(docHndl, 0))))
      return result.status = DB_ERROR_outofmemory, result;

  docSlot = fetchIdSlot(docMap, *docId);

  if (docSlot->bits)
    prevDoc = getObj(docMap, *docSlot);
  else
    prevDoc = NULL;

  //	allocate space in docStore for a new mvcc block

  if (docSlot->bits == 0 || verSize + prevDoc->doc->docMin > prevDoc->newestVer)
    if ((docAddr.bits = allocDocStore(docHndl, blkSize, false)))
      blkSize = db_rawSize(docAddr);
    else
      return result.status = DB_ERROR_outofmemory, result;
  else
    goto initVer;

  // init new block

  doc = getObj(docMap, docAddr);
  memset(doc, 0, sizeof(Doc));
  doc->prevAddr = *docSlot;
                                                                                                           
  doc->doc->ourAddr.bits = docAddr.bits;
  doc->doc->docId.bits = docId->bits;

  //	fill-in stopper (verSize == 0) at end of version chain

  stopSize = sizeof(struct Stopper_);
  stopSize += 15;
  stopSize &= -16;

  //  fill in stopper version

  ver = (Ver*)(doc->doc->base + blkSize - stopSize);
  ver->stop->offset = blkSize - stopSize;
  ver->stop->verSize = 0;

  doc->newestVer = blkSize - stopSize;

  //    configure new version under stopper

initVer:
  ver = (Ver*)(doc->doc->base + doc->newestVer - verSize);
  ver->stop->offset = doc->newestVer - verSize;
  ver->stop->verSize = verSize;

  ver->keys->vecMax = docStore->keyCnt;
  ver->keys->vecLen = 0;

  //  install locked new head of version chain --
  //  integrate by subtracting verSize from newestVer
  //  and store in pendingVer

  doc->txnId.bits = txnId.bits;
  doc->op = TxnWrite;

  ver->verNo = ++doc->verNo;
  doc->op = TxnWrite;

  docSlot->bits = ADDR_MUTEX_SET | docAddr.addr;

  result = mvcc_addDocWrToTxn(txnId, docMap, docId, 1, hndl);

  if (result.objType != objTxn) return result;

  result.object = doc;
  return result;
}

//  process new version document key

MVCCResult mvcc_ProcessKey(DbHandle hndl[1], DbHandle hndlIdx[1], Ver* prevVer,
                           Ver* ver, ObjId docId, KeyValue* srcKey) {
  Handle *docHndl = bindHandle(hndl, Hndl_docStore);
  MVCCResult result = {
      .value = 0, .count = 0, .objType = 0, .status = DB_OK};
  uint32_t size = sizeof(KeyValue);
  DbMap* docMap = MapAddr(docHndl);
  DbAddr insKey, addr;
  KeyValue *destKey;
  uint32_t hashKey;
  int slot;

	if( !docHndl )
		return result.objType = objErr, result.status = DB_ERROR_handleclosed, result;

	docMap = MapAddr(docHndl);
    size += srcKey->keyLen + srcKey->suffixLen;
    hashKey =
          hashVal(srcKey->bytes, srcKey->keyLen + srcKey->suffixLen);

    //  see if this key already indexed
	//  in previous version

    if (prevVer) {

    }

    //  otherwise install key for delete/update
    //  and insert key into its index

    insKey.bits = allocDocStore(docHndl, size + calc64(docId.bits), true);
    destKey = getObj(docMap, insKey);
    memcpy(destKey, srcKey, size);  
    destKey->keyHash = hashKey;

    size += store64((uint8_t *)destKey, size, docId.bits);
	addr.bits = insKey.bits;

	if(( slot = vectorPush(docMap, ver->keys, addr)))
		destKey->vecIdx = slot;
	else
    	return result.status = DB_ERROR_outofmemory, result.objType = objErr, result;

    result.status = insertKey(hndlIdx, destKey->bytes, destKey->keyLen, destKey->suffixLen);
    return result;
}

Doc* chainNextDoc(Handle* docHndl, DbAddr* docSlot, uint32_t valSize,
                  uint16_t keyCount) {
  uint32_t rawSize = valSize + sizeof(Doc) + sizeof(Ver) +
                     sizeof(struct Stopper_) + keyCount * sizeof(DbAddr);
  DbMap* docMap = MapAddr(docHndl);
  Doc *doc, *prevDoc;
  uint32_t verSize;
  DbAddr docAddr;
  Ver* ver;

  if (rawSize < 12 * 1024 * 1024) rawSize += rawSize / 2;

  //	allocate space in docStore for the new version

  if ((docAddr.bits = allocDocStore(docHndl, rawSize, false)))
    rawSize = db_rawSize(docAddr);
  else
    return NULL;

  //	set up the document header

  if (docSlot->bits) {
    prevDoc = getObj(docMap, *docSlot);
    prevDoc->nextAddr.bits = docAddr.bits;
  }

  doc = getObj(docMap, docAddr);
  memset(doc, 0, sizeof(Doc));

  doc->prevAddr.bits = docSlot->addr;
  doc->doc->ourAddr.bits = docAddr.bits;

  //	fill-in stopper (verSize == 0) at end of version chain

  verSize = sizeof(struct Stopper_);
  verSize += 15;
  verSize &= -16;

  //  fill in stopper version

  ver = (Ver*)(doc->doc->base + rawSize - verSize);
  ver->stop->offset = rawSize - verSize;
  ver->stop->verSize = 0;

  doc->newestVer = ver->stop->offset;
  ver = (Ver*)(doc->doc->base + doc->newestVer);
  ver->keys->vecMax = keyCount;
  ver->keys->vecLen = 0;

  //  install locked new head of version chain

  docSlot->bits = ADDR_MUTEX_SET | docAddr.addr;
  return doc;
}
