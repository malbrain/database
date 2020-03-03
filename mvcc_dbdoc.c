// mvcc document implementation for database project

#include "mvcc_dbapi.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbidx.h"
#include "mvcc_dbtxn.h"

//	allocate docStore power-of-two memory

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit) {
  DbAddr* free = listFree(docHndl, 0);
  DbAddr* wait = listWait(docHndl, 0);

  return allocObj(MapAddr(docHndl), free, wait, -1, size, zeroit);
}

//  install next head of doc chain

Doc* chainNextDoc(Handle* docHndl, DbAddr *docSlot, uint32_t valSize, uint16_t keyCount) {
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
  ver->offset = rawSize - verSize;
  ver->verSize = 0;

  doc->newestVer = ver->offset;
  ver = (Ver*)(doc->doc->base + doc->newestVer);
  ver->keys->vecMax = keyCount;
  ver->keys->vecLen = 0;

  //  install locked new head of version chain

  docSlot->bits = ADDR_MUTEX_SET | docAddr.addr;
  return doc;
}

//	insert an uncommitted new document into a docStore
    
MVCCResult mvcc_InsertDoc(DbHandle hndl[1], uint8_t* val, uint32_t valSize,
                    ObjId txnId, uint32_t keyCnt) {
  Handle *docHndl = bindHandle(hndl, Hndl_docStore);
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objVer, .status = DB_OK};
  uint32_t verSize;
  DbAddr *docSlot;
  DbMap* docMap;
  ObjId docId;
  Doc* doc;
  Ver* ver;

  if( !docHndl )
	return result.objType = objErr, result.status = DB_ERROR_handleclosed, result;

  //	assign a new docId slot

  docMap = MapAddr(docHndl);
  docId.bits = allocObjId(docMap, listFree(docHndl, 0), listWait(docHndl, 0));

  if (docId.bits)
    docSlot = fetchIdSlot(docMap, docId);
  else
    return result.status = DB_ERROR_outofmemory, result.objType = objErr, result;

  verSize = sizeof(Ver) + keyCnt * sizeof(DbAddr) + valSize;
  verSize += 15;
  verSize &= -16;

  //	start first version set

  if ((doc = chainNextDoc(docHndl, docSlot, valSize, keyCnt)))
    ver = (Ver*)(doc->doc->base + doc->newestVer);
  else
    return result.status = DB_ERROR_outofmemory, result.objType = objErr, result;

  //    finish Doc

  doc->txnId.bits = txnId.bits;
  doc->doc->docId.bits = docId.bits;
  doc->op = TxnInsert;

  //	fill-in new version

  memset(ver, 0, sizeof(Ver) + keyCnt * sizeof(DbAddr));
  ver->offset = doc->newestVer;
  ver->verSize = verSize;

  memcpy((uint8_t*)(ver + 1) + keyCnt * sizeof(DbAddr), val, valSize);
  return result.object = ver, result;
}

//  update existing Docment

MVCCResult mvcc_UpdateDoc(DbHandle hndl[1], uint8_t* val, uint32_t valSize,
                        uint64_t docBits, ObjId txnId, uint32_t keyCnt) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = 0, .status = DB_OK};
  Handle *docHndl = bindHandle(hndl, Hndl_docStore);
  Ver *ver, *prevVer;
  uint32_t verSize;
  DbAddr* docSlot;
  DbMap* docMap;
  ObjId docId;
  Doc* doc;

  if( !docHndl )
	return result.objType = objErr, result.status = DB_ERROR_handleclosed, result;

  docMap = MapAddr(docHndl);
  docId.bits = docBits;
  docSlot = fetchIdSlot(docMap, docId);
  doc = getObj(docMap, *docSlot);

  prevVer = (Ver*)(doc->doc->base + doc->newestVer);

  verSize = sizeof(Ver) + keyCnt * sizeof(DbAddr) + valSize;
  verSize += 15;
  verSize &= -16;

  //	start a new version set?

  if (verSize + sizeof(Doc) > doc->newestVer) {
    if ((doc = chainNextDoc(docHndl, docSlot, valSize, keyCnt)))
      doc->op = TxnUpdate;
    else
      return result.objType = objErr, result.status = DB_ERROR_outofmemory, result;
  }

  doc->doc->docId.bits = docId.bits;
  doc->txnId.bits = txnId.bits;

  ver = (Ver*)(doc->doc->base + doc->newestVer);
  memset(ver, 0, sizeof(Ver) + keyCnt * sizeof(DbAddr));

  ver->offset = doc->newestVer;
  ver->verSize = verSize;

  if (prevVer->commit) doc->verNo++;

  memcpy((uint8_t*)(ver + 1) + keyCnt * sizeof(DbAddr), val, valSize);

  doc->newestVer -= verSize;
  assert(doc->newestVer >= sizeof(Doc));
  return result.object = ver, result.objType = objVer, result;
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
