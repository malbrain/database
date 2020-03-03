// mvcc document implementation for database project

#include "mvcc_dbapi.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbidx.h"

DbStatus processKeys(Handle* docHndl, Ver* prevVer, Ver* ver, ObjId docId, uint16_t keyCount,
                     uint8_t *keyList);
uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit);

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
  doc->ourAddr.bits = docAddr.bits;

  //	fill-in stopper (verSize == 0) at end of version chain

  verSize = sizeof(struct Stopper_);
  verSize += 15;
  verSize &= -16;

  //  fill in stopper version

  ver = (Ver*)((uint8_t*)doc + rawSize - verSize);
  ver->offset = rawSize - verSize;
  ver->verSize = 0;

  doc->lastVer = ver->offset;

  //  install locked new head of version chain

  docSlot->bits = ADDR_MUTEX_SET | docAddr.addr;
  return doc;
}

//	insert an uncommitted new document into a docStore
    
MVCCResult mvcc_InsertDoc(Handle* docHndl, uint8_t* val, uint32_t valSize,
                    ObjId txnId, uint16_t keyCount, uint8_t *keyList) {
  DbMap* docMap = MapAddr(docHndl);
  
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objVer, .status = DB_OK};
  uint32_t verSize;
  DbAddr *docSlot;
  ObjId docId;
  Doc* doc;
  Ver* ver;

  //	assign a new docId slot

  docId.bits = allocObjId(docMap, listFree(docHndl, 0), listWait(docHndl, 0));

  if (docId.bits)
    docSlot = fetchIdSlot(docMap, docId);
  else
    return result.status = DB_ERROR_outofmemory, result.objType = objErr, result;

  verSize = sizeof(Ver) + keyCount * sizeof(DbAddr) + valSize;
  verSize += 15;
  verSize &= -16;

  //	start first version set

  if ((doc = chainNextDoc(docHndl, docSlot, valSize, keyCount)))
    ver = (Ver*)((uint8_t*)doc + doc->lastVer);
  else
    return result.status = DB_ERROR_outofmemory, result.objType = objErr, result;

  //    finish Doc

  doc->txnId.bits = txnId.bits;
  doc->docId.bits = docId.bits;
  doc->op = TxnInsert;

  //	fill-in new version

  memset(ver, 0, sizeof(Ver) + keyCount * sizeof(DbAddr));
  ver->offset = doc->lastVer;
  ver->verSize = verSize;

  memcpy((uint8_t*)(ver + 1) + keyCount * sizeof(DbAddr), val, valSize);

  result.status = processKeys(docHndl, NULL, ver, docId, keyCount, keyList);  
  return result.object = ver, result;
}

//  update existing Docment

MVCCResult mvcc_UpdateDoc(Handle* docHndl, uint8_t* val, uint32_t valSize,
                        uint64_t docBits, ObjId txnId, uint16_t keyCount, uint8_t *keyList) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  DbMap* docMap = MapAddr(docHndl);
  Ver *ver, *prevVer;
  uint32_t verSize;
  DbAddr* docSlot;
  ObjId docId;
  Doc* doc;

  docId.bits = docBits;
  docSlot = fetchIdSlot(docMap, docId);
  doc = getObj(docMap, *docSlot);

  prevVer = (Ver*)((uint8_t*)doc + doc->lastVer);

  verSize = sizeof(Ver) + keyCount * sizeof(DbAddr) + valSize;
  verSize += 15;
  verSize &= -16;

  //	start a new version set?

  if (verSize + sizeof(Doc) > doc->lastVer) {
    if ((doc = chainNextDoc(docHndl, docSlot, valSize, keyCount)))
      doc->op = TxnUpdate;
    else
      return result.objType = objErr, result.status = DB_ERROR_outofmemory, result;
  }

  doc->docId.bits = docId.bits;
  doc->txnId.bits = txnId.bits;

  ver = (Ver*)((uint8_t*)doc + doc->lastVer);
  memset(ver, 0, sizeof(Ver) + keyCount * sizeof(DbAddr));

  ver->verNo = prevVer->verNo + 1;
  ver->offset = doc->lastVer;
  ver->verSize = verSize;

  if (prevVer->commit) ver->verNo++;

  memcpy((uint8_t*)(ver + 1) + keyCount * sizeof(DbAddr), val, valSize);

  doc->lastVer -= verSize;
  assert(doc->lastVer >= sizeof(Doc));
 
  result.status = processKeys(docHndl, prevVer, ver, docId, keyCount, keyList);
  return result.object = ver, result.objType = objVer, result;
}

//	allocate docStore power-of-two memory

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit) {
	DbAddr* free = listFree(docHndl, 0);
	DbAddr* wait = listWait(docHndl, 0);

	return allocObj(MapAddr(docHndl), free, wait, -1, size, zeroit);
}

//  process document keyList

DbStatus processKeys(Handle* docHndl, Ver* prevVer, Ver* ver, ObjId docId, uint16_t keyCount,
                     uint8_t* keyList) {
  IndexKeyValue* srcKey, *destKey;
  DbMap* docMap = MapAddr(docHndl);
  DbStatus status;
  uint32_t hashKey;
  DocIndexes* docIdx;
  DbAddr insKey;
  int idx;

  docIdx = getObj(docMap, docHndl->clientAddr);

  for (idx = 0; idx < keyCount; idx++) {
    uint32_t size = sizeof(IndexKeyValue);
    srcKey = (IndexKeyValue*)keyList;
    size += srcKey->keyLen + srcKey->suffixLen;
    hashKey =
          hashVal(srcKey->bytes, srcKey->keyLen + srcKey->suffixLen);

    //  advance to next key

    keyList += size;

    //  see if this key already indexed

    if (prevVer) {

    }

    //  otherwise save key for delete/update
    //  and insert key into its index

    insKey.bits = allocDocStore(docHndl, size + calc64(docId.bits), true);
    destKey = getObj(docMap, insKey);
    memcpy(destKey, srcKey, size);  
    destKey->keyHash = hashKey;

    size += store64((uint8_t *)destKey, size, docId.bits);
    ((DbAddr*)(ver + 1))[idx] = insKey;

    if ((status = insertKey(&docIdx->idxHndls[destKey->keyIdx], destKey->bytes, destKey->keyLen, destKey->suffixLen))) 
        return status;
  }

  return DB_OK;
}