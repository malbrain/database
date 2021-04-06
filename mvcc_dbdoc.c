// mvcc document implementation for database project

#include "mvcc.h"

//	allocate docStore power-of-two memory

uint64_t allocDocStore(Handle* docHndl, uint32_t size, bool zeroit) {
  DbAddr* free = listFree(docHndl, 0);
  DbAddr* wait = listWait(docHndl, 0);

  return allocObj(MapAddr(docHndl), free, wait, -1, size, zeroit);
}

//    allocate and install new document versions
//  docSlot points at slot containing the DbAddr of the doc

MVCCResult chainNewDoc(Handle* docHndl, DbAddr* docSlot, uint32_t verSize) {
  MVCCResult result = { .value = 0, .count = 0, .objType = objDoc, .status = DB_OK};
  DbMap* docMap = MapAddr(docHndl);
  DocStore* docStore;
  Doc *doc, *prevDoc;
  uint32_t stopSize, rawSize, blkSize;
  DbAddr docAddr;
  Ver* ver;

  stopSize = sizeof(ver->stop);
  stopSize += 15;
  stopSize &= -16;

  docStore = (DocStore *)(docMap + 1);
  blkSize = docStore->blkSize;

  verSize += stopSize + sizeof(Doc);

  while( verSize > blkSize )
    blkSize *= 2;

  //	set up the document 

  if( !docSlot )
    return result.status = DB_ERROR_badobjslot, result;

  if (docSlot->addr)
    prevDoc = getObj(docMap, *docSlot);
  else
    prevDoc = NULL;

  //  allocate space in docStore for the new Document and version
    
  if ((docAddr.bits = allocDocStore(docHndl, blkSize, false)))
    doc = getObj(docMap, docAddr);
  else 
    return result.status = DB_ERROR_outofmemory, result;

  rawSize = db_rawSize(docAddr);

  // init new document block

  memset(doc, 0, sizeof(Doc));           
  doc->commitVer = rawSize - stopSize;
  doc->prevAddr = *docSlot;

  doc->dbDoc->docId.bits = docAddr.bits;
  doc->dbDoc->docType = VerMvcc;

  //  fill-in stopper version (verSize == 0) at end of document

  ver = (Ver*)(doc->dbDoc->base + rawSize - stopSize);
  ver->stop->offset = rawSize - stopSize;
  ver->stop->verSize = 0;

  //  install locked new head of version chain

  docSlot->bits = ADDR_MUTEX_SET | docAddr.addr;
  return result;
}

//  process new document's version keys

MVCCResult mvcc_ProcessKeys(DbHandle hndl[1], DbHandle hndlIdx[1], Ver* prevVer, Ver* ver, DocId docId, KeyValue* srcKey) {

  Handle *docHndl = bindHandle(hndl, Hndl_docStore);
  Handle *idxHndl = bindHandle(hndlIdx, Hndl_anyIdx);
  MVCCResult result = {
      .value = 0, .count = 0, .objType = 0, .status = DB_OK};
  uint32_t size = sizeof(KeyValue);
  DbMap* docMap = MapAddr(docHndl);
  DbAddr insKey, addr;
  KeyValue *destKey;
  uint32_t hashKey, verSize;
  int slot;

	if( !docHndl )
		return result.objType = objErr, result.status = DB_ERROR_handleclosed, result;

	docMap = MapAddr(docHndl);
    size += srcKey->keyLen + srcKey->suffix;
    hashKey =
          hashVal(srcKey->bytes, srcKey->keyLen + srcKey->suffix);

    //  see if this key already indexed
	//  in previous version

    //	fill-in stopper (verSize == 0) at end of version chain

    if (prevVer) {
     verSize = sizeof(struct Stop);

    }

    //  otherwise install key for delete/update
    //  and i key into its index

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

//    result.status = insertKeyValue(idxHndl, destKey);
    return result;
}

MVCCResult mvcc_installNewVersion(Handle *docHndl, uint32_t valSize, DocId *docSlot, uint16_t keyCnt) { 
  MVCCResult result = {
    .value = 0, .count = 0, .objType = objDoc, .status = DB_OK};
  DbMap* docMap = MapAddr(docHndl);
  Doc *prevDoc;
  uint32_t verSize;
  Ver* ver;
  Doc* doc;

  if (docSlot)
    prevDoc = getObj(docMap, *docSlot);
  else
    return result.status = DB_ERROR_badhandle, result;

  verSize = sizeof(Ver) + keyCnt * sizeof(DbAddr) + valSize;

  //	allocate space in docStore for a new mvcc version
  //   or add new version to existing Document

  if (verSize > prevDoc->commitVer )
    result = mvcc_installNewVersion(docHndl, verSize, docSlot, keyCnt );
  
  if( result.status != DB_ERROR_outofmemory) 
      return result;
 
  // new version fits in existing document

  result =(MVCCResult) {
    .value = 0, .count = 0, .objType = 0, .status = DB_OK};

  //  configure pending version under commit (committed)
  //  install new head of version chain --
  //  subtract verSize from commitVer
  //  and store in pendingVer

  doc = getObj(docMap, *docSlot);
  doc->pendingVer = doc->commitVer - verSize;

  ver = (Ver*)(doc->dbDoc->base + doc->pendingVer);
  ver->verNo = ++doc->verNo;

  ver->stop->offset = doc->pendingVer;
  ver->stop->verSize = verSize;

  ver = (Ver*)(doc->dbDoc->base + doc->commitVer);
  ver->keys->vecMax = keyCnt;
  ver->keys->vecLen = 0;

  //  install version offset in doc

  doc->op = OpWrt;

  result.object = doc;
  return result;
}



MVCCResult mvcc_WriteDoc(Txn *txn, DbHandle dbHndl[1], DocId *docId, uint32_t valSize,  uint8_t *valBytes, uint16_t keyCnt) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = 0, .status = DB_OK };
  Handle *docHndl = bindHandle(dbHndl, Hndl_docStore);
  DbMap *docMap = MapAddr(docHndl);
  Doc *doc;
  DbAddr *docSlot;

  if (!docId->bits)
    docId->bits = allocObjId(docMap, listFree(docHndl, ObjIdType),
    listWait(docHndl, ObjIdType));

  docSlot = fetchIdSlot(docMap, *docId);

  result = mvcc_installNewVersion(docHndl, valSize, docId, keyCnt);

  if (result.status == DB_OK) {
    doc = result.object;
    result = mvcc_addDocWrToTxn(txn, docHndl, (Doc *)result.object);

    memcpy(doc + 1, valBytes, valSize);
  }
  
  return result;
}