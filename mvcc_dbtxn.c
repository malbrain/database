//  implement transactions

#include "mvcc_dbapi.h"
#include "mvcc_dbdoc.h"
#include "mvcc_dbtxn.h"

//  Txn arena free txn frames

void initTxn(int maxclients);
DbMap* txnMap, memMap[1];
uint8_t txnInit[1];

 #ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#ifndef _WIN32
#include <time.h>
#include <sys/time.h>
#endif

//	GlobalTxn structure, follows ArenaDef in Txn file

typedef struct {
  uint64_t rdtscUnits;
  DbAddr txnFree[1];    // frames of available txnId
	DbAddr txnWait[1];		// frames of waiting txnId
    uint16_t maxClients;	// number of timestamp slots
    Timestamp baseTs[1];	// ATOMIC-ALIGN master timestamp slot,
							// followed by timestamp client slots
} GlobalTxn;

GlobalTxn* globalTxn;

//	initialize transaction database

void initTxn(int maxClients) {
	ArenaDef arenaDef[1];

	lockLatch(txnInit);

	if (*txnInit & TYPE_BITS) {
		unlockLatch(txnInit);
		return;
	}

	// configure transaction table

	memset(arenaDef, 0, sizeof(arenaDef));
	arenaDef->params[OnDisk].boolVal = hndlMap->arenaDef->params[OnDisk].boolVal;
	arenaDef->arenaXtra = sizeof(GlobalTxn) + sizeof(Timestamp) * maxClients;
	arenaDef->arenaType = Hndl_txns;
	arenaDef->objSize = sizeof(Txn);

	txnMap = openMap(NULL, "Txns", 4, arenaDef, NULL);
	txnMap->db = txnMap;

	globalTxn = (GlobalTxn*)(txnMap->arena + 1);

	if (globalTxn->maxClients == 0) {
          globalTxn->rdtscUnits = timestampInit(globalTxn->baseTs, maxClients + 1);
          globalTxn->maxClients = maxClients + 1;
        } else
          rdtscUnits = globalTxn->rdtscUnits;

	*txnMap->arena->type = Hndl_txns;
	*txnInit = Hndl_txns;
}

//	fetch and lock txn

Txn* mvcc_fetchTxn(ObjId txnId) {
	Txn* txn = fetchIdSlot(txnMap, txnId);

	lockLatch(txn->state);
	return txn;
}

// Add docId to txn write set
//	add version creation to txn write-set
//  do not call for "update my writes"

MVCCResult mvcc_addDocWrToTxn(ObjId txnId, DbMap *docMap, ObjId *docId, int tot,
                              DbHandle hndl[1]) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  uint64_t values[1024 + 16];
  DbAddr *slot;
  ObjId objId;
  int cnt = 0;
  Doc *doc;
  Ver *ver;
  Txn *txn;

  txn = mvcc_fetchTxn(txnId);

  if (txn->isolation == TxnNotSpecified)
      goto wrXit;

  if ((*txn->state & TYPE_BITS) != TxnGrow) {
    result.status = DB_ERROR_txn_being_committed;
    goto wrXit;
  }

  objId.bits = hndl->hndlId.bits;
  objId.xtra = TxnHndl;

  if (txn->hndlId->bits != hndl->hndlId.bits) {
    txn->hndlId->bits = hndl->hndlId.bits;
    values[cnt++] = objId.bits;
  }

  while (tot--) {
    docId->xtra = TxnWrt;

    slot = fetchIdSlot(docMap, *docId++);
    doc = getObj(docMap, *slot);
    ver = (Ver *)(doc->doc->base + doc->newestVer);

    values[cnt++] = docId->bits;

    if (txn->isolation == TxnSerializable) {
      Ver *prevVer = (Ver *)(ver->verBase + ver->verSize);

      if (prevVer) timestampCAS(txn->pstamp, prevVer->pstamp, -1);

      if (timestampCmp(txn->sstamp, txn->pstamp) <= 0) {
        result.status = DB_ERROR_txn_not_serializable;
        goto wrXit;
      }
    }
  }

  result.status = addValuesToFrame(txnMap, txn->docFrame, txn->docFirst, values, cnt)
               ? (DbStatus)DB_OK : DB_ERROR_outofmemory;

  if(result.status) {
    result.status = DB_ERROR_txn_being_committed;
    goto wrXit;
  }

  txn->wrtCount += tot;

wrXit:
  unlockLatch(txn->state);
  return result;
}

//	add docId and verNo to txn read-set
//	do not call for "read my writes"

MVCCResult mvcc_addDocRdToTxn(ObjId txnId, DbMap *docMap, ObjId docId, Ver* ver, DbHandle hndl[1]) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  Doc *doc = (Doc *)(ver->verBase - ver->offset);
  Txn *txn = mvcc_fetchTxn(txnId);
    uint64_t values[3];
	DbAddr* slot;
	int cnt = 0;
	ObjId objId;

	if (txn->isolation != TxnSerializable) goto rdXit;

	if ((*txn->state & TYPE_BITS) != TxnGrow) {
          result.status = DB_ERROR_txn_being_committed;
          goto rdXit;
        }
    
    //  switch docStore?

    objId.bits = hndl->hndlId.bits;
	objId.xtra = TxnHndl;

    slot = fetchIdSlot(hndlMap, objId);

	if (txn->hndlId->bits != hndl->hndlId.bits) {
		txn->hndlId->bits = hndl->hndlId.bits;
		values[cnt++] = objId.bits;
	}

    //  capture predecessor and successor timestamp ranges
    // or add to read set to chk later

    timestampCAS(txn->pstamp, ver->commit, -1);

    // if no successor, add to txn read set

    if (ver->sstamp->tsBits[1] == ~0ULL) {
        docId.xtra = TxnRdr;
        values[cnt++] = docId.bits;
        values[cnt++] = doc->verNo;
    } else
        timestampCAS(txn->sstamp, ver->sstamp, 1);

    if (cnt)
		addValuesToFrame(txnMap, txn->rdrFrame, txn->rdrFirst, values, cnt);

    // verify exclusion window validity

    if (timestampCmp(txn->sstamp, txn->pstamp) <= 0)
		result.status = DB_ERROR_txn_not_serializable;

rdXit:
	unlockLatch(txn->state);
	return result;
}

// 	begin a new Txn

MVCCResult mvcc_BeginTxn(Params* params, ObjId nestedTxn) {
  MVCCResult result = {.value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  uint16_t tsClnt;
  ObjId txnId;
  Txn* txn;

  if (!*txnInit) initTxn(1024);

  if ((tsClnt = timestampClnt(globalTxn->baseTs, globalTxn->maxClients)))
    timestampNext(globalTxn->baseTs, tsClnt);
  else
    return result.status = MVCC_NoTimestampSlots, result;

  if ((txnId.bits = allocObjId(txnMap, globalTxn->txnFree, globalTxn->txnWait)))
    txn = fetchIdSlot(txnMap, txnId);
  else {
    timestampQuit(globalTxn->baseTs, tsClnt);
    return result.status = MVCC_outofmemory, result;
  }

  memset(txn, 0, sizeof(Txn));
  timestampInstall(txn->reader, globalTxn->baseTs + tsClnt);

  if (params[Concurrency].intVal)
    txn->isolation = params[Concurrency].charVal;
  else
    txn->isolation = TxnSnapShot;

  txn->tsClnt = tsClnt;
  txn->nextTxn = nestedTxn.bits;
  *txn->state = TxnGrow;

  txn->sstamp->tsBits[1] = ~0ULL;
  result.value = txnId.bits;
  return result;
}

MVCCResult mvcc_RollbackTxn(Params *params, uint64_t txnBits) {
MVCCResult result = {
    .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};

return result;
}

//	retrieve version by verNo

Ver *mvcc_getVersion(DbMap *map, Doc *doc, uint64_t verNo) {
  uint32_t offset, size;
  Ver *ver;

  offset = doc->newestVer;

  //	enumerate previous document versions

  do {
    ver = (Ver *)(doc->doc->base + offset);

    //  continue to next version chain on stopper version

    if (!(size = ver->verSize)) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->newestVer;
        continue;
      } else
        return NULL;
    }

    if (doc->verNo == verNo) break;

  } while ((offset += size));

  return ver;
}

//  find appropriate document version per reader timestamp

MVCCResult mvcc_findDocVer(DbMap *map, Doc *doc, DbMvcc *dbMvcc) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objVer, .status = DB_OK};
  uint32_t offset, size;
  DbStatus stat;
  ObjId txnId;
  Ver *ver;

  offset = doc->newestVer;

  //  is there a pending update for the document
  //	made by our transaction?

  if ((txnId.bits = doc->txnId.bits)) {
    ver = (Ver *)(doc->doc->base + offset);

    if (dbMvcc->txnId.bits == txnId.bits) {
      result.object = ver;
      return result;
    } 

    // otherwise find a previously committed version

    offset += ver->verSize;
  }

  //	examine previously committed document versions

  do {
    ver = (Ver *)(doc->doc->base + offset);

    //  continue to next version chain on stopper version

    if (!ver->verSize) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->newestVer;
        continue;
      } else
        return result.status = DB_ERROR_no_visible_version, result;
    }

      if (timestampCmp(ver->commit, dbMvcc->reader) <= 0)
        break;

  } while ((offset += ver->verSize));

  //	add this document to the txn read-set

  if (dbMvcc->txnId.bits)
    result = mvcc_addDocRdToTxn(dbMvcc->txnId, map, doc->doc->docId, ver, dbMvcc->hndl);
  result.objType = objVer;
  result.object = ver;
  return result;
}

//  find predecessor version

Ver *mvcc_firstCommittedVersion(DbMap *map, Doc *doc, ObjId docId) {
  uint32_t offset = doc->newestVer;
  uint32_t size;
  Ver *ver;

  ver = (Ver *)(doc->doc->base + offset);

  if (!(size = ver->verSize)) {
    if (doc->prevAddr.bits)
      doc = getObj(map, doc->prevAddr);
    else
      return 0;

    offset = doc->newestVer;
  }

  return (Ver *)(doc->doc->base + offset + ver->verSize);
}

//	verify and commit txn under
//	Serializable isolation

bool SSNCommit(Txn *txn) {
  DbAddr *slot, next, addr;
  Ver *ver, *prevVer;
  bool result = true;
  uint64_t *wrtMmbr;
  uint32_t offset;
  DbAddr wrtSet[1];
  Handle *docHndl;
  DbMap *docMap;
  int frameSet;
  ObjId docId;
  ObjId objId;
  int nSlot;
  Doc *doc;
  int idx;

  wrtSet->bits = 0;
  iniMmbr(memMap, wrtSet, txn->wrtCount);

  // make a WrtSet deduplication
  // mmbr hash table

  next.bits = txn->docFirst->bits;
  docHndl = NULL;
  frameSet = 0;

  //  PreCommit

  //  construct de-duplication hash table for wrtSet
  //	and finalize txn->pstamp

  while (next.addr) {
    Frame *frame = getObj(txnMap, next);

    // when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->docFrame->nslot;
    else
      nSlot = FrameSlots;

    //  finalize txn wrt set
    //  nb. each doc has an uncommitted  version
    //  below the newest committed version
    //  at doc->newestVer

    for (idx = 0; idx < nSlot; idx++) {

      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        Ver *prevVer;

        doc = getObj(docMap, *docSlot);
        doc->op |= TxnCommit;

        //  set minimum txn predecessor timestamp
        //  our txn wrote the successor to prevVer

        prevVer = (Ver *)(doc->doc->base + doc->newestVer);

        //	keep larger pstamp

        timestampCAS(txn->pstamp, prevVer->pstamp, -1);

        unlockLatch(docSlot->latch);
        frameSet = 0;
        continue;
      }

      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          docHndl = fetchIdSlot(hndlMap, objId);
          docMap = MapAddr(docHndl);
          continue;

        case TxnWrt:
          docId.bits = objId.bits;
          wrtMmbr = setMmbr(memMap, wrtSet, docId.bits, true);

          // add docId to wrtSet dedup hash table
          // this txn owns the document

          *wrtMmbr = docId.bits;
          frameSet = 1;
          break;
      }
    }

    next.bits = frame->prev.bits;
  }

  if (docHndl) releaseHandle(docHndl, NULL);

  //  PreCommit

  // finalize txn->sstamp from the readSet

  next.bits = txn->rdrFirst->bits;
  docHndl = NULL;
  frameSet = 0;

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

    // when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->rdrFrame->nslot;
    else
      nSlot = FrameSlots;

    for (idx = 0; idx < nSlot; idx++) {
      // finish TxnDoc steps

      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        uint64_t verNo = frame->slots[idx];

        frameSet = 0;

        // if we also write this read-set mmbr, skip it

        if (*setMmbr(memMap, wrtSet, docId.bits, false)) continue;

        doc = getObj(docMap, *docSlot);
        prevVer = mvcc_getVersion(docMap, doc, verNo);

        //	keep smaller sstamp

        timestampCAS(txn->sstamp, prevVer->sstamp, 1);
        continue;
      }

      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          docHndl = fetchIdSlot(hndlMap, objId);
          continue;

        case TxnRdr:
        case TxnWrt:
          docId.bits = objId.bits;
          frameSet = 1;
          continue;
      }
    }

    next.bits = frame->prev.bits;
  }

  if (timestampCmp(txn->sstamp, txn->pstamp) <= 0) 
	  result = false;

  if (result)
    *txn->state = TxnCommit | MUTEX_BIT;
  else
    *txn->state = TxnRollback | MUTEX_BIT;

  if (docHndl) releaseHandle(docHndl, NULL);

  //  Post Commit

  //  process the reader pstamp from our commit time
  //	return reader set Frames.

  next.bits = txn->rdrFirst->bits;
  docHndl = NULL;
  frameSet = 0;

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

    // when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->rdrFrame->nslot;
    else
      nSlot = FrameSlots;

    for (idx = 0; idx < nSlot; idx++) {
      // finish TxnDoc steps

      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        uint64_t verNo = frame->slots[idx];

        frameSet = 0;

        // if we also write this read-set mmbr, skip it

        if (*setMmbr(memMap, wrtSet, docId.bits, false)) continue;

        doc = getObj(docMap, *docSlot);
        ver = mvcc_getVersion(docMap, doc, verNo);

        //	keep larger ver pstamp

        timestampCAS(ver->pstamp, txn->commit, -1);
        continue;
      }

      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnKill:
          continue;

        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          docHndl = fetchIdSlot(hndlMap, objId);
          continue;

        case TxnWrt:
        case TxnRdr:
          docId.bits = objId.bits;
          frameSet = 0;
          continue;
      }
    }

    next.bits = frame->prev.bits;
    returnFreeFrame(txnMap, addr);
  }

  if (docHndl) releaseHandle(docHndl, NULL);

  //  finally install wrt set versions

  next.bits = txn->docFirst->bits;
  docHndl = NULL;
  frameSet = 0;

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

    // when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->docFrame->nslot;
    else
      nSlot = FrameSlots;

    for (idx = 0; idx < nSlot; idx++) {
      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnKill:
          continue;

        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          docHndl = fetchIdSlot(hndlMap, objId);
          continue;

        case TxnWrt:
          slot = fetchIdSlot(docMap, objId);
          lockLatch(slot->latch);
          break;
      }

      // find previous version

      doc = getObj(docMap, *slot);
      ver = (Ver *)(doc->doc->base + doc->newestVer);

      offset = doc->newestVer + ver->verSize;
      prevVer = (Ver *)(doc->doc->base + offset);

      //	at top end, find in prev doc block

      if (!prevVer->verSize) {
        if (doc->prevAddr.bits) {
          Doc *prevDoc = getObj(docMap, doc->prevAddr);
          prevVer = (Ver *)((uint8_t *)prevDoc->doc->base + prevDoc->newestVer);
        } else
          prevVer = NULL;
      }

      if (prevVer) timestampInstall(prevVer->sstamp, txn->commit);

      timestampInstall(ver->commit, txn->commit);
      timestampInstall(ver->pstamp, txn->commit);
      ver->sstamp->tsBits[1] = ~0ULL;

      doc->txnId.bits = 0;
      doc->op = TxnDone;

      unlockLatch(slot->latch);
      continue;
    }

    next.bits = frame->prev.bits;
    returnFreeFrame(txnMap, addr);
  }

  if (docHndl) releaseHandle(docHndl, NULL);

  return result;
}

//	commit txn under snapshot isolation
//	always succeeds

bool snapshotCommit(Txn *txn) {
  DbAddr *slot, addr, next;
  Handle *docHndl;
  DbMap *docMap;
  int nSlot, idx;
  ObjId objId;
  Doc *doc;
  Ver *ver;

  next.bits = txn->docFirst->bits;
  docHndl = NULL;

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

	// when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->docFrame->nslot;
    else
      nSlot = FrameSlots;

    for (idx = 0; idx < nSlot; idx++) {
      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnKill:
          continue;

        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          docHndl = fetchIdSlot(hndlMap, objId);
          docMap = MapAddr(docHndl);
          continue;

        case TxnRdr:
        case TxnWrt:
          slot = fetchIdSlot(docMap, objId);
          lockLatch(slot->latch);
          break;
      }

      doc = getObj(docMap, *slot);
      ver = (Ver *)(doc->doc->base + doc->newestVer);

      timestampInstall(ver->commit, txn->commit);
      doc->txnId.bits = 0;
      doc->op = TxnDone;

      unlockLatch(slot->latch);

      //	TODO: add previous doc versions to wait queue
    }

    //  return processed docFirst,
    //	advance to next frame

    next.bits = frame->prev.bits;
    returnFreeFrame(txnMap, addr);
  }

  return true;
}

MVCCResult mvcc_CommitTxn(Params *params, uint64_t txnBits) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  ObjId txnId;
  Txn *txn;

  txnId.bits = txnBits;
  txn = mvcc_fetchTxn(txnId);

  if ((*txn->state & TYPE_BITS) == TxnGrow)
    *txn->state = TxnShrink | MUTEX_BIT;
  else {
    unlockLatch(txn->state);
    return result.status = DB_ERROR_txn_being_committed, result;
  }

  //	commit the transaction

  timestampNext(globalTxn->baseTs, txn->tsClnt);
  timestampInstall(txn->commit, globalTxn->baseTs + txn->tsClnt);

  switch (txn->isolation) {
    case TxnSerializable:
      if (SSNCommit(txn)) break;

    case TxnSnapShot:
      snapshotCommit(txn);
      break;

    default:
      return result;
  }

  //	TODO: recycle the txnId

  //	remove nested txn
  //	and unlock

  timestampQuit(globalTxn->baseTs, txn->tsClnt);
  *(uint64_t *)fetchIdSlot(txnMap, txnId) = txn->nextTxn;
  *txn->state = TxnDone;
  return result;
}

