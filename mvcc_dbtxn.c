//  implement transactions

#include "mvcc_dbdoc.h"

extern DbMap memMap[1];
extern DbMap* hndlMap;

Catalog* catalog;
// CcMethod* cc;

//  Txn arena free txn frames

void initTxn(int maxclients);
DbMap* txnMap;
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
	DbAddr txnFree[1];		// frames of available txnId
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
	arenaDef->baseSize = sizeof(GlobalTxn) + sizeof(Timestamp) * maxClients;
	arenaDef->arenaType = Hndl_txns;
	arenaDef->objSize = sizeof(Txn);

	txnMap = openMap(NULL, "Txns", 4, arenaDef, NULL);
	txnMap->db = txnMap;

	globalTxn = (GlobalTxn*)(txnMap->arena + 1);

	if (globalTxn->maxClients == 0) {
          timestampInit(globalTxn->baseTs, maxClients + 1);
          globalTxn->maxClients = maxClients + 1;
	}

	*txnMap->arena->type = Hndl_txns;
	*txnInit = Hndl_txns;
}

//	fetch and lock txn

Txn* fetchTxn(ObjId txnId) {
	Txn* txn = fetchIdSlot(txnMap, txnId);

	lockLatch(txn->state);
	return txn;
}

//	add docId and verNo to txn read-set
//	do not call for "read my writes"

DbStatus addDocRdToTxn(ObjId txnId, ObjId docId, Ver* ver, uint64_t hndlBits) {
	Txn* txn = fetchTxn(txnId);
    DbStatus stat = DB_OK;
    uint64_t values[3];
	Handle* docHndl;
	DbAddr* slot;
	int cnt = 0;
	ObjId objId;

	if (txn->isolation != Serializable) {
		unlockLatch(txn->state);
		return stat;
	}

	objId.bits = hndlBits;
	objId.xtra = TxnHndl;

	slot = slotHandle(objId);
	docHndl = getObj(hndlMap, *slot);

	if (txn->hndlId->bits != docHndl->hndlId.bits) {
		txn->hndlId->bits = docHndl->hndlId.bits;
		atomicAdd32(docHndl->bindCnt, 1);
		values[cnt++] = objId.bits;
	}

	docId.xtra = TxnDoc;
	values[cnt++] = docId.bits;
	values[cnt++] = ver->verNo;

	if ((*txn->state & TYPE_BITS) != TxnGrow)
		stat = DB_ERROR_txn_being_committed;
	else if (ver->sstamp->tsBits[1] < ~0ULL)
		addValuesToFrame(txnMap, txn->rdrFrame, txn->rdrFirst, values, cnt);
	else
		timestampCAS(txn->sstamp, ver->sstamp, -1);

	if (stat == DB_OK)
      if (timestampCmp(txn->sstamp, txn->pstamp) <= 0)
		stat = DB_ERROR_txn_not_serializable;

	unlockLatch(txn->state);
	return stat;
}

//	add version creation to txn write-set
//  do not call for "update my writes"

DbStatus addDocWrToTxn(ObjId txnId, ObjId docId, Ver* ver, Ver* prevVer, uint64_t hndlBits) {
	Txn* txn = fetchTxn(txnId);
    DbStatus stat = DB_OK;
    uint64_t values[2];
	Handle* docHndl;
	DbAddr* slot;
	int cnt = 0;
	ObjId objId;

	if (txn->isolation == NotSpecified) {
		unlockLatch(txn->state);
		return stat;
	}

	objId.bits = hndlBits;
	objId.xtra = TxnHndl;

	slot = slotHandle(objId);
	docHndl = getObj(hndlMap, *slot);

	if (txn->hndlId->bits != hndlBits) {
		txn->hndlId->bits = docHndl->hndlId.bits;
		atomicAdd32(docHndl->bindCnt, 1);
		values[cnt++] = objId.bits;
	}

	txn->wrtCount++;

	docId.xtra = TxnDoc;
	values[cnt++] = docId.bits;

	if (txn->isolation == Serializable) {
		if (prevVer)
			timestampCAS(txn->pstamp, prevVer->pstamp, -1);

		if (timestampCmp(txn->sstamp, txn->pstamp) <= 0)
			stat = DB_ERROR_txn_not_serializable;
	}

	if ((*txn->state & TYPE_BITS) == TxnGrow)
		stat = addValuesToFrame(txnMap, txn->docFrame, txn->docFirst, values, cnt) ? (DbStatus)DB_OK : DB_ERROR_outofmemory;
	else
		stat = DB_ERROR_txn_being_committed;

	unlockLatch(txn->state);
	return stat;
}

// 	begin a new Txn

uint64_t beginTxn(Params* params, uint64_t* nestedTxnBits) {
  uint16_t tsClnt;
  ObjId txnId;
  Txn* txn;

  if (!*txnInit) initTxn(1024);

  if ((tsClnt = timestampClnt(globalTxn->baseTs, globalTxn->maxClients)))
    timestampNext(globalTxn->baseTs, tsClnt);
  else
    return 0;

  if ((txnId.bits = allocObjId(txnMap, globalTxn->txnFree, globalTxn->txnWait)))
    txn = fetchIdSlot(txnMap, txnId);
  else {
    timestampQuit(globalTxn->baseTs, tsClnt);
    return 0;
  }

  memset(txn, 0, sizeof(Txn));
  timestampInstall(txn->reader, globalTxn->baseTs + tsClnt);

  if (params[Concurrency].intVal)
    txn->isolation = params[Concurrency].charVal;
  else
    txn->isolation = SnapShot;

  txn->tsClnt = tsClnt;
  txn->nextTxn = *nestedTxnBits;
  *txn->state = TxnGrow;

  txn->sstamp->tsBits[1] = ~0ULL;
  return *nestedTxnBits = txnId.bits;
}

DbStatus rollbackTxn(Params *params, uint64_t *txnBits) { return DB_OK; }

//	retrieve version by verNo

Ver *getVersion(DbMap *map, Doc *doc, uint64_t verNo) {
  uint32_t offset, size;
  Ver *ver;

  offset = doc->lastVer;

  //	enumerate previous document versions

  do {
    ver = (Ver *)((uint8_t *)doc + offset);

    //  continue to next version chain on stopper version

    if (!(size = ver->verSize)) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->lastVer;
        continue;
      } else
        return NULL;
    }

    if (ver->verNo == verNo) break;

  } while ((offset += size));

  return ver;
}

//  find appropriate document version per reader timestamp

DbStatus findDocVer(DbMap *map, Doc *doc, DbMvcc *dbMvcc, Ver *ver[1]) {
  uint32_t offset, size;
  DbStatus stat;
  ObjId txnId;

  offset = doc->lastVer;

  //  is there a pending update for the document
  //	made by our transaction?

  if ((txnId.bits = doc->txnId.bits)) {
    *ver = (Ver *)((uint8_t *)doc + offset);

    if (dbMvcc->txnId.bits == txnId.bits) 
		return DB_OK;

    // otherwise find a previously committed version

    offset += ver[0]->verSize;
  }

  //	examine previously committed document versions

  do {
    ver[0] = (Ver *)((uint8_t *)doc + offset);

    //  continue to next version chain on stopper version

    if (!(size = ver[0]->verSize)) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->lastVer;
        continue;
      } else
        return DB_ERROR_no_visible_version;
    }

    if (dbMvcc->isolation == Serializable) break;

    if (ver[0]->commit->tsBits < dbMvcc->reader->tsBits) break;

  } while ((offset += size));

  //	add this document to the txn read-set

  if (dbMvcc->txnId.bits)
    if ((stat = addDocRdToTxn(dbMvcc->txnId, doc->docId, ver[0],
                              dbMvcc->docHndl->hndlBits)))
      return stat;

  return DB_OK;
  }

Ver *firstCommittedVersion(DbMap *map, Doc *doc, ObjId docId) {
  uint32_t offset = doc->lastVer;
  uint32_t size;
  Ver *ver;

  ver = (Ver *)((uint8_t *)doc + offset);

  if (!(size = ver->verSize)) {
    if (doc->prevAddr.bits)
      doc = getObj(map, doc->prevAddr);
    else
      return 0;

    offset = doc->lastVer;
  }

  return (Ver *)((uint8_t *)doc + offset + ver->verSize);
}

//	verify and commit txn under
//	Serializable isolation

bool SSNCommit(Txn *txn) {
  DbAddr *slot, next, addr;
  Ver *ver, *prevVer;
  bool result = true;
  uint64_t *wrtMmbr;
  DbAddr wrtSet[1];
  Handle *docHndl;
  uint32_t offset;
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

    for (idx = 0; idx < nSlot; idx++) {
      //  finalize TxnDoc

      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docHndl->map, docId);
        uint64_t verNo = frame->slots[idx];

        lockLatch(docSlot->latch);

        doc = getObj(docHndl->map, *docSlot);
        doc->op |= Committing;

        prevVer = getVersion(docHndl->map, doc, verNo - 1);

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

          slot = fetchIdSlot(hndlMap, objId);
          docHndl = getObj(hndlMap, *slot);
          continue;

        case TxnDoc:
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
        DbAddr *docSlot = fetchIdSlot(docHndl->map, docId);
        uint64_t verNo = frame->slots[idx];

        frameSet = 0;

        // if we also write this read-set mmbr, skip it

        if (*setMmbr(memMap, wrtSet, docId.bits, false)) continue;

        doc = getObj(docHndl->map, *docSlot);
        prevVer = getVersion(docHndl->map, doc, verNo);

        //	keep smaller sstamp

        timestampCAS(txn->sstamp, prevVer->sstamp, 1);
        continue;
      }

      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);

          slot = fetchIdSlot(hndlMap, objId);
          docHndl = getObj(hndlMap, *slot);
          continue;

        case TxnDoc:
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
        DbAddr *docSlot = fetchIdSlot(docHndl->map, docId);
        uint64_t verNo = frame->slots[idx];

        frameSet = 0;

        // if we also write this read-set mmbr, skip it

        if (*setMmbr(memMap, wrtSet, docId.bits, false)) continue;

        doc = getObj(docHndl->map, *docSlot);
        ver = getVersion(docHndl->map, doc, verNo);

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

          slot = fetchIdSlot(hndlMap, objId);
          docHndl = getObj(hndlMap, *slot);
          continue;

        case TxnDoc:
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

          slot = fetchIdSlot(hndlMap, objId);
          docHndl = getObj(hndlMap, *slot);
          continue;

        case TxnDoc:
          slot = fetchIdSlot(docHndl->map, objId);
          lockLatch(slot->latch);
          break;
      }

      // find previous version

      doc = getObj(docHndl->map, *slot);
      ver = (Ver *)((uint8_t *)doc + doc->lastVer);

      offset = doc->lastVer + ver->verSize;
      prevVer = (Ver *)((uint8_t *)doc + offset);

      //	at top end, find in prev doc block

      if (!prevVer->verSize) {
        if (doc->prevAddr.bits) {
          Doc *prevDoc = getObj(docHndl->map, doc->prevAddr);
          prevVer = (Ver *)((uint8_t *)prevDoc + prevDoc->lastVer);
        } else
          prevVer = NULL;
      }

      if (prevVer) timestampInstall(prevVer->sstamp, txn->commit);

      timestampInstall(ver->commit, txn->commit);
      timestampInstall(ver->pstamp, txn->commit);
      ver->sstamp->tsBits[1] = ~0ULL;

      doc->txnId.bits = 0;
      doc->op = Done;

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

          slot = fetchIdSlot(hndlMap, objId);
          docHndl = getObj(hndlMap, *slot);
          continue;

        case TxnDoc:
          slot = fetchIdSlot(docHndl->map, objId);
          lockLatch(slot->latch);
          break;
      }

      doc = getObj(docHndl->map, *slot);
      ver = (Ver *)((uint8_t *)doc + doc->lastVer);

      timestampInstall(ver->commit, txn->commit);
      doc->txnId.bits = 0;
      doc->op = Done;

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

DbStatus commitTxn(Params *params, uint64_t *txnBits) {
  ObjId txnId;
  Txn *txn;

  txnId.bits = *txnBits;
  txn = fetchTxn(txnId);

  if ((*txn->state & TYPE_BITS) == TxnGrow)
    *txn->state = TxnShrink | MUTEX_BIT;
  else {
    unlockLatch(txn->state);
    return DB_ERROR_txn_being_committed;
  }

  //	commit the transaction

  timestampNext(globalTxn->baseTs, txn->tsClnt);
  timestampInstall(txn->commit, globalTxn->baseTs + txn->tsClnt);

  switch (txn->isolation) {
    case Serializable:
      if (SSNCommit(txn)) break;

    case SnapShot:
      snapshotCommit(txn);
      break;

    default:
      return DB_OK;
  }

  //	TODO: recycle the txnId

  //	remove nested txn
  //	and unlock

  timestampQuit(globalTxn->baseTs, txn->tsClnt);
  *txnBits = txn->nextTxn;
  *txn->state = Done;
  return DB_OK;
}
