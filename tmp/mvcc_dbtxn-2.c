//  implement transactions

#include "mvcc_dbapi.h"

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
  Txn *txn = mvcc_fetchTxn(txnId);
  uint64_t values[1024 + 16];
  Ver *ver, *prevVer;
  DbAddr *slot;
  ObjId objId;
  int cnt = 0;
  Doc *doc;

  if (txn->isolation == TxnNotSpecified)
      goto wrXit;

  if ((*txn->state & TYPE_BITS) != TxnGrowing) {
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
    slot = fetchIdSlot(docMap, *docId++);
    doc = getObj(docMap, *slot);

    // ignore rewrite of our own new version

    if (doc->op == TxnWrt && doc->txnId.bits == txnId.bits) 
        continue;

    prevVer = (Ver *)(doc->doc->base + doc->newestVer);

    docId->xtra = TxnWrt;
    values[cnt++] = docId->bits;
    values[cnt++] = atomicINC64(&doc->verNo) + 1;
    
    if (txn->isolation == TxnSerializable) {
      if (doc->newestVer)
          timestampCAX(txn->pstamp, prevVer->pstamp, -1, 'r', 'b');

      //    exclusion test

      if (timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0) {
        result.status = DB_ERROR_txn_not_serializable;
        goto wrXit;
      }
    }
  }

  result.status = addValuesToFrame(txnMap, txn->wrtFrame, txn->wrtFirst, values, cnt)
               ? (DbStatus)DB_OK : DB_ERROR_outofmemory;

  if(result.status == DB_OK)
      txn->wrtCount += tot;

wrXit:
  unlockLatch(txn->state);
  return result;
}

//  ssn_read: add docId to txn read-set

MVCCResult mvcc_addDocRdToTxn(ObjId txnId, DbMap *docMap, ObjId docId, Ver* ver, DbHandle hndl[1]) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  Doc *doc = (Doc *)(ver->verBase - ver->stop->offset);
  Txn *txn = mvcc_fetchTxn(txnId);
    uint64_t values[3];
	DbAddr* slot;
	int cnt = 0;
	ObjId objId;

	if (txn->isolation != TxnSerializable)
        goto rdXit;

	if ((*txn->state & TYPE_BITS) != TxnGrowing) {
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

    //  is this a read of our own new version?

    if (doc->op == TxnWrt && doc->txnId.bits == txnId.bits)
        goto rdXit;

    // since we only read committed versions,
    // capture the largest commit stamp read
    // by the transaction in txn->pstamp

    timestampCAX(txn->pstamp, ver->commit, -1, 'r', 'b');

    // if no successor to ver yet, add to txn read set
    //  to check later for a new version created during
    //  our lifetime

    if (ver->sstamp->tsBits[1] == ~0ULL) {
        docId.xtra = TxnRdr;
        values[cnt++] = docId.bits;
        values[cnt++] = doc->verNo;
    } else
        timestampCAX(txn->sstamp, ver->sstamp, 1, 'r', 'b');

    if (cnt)
		addValuesToFrame(txnMap, txn->rdrFrame, txn->rdrFirst, values, cnt);

    // verify exclusion window validity

    if (timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0)
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
  timestampInstall(txn->reader, globalTxn->baseTs + tsClnt, 0, 0);

  if (params[Concurrency].intVal)
    txn->isolation = params[Concurrency].charVal;
  else
    txn->isolation = TxnSnapShot;

  txn->tsClnt = tsClnt;
  txn->nextTxn = nestedTxn.bits;
  *txn->state = TxnGrowing;

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

    if (!(size = ver->stop->verSize)) {
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
  uint32_t offset;
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

    offset += ver->stop->verSize;
  }

  //	examine previously committed document versions

  do {
    ver = (Ver *)(doc->doc->base + offset);

    //  continue to next version chain on stopper version

    if (!ver->stop->verSize) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->newestVer;
        continue;
      } else
        return result.status = DB_ERROR_no_visible_version, result;
    }

      if (timestampCmp(ver->commit, dbMvcc->reader, 'l', 'l') <= 0)
        break;

  } while ((offset += ver->stop->verSize));

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

  if (!(size = ver->stop->verSize)) {
    if (doc->prevAddr.bits)
      doc = getObj(map, doc->prevAddr);
    else
      return 0;

    offset = doc->newestVer;
  }

  return (Ver *)(doc->doc->base + offset + ver->stop->verSize);
}

//	verify and commit txn under
//	Serializable isolation

bool SSNCommit(Txn *txn, ObjId txnId) {
  DbAddr next, *slot, addr, finalAddr;
  Timestamp pstamp[1];
  Ver *ver, *prevVer;
  bool result = true;
  DbMap *docMap;
  Handle *docHndl;
  uint64_t verNo;
  bool frameSet;
  ObjId docId;
  ObjId objId;
  Doc *doc;

  if ((next.bits = txn->rdrFirst->bits))
    finalAddr.bits = txn->rdrFrame->bits;
  else {
    next.bits = txn->rdrFrame->bits;
    finalAddr.bits = 0;
  }

  timestampCAX(txn->sstamp, txn->commit, 1, 0, 0);
  docHndl = NULL;
  frameSet = false;

  // evaluate reads of versions by this txn
  // that were later overwritten concurrently with our
  // transaction and finalize pi(txn)

  //  precommit

  while (next.addr) {
    Frame *frame = getObj(txnMap, next);

    for (int idx = 0; idx < next.nslot; idx++) {
      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        verNo = frame->slots[idx];
        doc = getObj(docMap, *docSlot);

        frameSet = false;

        //  is this a read of our own new version?

        if (doc->op == TxnWrt)
          if (doc->txnId.bits == txnId.bits) continue;

        //  is there another committed version
        //  after our version?  Was there another
        //  version committed after our read?

        if (verNo + 1 < doc->verNo)
          ver = mvcc_getVersion(docMap, doc, verNo + 1);
        else
          continue;

        //  is our read version overwritten yet?  check
        //  if it was committed with higher timestamp

        waitNonZero64(ver->commit->tsBits + 1);

        if (timestampCmp(txn->commit, ver->commit, 0, 0) < 0) continue;

        timestampCAX(txn->sstamp, ver->sstamp, 1, 'r', 'b');
        continue;
      }

      objId.bits = frame->slots[idx];

      switch (objId.xtra) {
        case TxnHndl:
          if (docHndl) releaseHandle(docHndl, NULL);
          docHndl = bindHandle((DbHandle *)(frame->slots + idx), Hndl_docStore);
          docMap = MapAddr(docHndl);
          continue;

        case TxnRdr:
          docId.bits = objId.bits;
          frameSet = true;
          continue;
      }
    }

    if (!(next.bits = frame->prev.bits)) {
      next.bits = finalAddr.bits;
      finalAddr.bits = 0;
    }
  }

  if (docHndl) releaseHandle(docHndl, NULL);

  //    evaluate writes by this txn
  //    to finalize eta(tn)

  docHndl = NULL;
  frameSet = false;

  if ((next.bits = txn->wrtFirst->bits))
    finalAddr.bits = txn->wrtFrame->bits;
  else {
    next.bits = txn->wrtFrame->bits;
    finalAddr.bits = 0;
  }

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

    for (int idx = 0; idx < addr.nslot; idx++) {
      if (frameSet) {
        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        verNo = frame->slots[idx];
        doc = getObj(docMap, *docSlot);

        frameSet = false;

        //  is this a read of our own new version?

        if (doc->op == TxnWrt)
          if (doc->txnId.bits == txnId.bits) continue;

        //  is there another committed version
        //  after our version?  Was there another
        //  version committed after our read?

        if (verNo + 1 < doc->verNo) {
          ver = mvcc_getVersion(docMap, doc, verNo + 1);

          //  is our read version overwritten yet?  check
          //  if it was committed with higher timestamp

          waitNonZero64(ver->commit->tsBits + 1);

          if (timestampCmp(txn->commit, ver->commit, 0, 0) > 0)
            timestampCAX(txn->sstamp, ver->sstamp, 1, 'r', 'b');

          continue;
        }

        objId.bits = frame->slots[idx];

        switch (objId.xtra) {
          case TxnHndl:
            if (docHndl) releaseHandle(docHndl, NULL);
            docHndl =
                bindHandle((DbHandle *)(frame->slots + idx), Hndl_docStore);
            docMap = MapAddr(docHndl);
            continue;

          case TxnWrt:
            docId.bits = objId.bits;
            frameSet = true;
            continue;
        }
      }

      if (!(next.bits = frame->prev.bits)) {
        next.bits = finalAddr.bits;
        finalAddr.bits = 0;
      }
    }

    // final pre-commit step
    //    exclusion test

    result = timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0 ? false : true;

    if (result)
      *txn->state = TxnCommit | MUTEX_BIT;
    else
      *txn->state = TxnRollback | MUTEX_BIT;

    if (docHndl) releaseHandle(docHndl, NULL);

    docHndl = NULL;
    frameSet = false;

    // finalize txn->sstamp from the readSet

    //  Post Commit

    //  process the reader pstamp from our commit time
    //	return reader set Frames.

    if ((next.bits = txn->rdrFirst->bits))
      finalAddr.bits = txn->rdrFrame->bits;
    else {
      next.bits = txn->rdrFrame->bits;
      finalAddr.bits = 0;
    }

    while ((addr.bits = next.bits)) {
      Frame *frame = getObj(txnMap, addr);

      for (int idx = 0; idx < addr.nslot; idx++) {
        if (frameSet) {
          DbAddr *docSlot = fetchIdSlot(docMap, docId);
          verNo = frame->slots[idx];
          frameSet = false;

          // if we also write this read-set mmbr, skip it

          doc = getObj(docMap, *docSlot);

          if (doc->verNo == verNo && (doc->op & TxnCommitting)) continue;

          ver = mvcc_getVersion(docMap, doc, verNo);

          //	keep largest ver pstamp

          timestampInstall(pstamp, ver->pstamp, 's', 'b');

          while (timestampCmp(pstamp, txn->commit, 0, 0) > 0)
            if (atomicCAS128(ver->pstamp->tsBits, pstamp->tsBits, txn->commit->tsBits)) 
              break;
            else
              timestampInstall(pstamp, ver->pstamp, 's', 'b');
        }

        objId.bits = frame->slots[idx];

        switch (objId.xtra) {
          case TxnKill:
            continue;

          case TxnHndl:
            if (docHndl) releaseHandle(docHndl, NULL);
            docHndl =
                bindHandle((DbHandle *)(frame->slots + idx), Hndl_docStore);
            docMap = MapAddr(docHndl);
            continue;

          case TxnRdr:
            docId.bits = objId.bits;
            frameSet = true;
            continue;
        }
      }

      if (!(next.bits = frame->prev.bits)) {
        next.bits = finalAddr.bits;
        finalAddr.bits = 0;
      }

      returnFreeFrame(txnMap, addr);
    }

    if (docHndl) releaseHandle(docHndl, NULL);

    docHndl = NULL;
    frameSet = false;

    //  finally, install wrt set versions

    if ((next.bits = txn->wrtFirst->bits))
      finalAddr.bits = txn->wrtFrame->bits;
    else {
      next.bits = txn->wrtFrame->bits;
      finalAddr.bits = 0;
    }

    while ((addr.bits = next.bits)) {
      Frame *frame = getObj(txnMap, addr);

      for (int idx = 0; idx < addr.nslot; idx++) {
        objId.bits = frame->slots[idx];

        switch (objId.xtra) {
          case TxnKill:
            continue;

          case TxnHndl:
            if (docHndl) releaseHandle(docHndl, NULL);
            docHndl =
                bindHandle((DbHandle *)(frame->slots + idx), Hndl_docStore);
            docMap = MapAddr(docHndl);
            continue;
          default:
            continue;
          case TxnWrt:
            verNo = frame->slots[idx];
            slot = fetchIdSlot(docMap, objId);
            lockLatch(slot->latch);

            doc = getObj(docMap, *slot);
            if (doc->verNo == verNo) break;

            if (doc->op & TxnCommitting) break;

            unlockLatch(slot->latch);
            continue;
        }

        // find previous version

        prevVer = (Ver *)(doc->doc->base + doc->newestVer);
        ver = (Ver *)(doc->doc->base + doc->pendingVer);

        if (doc->newestVer) timestampInstall(prevVer->sstamp, txn->commit, 'd', 'd');

        timestampInstall(ver->commit, txn->commit, 'd', 'd');
        timestampInstall(ver->pstamp, txn->commit, 'd', 'd');
        ver->sstamp->tsBits[0] = 0;
        ver->sstamp->tsBits[1] = ~0ULL;
        doc->txnId.bits = 0;
        doc->op = TxnDone;

        unlockLatch(slot->latch);
        continue;
      }

      if (!(next.bits = frame->prev.bits)) {
        next.bits = finalAddr.bits;
        finalAddr.bits = 0;
      }

      returnFreeFrame(txnMap, addr);
    }
  }

  if (docHndl) 
      releaseHandle(docHndl, NULL);

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

  next.bits = txn->wrtFirst->bits;
  docHndl = NULL;

  while ((addr.bits = next.bits)) {
    Frame *frame = getObj(txnMap, addr);

	// when we get to the last frame,
    //	pull the count from free head

    if (!frame->prev.bits)
      nSlot = txn->wrtFrame->nslot;
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
      ver = (Ver *)(doc->doc->base + doc->pendingVer);

      timestampInstall(ver->commit, txn->commit, 'd', 0);
      doc->txnId.bits = 0;
      doc->op = TxnDone;

      unlockLatch(slot->latch);

      //	TODO: add previous doc versions to wait queue
    }

    //  return processed wrtFirst,
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

  if ((*txn->state & TYPE_BITS) == TxnGrowing)
    *txn->state = TxnCommitting | MUTEX_BIT;
  else {
    unlockLatch(txn->state);
    return result.status = DB_ERROR_txn_being_committed, result;
  }

  //	commit the transaction

  timestampNext(globalTxn->baseTs, txn->tsClnt);
  timestampInstall(txn->commit, globalTxn->baseTs + txn->tsClnt, 0, 0);

  switch (txn->isolation) {
    case TxnSerializable:
      if (SSNCommit(txn, txnId)) break;

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