//  implement transactions

#include "mvcc.h"

//  Txn arena free txn frames

void initTxn(int maxclients);

uint8_t txnInit[1];

 #ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#ifndef _WIN32
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
    Txn *txn;

	txn = fetchIdSlot(txnMap, txnId);

	lockLatch(txn->latch);
	return txn;
}

void mvcc_releaseTxn(Txn *txn) {
  if( txn )
    unlockLatch(txn->latch); 
}

// Add docId to txn write set
//	add version creation to txn write-set
//  Ignore "update my writes"

MVCCResult mvcc_addDocWrToTxn(Txn* txn, Handle *docHndl, Doc* doc) {
MVCCResult result = (MVCCResult) {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  uint64_t values[16];
  DocId docId;
  ObjId objId;
  Ver *prevVer;
  int cnt = 0;

  while (true) {
    if (txn->isolation == TxnNotSpecified) 
      break;

    if (txn->state != TxnGrowing) {
      result.status = DB_ERROR_txn_being_committed;
      break;
    }

    // ignore rewrite of our own new version

    if (doc->op == OpWrt)
        if( doc->txnId.bits == txn->txnId.bits) 
                break;

    objId.addr = docHndl->hndlId.addr;
    objId.step = TxnMap;

    if (txn->hndlId.addr != docHndl->hndlId.addr) {
      txn->hndlId.addr = objId.addr;
      values[cnt++] = objId.bits;
    }

    prevVer = (Ver *)(doc->dbDoc->base + doc->commitVer);
      
    docId.bits = doc->dbDoc->docId.bits;
    docId.step = TxnWrt;
    values[cnt++] = docId.bits;

    if (txn->isolation == TxnSerializable) {
      if (doc->commitVer)
        timestampCAX(txn->pstamp, prevVer->pstamp, -1, 'r', 'b');

      //    exclusion test

      if (timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0) {
        result.status = DB_ERROR_txn_not_serializable;
        break;
      }
    }
    break;
  }

  result.status =
      addValuesToFrame(txnMap, txn->wrtFrame, txn->wrtFirst, values, cnt)
          ? (DbStatus)DB_OK
          : DB_ERROR_outofmemory;

  if (result.status == DB_OK) txn->wrtCount += 1;

  return result;
}

//  ssn_read: add docId to txn read-set

MVCCResult mvcc_addDocRdToTxn(Txn *txn, Handle *docHndl, Ver* ver) {
MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};
  Doc *doc = (Doc *)(ver->verBase - ver->stop->offset);
  uint64_t values[16];
  int cnt = 0;
  ObjId objId;
  DocId docId;
  while (true) {
    if (txn->isolation != TxnSerializable) break;

    if (txn->state != TxnGrowing) {
      result.status = DB_ERROR_txn_being_committed;
      break;
    }

    //  switch docStore?

    objId.addr = docHndl->hndlId.addr;
    docId.step = TxnMap;

    if (txn->hndlId.addr != docHndl->hndlId.addr) {
      txn->hndlId = objId;
      values[cnt++] = objId.bits;
    }

    //  ignore a read of our own new version

    if (doc->op == OpWrt)
      if (doc->txnId.bits == txn->txnId.bits)
        break;

    // otherwise since we only read committed versions,
    // capture the largest commit stamp read
    // by the transaction in txn->pstamp

    timestampCAX(txn->pstamp, ver->commit, -1, 'r', 'b');

    // if no successor to ver yet, add to txn read set
    //  to check later for a new version created during
    //  our lifetime

    if (ver->sstamp->lowHi[1] == ~0ULL) {
      docId.bits = docId.bits;
      docId.step = TxnRdr;
      values[cnt++] = docId.bits;
      values[cnt++] = doc->verNo;
    } else
      timestampCAX(txn->sstamp, ver->sstamp, 1, 'r', 'b');

    // verify exclusion window validity

    if (timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0)
      result.status = DB_ERROR_txn_not_serializable;
  }

  if (cnt) 
      result.status = addValuesToFrame(txnMap, txn->rdrFrame, txn->rdrFirst, values, cnt);

  return result;
}

// 	begin a new Txn

MVCCResult mvcc_beginTxn(Params* params, ObjId nestedTxn) {
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

  txn->txnId.bits = txnId.bits;
  txn->tsClnt = tsClnt;
  txn->nextTxn.bits = nestedTxn.bits;
  txn->state = TxnGrowing;

  txn->sstamp->lowHi[1] = ~0ULL;
  result.value = txnId.bits;
  return result;
}

MVCCResult mvcc_rollbackTxn(Params *params, uint64_t txnBits) {
MVCCResult result = {
    .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};

return result;
}

//	retrieve version by verNo

Ver * mvcc_getVersion(DbMap *map, Doc *doc, uint64_t verNo) {
  uint32_t offset, size;
  Ver *ver;

  offset = doc->commitVer;

  //	enumerate previous document versions

  do {
    ver = (Ver *)(doc->dbDoc->base + offset);

    //  continue to next version chain on stopper version

    if (!(size = ver->stop->verSize)) {
      if (doc->prevAddr.bits) {
        doc = getObj(map, doc->prevAddr);
        offset = doc->commitVer;
        continue;
      } else
        return NULL;
    }

    if (doc->verNo == verNo) break;

  } while ((offset += size));

  return ver;
}

  Ver * mvcc_getVersion(DbMap *map, Doc *doc, uint64_t verNo);

//  find appropriate document version per reader timestamp

MVCCResult mvcc_findDocVer(Txn *txn, Doc *doc, Handle *docHndl) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objVer, .status = DB_OK};
  DbMap *docMap = MapAddr(docHndl);                            
  uint32_t offset;
  Ver *ver;

  //  is there a pending update for the document
  //	made by our transaction?

  if(txn) {
      if ((txn->txnId.bits == doc->txnId.bits)) {
        if (offset = doc->pendingVer) {
           ver = (Ver *)(doc->dbDoc->base + offset);
             result.object = ver;
                  return result;
              }
      }
  }

  // otherwise find a previously committed version

  offset = doc->commitVer;

  do {
    ver = (Ver *)(doc->dbDoc->base + offset);

    //  continue to next version chain on stopper version

    if (!ver->stop->verSize) {
      if (doc->prevAddr.bits) {
        doc = getObj(docMap, doc->prevAddr);
        offset = doc->commitVer;
        continue;
      } else
        return result.status = DB_ERROR_no_visible_version, result;
    }

    if( txn )
      if (timestampCmp(ver->commit, txn->commit, 'l', 'l') <= 0)
        break;

  } while ((offset += ver->stop->verSize));

  //	add this document to the txn read-set

  if(txn)
    result = mvcc_addDocRdToTxn(txn, docHndl, ver);

  result.objType = objVer;
  result.object = ver;
  return result;
}

//  find predecessor version

Ver *mvcc_firstCommittedVersion(DbMap *map, Doc *doc, DocId docId) {
  uint32_t offset = doc->commitVer;
  uint32_t size;
  Ver *ver;

  ver = (Ver *)(doc->dbDoc->base + offset);

  if (!(size = ver->stop->verSize)) {
    if (doc->prevAddr.bits)
      doc = getObj(map, doc->prevAddr);
    else
      return 0;

    offset = doc->commitVer;
  }

  return (Ver *)(doc->dbDoc->base + offset + ver->stop->verSize);
}

//	verify and commit txn under
//	Serializable isolation

bool SSNCommit(Txn *txn) {
bool result = true;
/*
//    scan1 wrt start

  timestampCAX(txn->sstamp, txn->commit, 1, 0, 0);

  //    evaluate writes by this txn
  //    to finalize eta(tn)
  //  def ssn_commit(t):
  //  t.cstamp = next_timestamp() # begin pre-commit
  //  for v in t.writes: # finalize \eta(T)
  //  t.pstamp = max(t.pstamp, v.pstamp)

  if(( result = mvcc_scan1(txn)))
    return result;

  // # finalize \pi(T)
  // t.sstamp = min(t.sstamp, t.cstamp)  // final pre-commit step
  //    exclusion test

  result = timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0 ? false : true;

  if (result)
      txn->state = TxnCommitted;
  else
      txn->state = TxnRollback;

  // finalize txn->sstamp from the readSet
    
  //    scan2 rdr start
  //  Post Commit

  //  process the reader pstamp from our commit time
  //	return reader set Frames.

  if( result = mvcc_scan2(txn) )
    return result;

  // if we also write this read-set mmbr, skip it

  doc = getObj(docMap, *docSlot);

  if (doc->verNo == verNo && (doc->op & OpCommit)) continue;

  ver = mvcc_getVersion(docMap, doc, verNo);

  //	keep largest ver pstamp

  timestampInstall(pstamp, ver->pstamp, 's', 'b');

  while (timestampCmp(pstamp, txn->commit, 0, 0) > 0)
    if (atomicCAS128(ver->pstamp->tsBits, pstamp->tsBits, txn->commit->tsBits)) 
              break;
            else
              timestampInstall(pstamp, ver->pstamp, 's', 'b');
  
  objId.bits = frame->slots[idx];

        switch (objId.step) {
          case TxnRaw:
            continue;

          case TxnMap:
            if (docHndl) releaseHandle(docHndl);
            docHndl = fetchIdSlot(hndlMap, objId);
            docMap = MapAddr(docHndl);
            continue;

          case TxnRdr:
            docId.bits = objId.bits;
            continue;
        }
      }

      if (!(next.bits = frame->prev.bits)) {
        next.bits = finalAddr.bits;
        finalAddr.bits = 0;
      }

      returnFreeFrame(txnMap, addr);
    }

  }

  if (docHndl) 
      releaseHandle(docHndl);

  returnFreeFrame(txnMap, addr);
 */ return result;
}

//	commit txn under snapshot isolation
//	always succeeds

bool snapshotCommit(Txn *txn) {
  DbAddr *slot, addr, next;
  Handle *docHndl;
  DbMap *docMap = NULL;
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

      switch (objId.step) {
        case TxnRaw:
          continue;

        case TxnMap:
          if (docHndl)
            releaseHandle(docHndl);
          if( (docHndl = fetchIdSlot(hndlMap, objId))) {
            docMap = MapAddr(docHndl);
            continue;
          }
          return false;

        case TxnRdr:
        case TxnWrt:
          if( docMap )
            slot = fetchIdSlot(docMap, objId);
          else
            continue;

          lockLatch(slot->latch);

          doc = getObj(docMap, *slot);
          ver = (Ver *)(doc->dbDoc->base + doc->pendingVer);

          timestampInstall(ver->commit, txn->commit, 'd', 0);
          doc->txnId.bits = 0;
          doc->op = TxnDone;

          unlockLatch(slot->latch);
          continue;
      } 

      //  TODO: add previous doc versions to wait queue
    }
        
    //  return processed wrtFirst,
    //	advance to next frame

    next.bits = frame->prev.bits;
    returnFreeFrame(txnMap, addr);
  }

  return true;
}

MVCCResult mvcc_commitTxn(Txn *txn, Params *params) {
  MVCCResult result = {
      .value = 0, .count = 0, .objType = objTxn, .status = DB_OK};

  if (txn->state == TxnGrowing)
    txn->state = TxnCommitting;
  else {
    unlockLatch(txn->latch);
    return result.status = DB_ERROR_txn_being_committed, result;
  }

  //	commit the transaction

  timestampNext(globalTxn->baseTs, txn->tsClnt);
  timestampInstall(txn->commit, globalTxn->baseTs + txn->tsClnt, 0, 0);

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
  *(uint64_t *)fetchIdSlot(txnMap, txn->txnId) = txn->nextTxn.bits;
  txn->state = TxnDone;
  return result;
}
