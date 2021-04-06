//  scan txn steps

//  implement transactions

#include "mvcc.h"

DbStatus mvcc_scan1(Txn* txn) {
  DbAddr next, finalAddr;
  DbAddr *docSlot;
  bool result = true;
  DbMap *docMap = NULL;
  Handle *docHndl = NULL;
  uint64_t verNo = 0;
  DocId docId;
  ObjId objId;
  Frame *frame;
  Doc *doc;
  Ver *ver;
  int idx;
    
  //for v in t.writes: # finalize \eta(T)
  //t.pstamp = max(t.pstamp, v.pstamp


  // evaluate writes of versions by this txn
  // that were later overwritten concurrently with our
  // transaction and finalize pi(txn)

  //  precommit
  //  scan1 wrt start

  if ((next.bits = txn->wrtFirst->bits))
    finalAddr.bits = txn->wrtFrame->bits;
  else {
    next.bits = txn->wrtFrame->bits;
    finalAddr.bits = 0;
  }

  while (next.addr) {
    frame = getObj(txnMap, next);

    for (idx = 0; idx < next.nslot; idx++) {
      objId.bits = frame->slots[idx];

      switch (objId.step) {
        default:
          continue;

        case TxnMap:
          if (docHndl)
            releaseHandle(docHndl);

          if(docHndl = fetchIdSlot(hndlMap, objId)) {
            docMap = MapAddr(docHndl);
            continue;
          }
          
          return DB_ERROR_badtxnstep;

        case TxnVer:
          verNo = objId.verNo;
          continue;

        case TxnWrt:
          docId.bits = objId.bits;
          docSlot = fetchIdSlot(docMap, docId);
          doc = getObj(docMap, *docSlot);
          break;
      }

      //  for v in t.writes: # finalize \eta(T)
      //  t.pstamp = max(t.pstamp, v.pstamp)
 
      //  ignore a read of our own new version

      if (doc->op == OpWrt)
        if (doc->txnId.bits == txn->txnId.bits)
            continue;

      //  is there another committed version
      //  after our version?  Was there another
      //  version committed after our read?

      if (verNo + 1 < doc->verNo)
        ver = mvcc_getVersion(docMap, doc, verNo + 1);
      else
        continue;

      //  is our read version overwritten yet?  check
      //  if it was committed with higher timestamp

      waitNonZero64(ver->commit->lowHi + 1);

      if (timestampCmp(txn->commit, ver->commit, 0, 0) < 0) continue;

      timestampCAX(txn->sstamp, ver->sstamp, 1, 'r', 'b');
      continue;
    }

    if (!(next.bits = frame->prev.bits)) {
      next.bits = finalAddr.bits;
      finalAddr.bits = 0;
    }
  }

  if (docHndl) releaseHandle(docHndl);
  return DB_OK;
}