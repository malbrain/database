//  scan txn steps

//  implement transactions

#include "mvcc.h"

DbStatus mvcc_scan3(Txn *txn) {
  DbAddr addr, next, finalAddr;
  Timestamp pstamp[1];
  Ver *ver;
  bool result = true;
  DbMap *docMap = NULL;
  Handle *docHndl = NULL;
  uint64_t verNo;
  DocId docId;
  ObjId objId;
  Frame *frame;
  Doc *doc;
  int idx;

    // final pre-commit step
    //    exclusion test

    // # finalize \pi(T)
    // t.sstamp = min(t.sstamp, t.cstamp)
    // for v in t.reads:
    // t.sstamp = min(t.sstamp, v.sstamp)
    // ssn_check_exclusion(t)
    // t.status = COMMITTED

    result = timestampCmp(txn->sstamp, txn->pstamp, 0, 0) <= 0 ? false : true;

    if (result)
      txn->state = TxnCommitted;
    else
      txn->state = TxnRollback;

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
      frame = getObj(txnMap, addr);

      for (idx = 0; idx < addr.nslot; idx++) {
        objId.bits = frame->slots[idx];

        switch (objId.step) {
          case TxnRaw:
            continue;

          default:
            continue;

          case TxnMap:
            if (docHndl) releaseHandle(docHndl);
            docHndl = fetchIdSlot(hndlMap, objId);
            docMap = MapAddr(docHndl);
            continue;

          case TxnRdr:
            docId.bits = objId.bits;
            break;
        }

        DbAddr *docSlot = fetchIdSlot(docMap, docId);
        verNo = frame->slots[idx];

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
      }

      if (!(next.bits = frame->prev.bits)) {
        next.bits = finalAddr.bits;
        finalAddr.bits = 0;
      }

      returnFreeFrame(txnMap, addr);
    }

    if (docHndl) releaseHandle(docHndl);

    docHndl = NULL;
    return DB_OK;
}
//  scan 3 rdr end
