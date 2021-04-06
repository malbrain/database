//  scan txn steps

//  implement transactions

#include "mvcc.h"

DbStatus mvcc_scan4(Txn *txn) {
  DbAddr *slot, addr, next, finalAddr;
  Ver *ver, *prevVer;
  bool result = true;
  DbMap *docMap = NULL;
  Handle *docHndl = NULL;
  uint64_t verNo;
  ObjId objId;
  Frame *frame;
  Doc *doc;
  int idx;

    //  finally, commit wrt set version
 
    if ((next.bits = txn->wrtFirst->bits))
      finalAddr.bits = txn->wrtFrame->bits;
    else {
      next.bits = txn->wrtFrame->bits;
      finalAddr.bits = 0;
    }

    //  pre-commit
    // Scan transaction reads

    while ((addr.bits = next.bits)) {
      frame = getObj(txnMap, addr);

      for (idx = 0; idx < addr.nslot; idx++) {
        objId.bits = frame->slots[idx];

        switch (objId.step) {
          case TxnRaw:
            continue;

          case TxnMap:
            if (docHndl) releaseHandle(docHndl);
            docHndl = fetchIdSlot(hndlMap, objId);
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

            if (doc->op & OpCommit) break;

            unlockLatch(slot->latch);
            continue;
        }

        // find previous version

        prevVer = (Ver *)(doc->dbDoc->base + doc->commitVer);
        ver = (Ver *)(doc->dbDoc->base + doc->pendingVer);

        if (doc->commitVer) timestampInstall(prevVer->sstamp, txn->commit, 'd', 'd');

        timestampInstall(ver->commit, txn->commit, 'd', 'd');
        timestampInstall(ver->pstamp, txn->commit, 'd', 'd');
        ver->sstamp->lowHi[0] = 0;
        ver->sstamp->lowHi[1] = ~0ULL;
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
    if (docHndl)
        releaseHandle(docHndl);

    return DB_OK;
}