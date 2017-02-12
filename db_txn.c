#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_frame.h"
#include "db_map.h"

void addVerToTxn(DbMap *database, Txn *txn, Ver *ver, TxnCmd cmd) {
	ObjId docId;

	docId.cmd = cmd;
	addSlotToFrame (database, txn->frame, NULL, docId.bits);
}

//  find appropriate document version per txn beginning timestamp

Ver *findDocVer(DbMap *map, ObjId docId, Txn *txn) {
DbAddr *addr = fetchIdSlot(map, docId);
DbMap *db = map->db;
uint32_t offset;
uint64_t txnTs;
Txn *docTxn;
int verIdx;
Doc *doc;
Ver *ver;

  //	examine prior versions

  while (addr->bits) {
	doc = getObj(map, *addr);
	offset = sizeof(Doc);

	for (verIdx = 0; verIdx < doc->verCnt; verIdx++) {
	  // is this outside a txn? or
	  // is version in same txn?

	  ver = (Ver *)((uint8_t *)doc + offset);

	  if (txn && ver->txnId.bits == txn->txnId.bits)
		return ver;

	  // is the version permanent?

	  if (!ver->txnId.bits)
		return ver;

	  // is version committed before our txn began?

	  docTxn = fetchIdSlot(db, ver->txnId);

	  if (isCommitted(docTxn->timestamp))
		  if (docTxn->timestamp < txn->timestamp)
			return ver;

	  //	advance txn ts past doc version ts
	  //	and move onto next doc version

	  while (isReader((txnTs = txn->timestamp)) && txnTs < docTxn->timestamp)
		compareAndSwap(&txn->timestamp, txnTs, docTxn->timestamp);


	  offset += ver->size;
	}

	addr = doc->prevDoc;
  }

  return NULL;
}

