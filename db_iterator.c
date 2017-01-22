#include "db.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"
#include "db_txn.h"
#include "db_iterator.h"

//
// create and position the start of an iterator
//

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], ObjId txnId, Params *params) {
Handle *docStore;
Iterator *it;
Txn *txn;

	memset (hndl, 0, sizeof(DbHandle));

	if (!(docStore = bindHandle(docHndl)))
		return DB_ERROR_handleclosed;

	hndl->hndlBits = makeHandle(docStore->map, sizeof(Iterator), Hndl_iterator);

	it = (Iterator *)(getHandle(hndl) + 1);

	if (txnId.bits) {
		txn = fetchIdSlot(docStore->map->db, txnId);
		it->ts = txn->timestamp;
	} else
		it->ts = allocateTimestamp(docStore->map->db, en_reader);

	if (params[IteratorEnd].boolVal) {
		it->docId.bits = docStore->map->arena->segs[docStore->map->arena->currSeg].nextId.bits;
		it->state = IterRightEof;
	} else {
		it->docId.bits = 0;
		it->state = IterLeftEof;
	}

	it->txnId.bits = txnId.bits;
	releaseHandle(docStore);
	return DB_OK;
}

//
// increment a segmented ObjId
//

bool incrObjId(Iterator *it, DbMap *map) {
ObjId start = it->docId;

	while (it->docId.seg <= map->arena->objSeg) {
		if (++it->docId.index <= map->arena->segs[it->docId.seg].nextId.index)
			return true;

		it->docId.index = 0;
		it->docId.seg++;
	}

	it->docId = start;
	return false;
}

//
// decrement a segmented recordId
//

bool decrObjId(Iterator *it, DbMap *map) {
ObjId start = it->docId;

	while (it->docId.index) {
		if (--it->docId.index)
			return true;
		if (!it->docId.seg)
			break;

		it->docId.seg--;
		it->docId.index = map->arena->segs[it->docId.seg].nextId.index + 1;
	}

	it->docId = start;
	return false;
}

//
//  advance iterator forward
//

Ver *iteratorNext(DbHandle hndl[1]) {
Handle *docStore;
Txn *txn = NULL;
Ver *ver = NULL;
Iterator *it;

	if (!(docStore = bindHandle(hndl)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	while (incrObjId(it, docStore->map))
		if ((ver = findDocVer(docStore->map, it->docId, txn)))
			break;

	if (ver)
		it->state = IterPosAt;
	else
		it->state = IterRightEof;

	releaseHandle(docStore);
	return ver;
}

//
//  advance iterator backward
//

Ver *iteratorPrev(DbHandle hndl[1]) {
Handle *docStore;
Txn *txn = NULL;
Ver *ver = NULL;
Iterator *it;

	if (!(docStore = bindHandle(hndl)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	while (decrObjId(it, docStore->map))
		if ((ver = findDocVer(docStore->map, it->docId, txn)))
			break;

	if (ver)
		it->state = IterPosAt;
	else
		it->state = IterLeftEof;

	releaseHandle(docStore);
	return ver;
}

//
//  set iterator to specific objectId
//	return most recent mvcc version
//

Ver *iteratorSeek(DbHandle hndl[1], IteratorPos pos, ObjId docId) {
Handle *docStore;
Txn *txn = NULL;
Ver *ver = NULL;
Iterator *it;

	if (!(docStore = bindHandle(hndl)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	switch (pos) {
	  case PosBegin:
		it->docId.bits = 0;
		it->state = IterLeftEof;

		while (incrObjId(it, docStore->map))
		  if ((ver = findDocVer(docStore->map, it->docId, txn)))
			break;

		if (ver)
			it->state = IterPosAt;
		else
			it->state = IterRightEof;

		break;

	  case PosEnd:
		it->docId.bits = docStore->map->arena->segs[docStore->map->arena->currSeg].nextId.bits;
		it->state = IterRightEof;

		while (decrObjId(it, docStore->map))
		  if ((ver = findDocVer(docStore->map, it->docId, txn)))
			break;

		if (ver)
			it->state = IterPosAt;
		else
			it->state = IterLeftEof;

		break;

	  case PosAt:
		it->docId.bits = docId.bits;
		it->state = IterNone;

		if ((ver = findDocVer(docStore->map, it->docId, txn)))
			it->state = IterPosAt;

		break;
	}

	releaseHandle(docStore);
	return ver;
}
