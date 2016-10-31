#include "db.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_txn.h"
#include "db_iterator.h"

//
// create and position the start of an iterator
//

DbStatus createIterator(DbHandle hndl[1], DbHandle docHndl[1], uint64_t txnBits) {
Handle *docStore;
DbStatus stat;
Iterator *it;
ObjId txnId;
Txn *txn;

	memset (hndl, 0, sizeof(DbHandle));

	if ((stat = bindHandle(docHndl, &docStore)))
		return stat;

	hndl->hndlBits = makeHandle(docStore->map, sizeof(Iterator), 0, Hndl_iterator);

	it = (Iterator *)(getHandle(hndl) + 1);

	if ((txnId.bits = txnBits)) {
		txn = fetchIdSlot(docStore->map->db, txnId);
		it->ts = txn->timestamp;
	} else
		it->ts = allocateTimestamp(docStore->map->db, en_reader);

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

//  TODO:  lock the record

Doc *iteratorNext(DbHandle hndl[1]) {
Handle *docStore;
Txn *txn = NULL;
Doc *doc = NULL;
DbStatus stat;
Iterator *it;

	if ((stat = bindHandle(hndl, &docStore)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	while (incrObjId(it, docStore->map))
		if ((doc = findDocVer(docStore->map, it->docId, txn)))
			break;

	return doc;
}

//
//  advance iterator backward
//

//  TODO:  lock the record

Doc *iteratorPrev(DbHandle hndl[1]) {
Handle *docStore;
Txn *txn = NULL;
Doc *doc = NULL;
DbStatus stat;
Iterator *it;

	if ((stat = bindHandle(hndl, &docStore)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	while (decrObjId(it, docStore->map))
		if ((doc = findDocVer(docStore->map, it->docId, txn)))
			break;

	return doc;
}

//
//  set iterator to specific objectId
//

//  TODO:  lock the record

Doc *iteratorSeek(DbHandle hndl[1], uint64_t objBits) {
Handle *docStore;
Txn *txn = NULL;
DbStatus stat;
Iterator *it;
ObjId docId;
Doc *doc;

	if ((stat = bindHandle(hndl, &docStore)))
		return NULL;

	it = (Iterator *)(docStore + 1);

	if (it->txnId.bits)
		txn = fetchIdSlot(docStore->map->db, it->txnId);

	it->docId.bits = objBits;

	doc = findDocVer(docStore->map, it->docId, txn);

	releaseHandle(docStore);
	return doc;
}
