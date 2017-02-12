#include "db.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"
#include "db_api.h"
#include "db_iterator.h"

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

void *iteratorNext(Handle *itHndl) {
Iterator *it;
DbAddr addr;
void *doc;

	it = (Iterator *)(itHndl + 1);

	while (incrObjId(it, itHndl->map)) {
	  if ((addr.bits = *(uint64_t *)fetchIdSlot(itHndl->map, it->docId))) {
		doc = getObj(itHndl->map, addr);
		it->state = IterPosAt;
		return doc;
	  }
	}

	it->state = IterRightEof;
	return NULL;
}

//
//  advance iterator backward
//

void *iteratorPrev(Handle *itHndl) {
Iterator *it;
DbAddr addr;
void *doc;

	it = (Iterator *)(itHndl + 1);

	while (decrObjId(it, itHndl->map)) {
	  if ((addr.bits = *(uint64_t *)fetchIdSlot(itHndl->map, it->docId))) {
		doc = getObj(itHndl->map, addr);
		it->state = IterPosAt;
		return doc;
	  }
	}

	it->state = IterLeftEof;
	return NULL;
}

//
//  set iterator to specific objectId
//	return document
//

void *iteratorSeek(Handle *itHndl, IteratorOp op, ObjId docId) {
void *doc = NULL;
Iterator *it;
DbAddr addr;

	it = (Iterator *)(itHndl + 1);

	switch (op) {
	  case IterBegin:
		it->docId.bits = 0;
		it->state = IterLeftEof;
		break;

	  case IterEnd:
		it->docId.bits = itHndl->map->arena->segs[itHndl->map->arena->currSeg].nextId.bits;
		it->state = IterRightEof;
		break;

	  case IterSeek:
		it->docId.bits = docId.bits;

		if ((addr.bits = *(uint64_t *)fetchIdSlot(itHndl->map, it->docId))) {
			doc = getObj(itHndl->map, addr);
			it->state = IterPosAt;
		} else
			it->state = IterNone;
		break;
	}

	return doc;
}
