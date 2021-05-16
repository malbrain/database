#include "base64.h"
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
uint64_t start = it->docId.bits;
uint64_t mask, *tstNull, span;

	while (it->docId.seg <= map->arena->objSeg) {
		while (++it->docId.off <= map->arena->segs[it->docId.seg].maxId) {

			span = map->objSize;

			tstNull = (uint64_t *)(map->base[it->docId.seg] + map->arena->segs[it->docId.seg].size - it->docId.off * span);

			while(span > 8 )
				if(*tstNull++ )
					return true;
				else
					span -= 8;

			mask = (256ULL << (span * 8)) - 1;
	
			if(*tstNull & mask )
				return true;
			else
				continue;
		}

		it->docId.off = 0;
		it->docId.seg++;
	}

	it->docId.bits = start;
	return false;
}

//
// decrement a segmented recordId
//

bool decrObjId(Iterator *it, DbMap *map) {
uint64_t start = it->docId.bits;
uint64_t mask, *tstNull, span;

	while (true) {
		if(it->docId.off) {
			span = map->objSize;

			tstNull = (uint64_t *)(map->base[it->docId.seg] + map->arena->segs[it->docId.seg].size - it->docId.off * span);

			while(span > 8 )
				if(*tstNull++ )
					return true;
				else
					span -= 8;

			mask = (256ULL << (span * 8)) - 1;
	
			if(*tstNull & mask )
				return true;
			else
				continue;
		}

		if(*tstNull & mask )
			return true;

		if (it->docId.seg) {
			it->docId.seg--;
			it->docId.off = map->arena->segs[it->docId.seg].maxId + 1;
			continue;
		}

		it->docId.bits = start;
		return false;
	}
}

//	advance/reverse iterator

DbStatus iteratorMove(DbHandle hndl, IteratorOp op, DocId *docId) {
	DbStatus stat = DB_OK;
  Handle *docHndl;
  Iterator *it;
  DbMap *docMap;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
    return DB_ERROR_handleclosed;

  it = getObj(hndlMap, docHndl->clientAddr);

  switch (op) {
    case IterNext:
			if (incrObjId(it, docMap)) {
				it->state = IterPosAt;
				docId->bits = it->docId.bits;
			} else { 
				it->state = IterRightEof;
				stat = DB_ITERATOR_eof;
			}
			
			break;

		case IterPrev:
			if (decrObjId(it, docMap)) {
				it->state = IterPosAt;
				docId->bits = it->docId.bits;
			} else {
				it->state = IterLeftEof;
				stat = DB_ITERATOR_eof;
			}
			
			break;

		case IterBegin:
			it->docId.bits = 0;
			docId->bits = it->docId.bits;
			it->state = IterLeftEof;
			stat = DB_ITERATOR_eof;
			break;


		case IterEnd:
			it->docId.seg = docMap->arena->objSeg;
			it->docId.off = docMap->arena->segs[it->docId.seg].maxId;

				docId->bits = it->docId.bits;
				it->state = IterRightEof;
				stat = DB_ITERATOR_eof;
				break;

    case IterSeek:
			if((it->docId.bits = docId->bits))
				it->state = IterPosAt;
			else
	      stat = DB_ITERATOR_notfound;
	
			break;
  }

  docId->bits = it->docId.bits;
  releaseHandle(docHndl);
  return stat;
}

//
//  advance iterator forward
//

DbDoc *iteratorNext(DbHandle hndl) {
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;
  Iterator *it;
	DbDoc *dbDoc = NULL;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
    return NULL;

  it = getObj(hndlMap, docHndl->clientAddr);

	if (incrObjId(it, docMap)) {
		slot = fetchIdSlot(docMap, it->docId);
		it->state = IterPosAt;
		dbDoc = getObj(docMap, *slot);
	} else
		it->state = IterRightEof;

  releaseHandle(docHndl);
	return NULL;			
}

//
//  advance iterator backward
//

DbDoc *iteratorPrev(DbHandle hndl) {
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;
  Iterator *it;
	DbDoc *dbDoc = NULL;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
	  return NULL;

  it = getObj(hndlMap, docHndl->clientAddr);

	if (decrObjId(it, docMap)) {
		slot = fetchIdSlot(docMap, it->docId);
		dbDoc = getObj(docMap, *slot);
		it->state = IterPosAt;
	} else 
		it->state = IterLeftEof;

	return dbDoc;			
}

//
//  set iterator to specific objectId
//

DbDoc *iteratorFetch(DbHandle hndl, ObjId docId) {
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;
	DbDoc *dbDoc;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
    return NULL;

	slot = fetchIdSlot(docMap, docId);
	dbDoc = getObj(docMap, *slot);
  releaseHandle(docHndl);
	return dbDoc;
}

//
//  set iterator to specific objectId
//

DbDoc *iteratorSeek(DbHandle hndl, ObjId docId) {
  Handle *docHndl;
  DbAddr *slot;
  DbMap *docMap;
  Iterator *it;
	DbDoc *dbDoc;

  if ((docHndl = bindHandle(hndl, Hndl_docStore)))
    docMap = MapAddr(docHndl);
  else
    return NULL;

	it = getObj(hndlMap, docHndl->clientAddr);
	it->docId.bits = docId.bits;

	slot = fetchIdSlot(docMap, docId);
	dbDoc = getObj(docMap, *slot);
  releaseHandle(docHndl);
	return dbDoc;
}
