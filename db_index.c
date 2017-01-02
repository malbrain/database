#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	remove a dropped index from the docStore child skiplist
//	call with child delete skiplist entry

void removeIdx(Handle *docHndl, SkipEntry *entry) {
DocStore *docStore;
ObjId hndlId;

	docStore = (DocStore *)(docHndl + 1);

	//	find the childId in our indexes skiplist
	//	and return the handle

	if ((hndlId.bits = skipDel(docHndl->map, docStore->indexes->head, *entry->val))) {
		HandleId *slot = slotHandle(hndlId.bits);

		lockLatch(slot->addr->latch);
		destroyHandle(docHndl->map, slot->addr);
	}
}

//  open and install index DbHandle in docHndl cache
//	call with docStore handle and arenaDef address.

void installIdx(Handle *docHndl, SkipEntry *entry) {
uint64_t *hndlAddr;
DocStore *docStore;
RedBlack *rbEntry;
HandleType type;
DbAddr rbAddr;
DbMap *child;

	docStore = (DocStore *)(docHndl + 1);

	hndlAddr = skipAdd(docHndl->map, docStore->indexes->head, *entry->key);

	rbAddr.bits = *entry->val;
	rbEntry = getObj(docHndl->map->db, rbAddr);

	child = arenaRbMap(docHndl->map, rbEntry);
	type = *child->arena->type;

	*hndlAddr = makeHandle(child, 0, type);
}

//	create new index handles based on children of the docStore.
//	call with docStore handle.

DbStatus installIndexes(Handle *docHndl) {
ArenaDef *arenaDef = docHndl->map->arenaDef;
DocStore *docStore;
uint64_t maxId = 0;
SkipNode *skipNode;
DbAddr *next;
int idx;

	docStore = (DocStore *)(docHndl + 1);

	if (docStore->childId >= arenaDef->childId)
		return DB_OK;

	readLock (arenaDef->idList->lock);
	writeLock (docStore->indexes->lock);

	next = arenaDef->idList->head;

	//	open maps based on childId skip list
	// 	of arenaDef to continue the index handle list

	//	processs the skip list nodes one at a time
	//	most recent first

	while (next->addr) {
		skipNode = getObj(docHndl->map->db, *next);
		idx = next->nslot;

		if (!maxId)
			maxId = *skipNode->array[next->nslot - 1].key;

		while (idx--)
		  if (*skipNode->array[idx].key > docStore->childId) {
			  installIdx(docHndl, &skipNode->array[idx]);
			  docStore->idxCnt++;
		  } else
			break;

		next = skipNode->next;
	}

	docStore->childId = maxId;
	writeUnlock (docStore->indexes->lock);
	readUnlock (arenaDef->idList->lock);
	return DB_OK;
}

//	install index key for a document
//	call with docStore handle

DbStatus installIndexKey(Handle *docHndl, SkipEntry *entry, Ver *ver) {
ArenaDef *arenaDef;
char key[MAX_key];
DbHandle hndl[1];
uint64_t *verPtr;
ParamVal *spec;
int keyLen = 0;
Handle *index;
DbStatus stat;

	hndl->hndlBits = *entry->val;

	if (!(index = bindHandle(hndl)))
		return DB_ERROR_handleclosed;

	arenaDef = index->map->arenaDef;

	if ((spec = getParamIdx(arenaDef->params, IdxKeyPartial)))
	  if (!evalPartial(ver, spec)) {
		releaseHandle(index);
		return DB_OK;
	  }

	spec = getObj(index->map, dbindex(index->map)->idxKeys);
	keyLen = keyGenerator(key, ver, spec);
	keyLen = store64(key, keyLen, ver->docId.bits);

	if (arenaDef->params[UseTxn].boolVal)
		keyLen = store64(key, keyLen, ver->version);

	//	add the version for the indexId
	//	to the verKeys skiplist

	verPtr = skipAdd(docHndl->map, ver->verKeys, *entry->key);
	*verPtr = ver->version;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artInsertKey(index, key, keyLen);
		break;

	case Hndl_btree1Index:
		stat = btree1InsertKey(index, (uint8_t *)key, keyLen, 0, Btree1_indexed);
		break;
	}

	releaseHandle(index);
	return stat;
}

//	install keys for a document insert
//	call with docStore handle

DbStatus installIndexKeys(Handle *docHndl, Ver *ver) {
SkipNode *skipNode;
DocStore *docStore;
DbAddr *next;
DbStatus stat;
int idx;

	docStore = (DocStore *)(docHndl + 1);

	readLock (docStore->indexes->lock);
	next = docStore->indexes->head;

	ver->verKeys->bits = skipInit(docHndl->map, docStore->idxCnt);

	//	scan indexes skiplist of index handles
	//	and install keys for document

	while (next->addr) {
	  skipNode = getObj(docHndl->map, *next);
	  idx = next->nslot;

	  while (idx--) {
		  if ((stat = installIndexKey(docHndl, &skipNode->array[idx], ver)))
			return stat;
	  }

	  next = skipNode->next;
	}

	readUnlock (docStore->indexes->lock);
	return DB_OK;
}

