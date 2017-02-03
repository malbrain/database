//	manage index handles associated with a docStore

#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_api.h"
#include "db_index.h"
#include "db_map.h"

//	remove a dropped index from the docStore child skiplist
//	call with child delete skiplist entry

void removeIdx(Handle *docHndl, SkipEntry *entry) {
DocStore *docStore;
ObjId hndlId;

	docStore = (DocStore *)(docHndl + 1);

	//	find the childId in our indexes skiplist
	//	and return the handle

	if ((hndlId.bits = skipDel(docHndl->map, docStore->indexes->head, *entry->val))) {
		DbAddr *slot = slotHandle(hndlId);

		lockLatch(slot->latch);
		destroyHandle(docHndl->map, slot);
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
