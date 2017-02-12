//	manage index handles associated with a docStore

#include "db.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_api.h"
#include "db_index.h"
#include "db_map.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

//	insert a key into an index

DbStatus dbInsertKey (Handle *idxHndl, void *keyBytes, uint32_t keyLen) {
	DbStatus stat;

	switch (*idxHndl->map->arena->type) {
	case Hndl_artIndex:
		stat = artInsertKey(idxHndl, keyBytes, keyLen);
		break;

	case Hndl_btree1Index:
		stat = btree1InsertKey(idxHndl, keyBytes, keyLen, 0, Btree1_indexed);
		break;
	}

	return stat;
}

//	allocate docStore power-of-two memory

uint64_t dbAllocDocStore(Handle *docHndl, uint32_t size, bool zeroit) {
DbAddr *free = listFree(docHndl,0);
DbAddr *wait = listWait(docHndl,0);

	return allocObj(docHndl->map, free, wait, -1, size, zeroit);
}

//  open and install index DbHandle in docHndl cache
//	call with docStore handle and arenaDef address.

void dbInstallIdx(Handle *docHndl, SkipEntry *entry) {
SkipEntry *skipEntry;
DocStore *docStore;
RedBlack *rbEntry;
HandleType type;
DbAddr rbAddr;
DbMap *child;

	docStore = (DocStore *)(docHndl + 1);

	skipEntry = skipAdd(docHndl->map, docStore->indexes->head, *entry->key);

	rbAddr.bits = *entry->val;
	rbEntry = getObj(docHndl->map->db, rbAddr);

	child = arenaRbMap(docHndl->map, rbEntry);
	type = *child->arena->type;

	*skipEntry->val = makeHandle(child, 0, type);
}

//	create new index handles based on children of the docStore.
//	call with docStore handle.

DbStatus dbInstallIndexes(Handle *docHndl) {
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
			  dbInstallIdx(docHndl, &skipNode->array[idx]);
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
