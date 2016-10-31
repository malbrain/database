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

extern int maxType[8];

//	remove a dropped index from the docStore indexes skiplist
//	call with arena child skiplist entry

void removeIdx(Handle *docHndl, SkipEntry *entry) {
DocStore *docStore;
uint64_t bits;

	docStore = (DocStore *)(docHndl + 1);

	//	find the childId in our indexes skiplist
	//	and return the handle

	if ((bits = skipDel(docHndl->map, docStore->indexes->head, *entry->key)))
		destroyHandle(slotHandle(bits));

	return;

}

//  open and install index DbHandle in docHndl cache
//	call with docStore handle and arenaDef address.

void installIdx(Handle *docHndl, SkipEntry *entry) {
uint64_t *hndlAddr;
DocStore *docStore;
RedBlack *rbEntry;
DbAddr rbAddr;
DbMap *child;
DbAddr addr;

	docStore = (DocStore *)(docHndl + 1);

	hndlAddr = skipAdd(docHndl->map, docStore->indexes->head, *entry->key);

	rbAddr.bits = *entry->val;
	rbEntry = getObj(docHndl->map->parent->db, rbAddr);

	child = arenaRbMap(docHndl->map, rbEntry);

	*hndlAddr = makeHandle(child, 0, maxType[*child->arena->type], *child->arena->type);
}

//	create new index handles based on children of the docStore.
//	call with docStore handle.

DbStatus installIndexes(Handle *docHndl) {
ArenaDef *arenaDef = docHndl->map->arenaDef;
DocStore *docStore;
uint64_t maxId = 0;
SkipNode *skipNode;
SkipEntry *entry;
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
			if (*skipNode->array[idx].key & CHILDID_DROP) {
			  removeIdx(docHndl, &skipNode->array[idx]);
			  docStore->idxCnt--;
			} else {
			  installIdx(docHndl, &skipNode->array[idx]);
			  docStore->idxCnt++;
			}
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

DbStatus installIndexKey(Handle *docHndl, SkipEntry *entry, Doc *doc) {
uint8_t key[MAX_key];
DbHandle hndl[1];
uint64_t *verPtr;
DbObject *spec;
Handle *index;
DbStatus stat;
DbAddr addr;
int keyLen;

	hndl->hndlBits = *entry->val;
	index = getHandle(hndl);

	//  bind the dbIndex handle
	//	and capture timestamp if
	//	this is the first bind

	if (atomicAdd32(index->calls->entryCnt, 1) == 1)
		index->calls->entryTs = atomicAdd64(&index->map->arena->nxtTs, 1);

	if ((addr.bits = index->map->arenaDef->partialAddr)) {
		spec = getObj(index->map->db, addr);
		if (!partialEval(doc, spec))
			return DB_OK;
	}

	addr.bits = index->map->arenaDef->specAddr;
	spec = getObj(index->map->db, addr);

	keyLen = keyGenerator(key, doc, spec);
	keyLen = store64(key, keyLen, doc->docId.bits);

	if (index->map->arenaDef->useTxn)
		keyLen = store64(key, keyLen, doc->version);

	//	add the version for the indexId
	//	to the verKeys skiplist

	verPtr = skipAdd(docHndl->map, doc->verKeys, *entry->key);
	*verPtr = doc->version;

	switch (*index->map->arena->type) {
	case Hndl_artIndex:
		stat = artInsertKey(index, key, keyLen);
		break;

	case Hndl_btree1Index:
		stat = btree1InsertKey(index, key, keyLen, 0, Btree1_indexed);
		break;
	}

	atomicAdd32(index->calls->entryCnt, -1);
	return stat;
}

//	install keys for a document insert
//	call with docStore handle

DbStatus installIndexKeys(Handle *docHndl, Doc *doc) {
SkipNode *skipNode;
DocStore *docStore;
DbAddr *next;
DbStatus stat;
int idx;

	docStore = (DocStore *)(docHndl + 1);

	readLock (docStore->indexes->lock);
	next = docStore->indexes->head;

	doc->verKeys->bits = skipInit(docHndl->map, docStore->idxCnt);

	//	scan indexes skiplist of index handles
	//	and install keys for document

	while (next->addr) {
	  skipNode = getObj(docHndl->map, *next);
	  idx = next->nslot;

	  while (idx--) {
		if (~*skipNode->array[idx].key & CHILDID_DROP)
		  if ((stat = installIndexKey(docHndl, &skipNode->array[idx], doc)))
			return stat;
	  }

	  next = skipNode->next;
	}

	readUnlock (docStore->indexes->lock);
	return DB_OK;
}

