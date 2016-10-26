#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

extern int maxType[8];

//	remove a dropped index from the docStore indexes skiplist
//	call with arena child skiplist entry

void removeIdx(Handle *hndl, SkipEntry *entry) {
DocStore *docStore;
Handle *index;
uint64_t bits;

	docStore = (DocStore *)(hndl + 1);

	//	find the childId in our indexes skiplist
	//	and return the handle

	if ((bits = skipDel(hndl->map, docStore->indexes->head, *entry->key)))
		index = db_memObj(bits);
	else
		return;

	returnHandle(index);
}

//  open and install index DbHandle in hndl cache
//	call with docStore handle and arenaDef address.

void installIdx(Handle *hndl, SkipEntry *entry) {
uint64_t *hndlAddr;
DocStore *docStore;
RedBlack *rbEntry;
DbAddr rbAddr;
DbMap *child;
DbAddr addr;

	docStore = (DocStore *)(hndl + 1);

	hndlAddr = skipAdd(hndl->map, docStore->indexes->head, *entry->key);

	rbAddr.bits = *entry->val;
	rbEntry = getObj(hndl->map->parent->db, rbAddr);

	child = arenaRbMap(hndl->map, rbEntry);

	*hndlAddr = makeHandle(child, 0, maxType[*child->arena->type], *child->arena->type);
}

//	create new index handles based on children of the docStore.
//	call with docStore handle.

DbStatus installIndexes(Handle *hndl) {
ArenaDef *arenaDef = hndl->map->arenaDef;
DocStore *docStore;
uint64_t maxId = 0;
SkipNode *skipNode;
SkipEntry *entry;
DbAddr *next;
int idx;

	docStore = (DocStore *)(hndl + 1);

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
		skipNode = getObj(hndl->map->db, *next);
		idx = next->nslot;

		if (!maxId)
			maxId = *skipNode->array[next->nslot - 1].key;

		while (idx--)
		  if (*skipNode->array[idx].key > docStore->childId) {
			if (*skipNode->array[idx].key & CHILDID_DROP) {
			  removeIdx(hndl, &skipNode->array[idx]);
			  docStore->idxCnt--;
			} else {
			  installIdx(hndl, &skipNode->array[idx]);
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

DbStatus installIndexKey(Handle *hndl, SkipEntry *entry, Document *doc) {
uint8_t key[MAX_key];
DbIndex *dbIndex;
uint64_t *verPtr;
Handle *index;
Object *spec;
DbStatus stat;
int keyLen;

	index = db_memObj(*entry->val);

	//  bind the dbIndex handle
	//	and capture timestamp if
	//	this is the first bind

	if (atomicAdd32(index->calls->entryCnt, 1) == 1)
		index->calls->entryTs = atomicAdd64(&index->map->arena->nxtTs, 1);

	dbIndex = dbindex(index->map);

	spec = getObj(index->map, dbIndex->keySpec);
	keyLen = keyGenerator(key, doc, spec);

	keyLen = store64(key, keyLen, doc->docId.bits);

	if (index->map->arenaDef->useTxn)
		keyLen = store64(key, keyLen, doc->version);

	//	add the version for the indexId
	//	to the verKeys skiplist

	verPtr = skipAdd(hndl->map, doc->verKeys, *entry->key);
	*verPtr = doc->version;

	switch (*index->map->arena->type) {
	case ARTreeIndexType:
		stat = artInsertKey(index, key, keyLen);
		break;

	case Btree1IndexType:
		stat = btree1InsertKey(index, key, keyLen, 0, Btree1_indexed);
		break;
	}

	atomicAdd32(index->calls->entryCnt, -1);
	return stat;
}

//	install keys for a document insert
//	call with docStore handle

DbStatus installIndexKeys(Handle *hndl, Document *doc) {
SkipNode *skipNode;
DocStore *docStore;
DbAddr *next;
DbStatus stat;
int idx;

	docStore = (DocStore *)(hndl + 1);

	readLock (docStore->indexes->lock);
	next = docStore->indexes->head;

	doc->verKeys->bits = skipInit(hndl->map, docStore->idxCnt);

	//	scan indexes skiplist of index handles
	//	and install keys for document

	while (next->addr) {
	  skipNode = getObj(hndl->map, *next);
	  idx = next->nslot;

	  while (idx--) {
		if (~*skipNode->array[idx].key & CHILDID_DROP)
		  if ((stat = installIndexKey(hndl, &skipNode->array[idx], doc)))
			return stat;
	  }

	  next = skipNode->next;
	}

	readUnlock (docStore->indexes->lock);
	return DB_OK;
}

DbStatus storeDoc(Handle *hndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
DocArena *docArena;
ArenaDef *arenaDef;
Txn *txn = NULL;
Document *doc;
DbAddr *slot;
ObjId docId;
DbAddr addr;
int idx;

	docArena = docarena(hndl->map);

	if (txnId.bits)
		txn = fetchIdSlot(hndl->map->db, txnId);

	if ((addr.bits = allocObj(hndl->map, hndl->list->free, hndl->list->tail, -1, objSize + sizeof(Document), false)))
		doc = getObj(hndl->map, addr);
	else
		return DB_ERROR_outofmemory;

	docId.bits = allocObjId(hndl->map, hndl->list, docArena->docIdx);

	memset (doc, 0, sizeof(Document));

	if (txn)
		doc->txnId.bits = txnId.bits;

	doc->docId.bits = docId.bits;
	doc->size = objSize;

	memcpy (doc + 1, obj, objSize);

	if (result)
		result->bits = docId.bits;

	// assign document to docId slot

	slot = fetchIdSlot(hndl->map, docId);
	slot->bits = addr.bits;

	//	add keys for the document
	//	enumerate children (e.g. indexes)

	installIndexKeys(hndl, doc);

	if (txn)
		addIdToTxn(hndl->map->db, txn, docId, addDoc); 

	return DB_OK;
}
