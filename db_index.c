#include "db.h"
#include "db_txn.h"
#include "db_object.h"
#include "db_arena.h"
#include "db_index.h"
#include "db_map.h"
#include "db_api.h"
#include "btree1/btree1.h"
#include "artree/artree.h"

extern DbMap memMap[1];
extern int maxType[8];

//  open and install index DbHandle in hndl cache
//	call with docStore handle and arenaDef address.

void installIdx(Handle *hndl, ArrayEntry *entry) {
uint64_t *hndlAddr;
RedBlack *rbEntry;
Handle *childHndl;
DocHndl *docHndl;
DbAddr rbAddr;
DbMap *child;
DbAddr addr;

	docHndl = (DocHndl *)(hndl + 1);

	//  no more than 255 indexes

	if (docHndl->idxCnt < 255)
		docHndl->idxCnt++;
	else
		return;

	hndlAddr = skipAdd(hndl->map, docHndl->indexes->head, *entry->key);

	rbAddr.bits = *entry->val;
	rbEntry = getObj(hndl->map->parent->db, rbAddr);
	child = arenaRbMap(hndl->map, rbEntry);

	*hndlAddr = makeHandle(child, 0, maxType[*child->arena->type]);
}

//	create new index handles based on children of the docStore.
//	call with docStore handle.

Status installIndexes(Handle *hndl) {
ArenaDef *arenaDef = hndl->map->arenaDef;
DbAddr *next = arenaDef->idList->head;
uint64_t maxId = 0;
SkipList *skipList;
ArrayEntry *entry;
DocHndl *docHndl;
int idx;

	docHndl = (DocHndl *)(hndl + 1);

	if (docHndl->childId < arenaDef->childId)
		readLock2 (arenaDef->idList->lock);
	else
		return OK;

	writeLock2 (docHndl->indexes->lock);

	//	open maps based on childId skip list
	// 	of arenaDef to continue the index handle list

	//	processs the skip list nodes one at a time

	while (next->addr) {
		skipList = getObj(hndl->map->db, *next);
		idx = next->nslot;

		if (!maxId)
			maxId = *skipList->array[next->nslot - 1].key;

		// process the skip list entries of arenaDef addr

		while (idx--)
			if (*skipList->array[idx].key > docHndl->childId)
				installIdx(hndl, &skipList->array[idx]);
			else
				break;

		next = skipList->next;
	}

	docHndl->childId = maxId;
	writeUnlock2 (docHndl->indexes->lock);
	readUnlock2 (arenaDef->idList->lock);
	return OK;
}

Status installIndexKey(Handle *hndl, ArrayEntry *entry, Document *doc) {
ArrayEntry *versions = getObj(hndl->map, *doc->verKeys);
uint8_t key[MAX_key];
uint64_t *verPtr;
DbHandle *dbHndl;
Handle *idxHndl;
DbIndex *index;
Object *spec;
Status stat;
DbAddr addr;
int keyLen;

	addr.bits = *entry->val;
	dbHndl = getObj(memMap, addr);

	if ((stat = bindHandle(dbHndl, &idxHndl)))
		return stat;

	index = dbindex(idxHndl->map);

	spec = getObj(idxHndl->map, index->keySpec);
	keyLen = keyGenerator(key, doc + 1, doc->size, spec + 1, spec->size);

	keyLen = store64(key, keyLen, doc->docId.bits);

	if (idxHndl->map->arenaDef->useTxn)
		keyLen = store64(key, keyLen, doc->version);

	verPtr = arrayAdd(versions, doc->verKeys->nslot++, *entry->key);
	*verPtr = doc->version;

	switch (*idxHndl->map->arena->type) {
	case ARTreeIndexType:
		stat = artInsertKey(idxHndl, key, keyLen);
		break;

	case Btree1IndexType:
		stat = btree1InsertKey(idxHndl, key, keyLen, 0, Btree1_indexed);
		break;
	}

	releaseHandle(idxHndl);
	return stat;
}

//	install keys for a document insert
//	call with docStore handle

Status installIndexKeys(Handle *hndl, Document *doc) {
SkipList *skipList;
ArrayEntry *array;
DocHndl *docHndl;
DbAddr *next;
Status stat;
int idx;

	docHndl = (DocHndl *)(hndl + 1);

	next = docHndl->indexes->head;
	readLock2 (docHndl->indexes->lock);

	//	install keys for document

	doc->verKeys->bits = allocBlk (hndl->map, docHndl->idxCnt * sizeof(ArrayEntry), true);

	while (next->addr) {
	  skipList = getObj(hndl->map, *next);
	  idx = next->nslot;

	  while (idx--) {
		if (~*skipList->array[idx].key & CHILDID_DROP)
		  if ((stat = installIndexKey(hndl, &skipList->array[idx], doc)))
			return stat;
	  }

	  next = skipList->next;
	}

	readUnlock2 (docHndl->indexes->lock);
	return OK;
}

Status storeDoc(Handle *hndl, void *obj, uint32_t objSize, ObjId *result, ObjId txnId) {
DocStore *docStore;
ArenaDef *arenaDef;
Txn *txn = NULL;
Document *doc;
DbAddr *slot;
ObjId docId;
DbAddr addr;
int idx;

	docStore = docstore(hndl->map);

	if (txnId.bits)
		txn = fetchIdSlot(hndl->map->db, txnId);

	if ((addr.bits = allocObj(hndl->map, hndl->list->free, hndl->list->tail, -1, objSize + sizeof(Document), false)))
		doc = getObj(hndl->map, addr);
	else
		return ERROR_outofmemory;

	docId.bits = allocObjId(hndl->map, hndl->list, docStore->docIdx);

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

	return OK;
}
