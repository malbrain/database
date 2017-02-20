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

