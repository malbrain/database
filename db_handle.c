#include "db.h"
#include "db_redblack.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"

//	local process Handle arena

char hndlInit[1];

DbArena hndlArena[1];
DbMap hndlMap[1];

FreeList hndlList[ObjIdType + 1];
DbAddr hndlRbRoot[1];
ArenaDef hndlDef[1];

//	return HandleId slot from bits

HandleId *slotHandle(uint64_t hndlBits) {
ObjId hndlId;

	hndlId.bits = hndlBits;
	return fetchIdSlot (hndlMap, hndlId);
}

//	return Handle from bits

Handle *getHandle(DbHandle *hndl) {
RedBlack *rbEntry;
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->latch & ALIVE_BIT))
		return NULL;

	rbEntry = (RedBlack *)getObj(hndlMap, slot->addr);
	return rbPayload(rbEntry);
}

//	make handle from map pointer
//	return hndlId.bits in hndlArena or zero

void initHandleMap(void) {
    hndlMap->arena = hndlArena;
    hndlMap->db = hndlMap;

#ifdef _WIN32
    hndlMap->hndl = INVALID_HANDLE_VALUE;
#else
    hndlMap->hndl = -1;
#endif

    memset (hndlDef, 0, sizeof(ArenaDef));
	hndlDef->objSize = sizeof(HandleId);

    initArena(hndlMap, hndlDef);
}

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type) {
Handle *hndl, *oldHndl = NULL;
PathStk pathStk[1];
RedBlack *rbEntry;
uint64_t *inUse;
HandleId *slot;
ObjId hndlId;
uint32_t amt;
uint16_t idx;
DbAddr addr;

	// first call?

	if (!*hndlInit) {
	  lockLatch(hndlInit);

	  if (!(*hndlInit & ALIVE_BIT))
		initHandleMap();

	  *hndlInit = ALIVE_BIT;
	}

	// total size of the Handle

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled HandleId

	if (!(hndlId.bits = allocObjId(hndlMap, hndlList, 0)))
		return 0;

	slot = fetchIdSlot (hndlMap, hndlId);

	//	find previous Handle for this path
	//  or make a new one

	if ((rbEntry = rbFind(hndlMap, hndlRbRoot, map->path, map->pathLen, pathStk))) {
		oldHndl = rbPayload(rbEntry);
		addr.bits = allocBlk(hndlMap, amt, true);
		hndl = getObj(hndlMap, addr);
		hndl->next.bits = oldHndl->next.bits;
		oldHndl->next.bits = addr.bits;
	} else {
		rbEntry = rbNew(hndlMap, map->path, map->pathLen, amt);
		hndl = rbPayload(rbEntry);
	}

	//  initialize the new Handle

	hndl->xtraSize = xtraSize;	// size of following structure
	hndl->maxType = listMax;	// number of list entries
	hndl->hndlType = type;
	hndl->map = map;

	if (listMax) {
		idx = arrayAlloc(map, map->arena->listArray, sizeof(FreeList) * listMax);
		inUse = arrayBlk(map, map->arena->listArray, idx);
		hndl->list = (FreeList *)(inUse + 1) + ((idx % 64) - 1) * listMax;
		hndl->listIdx = idx;
	}

	idx = arrayAlloc(map, map->arena->hndlCalls, sizeof(HndlCall));
	hndl->calls = arrayElement(map, map->arena->hndlCalls, idx, sizeof(HndlCall));
	hndl->calls->entryIdx = idx;

	//  install in HandleId slot

	*slot->refCnt = 1;
	slot->addr.bits = rbEntry->addr.bits;
	*slot->latch = ALIVE_BIT;
	return hndlId.bits;
}

//	destroy handle

void destroyHandle(HandleId *slot) {
RedBlack *rbEntry, *nxtEntry;
PathStk pathStk[1];
uint64_t *inUse;
Handle *hndl;

	lockLatch(slot->latch);

	//	already destroyed?

	if (!(*slot->latch & ALIVE_BIT)) {
		*slot->latch = 0;
		return;
	}

	rbEntry = getObj(hndlMap, slot->addr);
	hndl = rbPayload(rbEntry);

	// release handle freeList

	if (hndl->list) {
		lockLatch(hndl->map->arena->listArray->latch);
		inUse = arrayBlk(hndl->map, hndl->map->arena->listArray, hndl->listIdx);
		inUse[0] &= ~(1ULL << (hndl->listIdx % 64));
		unlockLatch(hndl->map->arena->listArray->latch);
	}

	//	find and remove our rbEntry in the rb tree

	if ((nxtEntry = rbFind (hndlMap, hndlRbRoot, hndl->map->path, hndl->map->pathLen, pathStk)))
	  do {
		if (nxtEntry == rbEntry) {
			rbRemove (hndlMap, hndlRbRoot, pathStk);
			break;
		}

		if (memcmp(rbKey(nxtEntry), hndl->map->path, hndl->map->pathLen))
			break;
	  } while ((nxtEntry = rbNext(hndlMap, pathStk)));

	slot->addr.bits = 0;
	*slot->latch = 0;
}

//	close handle

void closeHandle(DbHandle *dbHndl) {
RedBlack *rbEntry;
HandleId *slot;
Handle *hndl;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	//	is this the last reference?

	if (atomicAdd32(slot->refCnt, -1))
		return;

	//  specific handle cleanup

	rbEntry = getObj(hndlMap, slot->addr);
	hndl = rbPayload(rbEntry);

	switch (hndl->hndlType) {
	case Hndl_cursor:
		dbCloseCursor((void *)(hndl + 1), hndl->map);
	}

	destroyHandle (slot);
}

//	bind handle for use in API call
//	return false if handle closed

DbStatus bindHandle(DbHandle *dbHndl, Handle **hndl) {
RedBlack *rbEntry;
HandleId *slot;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->latch & ALIVE_BIT))
		return DB_ERROR_handleclosed;

	rbEntry = getObj(hndlMap, slot->addr);
	*hndl = rbPayload(rbEntry);

	//	is there a DROP request for this arena?

	if (~(*hndl)->map->arena->mutex[0] & ALIVE_BIT) {
		destroyHandle (slot);
		return DB_ERROR_arenadropped;
	}

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	if (atomicAdd32((*hndl)->calls->entryCnt, 1) == 1)
		(*hndl)->calls->entryTs = atomicAdd64(&(*hndl)->map->arena->nxtTs, 1);

	return DB_OK;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
	atomicAdd32(hndl->calls->entryCnt, -1);
}
