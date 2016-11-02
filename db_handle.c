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
ArenaDef hndlDef[1];
DbAddr hndlRoot[1];

//	return HandleId slot from bits

HandleId *slotHandle(uint64_t hndlBits) {
ObjId hndlId;

	hndlId.bits = hndlBits;
	return fetchIdSlot (hndlMap, hndlId);
}

//	return Handle from bits

Handle *getHandle(DbHandle *hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->latch & ALIVE_BIT))
		return NULL;

	return getObj(hndlMap, slot->addr);
}

//	make handle from map pointer
//	return hndlId.bits in hndlArena or zero

void initHndlMap(void) {
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
Handle *hndl, *head;
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
		initHndlMap();

	  *hndlInit = ALIVE_BIT;
	}

	// total size of the Handle

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled HandleId

	if (!(hndlId.bits = allocObjId(hndlMap, hndlList, 0)))
		return 0;

	slot = fetchIdSlot (hndlMap, hndlId);

	//	find previous Handle for this path
	//  or make a new one, and link it to
	//	the head of the handle chain.

	lockLatch(hndlRoot->latch);

	if ((rbEntry = rbFind(hndlMap, hndlRoot, map->path, map->pathLen, pathStk))) {
		addr.bits = allocBlk(hndlMap, amt, true);

		head = getObj(hndlMap, rbEntry->payLoad);
		head->prev.bits = addr.bits;

		hndl = getObj(hndlMap, addr);
		hndl->next.bits = rbEntry->payLoad.bits;

		rbEntry->payLoad.bits = addr.bits;
	} else {
		rbEntry = rbNew(hndlMap, map->path, map->pathLen, amt);
		rbAdd(hndlMap, hndlRoot, rbEntry, pathStk);
		hndl = getObj(hndlMap, rbEntry->payLoad);
		addr.bits = rbEntry->payLoad.bits;
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
	slot->addr.bits = addr.bits;
	*slot->latch = ALIVE_BIT;

	unlockLatch(hndlRoot->latch);
	return hndlId.bits;
}

//	destroy handle

void destroyHandle(ObjId hndlId) {
Handle *hndl, *prevHndl, *nextHndl;
PathStk pathStk[1];
RedBlack *rbEntry;
HandleId *slot;
uint64_t *inUse;

	slot = fetchIdSlot (hndlMap, hndlId);
	lockLatch(slot->latch);

	//	already destroyed?

	if (!(*slot->latch & ALIVE_BIT)) {
		*slot->latch = 0;
		return;
	}

	hndl = getObj(hndlMap, slot->addr);

	// release handle freeList

	if (hndl->list) {
		lockLatch(hndl->map->arena->listArray->latch);
		inUse = arrayBlk(hndl->map, hndl->map->arena->listArray, hndl->listIdx);
		inUse[0] &= ~(1ULL << (hndl->listIdx % 64));
		unlockLatch(hndl->map->arena->listArray->latch);
	}

	lockLatch (hndlRoot->latch);

	//	remove our Handle from the red/black list

	rbEntry = rbFind (hndlMap, hndlRoot, hndl->map->path, hndl->map->pathLen, pathStk);

	if (hndl->prev.bits) {
		prevHndl = getObj(hndlMap, hndl->prev);
		prevHndl->next.bits = hndl->next.bits;
	} else
		rbEntry->payLoad.bits = hndl->next.bits;

	if (hndl->next.bits) {
		nextHndl = getObj(hndlMap, hndl->next);
		nextHndl->prev.bits = hndl->prev.bits;
	}

	freeBlk (hndlMap, slot->addr);

	slot->addr.bits = 0;
	*slot->latch = 0;

	freeId(hndlMap, hndlId);

	//	are we the last handle in the tree?

	if (!rbEntry->payLoad.bits)
		rbRemove (hndlMap, hndlRoot, pathStk);

	unlockLatch (hndlRoot->latch);
}

//	close handle

void closeHandle(DbHandle *dbHndl) {
HandleId *slot;
Handle *hndl;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	//	is this the last reference?

	if (atomicAdd32(slot->refCnt, -1))
		return;

	//  specific handle cleanup

	hndl = getObj(hndlMap, slot->addr);

	switch (hndl->hndlType) {
	case Hndl_cursor:
		dbCloseCursor((void *)(hndl + 1), hndl->map);
	}

	destroyHandle (hndlId);
}

//	bind handle for use in API call
//	return false if handle closed

DbStatus bindHandle(DbHandle *dbHndl, Handle **hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->latch & ALIVE_BIT))
		return DB_ERROR_handleclosed;

	*hndl = getObj(hndlMap, slot->addr);

	//	is there a DROP request for this arena?

	if (~(*hndl)->map->arena->mutex[0] & ALIVE_BIT) {
		destroyHandle (hndlId);
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
