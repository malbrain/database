#include "db.h"
#include "db_redblack.h"
#include "db_object.h"
#include "db_handle.h"
#include "db_arena.h"
#include "db_map.h"

//	Catalog/Handle arena

char hndlInit[1];
DbMap *hndlMap;
char *hndlPath;

//	return HandleId slot from bits

HandleId *slotHandle(uint64_t hndlBits) {
ObjId hndlId;

	hndlId.bits = hndlBits;
	return fetchIdSlot (hndlMap, hndlId);
}

//	return Handle from DbHandle

Handle *getHandle(DbHandle *hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->addr.latch & ALIVE_BIT))
		return NULL;

	return getObj(hndlMap, slot->addr);
}

void initHndlMap(char *path, int pathLen, char *name, int nameLen, bool onDisk) {
ArenaDef arenaDef[1];
int len;

	lockLatch(hndlInit);

	if (*hndlInit & ALIVE_BIT) {
		unlockLatch(hndlInit);
		return;
	}

	if (pathLen) {
		hndlPath = db_malloc(pathLen + 1, false);
		memcpy(hndlPath, path, pathLen);
		hndlPath[pathLen] = 0;
	}

	if (!name) {
		name = "_Catalog";
		nameLen = strlen(name);
	}

	memset(arenaDef, 0, sizeof(arenaDef));
	arenaDef->baseSize = sizeof(Catalog);
	arenaDef->objSize = sizeof(HandleId);
	arenaDef->arenaType = Hndl_catalog;
	arenaDef->onDisk = onDisk;

	hndlMap = openMap(NULL, name, nameLen, arenaDef);
	hndlMap->db = hndlMap;

	*hndlMap->arena->type = Hndl_catalog;
	*hndlInit = ALIVE_BIT;
}

//	make handle from map pointer
//	return hndlId.bits in hndlMap or zero

uint64_t makeHandle(DbMap *map, uint32_t xtraSize, uint32_t listMax, HandleType type) {
Handle *hndl, *head;
HndlCall *hndlCall;
PathStk pathStk[1];
RedBlack *rbEntry;
Catalog *catalog;
uint64_t *inUse;
HandleId *slot;
ObjId hndlId;
uint32_t amt;
uint16_t idx;
DbAddr addr;

	// first call?

	if (!(*hndlInit & ALIVE_BIT))
		initHndlMap(NULL, 0, NULL, 0, true);

	// total size of the Handle

	amt = sizeof(Handle) + xtraSize;

	//	get a new or recycled HandleId

	catalog = (Catalog *)(hndlMap->arena + 1);
	if (!(hndlId.bits = allocObjId(hndlMap, catalog->list, 0)))
		return 0;

	slot = fetchIdSlot (hndlMap, hndlId);

	addr.bits = allocBlk(hndlMap, amt, true);
	hndl = getObj(hndlMap, addr);

	//  initialize the new Handle

	hndl->hndlId.bits = hndlId.bits;
	hndl->xtraSize = xtraSize;	// size of following structure
	hndl->maxType = listMax;	// number of list entries
	hndl->hndlType = type;
	hndl->map = map;

	if (listMax) {
		idx = arrayAlloc(map, map->arena->listArray, sizeof(FreeList) * listMax);
		inUse = arrayBlk(map, map->arena->listArray, idx);
		hndl->list = (FreeList *)(inUse + ARRAY_size / 64) + ((idx % ARRAY_size) - 1) * listMax;
		hndl->listIdx = idx;
	}

	//	allocate and initialize a hndlCall for the handle

	idx = arrayAlloc(map, map->arena->hndlCalls, sizeof(HndlCall));
	hndl->calls.bits = arrayAddr(map, map->arena->hndlCalls, idx);
	hndl->callIdx = idx;

	hndlCall = arrayEntry(map, hndl->calls, idx, sizeof(HndlCall));
	hndlCall->hndlId.bits = hndlId.bits;

	//  install in HandleId slot

	slot->addr.bits = addr.bits;
	return hndlId.bits;
}

//	destroy handle
//	call with id slot locked

void destroyHandle(HandleId *slot) {
Handle *hndl = getObj(hndlMap, slot->addr);
Handle *prevHndl, *nextHndl;
PathStk pathStk[1];
RedBlack *rbEntry;
uint64_t *inUse;

	//	already destroyed?

	if (!(*slot->addr.latch & ALIVE_BIT))
		return;

	// release handle freeList

	if (hndl->list) {
		lockLatch(hndl->map->arena->listArray->latch);
		inUse = arrayBlk(hndl->map, hndl->map->arena->listArray, hndl->listIdx);
		inUse[hndl->listIdx % ARRAY_size / 64] &= ~(1ULL << (hndl->listIdx % 64));
		unlockLatch(hndl->map->arena->listArray->latch);
	}

	freeBlk (hndlMap, slot->addr);
	slot->addr.bits = 0;
}

//	bind handle for use in API call
//	return false if handle closed

DbStatus bindHandle(DbHandle *dbHndl, Handle **hndl) {
HndlCall *hndlCall;
HandleId *slot;
ObjId hndlId;

	hndlId.bits = dbHndl->hndlBits;
	slot = fetchIdSlot (hndlMap, hndlId);

	if (!(*slot->addr.latch & ALIVE_BIT))
		return DB_ERROR_handleclosed;

	*hndl = getObj(hndlMap, slot->addr);

	//	is there a DROP request for this arena?

	if (~(*hndl)->map->arena->mutex[0] & ALIVE_BIT) {
		lockLatch(slot->addr.latch);
		destroyHandle (slot);
		unlockLatch(slot->addr.latch);
		return DB_ERROR_arenadropped;
	}

	//	increment count of active binds
	//	and capture timestamp if we are the
	//	first handle bind

	if (atomicAdd32(slot->entryCnt, 1) == 1) {
		hndlCall = arrayEntry((*hndl)->map, (*hndl)->calls, (*hndl)->callIdx, sizeof(HndlCall));
		hndlCall->entryTs = atomicAdd64(&(*hndl)->map->arena->nxtTs, 1);
	}

	return DB_OK;
}

//	release handle binding

void releaseHandle(Handle *hndl) {
HandleId *slot;
ObjId hndlId;

	hndlId.bits = hndl->hndlId.bits;
	slot = fetchIdSlot (hndlMap, hndlId);

	atomicAdd32(slot->entryCnt, -1);
}

//	find arena's earliest bound handle
//	by scanning HndlCall array

uint64_t scanHandleTs(DbMap *map) {
uint64_t lowTs = map->arena->nxtTs + 1;
DbAddr *array = map->arena->hndlCalls;
DbAddr *addr;
int idx, seg;

  if (array->addr) {
	addr = getObj(map, *array);

	for (idx = 0; idx <= array->maxidx; idx++) {
	  uint64_t *inUse = getObj(map, addr[idx]);
	  HndlCall *call = (HndlCall *)(inUse + ARRAY_size / 64);

	  for (seg = 0; seg < ARRAY_size / 64; seg++) {
		uint64_t bits = inUse[seg];
		int slotIdx = 0;

		// sluff first idx

		while (slotIdx++, bits /= 2) {
		  if (bits & 1) {
			HandleId *slot = fetchIdSlot(hndlMap, call[seg * 64 + slotIdx - 1].hndlId);

			if (!slot->entryCnt[0])
			  continue;
			else
			  lowTs = call[slotIdx - 1].entryTs;
		  }
		}
	  }
	}
  }

  return lowTs;
}

